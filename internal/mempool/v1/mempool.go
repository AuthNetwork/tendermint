package v1

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	abci "github.com/tendermint/tendermint/abci/types"
	"github.com/tendermint/tendermint/config"
	"github.com/tendermint/tendermint/internal/libs/clist"
	"github.com/tendermint/tendermint/internal/mempool"
	"github.com/tendermint/tendermint/internal/proxy"
	"github.com/tendermint/tendermint/libs/log"
	tmmath "github.com/tendermint/tendermint/libs/math"
	"github.com/tendermint/tendermint/types"
)

var _ mempool.Mempool = (*TxMempool)(nil)

// TxMempoolOption sets an optional parameter on the TxMempool.
type TxMempoolOption func(*TxMempool)

// TxMempool implemements the Mempool interface and allows the application to
// set priority values on transactions in the CheckTx response. When selecting
// transactions to include in a block, higher-priority transactions are chosen
// first.  When evicting transactions from the mempool for size constraints,
// lower-priority transactions are evicted sooner.
//
// Within the mempool, transactions are ordered in order of arrival, and are
// gossiped to the rest of the network based on that order (gossip order does
// not take priority into account).
type TxMempool struct {
	// Immutable fields
	logger       log.Logger
	config       *config.MempoolConfig
	proxyAppConn proxy.AppConnMempool
	metrics      *mempool.Metrics
	cache        mempool.TxCache // seen transactions

	// Atomically-updated fields
	height   int64 // atomic: the latest height passed to Update
	txsBytes int64 // atomic: the total size of all transactions in the mempool, in bytes

	// The beginning and end of the subrange of txs that needs to be rechecked
	// after a block is committed (during the Update). Rechecks are processed in
	// sequential order after the transactions from the previous block have been
	// removed from the list.
	recheckCursor *clist.CElement // next expected response
	recheckEnd    *clist.CElement // re-checking stops here

	// Synchronized fields, protected by mtx.
	mtx                  *sync.RWMutex
	notifiedTxsAvailable bool
	txsAvailable         chan struct{} // one value sent per height when mempool is not empty
	preCheck             mempool.PreCheckFunc
	postCheck            mempool.PostCheckFunc

	txs        *clist.CList // valid transactions (passed CheckTx)
	txByKey    map[types.TxKey]*clist.CElement
	txBySender map[string]*clist.CElement // for sender != ""
}

// NewTxMempool constructs a new, empty priority mempool at the specified
// initial height and using the given config and options.
func NewTxMempool(
	logger log.Logger,
	cfg *config.MempoolConfig,
	proxyAppConn proxy.AppConnMempool,
	height int64,
	options ...TxMempoolOption,
) *TxMempool {

	txmp := &TxMempool{
		logger:       logger,
		config:       cfg,
		proxyAppConn: proxyAppConn,
		height:       height,
		metrics:      mempool.NopMetrics(),
		cache:        mempool.NopTxCache{},
		txByKey:      make(map[types.TxKey]*clist.CElement),
		txBySender:   make(map[string]*clist.CElement),
	}
	if cfg.CacheSize > 0 {
		txmp.cache = mempool.NewLRUTxCache(cfg.CacheSize)
	}

	proxyAppConn.SetResponseCallback(txmp.recheckTxCallback)

	for _, opt := range options {
		opt(txmp)
	}

	return txmp
}

// WithPreCheck sets a filter for the mempool to reject a transaction if f(tx)
// returns an error. This is executed before CheckTx. It only applies to the
// first created block. After that, Update() overwrites the existing value.
func WithPreCheck(f mempool.PreCheckFunc) TxMempoolOption {
	return func(txmp *TxMempool) { txmp.preCheck = f }
}

// WithPostCheck sets a filter for the mempool to reject a transaction if
// f(tx, resp) returns an error. This is executed after CheckTx. It only applies
// to the first created block. After that, Update overwrites the existing value.
func WithPostCheck(f mempool.PostCheckFunc) TxMempoolOption {
	return func(txmp *TxMempool) { txmp.postCheck = f }
}

// WithMetrics sets the mempool's metrics collector.
func WithMetrics(metrics *mempool.Metrics) TxMempoolOption {
	return func(txmp *TxMempool) { txmp.metrics = metrics }
}

// Lock obtains a write-lock on the mempool. A caller must be sure to explicitly
// release the lock when finished.
func (txmp *TxMempool) Lock() { txmp.mtx.Lock() }

// Unlock releases a write-lock on the mempool.
func (txmp *TxMempool) Unlock() { txmp.mtx.Unlock() }

// Size returns the number of valid transactions in the mempool. It is
// thread-safe.
func (txmp *TxMempool) Size() int { return txmp.txs.Size() }

// SizeBytes return the total sum in bytes of all the valid transactions in the
// mempool. It is thread-safe.
func (txmp *TxMempool) SizeBytes() int64 { return atomic.LoadInt64(&txmp.sizeBytes) }

// FlushAppConn executes FlushSync on the mempool's proxyAppConn.
//
// NOTE: The caller must obtain a write-lock via Lock() prior to execution.
func (txmp *TxMempool) FlushAppConn() error {
	return txmp.proxyAppConn.FlushSync(context.Background())
}

// EnableTxsAvailable enables the mempool to trigger events when transactions
// are available on a block by block basis.
func (txmp *TxMempool) EnableTxsAvailable() {
	txmp.mtx.Lock()
	defer txmp.mtx.Unlock()

	txmp.txsAvailable = make(chan struct{}, 1)
}

// TxsAvailable returns a channel which fires once for every height, and only
// when transactions are available in the mempool. It is thread-safe.
func (txmp *TxMempool) TxsAvailable() <-chan struct{} { return txmp.txsAvailable }

// CheckTx adds the given transaction to the mempool if it fits and passes the
// application's ABCI CheckTx method.
//
// CheckTx reports an error without adding tx if:
//
// - The size of tx exceeds the configured maximum transaction size.
// - The pre-check hook is defined and reports an error for tx.
// - The transaction already exists in the cache.
// - The proxy connection to the application fails.
//
// If tx passes all of the above conditions, it is passed (asynchronously) to
// the application's ABCI CheckTx method and this CheckTx method returns nil.
// If cb != nil, it is called when the ABCI request completes to report the
// application response.
//
// If the application accepts the transaction and the mempool is full, the
// mempool evicts the lowest-priority transaction whose priority is (strictly)
// lower than the priority of tx, and adds tx instead. If no such transaction
// exists, tx is discarded.
func (txmp *TxMempool) CheckTx(
	ctx context.Context,
	tx types.Tx,
	cb func(*abci.Response),
	txInfo mempool.TxInfo,
) error {

	// During the initial phase of CheckTx, we do not need to modify any state.
	// A transaction will not actually be added to the mempool until it survives
	// a call to the ABCI CheckTx method and size constraint checks.
	txmp.mtx.RLock()
	defer txmp.mtx.RUnlock()

	// Reject transactions in excess of the configured maximum transaction size.
	if len(tx) > txmp.config.MaxTxBytes {
		return types.ErrTxTooLarge{Max: txmp.config.MaxTxBytes, Actual: len(tx)}
	}

	// If a precheck hook is defined, call it before invoking the application.
	if txmp.preCheck != nil {
		if err := txmp.preCheck(tx); err != nil {
			return types.ErrPreCheck{Reason: err}
		}
	}

	// Early exit if the proxy connection has an error.
	if err := txmp.proxyAppConn.Error(); err != nil {
		return err
	}

	txKey := tx.Key()

	// Check for the transaction in the cache.
	if !txmp.cache.Push(tx) {
		// If the cached transaction is also in the pool, record its sender.
		if w, ok := txmp.txbyKey[txKey]; ok {
			w.senders[txInfo.SenderId] = true // FIXME: synchronize
		}
		return types.ErrTxInCache
	}

	// Initiate an ABCI CheckTx for this transaction. The callback is
	// responsible for adding the transaction to the pool if it survives.
	reqRes, err := txmp.proxyAppConn.CheckTxAsync(ctx, abci.RequestCheckTx{Tx: tx})
	if err != nil {
		txmp.cache.Delete(txKey)
		return err
	}

	reqRes.SetCallback(func(res *abci.Response) {
		if txmp.recheckCursor != nil {
			panic("recheck cursor is non-nil in CheckTx callback")
		}

		wtx := &WrappedTx{
			tx:        tx,
			hash:      txHash,
			timestamp: time.Now().UTC(),
			height:    txmp.height,
		}
		txmp.initTxCallback(wtx, res, txInfo)

		if cb != nil {
			cb(res)
		}
	})

	return nil
}

func (txmp *TxMempool) RemoveTxByKey(txKey types.TxKey) error {
	txmp.mtx.Lock()
	defer txmp.mtx.Unlock()

	if elt, ok := txmp.txByKey[txKey]; ok {
		delete(txmp.txByKey, txKey)
		delete(txmp.txBySender, elt.Value.(*WrappedTx).sender)
		clist.Remove(txmp.txs, elt)
		elt.DetachPrev()
		elt.DetachNext()
		return nil
	}
	return fmt.Errorf("transaction %x not found", txKey)
}

// Flush purges the contents of the mempool and the cache, leaving both empty.
// The current height is not modified by this operation.
func (txmp *TxMempool) Flush() {
	txmp.mtx.Lock()
	defer txmp.mtx.Unlock()

	atomic.SwapInt64(&txmp.txsBytes, 0)
	// FIXME: update recheck cursors?
	txmp.txs = nil
	txmp.txByKey = make(map[types.TxKey]*clist.CElement)
	txmp.txBySender = make(map[string]*clist.CElement)
	txmp.cache.Reset()
}

// ReapMaxBytesMaxGas returns a list of transactions within the provided size
// and gas constraints. Transaction are retrieved in priority order.
//
// NOTE:
// - A read-lock is acquired.
// - Transactions returned are not actually removed from the mempool transaction
//   store or indexes.
func (txmp *TxMempool) ReapMaxBytesMaxGas(maxBytes, maxGas int64) types.Txs {
	txmp.mtx.RLock()
	defer txmp.mtx.RUnlock()

	var (
		totalGas  int64
		totalSize int64
	)

	// wTxs contains a list of *WrappedTx retrieved from the priority queue that
	// need to be re-enqueued prior to returning.
	wTxs := make([]*WrappedTx, 0, txmp.priorityIndex.NumTxs())
	defer func() {
		for _, wtx := range wTxs {
			txmp.priorityIndex.PushTx(wtx)
		}
	}()

	txs := make([]types.Tx, 0, txmp.priorityIndex.NumTxs())
	for txmp.priorityIndex.NumTxs() > 0 {
		wtx := txmp.priorityIndex.PopTx()
		txs = append(txs, wtx.tx)
		wTxs = append(wTxs, wtx)
		size := types.ComputeProtoSizeForTxs([]types.Tx{wtx.tx})

		// Ensure we have capacity for the transaction with respect to the
		// transaction size.
		if maxBytes > -1 && totalSize+size > maxBytes {
			return txs[:len(txs)-1]
		}

		totalSize += size

		// ensure we have capacity for the transaction with respect to total gas
		gas := totalGas + wtx.gasWanted
		if maxGas > -1 && gas > maxGas {
			return txs[:len(txs)-1]
		}

		totalGas = gas
	}

	return txs
}

// ReapMaxTxs returns a list of transactions within the provided number of
// transactions bound. Transaction are retrieved in priority order.
//
// NOTE:
// - A read-lock is acquired.
// - Transactions returned are not actually removed from the mempool transaction
//   store or indexes.
func (txmp *TxMempool) ReapMaxTxs(max int) types.Txs {
	txmp.mtx.RLock()
	defer txmp.mtx.RUnlock()

	numTxs := txmp.priorityIndex.NumTxs()
	if max < 0 {
		max = numTxs
	}

	cap := tmmath.MinInt(numTxs, max)

	// wTxs contains a list of *WrappedTx retrieved from the priority queue that
	// need to be re-enqueued prior to returning.
	wTxs := make([]*WrappedTx, 0, cap)
	defer func() {
		for _, wtx := range wTxs {
			txmp.priorityIndex.PushTx(wtx)
		}
	}()

	txs := make([]types.Tx, 0, cap)
	for txmp.priorityIndex.NumTxs() > 0 && len(txs) < max {
		wtx := txmp.priorityIndex.PopTx()
		txs = append(txs, wtx.tx)
		wTxs = append(wTxs, wtx)
	}

	return txs
}

// Update iterates over all the transactions provided by the caller, i.e. the
// block producer, and removes them from the cache (if applicable) and removes
// the transactions from the main transaction store and associated indexes.
// Finally, if there are trainsactions remaining in the mempool, we initiate a
// re-CheckTx for them (if applicable), otherwise, we notify the caller more
// transactions are available.
//
// NOTE:
// - The caller must explicitly acquire a write-lock via Lock().
func (txmp *TxMempool) Update(
	blockHeight int64,
	blockTxs types.Txs,
	deliverTxResponses []*abci.ResponseDeliverTx,
	newPreFn mempool.PreCheckFunc,
	newPostFn mempool.PostCheckFunc,
) error {

	txmp.height = blockHeight
	txmp.notifiedTxsAvailable = false

	if newPreFn != nil {
		txmp.preCheck = newPreFn
	}
	if newPostFn != nil {
		txmp.postCheck = newPostFn
	}

	for i, tx := range blockTxs {
		if deliverTxResponses[i].Code == abci.CodeTypeOK {
			// add the valid committed transaction to the cache (if missing)
			_ = txmp.cache.Push(tx)
		} else if !txmp.config.KeepInvalidTxsInCache {
			// allow invalid transactions to be re-submitted
			txmp.cache.Remove(tx)
		}

		// remove the committed transaction from the transaction store and indexes
		if wtx := txmp.txStore.GetTxByHash(tx.Key()); wtx != nil {
			txmp.removeTx(wtx, false)
		}
	}

	txmp.purgeExpiredTxs(blockHeight)

	// If there any uncommitted transactions left in the mempool, we either
	// initiate re-CheckTx per remaining transaction or notify that remaining
	// transactions are left.
	if txmp.Size() > 0 {
		if txmp.config.Recheck {
			txmp.logger.Debug(
				"executing re-CheckTx for all remaining transactions",
				"num_txs", txmp.Size(),
				"height", blockHeight,
			)
			txmp.updateReCheckTxs()
		} else {
			txmp.notifyTxsAvailable()
		}
	}

	txmp.metrics.Size.Set(float64(txmp.Size()))
	return nil
}

// initTxCallback handle the ABCI CheckTx response for the first time a
// transaction is added to the mempool, rather than a recheck after a block is
// committed.
//
// If either the application rejected the transaction or a post-check hook is
// defined and rejects the transaction, it is discarded.
//
// Otherwise, if the mempool is full, check for a lower-priority transaction
// that can be evicted to make room for the new one. If no such transaction
// exists, this transaction is logged and dropped; otherwise the selected
// transaction is evicted.
//
// Finally, the new transaction is added and size stats updated.
func (txmp *TxMempool) initTxCallback(wtx *WrappedTx, res *abci.Response, txInfo mempool.TxInfo) {
	checkTxRes, ok := res.Value.(*abci.Response_CheckTx)
	if !ok {
		return
	}

	var err error
	if txmp.postCheck != nil {
		err = txmp.postCheck(wtx.tx, checkTxRes.CheckTx)
	}

	if err != nil || checkTxRes.CheckTx.Code != abci.CodeTypeOK {
		txmp.logger.Info(
			"rejected bad transaction",
			"priority", wtx.priority,
			"tx", fmt.Sprintf("%X", wtx.tx.Hash()),
			"peer_id", txInfo.SenderNodeID,
			"code", checkTxRes.CheckTx.Code,
			"post_check_err", err,
		)

		txmp.metrics.FailedTxs.Add(1)

		// Remove the invalid transaction from the cache, unless the operator has
		// instructed us to do otherwise.
		if !txmp.config.KeepInvalidTxsInCache {
			txmp.cache.Remove(wtx.tx)
		}

		// If there was a post-check error, record its text in the result for
		// debugging purposes.
		if err != nil {
			checkTxRes.CheckTx.MempoolError = err.Error()
		}
		return
	}

	// FIXME: Do we actually want this? The old mempool doesn't do it.  Inclined
	// to leave it out without a good reason.
	//
	// 	sender := checkTxRes.CheckTx.Sender
	// 	priority := checkTxRes.CheckTx.Priority
	//
	// 	if sender != "" {
	// 		if wtx := txmp.txStore.GetTxBySender(sender); wtx != nil {
	// 			txmp.logger.Error(
	// 				"rejected incoming good transaction; tx already exists for sender",
	// 				"tx", fmt.Sprintf("%X", wtx.tx.Hash()),
	// 				"sender", sender,
	// 			)
	// 			txmp.metrics.RejectedTxs.Add(1)
	// 			return
	// 		}
	// 	}

	// At this point the application has ruled the transaction valid, but the
	// mempool might be full. If so, find the lowest-priority item with lower
	// priority than the application assigned to this new one, and evict it in
	// favor of tx. If no such item exists, we discard tx.

	if err := txmp.canAddTx(wtx); err != nil {

		evictTxs := txmp.priorityIndex.GetEvictableTxs(
			priority,
			int64(wtx.Size()),
			txmp.SizeBytes(),
			txmp.config.MaxTxsBytes,
		)
		if len(evictTxs) == 0 {
			// No room for the new incoming transaction so we just remove it from
			// the cache.
			txmp.cache.Remove(wtx.tx)
			txmp.logger.Error(
				"rejected incoming good transaction; mempool full",
				"tx", fmt.Sprintf("%X", wtx.tx.Hash()),
				"err", err.Error(),
			)
			txmp.metrics.RejectedTxs.Add(1)
			return
		}

		// evict an existing transaction(s)
		//
		// NOTE:
		// - The transaction, toEvict, can be removed while a concurrent
		//   reCheckTx callback is being executed for the same transaction.
		for _, toEvict := range evictTxs {
			txmp.removeTx(toEvict, true)
			txmp.logger.Debug(
				"evicted existing good transaction; mempool full",
				"old_tx", fmt.Sprintf("%X", toEvict.tx.Hash()),
				"old_priority", toEvict.priority,
				"new_tx", fmt.Sprintf("%X", wtx.tx.Hash()),
				"new_priority", wtx.priority,
			)
			txmp.metrics.EvictedTxs.Add(1)
		}
	}

	wtx.gasWanted = checkTxRes.CheckTx.GasWanted
	wtx.priority = priority
	wtx.sender = sender
	wtx.peers = map[uint16]struct{}{
		txInfo.SenderID: {},
	}

	txmp.metrics.TxSizeBytes.Observe(float64(wtx.Size()))
	txmp.metrics.Size.Set(float64(txmp.Size()))

	txmp.insertTx(wtx)
	txmp.logger.Debug(
		"inserted good transaction",
		"priority", wtx.priority,
		"tx", fmt.Sprintf("%X", wtx.tx.Hash()),
		"height", txmp.height,
		"num_txs", txmp.Size(),
	)
	txmp.notifyTxsAvailable()

}

// recheckTxCallback handles the responses from ABCI CheckTx calls issued
// during the recheck phase of a block Update. It updates the recheck cursors
// and removes any transactions invalidated by the application.
//
// This callback is NOT executed for the initial CheckTx on a new transaction;
// that case is handled by initTxCallback instead.
func (txmp *TxMempool) recheckTxCallback(req *abci.Request, res *abci.Response) {
	// If we are not performing a recheck, ignore this response.
	if txmp.recheckCursor == nil {
		return
	}

	txmp.metrics.RecheckTimes.Add(1)

	checkTxRes, ok := res.Value.(*abci.Response_CheckTx)
	if !ok {
		txmp.logger.Error("received incorrect type in mempool callback",
			"expected", reflect.TypeOf(&abci.Response_CheckTx{}).Name(),
			"got", reflect.TypeOf(res.Value).Name(),
		)
		return
	}
	tx := req.GetCheckTx().Tx
	wtx := txmp.recheckCursor.Value.(*WrappedTx)

	// Scan for the transaction reported by the ABCI callback in the recheck list.
	//
	// TODO(creachadair): By construction this should always be the next element
	// unless either the ABCI call failed or the target transaction was evicted.
	// In the first case, we should skip an item in the list, but but in the
	// second we should be skipping the _checked_ transaction rather than
	// advancing the list. Right now we don't have a way to tell.
	//
	// That means if a transaction is evicted before recheck reaches it, we will
	// not filter any invalid transactions after that in the sequence, because
	// this loop will scan to the end looking for it and then give up. We should
	// distinguish the cases.
	//
	for {
		if bytes.Equal(tx, wtx.tx) {
			break // found
		}

		txmp.logger.Error(
			"re-CheckTx transaction mismatch",
			"got", wtx.tx.Hash(),
			"expected", types.Tx(tx).Key(),
		)

		// If the recheck list is empty, we're done here.
		if txmp.recheckCursor == txmp.recheckEnd {
			txmp.recheckCursor = nil
			return
		}

		txmp.recheckCursor = txmp.recheckCursor.Next()
		wtx = txmp.recheckCursor.Value.(*WrappedTx)
	}

	// Only evaluate transactions that have not been removed. This can happen
	// if an existing transaction is evicted during CheckTx and while this
	// callback is being executed for the same evicted transaction.
	if !txmp.txStore.IsTxRemoved(wtx.hash) {
		var err error
		if txmp.postCheck != nil {
			err = txmp.postCheck(tx, checkTxRes.CheckTx)
		}

		if checkTxRes.CheckTx.Code == abci.CodeTypeOK && err == nil {
			wtx.priority = checkTxRes.CheckTx.Priority
		} else {
			txmp.logger.Debug(
				"existing transaction no longer valid; failed re-CheckTx callback",
				"priority", wtx.priority,
				"tx", fmt.Sprintf("%X", wtx.tx.Hash()),
				"err", err,
				"code", checkTxRes.CheckTx.Code,
			)

			if wtx.gossipEl != txmp.recheckCursor {
				panic("corrupted reCheckTx cursor")
			}

			txmp.removeTx(wtx, !txmp.config.KeepInvalidTxsInCache)
		}
	}

	// move reCheckTx cursor to next element
	if txmp.recheckCursor == txmp.recheckEnd {
		txmp.recheckCursor = nil
	} else {
		txmp.recheckCursor = txmp.recheckCursor.Next()
	}

	if txmp.recheckCursor == nil {
		txmp.logger.Debug("finished rechecking transactions")

		if txmp.Size() > 0 {
			txmp.notifyTxsAvailable()
		}
	}

	txmp.metrics.Size.Set(float64(txmp.Size()))
}

// updateReCheckTxs updates the recheck cursors by using the gossipIndex. For
// each transaction, it executes CheckTxAsync. The global callback defined on
// the proxyAppConn will be executed for each transaction after CheckTx is
// executed.
//
// NOTE:
// - The caller must have a write-lock when executing updateReCheckTxs.
func (txmp *TxMempool) updateReCheckTxs() {
	if txmp.Size() == 0 {
		panic("attempted to update re-CheckTx txs when mempool is empty")
	}

	txmp.recheckCursor = txmp.gossipIndex.Front()
	txmp.recheckEnd = txmp.gossipIndex.Back()
	ctx := context.Background()

	for e := txmp.gossipIndex.Front(); e != nil; e = e.Next() {
		wtx := e.Value.(*WrappedTx)

		// Only execute CheckTx if the transaction is not marked as removed which
		// could happen if the transaction was evicted.
		if !txmp.txStore.IsTxRemoved(wtx.hash) {
			_, err := txmp.proxyAppConn.CheckTxAsync(ctx, abci.RequestCheckTx{
				Tx:   wtx.tx,
				Type: abci.CheckTxType_Recheck,
			})
			if err != nil {
				// no need in retrying since the tx will be rechecked after the next block
				txmp.logger.Error("failed to execute CheckTx during rechecking", "err", err)
			}
		}
	}

	if _, err := txmp.proxyAppConn.FlushAsync(ctx); err != nil {
		txmp.logger.Error("failed to flush transactions during rechecking", "err", err)
	}
}

// canAddTx returns an error if we cannot insert the provided *WrappedTx into
// the mempool due to mempool configured constraints. Otherwise, nil is
// returned and the transaction can be inserted into the mempool.
func (txmp *TxMempool) canAddTx(wtx *WrappedTx) error {
	var (
		numTxs    = txmp.Size()
		sizeBytes = txmp.SizeBytes()
	)

	if numTxs >= txmp.config.Size || int64(wtx.Size())+sizeBytes > txmp.config.MaxTxsBytes {
		return types.ErrMempoolIsFull{
			NumTxs:      numTxs,
			MaxTxs:      txmp.config.Size,
			TxsBytes:    sizeBytes,
			MaxTxsBytes: txmp.config.MaxTxsBytes,
		}
	}

	return nil
}

func (txmp *TxMempool) insertTx(wtx *WrappedTx) {
	txmp.txStore.SetTx(wtx)
	txmp.priorityIndex.PushTx(wtx)
	txmp.heightIndex.Insert(wtx)
	txmp.timestampIndex.Insert(wtx)

	// Insert the transaction into the gossip index and mark the reference to the
	// linked-list element, which will be needed at a later point when the
	// transaction is removed.
	gossipEl := txmp.gossipIndex.PushBack(wtx)
	wtx.gossipEl = gossipEl

	atomic.AddInt64(&txmp.sizeBytes, int64(wtx.Size()))
}

func (txmp *TxMempool) removeTx(wtx *WrappedTx, removeFromCache bool) {
	if txmp.txStore.IsTxRemoved(wtx.hash) {
		return
	}

	txmp.txStore.RemoveTx(wtx)
	txmp.priorityIndex.RemoveTx(wtx)
	txmp.heightIndex.Remove(wtx)
	txmp.timestampIndex.Remove(wtx)

	// Remove the transaction from the gossip index and cleanup the linked-list
	// element so it can be garbage collected.
	txmp.gossipIndex.Remove(wtx.gossipEl)
	wtx.gossipEl.DetachPrev()

	atomic.AddInt64(&txmp.sizeBytes, int64(-wtx.Size()))

	if removeFromCache {
		txmp.cache.Remove(wtx.tx)
	}
}

// purgeExpiredTxs removes all transactions that have exceeded their respective
// height and/or time based TTLs from their respective indexes. Every expired
// transaction will be removed from the mempool entirely, except for the cache.
//
// NOTE: purgeExpiredTxs must only be called during TxMempool#Update in which
// the caller has a write-lock on the mempool and so we can safely iterate over
// the height and time based indexes.
func (txmp *TxMempool) purgeExpiredTxs(blockHeight int64) {
	now := time.Now()
	expiredTxs := make(map[types.TxKey]*WrappedTx)

	if txmp.config.TTLNumBlocks > 0 {
		purgeIdx := -1
		for i, wtx := range txmp.heightIndex.txs {
			if (blockHeight - wtx.height) > txmp.config.TTLNumBlocks {
				expiredTxs[wtx.tx.Key()] = wtx
				purgeIdx = i
			} else {
				// since the index is sorted, we know no other txs can be be purged
				break
			}
		}

		if purgeIdx >= 0 {
			txmp.heightIndex.txs = txmp.heightIndex.txs[purgeIdx+1:]
		}
	}

	if txmp.config.TTLDuration > 0 {
		purgeIdx := -1
		for i, wtx := range txmp.timestampIndex.txs {
			if now.Sub(wtx.timestamp) > txmp.config.TTLDuration {
				expiredTxs[wtx.tx.Key()] = wtx
				purgeIdx = i
			} else {
				// since the index is sorted, we know no other txs can be be purged
				break
			}
		}

		if purgeIdx >= 0 {
			txmp.timestampIndex.txs = txmp.timestampIndex.txs[purgeIdx+1:]
		}
	}

	for _, wtx := range expiredTxs {
		txmp.removeTx(wtx, false)
	}
}

func (txmp *TxMempool) notifyTxsAvailable() {
	if txmp.Size() == 0 {
		panic("attempt to notify txs available but mempool is empty!")
	}

	if txmp.txsAvailable != nil && !txmp.notifiedTxsAvailable {
		// channel cap is 1, so this will send once
		txmp.notifiedTxsAvailable = true

		select {
		case txmp.txsAvailable <- struct{}{}:
		default:
		}
	}
}
