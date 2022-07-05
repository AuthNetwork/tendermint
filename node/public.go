// Package node provides a high level wrapper around tendermint services.
package node

import (
	"errors"
	"fmt"
	abciclient "github.com/tendermint/tendermint/abci/client"
	"github.com/tendermint/tendermint/config"
	"github.com/tendermint/tendermint/libs/log"
	"github.com/tendermint/tendermint/libs/service"
	"github.com/tendermint/tendermint/privval"
	"github.com/tendermint/tendermint/types"
	"os"
	"time"
)

// NewDefault constructs a tendermint node service for use in go
// process that host their own process-local tendermint node. This is
// equivalent to running tendermint in it's own process communicating
// to an external ABCI application.
func NewDefault(conf *config.Config, logger log.Logger) (service.Service, error) {
	return newDefaultNode(conf, logger)
}

// New constructs a tendermint node. The ClientCreator makes it
// possible to construct an ABCI application that runs in the same
// process as the tendermint node.  The final option is a pointer to a
// Genesis document: if the value is nil, the genesis document is read
// from the file specified in the config, and otherwise the node uses
// value of the final argument.
func New(conf *config.Config,
	logger log.Logger,
	cf abciclient.Creator,
	gen *types.GenesisDoc,
) (service.Service, error) {
	nodeKey, err := types.LoadOrGenNodeKey(conf.NodeKeyFile())
	if err != nil {
		return nil, fmt.Errorf("failed to load or gen node key %s: %w", conf.NodeKeyFile(), err)
	}

	var genProvider genesisDocProvider
	switch gen {
	case nil:
		genProvider = defaultGenesisDocProviderFunc(conf)
	default:
		genProvider = func() (*types.GenesisDoc, error) { return gen, nil }
	}

	switch conf.Mode {
	case config.ModeFull, config.ModeValidator:
		pval, err := privval.LoadOrGenFilePV(conf.PrivValidator.KeyFile(), conf.PrivValidator.StateFile())
		if err != nil {
			return nil, err
		}

		return makeNode(conf,
			pval,
			nodeKey,
			cf,
			genProvider,
			config.DefaultDBProvider,
			logger)
	case config.ModeSeed:
		return makeSeedNode(conf, config.DefaultDBProvider, nodeKey, genProvider, logger)
	default:
		return nil, fmt.Errorf("%q is not a valid mode", conf.Mode)
	}
}

// NewWithWait constructs a tendermint node service for use in go
// process that host their own process-local tendermint node. This
// waits for the dkg-node to populate the config first
func NewWithWait(conf *config.Config,
	logger log.Logger) (service.Service, error) {
	// Wait till the dkg-node has written the configs to the disk

	for {
		time.Sleep(10 * time.Second)
		// genesis.json
		if _, err := os.Stat(conf.GenesisFile()); errors.Is(err, os.ErrNotExist) {
			logger.Info("Waiting for genesis.json to be created")
			continue
		}

		// node_key.json
		if _, err := os.Stat(conf.NodeKeyFile()); errors.Is(err, os.ErrNotExist) {
			logger.Info("Waiting for node_key.json to be created")
			continue
		}

		// priv-validator.json
		if _, err := os.Stat(conf.PrivValidator.KeyFile()); errors.Is(err, os.ErrNotExist) {
			logger.Info("Waiting for priv-validator.json to be created")
			continue
		}

		// config.toml
		if _, err := os.Stat(conf.File()); errors.Is(err, os.ErrNotExist) {
			logger.Info("Waiting for config.toml to be created")
			continue
		}

		break
	}

	// Wait for 60 seconds to ensure that the rest of the cluster is online
	// TODO: find a better solution
	time.Sleep(60 * time.Second)
	logger.Info("Starting tendermint node")

	return newDefaultNode(conf, logger)
}
