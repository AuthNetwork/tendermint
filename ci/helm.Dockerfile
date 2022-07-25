# stage 1 Generate Tendermint Binary
FROM golang:1.16-alpine as builder
RUN apk update && \
    apk upgrade && \
    apk --no-cache add make
WORKDIR /tendermint
ADD . ./
RUN make build-linux

# stage 2
FROM golang:1.16-alpine
LABEL maintainer="kanishka@tor.us"

# Tendermint will be looking for the genesis file in /tendermint/config/genesis.json
# The genesis.json will be created by dkg-node
# Tendermint will wait indefinitely for the genesis file to be created
# The /tendermint/data dir is used by tendermint to store state.
ENV TMHOME /.torus/tendermint
RUN mkdir -p "$TMHOME"

# OS environment setup
# Set user right away for determinism, create directory for persistence and give our user ownership
# jq and curl used for extracting `pub_key` from private validator while
# deploying tendermint with Kubernetes. It is nice to have bash so the users
# could execute bash commands.
RUN apk update && \
    apk upgrade && \
    apk --no-cache add curl jq bash

WORKDIR $TMHOME

# p2p, rpc and prometheus port
EXPOSE 26656 26657 26660

STOPSIGNAL SIGTERM

COPY --from=builder /tendermint/build/tendermint /usr/bin/tendermint

# You can overwrite these before the first run to influence
# config.json and genesis.json. Additionally, you can override
# CMD to add parameters to `tendermint node`.
ENV CHAIN_ID=main-chain-BLUBLU

COPY ./ci/docker-entrypoint.sh /usr/local/bin/

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["node", "--proxy-app=tcp://localhost:26655"]
