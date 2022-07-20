#!/bin/bash
set -e

echo "Waiting for $TMHOME/config/config.toml to be created"
until [ -f "$TMHOME/config/config.toml" ]
do
     sleep 10
done

exec tendermint "$@"
