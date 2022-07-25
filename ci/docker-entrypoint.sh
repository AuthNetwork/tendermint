#!/bin/bash
set -e

echo "Waiting for $TMHOME/config/config.toml to be created"
until [ -f "$TMHOME/config/config.toml" ]
do
     sleep 10
done

#current_epoch=$(date +%s)
#target_epoch=$(date -d '08:20' +%s)
#
#sleep_seconds=$(( $target_epoch - $current_epoch ))
#echo "sleeping till $sleep_seconds"
#sleep $sleep_seconds

exec tendermint "$@"
