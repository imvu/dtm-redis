#!/bin/bash

CONFIG_FILE=$1

if [ "$CONFIG_FILE" == "" ]; then
    SCRIPT=`readlink -f $0`
    SCRIPT_PATH=`dirname $SCRIPT`
    CONFIG_FILE="$SCRIPT_PATH/../config/single"
fi

rel/dtm_redis/bin/dtm_redis start -dtm_redis mode cluster -dtm_redis config '\"'$CONFIG_FILE'\"'

