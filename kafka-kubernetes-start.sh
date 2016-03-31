#!/usr/bin/env bash

if [[ -n "$BROKER_ID_PROTOTYPE" ]]; then
    echo "Generate Kafka Broker ID"
	function join { local IFS="$1"; shift; echo "$*"; }
	A=(${BROKER_ID_PROTOTYPE//\./ })
	ID=`join '' ${A[@]:1}`
	ID=${ID##*0}
	echo "Use broker ID: $ID"
	export KAFKA_BROKER_ID=$ID
fi

/usr/bin/start-kafka.sh

