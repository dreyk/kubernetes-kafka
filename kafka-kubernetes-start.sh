#!/usr/bin/env bash

if [[ -n "$BROKER_ID_PROTOTYPE" ]]; then
    echo "Generate Kafka Broker ID: for $BROKER_ID_PROTOTYPE"
	function join { local IFS="$1"; shift; echo "$*"; }
	A=(${BROKER_ID_PROTOTYPE//\./ })
	ID=`join '' ${A[@]:1}`
	ID=${ID##*0}
	echo "Use broker ID: $ID"
	export KAFKA_BROKER_ID=$ID
fi

trap "echo 'Rebalancing'; /usr/bin/kafka-autoextend-partitions.sh down; exit" SIGHUP SIGINT SIGTERM
/usr/bin/kafka-autoextend-partitions.sh up &
/usr/bin/start-kafka.sh

