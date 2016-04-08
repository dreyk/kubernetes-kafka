#!/usr/bin/env bash

if ! [[  $KAFKA_ADVERTISED_HOST_NAME =~ ^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$ ]]; then
	echo "KAFKA_ADVERTISED_HOST_NAME NOT DEFINED OR INVALID '$KAFKA_ADVERTISED_HOST_NAME' !!!"
	exit 1
fi

if [[ -n "$BROKER_ID_PROTOTYPE" && -z "$KAFKA_BROKER_ID" ]]; then
    echo "Generate Kafka Broker ID: for $BROKER_ID_PROTOTYPE"
	function join { local IFS="$1"; shift; echo "$*"; }
	A=(${BROKER_ID_PROTOTYPE//\./ })
	ID=`join '' ${A[@]:1}`
	ID=${ID##*0}
	echo "Use broker ID: $ID"
	export KAFKA_BROKER_ID=$ID
fi

if [[ -n "$ENABLE_AUTO_EXTEND" ]]; then
	trap "echo 'Rebalancing'; /usr/bin/kafka-autoextend-partitions.sh down; exit" SIGHUP SIGINT SIGTERM
	/usr/bin/kafka-autoextend-partitions.sh up &
	/usr/bin/start-kafka.sh
else
	/usr/bin/start-kafka.sh
fi