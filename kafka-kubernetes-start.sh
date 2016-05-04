#!/usr/bin/env bash

if ! [[  $KAFKA_ADVERTISED_HOST_NAME =~ ^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$ ]]; then
	echo "KAFKA_ADVERTISED_HOST_NAME NOT DEFINED OR INVALID '$KAFKA_ADVERTISED_HOST_NAME' !!!"
	exit 1
fi

if [[ -z "$KAFKA_BROKER_ID" ]]; then
    echo "Generate Kafka Broker ID: for $KUBERNETS_UID"
	ID=`$KAFKA_HOME/bin/kafka-run-class.sh kafka.admin.AutoExpandCommand --zookeeper=127.0.0.1:2181 -broker=$KUBERNETS_UID -mode=generate`
	echo "Use broker ID: $ID"
	export KAFKA_BROKER_ID=$ID
fi

if [[ -n "$ENABLE_AUTO_EXTEND" ]]; then
	echo "Enable auto exand"
	/usr/bin/kafka-autoextend-partitions.sh up &
	/usr/bin/start-kafka.sh
else
	/usr/bin/start-kafka.sh
fi