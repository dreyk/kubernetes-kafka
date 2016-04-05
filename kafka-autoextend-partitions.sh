#!/usr/bin/env bash


BROKER="$KAFKA_ADVERTISED_HOST_NAME:$KAFKA_ADVERTISED_PORT"

echo "Custom action $1 for $BROKER"
if [[ -z "$START_TIMEOUT" ]]; then
    START_TIMEOUT=600
fi

if [[ "$1" = "up" ]];then
	start_timeout_exceeded=false
	count=0
	step=10
	while netstat -lnt | awk '$4 ~ /:'$KAFKA_PORT'$/ {exit 1}'; do
    	echo "waiting for kafka to be ready"
    	sleep $step;
    	count=$(expr $count + $step)
    	if [ $count -gt $START_TIMEOUT ]; then
        	start_timeout_exceeded=true
        	break
    	fi
	done
fi

$KAFKA_HOME/bin/kafka-run-class.sh kafka.admin.AutoExpandCommand --zookeeper=$KAFKA_ZOOKEEPER_CONNECT --broker=$BROKER --updown=$1