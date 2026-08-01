#!/bin/sh

set -eu

admin=/home/rocketmq/rocketmq-5.5.0/bin/mqadmin
nameserver=nameserver:9876
topics="eventbus-orders eventbus-inventory-snapshots"
queues=3
brokers="eventbus-broker-a:10911 eventbus-broker-b:10921 eventbus-broker-c:10931"
groups="eventbus-grpc-sample eventbus-grpc-orders-sample eventbus-remoting-sample eventbus-remoting-orders-sample"

attempt=1
while [ "$attempt" -le 90 ]; do
    registered="$($admin clusterList -n "$nameserver" 2>/dev/null || true)"
    if printf '%s\n' "$registered" | grep -Fq eventbus-broker-a &&
        printf '%s\n' "$registered" | grep -Fq eventbus-broker-b &&
        printf '%s\n' "$registered" | grep -Fq eventbus-broker-c; then
        break
    fi

    echo "Waiting for the three EventBus Brokers ($attempt/90)..."
    attempt=$((attempt + 1))
    sleep 1
done

if [ "$attempt" -gt 90 ]; then
    echo "The three Brokers did not register before the timeout." >&2
    exit 1
fi

for broker in $brokers; do
    for topic in $topics; do
        $admin updateTopic \
            -n "$nameserver" \
            -b "$broker" \
            -t "$topic" \
            -r "$queues" \
            -w "$queues"
    done

    for group in $groups; do
        $admin updateSubGroup \
            -n "$nameserver" \
            -b "$broker" \
            -g "$group"
    done
done

for topic in $topics; do
    route="$($admin topicRoute -n "$nameserver" -t "$topic")"
    printf '%s\n' "$route"

    for name in eventbus-broker-a eventbus-broker-b eventbus-broker-c; do
        if ! printf '%s\n' "$route" | grep -Fq "$name"; then
            echo "Topic $topic route does not contain $name." >&2
            exit 1
        fi
    done
done

echo "EventBus sample resources are ready: $topics with $queues queues per Topic on each Broker."
