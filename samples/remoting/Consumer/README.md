# Remoting EventBus consumer

[English](README.md) | [简体中文](README.zh-CN.md)

This Generic Host creates clustering Remoting Push Consumers only; it does not configure a Producer. The default
registration directly registers a tagged order route and an untagged inventory route. EventBus forces one message per
Handler invocation while preserving configurable receive prefetch. The tagged order event has two Handlers, showing
that one `(Topic, Tag)` route selects one event type and can fan out after one deserialization.

The same process also creates the named `orders` registration with its own consumer group and a separate order Handler.
Its route uses the same topic but the literal `order-submitted-named` Tag, while the default route uses
`order-submitted`. A registration name is DI/client isolation, not RocketMQ routing metadata.

The default registration explicitly uses `RemotingPushQueueAssignmentMode.Broker`; the named `orders` registration
keeps the default `Client` assignment. This is intentional: queue assignment is selected per consumer registration,
while a Broker request mode is selected per `(Topic, Consumer Group)`. `Broker` asks the Broker to assign queues; it
does not directly select POP. The local stack relies on RocketMQ's normal PULL default until an operator adds a POP
request-mode configuration.

Start the [multi-Broker environment](../../../test-environments/rocketmq-multi-broker/README.md), then run:

```bash
dotnet run --project samples/remoting/Consumer
```

Override NameServer with `RocketMQ__NamesrvAddr`. Stop the process normally to exercise hosted shutdown.

## Optional Broker-assigned POP

After the local stack is running, configure POP for both topics consumed by the default group, then start this sample:

```shell
docker compose -f test-environments/rocketmq-multi-broker/compose.yaml exec broker-a \
  sh /home/rocketmq/rocketmq-5.5.0/bin/mqadmin setConsumeMode \
  -n nameserver:9876 -c DefaultCluster -t eventbus-orders \
  -g eventbus-remoting-sample -m POP -q 0

docker compose -f test-environments/rocketmq-multi-broker/compose.yaml exec broker-a \
  sh /home/rocketmq/rocketmq-5.5.0/bin/mqadmin setConsumeMode \
  -n nameserver:9876 -c DefaultCluster -t eventbus-inventory-snapshots \
  -g eventbus-remoting-sample -m POP -q 0
```

`-c DefaultCluster` applies the setting across the three masters in the sample stack. Change `-m POP` to `-m PULL` to
switch back. Do not mix `Client` and `Broker` instances in `eventbus-remoting-sample`; a request-mode transition can
redeliver messages that were not settled by the old receiver, so the sample Handlers, like production Handlers, remain
idempotent.
