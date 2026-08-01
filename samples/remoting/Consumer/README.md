# Remoting EventBus consumer

[English](README.md) | [简体中文](README.zh-CN.md)

This Generic Host creates clustering Remoting Push Consumers only; it does not configure a Producer. The default
registration directly registers a tagged order route and an untagged inventory route. EventBus forces one message per
Handler invocation while preserving configurable receive prefetch. The tagged order event has two Handlers, showing
that one `(Topic, Tag)` route selects one event type and can fan out after one deserialization.

The same process also creates the named `orders` registration with its own consumer group and a separate order Handler.
Its route uses the same topic but the literal `order-submitted-named` Tag, while the default route uses
`order-submitted`. A registration name is DI/client isolation, not RocketMQ routing metadata.

Start the [multi-Broker environment](../../../test-environments/rocketmq-multi-broker/README.md), then run:

```bash
dotnet run --project samples/remoting/Consumer
```

Override NameServer with `RocketMQ__NamesrvAddr`. Stop the process normally to exercise hosted shutdown.
