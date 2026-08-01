# gRPC EventBus consumer

[English](README.md) | [Simplified Chinese](README.zh-CN.md)

This Generic Host creates gRPC Push Consumers only; it does not configure a Producer. The default registration uses
direct Handler registration for a tagged order route and an untagged inventory route. The order route has two Handlers,
showing that one `(Topic, Tag)` selects one event type and can fan out after one deserialization. The untagged route
makes its topic use the RocketMQ `*` subscription filter.

The same process also creates the named `orders` registration with its own consumer group and a separate order Handler.
Its route uses the same topic but the literal `order-submitted-named` Tag, while the default route uses
`order-submitted`. A registration name is DI/client isolation, not RocketMQ routing metadata.

Start the [multi-Broker environment](../../../test-environments/rocketmq-multi-broker/README.md), then run:

```bash
dotnet run --project samples/grpc/Consumer
```

Override the Proxy endpoint with `RocketMQ__GrpcEndpoint`. Stop the process normally to exercise hosted shutdown.
