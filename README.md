# EventHorizon.RocketMQ.EventBus

[![.NET Build](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/actions/workflows/dotnet-build.yml/badge.svg?branch=main)](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/actions/workflows/dotnet-build.yml)
[![NuGet gRPC EventBus](https://img.shields.io/nuget/vpre/EventHorizon.RocketMQ.Grpc.EventBus.svg?label=NuGet%20gRPC%20EventBus)](https://www.nuget.org/packages/EventHorizon.RocketMQ.Grpc.EventBus)
[![NuGet Remoting EventBus](https://img.shields.io/nuget/vpre/EventHorizon.RocketMQ.Remoting.EventBus.svg?label=NuGet%20Remoting%20EventBus)](https://www.nuget.org/packages/EventHorizon.RocketMQ.Remoting.EventBus)
[![Codecov](https://codecov.io/gh/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/graph/badge.svg)](https://codecov.io/gh/eventhorizon-cli/EventHorizon.RocketMQ.EventBus)

[English](README.md) | [简体中文](README.zh-CN.md)

A strongly typed EventBus for
[EventHorizon.RocketMQ](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ). It provides the same event,
handler, routing, serialization, and hosting model for RocketMQ 5 gRPC and classic Remoting while keeping the two
protocol adapters independent.

## Choose a package

| Package | Use it for |
| --- | --- |
| [`EventHorizon.RocketMQ.Grpc.EventBus`](https://www.nuget.org/packages/EventHorizon.RocketMQ.Grpc.EventBus) | Services that connect through a RocketMQ 5 Proxy using gRPC |
| [`EventHorizon.RocketMQ.Remoting.EventBus`](https://www.nuget.org/packages/EventHorizon.RocketMQ.Remoting.EventBus) | Services that discover Brokers through NameServer and use classic Remoting |

Install one adapter for the protocol used by the service. The two adapters are independent and can be selected without
introducing the other protocol client.

## Supported model

- Strongly typed publishing and Push consumption
- Exact `(Topic, Tag)` routing, including untagged messages
- Direct handler registration or deterministic assembly scanning
- Microsoft dependency injection and Generic Host lifecycle
- Default and named/keyed client registrations
- Newtonsoft.Json by default, with a replaceable serializer per registration
- Structured publish, consume, outcome, and subscription-summary logs

Delivery is at least once, so handlers must make application side effects idempotent. Standalone Pull, SimpleConsumer,
LitePull, LitePush, FIFO, transactional, delayed, priority, batch, request-reply, SQL92, and runtime-subscription APIs
are outside the current EventBus surface.

Classic Remoting Push may use PULL or POP internally for Broker-owned assignments. That choice does not change the
EventBus API or handler contract.

## Quick start

Define an event with a stable route:

```csharp
public sealed class OrderSubmittedIntegrationEvent : IntegrationEvent
{
    public OrderSubmittedIntegrationEvent()
        : base("orders", "order-submitted")
    {
    }

    public Guid OrderId { get; init; }
}
```

Implement its handler:

```csharp
public sealed class OrderSubmittedIntegrationEventHandler
    : IIntegrationEventBusHandler<OrderSubmittedIntegrationEvent>
{
    public Task HandleAsync(
        OrderSubmittedIntegrationEvent integrationEvent,
        CancellationToken cancellationToken = default)
    {
        return Task.CompletedTask;
    }
}
```

Register the gRPC adapter and scan the application assembly:

```csharp
builder.Services
    .AddRocketMQGrpc(options => options.Endpoint = "http://localhost:8081")
    .AddGrpcEventBus(
        configureConsumer: options => options.GroupName = "ordering-service",
        configureProducer: static _ => { })
    .AddHandlersFromAssemblyOf<Program>();
```

Use `AddRocketMQRemoting` and `AddRemotingEventBus` instead when connecting through NameServer and classic Remoting.

`configureProducer` enables publishing and registers `IEventBus`. Omit it for a consumer-only service. A Push consumer
is added when the first handler is registered, so publisher-only services do not start an empty consumer.

Publish through the default registration:

```csharp
await eventBus.PublishAsync(
    new OrderSubmittedIntegrationEvent { OrderId = orderId },
    cancellationToken);
```

A named, Producer-enabled registration exposes keyed `IEventBus` under the same name:

```csharp
var ordersEventBus = serviceProvider.GetRequiredKeyedService<IEventBus>("orders");
```

## Routing and failures

`Topic` maps directly to the RocketMQ topic. A non-null `Tag` is one literal tag; `null` publishes an untagged message.
Within one EventBus registration, the ordinal, case-sensitive `(Topic, Tag)` pair identifies exactly one event type.
That event type may have multiple handlers, which run sequentially after one deserialization. `Topic` and `Tag` are
not written into the default JSON body.

Serialization and send failures are reported as `EventBusPublishException`. Caller-requested cancellation remains an
`OperationCanceledException`.

For consumption, a message succeeds only after all matching handlers complete. Handler failures request retry;
unknown routes and invalid payloads request dead-letter handling. The adapter maps those outcomes to the capabilities
of its protocol client.

## Logging

EventBus logging and full-payload logging are enabled by default for each registration:

```csharp
eventBusBuilder.ConfigureLogging(options =>
{
    options.Enabled = true;
    options.IncludePayload = false;
});
```

Payload logs may contain credentials, personal data, or other sensitive content. Configure category filters,
retention, and access controls for the deployment.

## Documentation

- [English documentation](docs/en-US/)
- [简体中文文档](docs/zh-CN/)
- [Samples](samples/)

## License

This project is licensed under the [MIT License](LICENSE).
