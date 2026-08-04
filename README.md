# EventHorizon.RocketMQ.EventBus

[![.NET Build](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/actions/workflows/dotnet-build.yml/badge.svg?branch=main)](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/actions/workflows/dotnet-build.yml)
[![NuGet gRPC EventBus](https://img.shields.io/nuget/vpre/EventHorizon.RocketMQ.Grpc.EventBus.svg?label=NuGet%20gRPC%20EventBus)](https://www.nuget.org/packages/EventHorizon.RocketMQ.Grpc.EventBus)
[![NuGet Remoting EventBus](https://img.shields.io/nuget/vpre/EventHorizon.RocketMQ.Remoting.EventBus.svg?label=NuGet%20Remoting%20EventBus)](https://www.nuget.org/packages/EventHorizon.RocketMQ.Remoting.EventBus)
[![Codecov](https://codecov.io/gh/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/graph/badge.svg)](https://codecov.io/gh/eventhorizon-cli/EventHorizon.RocketMQ.EventBus)

[English](README.md) | [简体中文](README.zh-CN.md)

A strongly typed EventBus layer for
[EventHorizon.RocketMQ](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ), following the practical
integration-event style used by Microsoft's eShop projects.

It provides one application-facing model over RocketMQ 5 gRPC and classic Remoting while keeping the protocols in
separate packages.

## Scope

The first release:

- publishes strongly typed integration events and consumes through Push consumers only;
- routes and deserializes one physical RocketMQ message per delivery;
- registers Handlers directly or by deterministic assembly scanning;
- resolves Handlers through Microsoft DI, with `Scoped` as the default lifetime;
- preserves the main client's default and named/keyed registration model;
- uses the main client's `IHostedService` registrations with Generic Host;
- uses Newtonsoft.Json by default and permits a registration-specific custom serializer;
- writes structured publish, consume, outcome, and subscription-summary logs; and
- includes unit tests, protocol-specific three-Broker integration tests, runnable Consumer and Web API Publisher
  samples, and a separate Compose environment.

Standalone Pull, Simple, LitePush, SQL92, runtime subscriptions, transactional or ordered messages, delay messages,
batch publishing, request-reply, and exactly-once delivery are outside this release. Classic Remoting Push may use
PULL or POP internally for Broker-owned assignments without changing the EventBus API or handler. A later public
delivery model requires an appropriate main-client hosted-delivery abstraction. Handlers must make their side effects
idempotent.

## Install

| Package | Responsibility |
| --- | --- |
| `EventHorizon.RocketMQ.Remoting.EventBus` | Classic Remoting Producer and clustering Push-consumer adapter |
| `EventHorizon.RocketMQ.Grpc.EventBus` | RocketMQ 5 gRPC Producer and Push-consumer adapter |

Install the adapter for the chosen RocketMQ protocol. Both adapters restore the shared EventBus implementation
transitively and never reference each other. That supporting package is intentionally unlisted from NuGet search and
is not a direct user installation target.

## Programming model

The public contracts live in `EventHorizon.RocketMQ.EventBus.Abstractions`, `.Events`, `.Exceptions`, and
`.Serialization`; registration extensions live in the root namespace of Core or the selected adapter.

```csharp
using EventHorizon.RocketMQ.EventBus;
using EventHorizon.RocketMQ.EventBus.Abstractions;
using EventHorizon.RocketMQ.EventBus.Events;
using EventHorizon.RocketMQ.Grpc.EventBus;
```

An event inherits `IntegrationEvent` and gives its stable route to the base constructor:

```csharp
public sealed class OrderSubmittedIntegrationEvent : IntegrationEvent
{
    public OrderSubmittedIntegrationEvent()
        : base("orders", "order-submitted")
    {
    }

    public Guid OrderId { get; init; }
}

public sealed class InventorySnapshotIntegrationEvent : IntegrationEvent
{
    public InventorySnapshotIntegrationEvent()
        : base("inventory-snapshots")
    {
    }

    public int Available { get; init; }
}
```

`Topic` maps directly to the RocketMQ topic. A non-null `Tag` maps to one literal tag; `null` publishes an untagged
message. The exact, ordinal `(Topic, Tag)` route selects exactly one event type; that event type may have multiple
Handlers, which run sequentially after one deserialization. `*` is a consumer filter expression, never an event tag.
If a topic has any untagged route, its consumer subscribes with `*` and local routing still matches `(Topic, null)`
exactly. Neither routing value is included in the default JSON body.

Handlers use `Task`, so ordinary asynchronous application work needs no adapter-specific convention:

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

Register a gRPC EventBus and scan an application assembly:

```csharp
builder.Services
    .AddRocketMQGrpc(options => options.Endpoint = "http://localhost:8081")
    .AddGrpcEventBus(
        configureConsumer: options => options.GroupName = "ordering-service",
        configureProducer: static _ => { })
    .AddHandlersFromAssemblyOf<Program>();
```

`AddHandler<THandler>()` registers one concrete Handler type. `AddHandlersFromAssemblyOf<TMarker>()` and
`AddHandlersFromAssembly(assembly)` discover Handlers during startup; each accepts an optional `ServiceLifetime`.
The same concrete Handler type may belong to only one EventBus registration in an `IServiceCollection`, across both
protocols and all registration names.

`configureProducer` is the publishing switch. When it is omitted, the registration creates no Producer, Producer
hosted service, or `IEventBus`. The first Handler creates the Push consumer, so publisher-only services do not create
an empty consumer and consumer-only services do not create a Producer.

For a named registration with publishing enabled, resolve the keyed publisher under the same name:

```csharp
var ordersEventBus = serviceProvider.GetRequiredKeyedService<IEventBus>("orders");
await ordersEventBus.PublishAsync(new OrderSubmittedIntegrationEvent { OrderId = orderId });
```

The default registration exposes an unkeyed `IEventBus`. `PublishAsync` accepts an optional
`CancellationToken`; serialization and send failures use `EventBusPublishException`, while caller cancellation remains
`OperationCanceledException`.

## Logging

The adapters log successful publish/consume activity at `Information`, and publish failures, EventBus-selected
`Retry`, and `DeadLetter` outcomes at `Error`. They also emit one aggregated subscription summary per consumer
registration. Publish and final Consumer outcome logs include the complete message content in the structured `Payload`
field as single-line JSON. Custom-serializer events use the built-in Newtonsoft.Json view when an event object is
available; unknown-route raw bodies use a Base64 JSON wrapper when needed. Consumer deserialization failures omit the
field. Logging and payload inclusion both default to enabled and can be configured per registration:

```csharp
eventBusBuilder.ConfigureLogging(options =>
{
    options.Enabled = true;
    options.IncludePayload = false;
});
```

Payload logs may contain sensitive data, so also configure ordinary category filters, retention, and access controls:

```json
{
  "Logging": {
    "LogLevel": {
      "EventHorizon.RocketMQ.Grpc.EventBus": "Information",
      "EventHorizon.RocketMQ.Remoting.EventBus": "Warning"
    }
  }
}
```

## Design

- [English documentation](docs/en-US/)
- [简体中文文档](docs/zh-CN/)

## License

This project is licensed under the [MIT License](LICENSE).
