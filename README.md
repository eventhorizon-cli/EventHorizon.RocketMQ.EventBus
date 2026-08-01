# EventHorizon.RocketMQ.EventBus

[English](README.md) | [Simplified Chinese](README.zh-CN.md)

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

Pull, Simple, POP, LitePush, SQL92, runtime subscriptions, transactional or ordered messages, delay messages, batch
publishing, request-reply, and exactly-once delivery are outside this release. A later delivery mode must use a
separate adapter entry point and can be added only after the main client exposes an appropriate public hosted-delivery
abstraction. Handlers must make their side effects idempotent.

## Packages

| Package | Responsibility |
| --- | --- |
| `EventHorizon.RocketMQ.EventBus` | Public contracts, Newtonsoft.Json serialization, routing, Handler registration, and common dispatch runtime |
| `EventHorizon.RocketMQ.Remoting.EventBus` | Classic Remoting Producer and clustering Push-consumer adapter |
| `EventHorizon.RocketMQ.Grpc.EventBus` | RocketMQ 5 gRPC Producer and Push-consumer adapter |

Both adapters depend on Core but never on each other. Installing an adapter restores Core transitively; an
application-owned event-contract project may reference Core directly.

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
- [Simplified Chinese documentation](docs/zh-CN/)

## License

This project is licensed under the [MIT License](LICENSE).
