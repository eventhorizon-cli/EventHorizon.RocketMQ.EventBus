# EventHorizon.RocketMQ.Remoting.EventBus

[English](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/src/EventHorizon.RocketMQ.Remoting.EventBus/README.md) |
[简体中文](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/src/EventHorizon.RocketMQ.Remoting.EventBus/README.zh-CN.md)

`EventHorizon.RocketMQ.Remoting.EventBus` is the strongly typed EventBus adapter for
[EventHorizon.RocketMQ.Remoting](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ). It adds integration-event
publishing and Push consumption to the classic RocketMQ Remoting client while keeping application event contracts,
routing, and serialization transport-neutral in `EventHorizon.RocketMQ.EventBus`.

The first release supports ordinary strongly typed event publishing and Push consumption only. It does not expose Pull,
LitePull, POP, FIFO, transactional, delayed, priority, batch, request-reply, SQL92, or runtime subscription APIs.
Remoting EventBus consumption uses clustering only, never broadcasting. Delivery is at least once; application handlers
must make their side effects idempotent.
POP can only be added through a separate adapter entry point after the main client exposes a documented public
hosted-delivery abstraction; it is not a mode on this Push API.

## Package and dependencies

Install the package with:

```shell
dotnet add package EventHorizon.RocketMQ.Remoting.EventBus
```

This package depends on the same-version `EventHorizon.RocketMQ.EventBus` Core package and
`EventHorizon.RocketMQ.Remoting`. Core is restored transitively, is not embedded in this package, and is not a direct
user installation target. The Remoting and gRPC EventBus adapters do not reference each other.

The adapter registers a closed protocol bridge through the existing public
`AddRemotingPushConsumer<TMessageHandler>` API. Its internal generic anchor is the first application Handler owned by
that EventBus registration. The adapter does not inspect service descriptors or access the client's internal role
identity.

## Connection architecture

```text
Application
    |
    v
EventHorizon.RocketMQ.Remoting.EventBus
    |
    v
EventHorizon.RocketMQ.Remoting
    |
    +--> NameServer route lookup
    |         |
    `---------> direct connections to advertised Brokers
```

`NamesrvAddr` addresses one or more NameServer endpoints. The Remoting client obtains route information from the
NameServer, then establishes direct connections to the Broker addresses advertised in that route information. The
application environment must therefore be able to reach those advertised Broker addresses; a Proxy is not the
Remoting EventBus endpoint.

The Remoting Push consumer is not a server-initiated Broker push protocol. It performs client-initiated long polling.
This adapter forces `ConsumeMessageBatchSize = 1`, so each transport callback delivers one physical message to the
EventBus. Receive `BatchSize` may still be greater than one for prefetch efficiency.

## Programming model

An application event declares stable RocketMQ routing metadata in its public parameterless constructor. A tag is
optional:

```csharp
public sealed class OrderSubmittedIntegrationEvent : IntegrationEvent
{
    public OrderSubmittedIntegrationEvent()
        : base("orders", "order-submitted")
    {
    }

    public Guid OrderId { get; init; }

    public decimal Total { get; init; }
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

Handlers implement the typed asynchronous contract:

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

`Topic` becomes the RocketMQ topic. A non-null `Tag` becomes one literal RocketMQ tag; `null` publishes an untagged
message. Within an EventBus registration, the ordinal, case-sensitive `(Topic, Tag)` pair identifies one event type.
`*` is a consumer `FilterExpression`, not an event tag. When any route for a topic is untagged, the Remoting consumer
subscribes with `*`, while local dispatch still selects `(Topic, null)` exactly. The transport metadata is not written
to the JSON body.

## Registration

The following default registration enables both publishing and consumption. It discovers handlers from the application
assembly. The resulting `IEventBus` registration is unkeyed for the default client registration.

```csharp
var builder = Host.CreateApplicationBuilder(args);

builder.Services
    .AddRocketMQRemoting(options => options.NamesrvAddr = "localhost:9876")
    .AddRemotingEventBus(
        configureConsumer: options =>
        {
            options.GroupName = "ordering-service";
            options.MaxConcurrency = 8;
        },
        configureProducer: static _ => { })
    .AddHandlersFromAssemblyOf<Program>();

using var host = builder.Build();
await host.RunAsync();
```

Use a named main-client registration when the host needs isolated clients. A Producer-enabled named EventBus exposes
keyed `IEventBus` under the same name; this example uses direct Handler registration rather than assembly scanning:

```csharp
builder.Services
    .AddRocketMQRemoting("orders", options => options.NamesrvAddr = "orders-nameserver:9876")
    .AddRemotingEventBus(
        configureConsumer: options => options.GroupName = "ordering-service",
        configureProducer: static _ => { })
    .AddHandler<OrderSubmittedIntegrationEventHandler>();

using var host = builder.Build();
var ordersEventBus = host.Services.GetRequiredKeyedService<IEventBus>("orders");
```

`AddHandlersFromAssemblyOf<TMarker>()` scans a marker type's assembly. `AddHandlersFromAssembly(assembly)` scans an
explicit `Assembly`; `AddHandler<THandler>()` registers one Handler type. Registration is startup-only and must finish
before the service provider or Host is built. There is no runtime subscribe, unsubscribe, Handler registration, or
assembly scanning API.

Within one EventBus registration, direct registrations retain call order and assembly scanning is deterministic.
Duplicate event-Handler pairs with the same lifetime are idempotent; conflicting lifetimes are configuration errors.
One Handler type cannot be registered with another default or named EventBus registration in the same service
collection. Handlers default to `Scoped`, and may instead be `Transient` or `Singleton`; singleton Handlers must be
thread-safe.

## Optional roles and lifecycle

`configureProducer` is the publishing capability switch:

| Configuration | Registered roles |
| --- | --- |
| `configureProducer` is non-null | One Remoting Producer and unkeyed `IEventBus` for the default registration, or keyed `IEventBus` for a named registration |
| `configureProducer` is null | No Producer, Producer hosted service, or `IEventBus` for that registration |
| The first Handler is registered | One clustering-mode Remoting Push consumer is added |
| Neither a Producer nor a Handler is registered | No EventBus transport role or hosted service is added |

Consequently, a consumer-only service omits `configureProducer`; a publisher-only service provides a non-null
`configureProducer` and registers no Handlers. Each default or named EventBus registration keeps its own route table,
serializer, Handler registrations and lifetimes, optional Producer, and optional Push consumer.

The adapter composes the main Remoting client's `IHostedService` registrations. Generic Host starts and stops the
actual Producer and Push consumer roles; applications using Generic Host must not manually call the underlying client's
`StartAsync` or `StopAsync` methods.

## Serialization and dispatch

Newtonsoft.Json is the default serializer. It writes the concrete event type as compact UTF-8 JSON with
`TypeNameHandling.None`; there is no envelope or .NET type name, and `Topic` and `Tag` are excluded from the body.
The startup route table selects the destination event type before deserialization.

Use `UseSerializer<TSerializer>()` to replace the per-registration serializer with an `IIntegrationEventSerializer`
implementation. Each EventBus registration owns a private-token-keyed singleton; using the same Serializer type in two
registrations creates two independent instances. Custom serializers must be thread-safe, deterministic, and compatible
between the event's producers and consumers.

Each delivered message uses one asynchronous DI scope. The adapter resolves all matching Handlers from that scope and
invokes them sequentially. A message succeeds only after every Handler completes successfully.

| Condition | Dispatch outcome |
| --- | --- |
| Route is known, payload is valid, and all Handlers finish | `Success` |
| Handler or application dependency resolution fails | EventBus returns `Retry` |
| The main client cannot create or dispose the delivery scope, or consume timeout expires | The underlying consumer retries; EventBus does not manufacture an outcome |
| Route is unknown or payload cannot be deserialized | `DeadLetter` |
| Host shutdown cancels delivery | Cancellation continues to the underlying consumer; no outcome is manufactured |

The Remoting adapter explicitly maps its internal outcome to
`EventHorizon.RocketMQ.Remoting.Consumer.ConsumeResult`. It does not expose a transport `ConsumeResult` in the EventBus
public API.

Serialization failures, transport send failures, and non-success Remoting send statuses are exposed as
`EventBusPublishException`. Cancellation requested by the caller remains an unwrapped `OperationCanceledException`.

## Logging

The adapter writes structured publish, consume, and outcome logs through `Microsoft.Extensions.Logging` under the
`EventHorizon.RocketMQ.Remoting.EventBus` category prefix. Successful publish and consume operations use `Information`;
publish failures, EventBus-selected `Retry`, and `DeadLetter` use `Error`. Consume timeout and delivery-scope lifecycle
failures are logged by the underlying client. Normal Host-shutdown cancellation is not an EventBus error.
Publish and final Consumer outcome logs include the complete message content in the structured `Payload` field as
single-line JSON. The default serializer reuses the actual JSON body. With a custom serializer, the built-in
Newtonsoft.Json serializer produces the logging view whenever the event object is available; an unreadable raw body is
represented as `{"encoding":"base64","data":"..."}`. A serialization failure before any body exists can leave
`Payload` empty. Consumer deserialization failures omit the field entirely.

EventBus logging and payload inclusion both default to enabled. Configure them independently for each default or named
registration; `Enabled = false` suppresses that registration's publish, Consumer, outcome, and subscription-summary
logs without affecting the underlying RocketMQ client logs:

```csharp
eventBusBuilder.ConfigureLogging(options =>
{
    options.Enabled = true;
    options.IncludePayload = false;
});
```

These full-payload logs can contain credentials, personal data, or other sensitive content. Configure category
filters, log retention, and access controls for the deployment. For example:

```json
{
  "Logging": {
    "LogLevel": {
      "EventHorizon.RocketMQ.Remoting.EventBus": "Information"
    }
  }
}
```

After a Consumer registration's routes have been validated and all local subscriptions materialized, the adapter emits
exactly one aggregated `Information` subscription summary for that EventBus registration, not one log per Handler. It
includes the registration name (`<default>` for the default registration), Consumer Group, Handler count,
subscription count, and deterministically ordered Topic plus tag `FilterExpression` values. It describes local client
configuration, not Broker acknowledgement.

## Further reading

- [EventBus design](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/docs/en-US/event-bus-design.md)
- [`ConsumeResult` handling design](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/docs/en-US/consume-result-design.md)
- [Serialization design](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/docs/en-US/serialization-design.md)
- [Testing, environments, and samples](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/docs/en-US/testing-design.md)
- [Underlying Remoting client samples](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/tree/main/samples/remoting)

## License

This project is licensed under the
[MIT License](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/LICENSE).
