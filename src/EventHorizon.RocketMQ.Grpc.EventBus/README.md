# EventHorizon.RocketMQ.Grpc.EventBus

[English](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/src/EventHorizon.RocketMQ.Grpc.EventBus/README.md) |
[简体中文](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/src/EventHorizon.RocketMQ.Grpc.EventBus/README.zh-CN.md)

`EventHorizon.RocketMQ.Grpc.EventBus` is the strongly typed EventBus adapter for
[EventHorizon.RocketMQ.Grpc](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ). It adds integration-event
publishing and Push consumption to the RocketMQ 5 gRPC client while keeping application event contracts, routing, and
serialization transport-neutral in `EventHorizon.RocketMQ.EventBus`.

The first release supports ordinary strongly typed event publishing and Push consumption only. It does not expose
Pull, SimpleConsumer, LitePush, FIFO, transactional, delayed, priority, batch, request-reply, SQL92, or runtime
subscription APIs. Delivery is at least once; application handlers must make their side effects idempotent. A future
public delivery model such as LitePush requires a separate adapter entry point and a documented main-client
hosted-delivery abstraction.

## Package and dependencies

Install the package with:

```shell
dotnet add package EventHorizon.RocketMQ.Grpc.EventBus
```

This package depends on the same-version `EventHorizon.RocketMQ.EventBus` Core package and
`EventHorizon.RocketMQ.Grpc`. Core is restored transitively, is not embedded in this package, and is not a direct user
installation target. The gRPC and Remoting EventBus adapters do not reference each other.

The adapter registers a closed protocol bridge through the existing public
`AddGrpcPushConsumer<TMessageHandler>` API. Its internal generic anchor is the first application Handler owned by that
EventBus registration. The adapter does not inspect service descriptors or access the client's internal role identity.

## Connection architecture

```text
Application
    |
    v
EventHorizon.RocketMQ.Grpc.EventBus
    |
    v
EventHorizon.RocketMQ.Grpc
    |
    v
RocketMQ 5 cluster-mode Proxy
    |
    +--> NameServer
    `--> Brokers
```

`Endpoint` addresses one or more RocketMQ Proxy endpoints. The application and its gRPC client do not query a
NameServer or establish direct Broker connections. The Proxy is the client-facing RocketMQ 5 endpoint and communicates
with the NameServer and Brokers in the cluster.

The gRPC Push consumer is not a server-initiated Broker push protocol. It uses client-initiated assignment queries and
`ReceiveMessage` long polling through the Proxy.

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
`*` is a consumer `FilterExpression`, not an event tag. When any route for a topic is untagged, the gRPC consumer
subscribes with `*`, while local dispatch still selects `(Topic, null)` exactly. The transport metadata is not written
to the JSON body.

## Registration

The following default registration enables both publishing and consumption. It discovers handlers from the application
assembly. The resulting `IEventBus` registration is unkeyed for the default client registration.

```csharp
var builder = Host.CreateApplicationBuilder(args);

builder.Services
    .AddRocketMQGrpc(options => options.Endpoint = "http://localhost:8081")
    .AddGrpcEventBus(
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
    .AddRocketMQGrpc("orders", options => options.Endpoint = "http://orders-proxy:8081")
    .AddGrpcEventBus(
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
| `configureProducer` is non-null | One gRPC Producer and unkeyed `IEventBus` for the default registration, or keyed `IEventBus` for a named registration |
| `configureProducer` is null | No Producer, Producer hosted service, or `IEventBus` for that registration |
| The first Handler is registered | One gRPC Push consumer is added |
| Neither a Producer nor a Handler is registered | No EventBus transport role or hosted service is added |

Consequently, a consumer-only service omits `configureProducer`; a publisher-only service provides a non-null
`configureProducer` and registers no Handlers. Each default or named EventBus registration keeps its own route table,
serializer, Handler registrations and lifetimes, optional Producer, and optional Push consumer.

The adapter composes the main gRPC client's `IHostedService` registrations. Generic Host starts and stops the actual
Producer and Push consumer roles; applications using Generic Host must not manually call the underlying client's
`StartAsync` or `StopAsync` methods.

## Serialization and dispatch

Newtonsoft.Json is the default serializer. It writes the concrete event type as compact UTF-8 JSON with
`TypeNameHandling.None`; there is no envelope or .NET type name, and `Topic` and `Tag` are excluded from the body.
The startup route table selects the destination event type before deserialization.

Use `UseSerializer<TSerializer>()` to replace the per-registration serializer with an
`IIntegrationEventSerializer` implementation. Each EventBus registration owns a private-token-keyed singleton;
using the same Serializer type in two registrations creates two independent instances. Custom serializers must be
thread-safe, deterministic, and compatible between the event's producers and consumers.

Each delivered message uses one asynchronous DI scope. The adapter resolves all matching Handlers from that scope and
invokes them sequentially. A message succeeds only after every Handler completes successfully.

| Condition | Dispatch outcome |
| --- | --- |
| Route is known, payload is valid, and all Handlers finish | `Success` |
| Handler or application dependency resolution fails | EventBus returns `Retry` |
| The main client cannot create or dispose the delivery scope, or consume timeout expires | The underlying consumer retries; EventBus does not manufacture an outcome |
| Route is unknown or payload cannot be deserialized | `DeadLetter` |
| Host shutdown cancels delivery | Cancellation continues to the underlying consumer; no outcome is manufactured |

The gRPC adapter explicitly maps its internal outcome to `EventHorizon.RocketMQ.Grpc.Consumer.ConsumeResult`.
`Success` maps to `Success`; EventBus `Retry` and `DeadLetter` both map to gRPC `Failure`. The gRPC Push handler has no
direct dead-letter result, so a deterministic EventBus `DeadLetter` classification is logged immediately but reaches
the service-side DLQ only after the consumer group's retry limit. The transport `ConsumeResult` is not exposed in the
EventBus public API.

Serialization failures and transport send failures are exposed as `EventBusPublishException`. Cancellation requested
by the caller remains an unwrapped `OperationCanceledException`.

## Logging

The adapter writes structured publish, consume, and outcome logs through `Microsoft.Extensions.Logging` under the
`EventHorizon.RocketMQ.Grpc.EventBus` category prefix. Successful publish and consume operations use `Information`;
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
      "EventHorizon.RocketMQ.Grpc.EventBus": "Information"
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
- [Underlying gRPC client samples](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/tree/main/samples/grpc)

## License

This project is licensed under the
[MIT License](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/LICENSE).
