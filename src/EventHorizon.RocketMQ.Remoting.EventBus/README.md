# EventHorizon.RocketMQ.Remoting.EventBus

[English](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/src/EventHorizon.RocketMQ.Remoting.EventBus/README.md) |
[简体中文](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/src/EventHorizon.RocketMQ.Remoting.EventBus/README.zh-CN.md)

`EventHorizon.RocketMQ.Remoting.EventBus` adds strongly typed event publishing and Push consumption to
[EventHorizon.RocketMQ.Remoting](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ).

Use this adapter when the application discovers Brokers through NameServer and connects with the classic RocketMQ
Remoting protocol. Delivery is at least once, so handlers must make application side effects idempotent.

## Install

```shell
dotnet add package EventHorizon.RocketMQ.Remoting.EventBus
```

The adapter includes the required Remoting client and shared EventBus dependencies. Applications normally need only
this package.

The current EventBus surface supports strongly typed publishing and clustering-mode Push consumption. It does not
expose standalone Pull, LitePull, broadcasting, FIFO, transactional, delayed, priority, batch, request-reply, SQL92,
or runtime-subscription APIs.

## Connect to RocketMQ

Set `NamesrvAddr` to one or more NameServer addresses. The client obtains route information from NameServer, then
connects directly to the Broker addresses advertised in that route. Those Broker addresses must be reachable from the
application environment.

```csharp
builder.Services.AddRocketMQRemoting(options =>
{
    options.NamesrvAddr = "localhost:9876";
});
```

A RocketMQ 5 Proxy address is not a Remoting `NamesrvAddr`. See the underlying Remoting client guide for TLS, ACL,
namespace, and multi-NameServer configuration.

## PULL and POP inside Push

The EventBus always exposes one Push-consumer programming model. Queue assignment controls which receive path the
underlying Remoting client uses:

| `QueueAssignmentMode` | Queue assignment and receive behavior |
| --- | --- |
| `RemotingPushQueueAssignmentMode.Client` | Default. The client assigns queues and receives with PULL. |
| `RemotingPushQueueAssignmentMode.Broker` | The Broker assigns queues; each returned assignment may use PULL or POP according to Broker configuration. |

Switching between these modes does not change the EventBus API or handler contract. Broker assignment requires the
corresponding Broker-side assignment request mode to be configured.

For POP, processing must finish within `PopInvisibleDuration`. Classic Remoting Push does not automatically renew the
receipt while a handler is running, so configure the invisible duration for the longest expected processing time.

The adapter sets `ConsumeMessageBatchSize = 1`, which keeps one physical message per EventBus handler invocation.
`PullBatchSize` and `PopBatchSize` may still be greater than one to preserve receive efficiency.

## Define events and handlers

Each event declares a stable RocketMQ route in its public parameterless constructor:

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
```

Implement the typed asynchronous handler contract:

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

`Topic` maps directly to the RocketMQ topic. A non-null `Tag` is one literal tag; `null` publishes an untagged message.
Within one registration, the ordinal, case-sensitive `(Topic, Tag)` pair identifies exactly one event type. `Topic`
and `Tag` are not included in the default JSON body.

## Register the EventBus

This registration enables publishing and consumption, then scans the application assembly for handlers:

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

Use `AddHandler<THandler>()` for one handler, `AddHandlersFromAssemblyOf<TMarker>()` for a marker assembly, or
`AddHandlersFromAssembly(assembly)` for an explicit assembly. Handler registration is startup-only. Handlers default
to `Scoped`; `Transient` and `Singleton` are also available, and singleton handlers must be thread-safe.

`configureProducer` enables publishing and registers `IEventBus`. Omit it for a consumer-only service. A Push consumer
is added when the first handler is registered, so a publisher-only service can enable the Producer without registering
handlers. Generic Host starts and stops the configured RocketMQ roles.

Named RocketMQ registrations are also supported. A named, Producer-enabled EventBus exposes keyed `IEventBus` under
the same name:

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

## Delivery behavior

Each message is deserialized once. All matching handlers run sequentially within one asynchronous DI scope, and the
message succeeds only after every handler completes.

| Condition | Remoting result |
| --- | --- |
| Route is known, payload is valid, and all handlers finish | `Success` |
| A handler or application dependency fails | `Retry` |
| Route is unknown or payload is invalid | `DeadLetter` |
| Host shutdown cancels delivery | Cancellation is propagated without manufacturing a result |

Serialization failures, transport send failures, and non-success Remoting send statuses use
`EventBusPublishException`. Caller-requested cancellation remains an unwrapped `OperationCanceledException`.

## Serialization and logging

Newtonsoft.Json is the default serializer. It writes compact UTF-8 JSON with `TypeNameHandling.None` and no event
envelope or .NET type name. Use `UseSerializer<TSerializer>()` to replace it for one EventBus registration.

Structured EventBus logs and full-payload logging are enabled by default:

```csharp
eventBusBuilder.ConfigureLogging(options =>
{
    options.Enabled = true;
    options.IncludePayload = false;
});
```

Payload logs may contain credentials, personal data, or other sensitive content. Configure category filters,
retention, and access controls for `EventHorizon.RocketMQ.Remoting.EventBus`.

## Further reading

- [EventBus design](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/docs/en-US/event-bus-design.md)
- [`ConsumeResult` handling](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/docs/en-US/consume-result-design.md)
- [Serialization contract](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/docs/en-US/serialization-design.md)
- [Runnable samples](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/tree/main/samples)
- [Underlying Remoting client guide](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/src/EventHorizon.RocketMQ.Remoting/README.md)

## License

This package is licensed under the
[MIT License](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/LICENSE).
