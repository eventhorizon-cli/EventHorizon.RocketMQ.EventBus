# EventHorizon.RocketMQ.Grpc.EventBus

[English](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/src/EventHorizon.RocketMQ.Grpc.EventBus/README.md) |
[简体中文](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/src/EventHorizon.RocketMQ.Grpc.EventBus/README.zh-CN.md)

`EventHorizon.RocketMQ.Grpc.EventBus` adds strongly typed event publishing and Push consumption to
[EventHorizon.RocketMQ.Grpc](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ).

Use this adapter when the application connects to a RocketMQ 5 Proxy through gRPC. Delivery is at least once, so
handlers must make application side effects idempotent.

## Install

```shell
dotnet add package EventHorizon.RocketMQ.Grpc.EventBus
```

This adapter includes the required gRPC client and shared EventBus dependencies. Applications normally need only this
package.

The current EventBus surface supports strongly typed publishing and Push consumption. It does not expose
SimpleConsumer, LitePush, FIFO, transactional, delayed, priority, batch, request-reply, SQL92, or runtime-subscription
APIs.

## Connect to RocketMQ

Set `Endpoint` to the RocketMQ 5 Proxy address. The application does not use a NameServer or Broker address as its
gRPC endpoint.

```csharp
builder.Services.AddRocketMQGrpc(options =>
{
    options.Endpoint = "http://localhost:8081";
});
```

The current adapter expects a cluster-mode RocketMQ 5 Proxy. The Proxy must be able to reach the NameServer and Brokers
in the RocketMQ cluster.

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
    .AddRocketMQGrpc("orders", options => options.Endpoint = "http://orders-proxy:8081")
    .AddGrpcEventBus(
        configureConsumer: options => options.GroupName = "ordering-service",
        configureProducer: static _ => { })
    .AddHandler<OrderSubmittedIntegrationEventHandler>();

using var host = builder.Build();
var ordersEventBus = host.Services.GetRequiredKeyedService<IEventBus>("orders");
```

## Delivery behavior

Each message is deserialized once. All matching handlers run sequentially within one asynchronous DI scope, and the
message succeeds only after every handler completes.

| Condition | gRPC result |
| --- | --- |
| Route is known, payload is valid, and all handlers finish | `Success` |
| A handler or application dependency fails | `Failure` |
| Route is unknown or payload is invalid | `Failure` |
| Host shutdown cancels delivery | Cancellation is propagated without manufacturing a result |

The gRPC Push handler has no separate dead-letter return value. EventBus logs an unknown route or invalid payload as
`DeadLetter`, but returns gRPC `Failure`; RocketMQ moves the message to the DLQ only after the consumer group's retry
limit is reached.

Serialization and send failures use `EventBusPublishException`. Caller-requested cancellation remains an unwrapped
`OperationCanceledException`.

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
retention, and access controls for `EventHorizon.RocketMQ.Grpc.EventBus`.

## Further reading

- [EventBus design](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/docs/en-US/event-bus-design.md)
- [`ConsumeResult` handling](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/docs/en-US/consume-result-design.md)
- [Serialization contract](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/docs/en-US/serialization-design.md)
- [Runnable samples](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/tree/main/samples)
- [Underlying gRPC client guide](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/src/EventHorizon.RocketMQ.Grpc/README.md)

## License

This package is licensed under the
[MIT License](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/LICENSE).
