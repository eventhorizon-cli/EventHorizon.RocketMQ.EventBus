# EventHorizon.RocketMQ.EventBus

[English](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/src/EventHorizon.RocketMQ.EventBus/README.md) |
[简体中文](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/src/EventHorizon.RocketMQ.EventBus/README.zh-CN.md)

`EventHorizon.RocketMQ.EventBus` is the protocol-neutral Core package for strongly typed integration events. It owns
the public event, Handler, publisher, serializer, registration, routing, and dispatch contracts without referencing a
gRPC or classic Remoting client package.

Applications normally install one adapter and receive Core transitively:

```shell
dotnet add package EventHorizon.RocketMQ.Grpc.EventBus
# or
dotnet add package EventHorizon.RocketMQ.Remoting.EventBus
```

An application-owned event-contract project can reference Core directly when it shares event types without carrying a
RocketMQ transport dependency:

```shell
dotnet add package EventHorizon.RocketMQ.EventBus
```

## Package boundary

```text
Application event contracts
            |
            v
EventHorizon.RocketMQ.EventBus
       ^                 ^
       |                 |
Grpc.EventBus      Remoting.EventBus
       |                 |
RocketMQ 5 Proxy   NameServer and Brokers
```

Core does not connect to RocketMQ, own sockets, or add a transport `IHostedService`. The selected adapter owns message
conversion, optional Producer and Push-consumer roles, transport settlement, and transport logging. The adapters do
not reference each other.

## Public contracts

| Namespace | Public API |
| --- | --- |
| `EventHorizon.RocketMQ.EventBus` | `IEventBusBuilder`, `EventBusLoggingOptions`, `ConfigureLogging`, and startup registration extensions |
| `.Abstractions` | `IEventBus` and `IIntegrationEventBusHandler<TIntegrationEvent>` |
| `.Events` | `IntegrationEvent` |
| `.Exceptions` | `EventBusPublishException` |
| `.Serialization` | `IIntegrationEventSerializer` and the default Newtonsoft.Json serializer |

`IntegrationEvent` carries immutable routing metadata through its base constructor:

```csharp
public sealed class OrderSubmittedIntegrationEvent : IntegrationEvent
{
    public OrderSubmittedIntegrationEvent()
        : base("orders", "order-submitted")
    {
    }
}

public sealed class InventorySnapshotIntegrationEvent : IntegrationEvent
{
    public InventorySnapshotIntegrationEvent()
        : base("inventory-snapshots")
    {
    }
}
```

`Topic` is the RocketMQ topic. A non-null `Tag` is one literal tag; `null` publishes an untagged message. The
case-sensitive, ordinal `(Topic, Tag)` pair identifies exactly one event type inside a registration. That event type
may have multiple Handlers, which run sequentially after one deserialization. `*` is never an event tag: it is a
consumer filter expression. When a topic has an untagged route, the adapter subscribes with `*`, while Core still
routes the received `(Topic, null)` exactly. `Topic` and `Tag` are excluded from the JSON body.

Handlers return `Task` and run sequentially from one asynchronous DI scope for each delivery:

```csharp
public sealed class OrderSubmittedHandler
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

Use `AddHandler<THandler>()` for one concrete Handler, `AddHandlersFromAssemblyOf<TMarker>()` for a marker assembly,
or `AddHandlersFromAssembly(Assembly)` for an explicit assembly. All are startup-only and accept an optional
`ServiceLifetime`; `Scoped` is the default. One concrete Handler type may belong to only one EventBus registration in
an `IServiceCollection`, including registrations owned by different adapters.

## Dispatch and serialization

Core routes and deserializes each physical message once, then invokes all matching Handlers sequentially. Handler or
dependency failures request `Retry`; unknown routes and invalid payloads request `DeadLetter`. Delivery is at least
once, so application effects must be idempotent.

The default serializer is Newtonsoft.Json with strict UTF-8, no envelope, and `TypeNameHandling.None`. It serializes
the concrete event type, and Core selects the destination type only from the startup route table. Replace both
directions for one registration with `UseSerializer<TSerializer>()`; custom serializers must be deterministic and
thread-safe.

`IEventBus.PublishAsync` has an optional `CancellationToken`. An adapter provides an unkeyed or keyed `IEventBus` only
when that registration enables a Producer. Serialization and send failures use `EventBusPublishException`; requested
cancellation remains `OperationCanceledException`.

## Boundaries and future modes

Core uses no main-client internals. If a required main-client capability is missing or defective, this repository opens
an issue in the main-client repository rather than modifying that repository. Push is the only delivery mode here.
LitePush or POP, if later supported, need a separate adapter entry point and a documented public hosted-delivery
abstraction from the main client; they do not become a switch on the Push API.

## Further reading

- [EventBus design](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/docs/en-US/event-bus-design.md)
- [`ConsumeResult` handling](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/docs/en-US/consume-result-design.md)
- [Serialization contract](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/docs/en-US/serialization-design.md)
- [Testing, environments, and samples](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/docs/en-US/testing-design.md)

## License

This package is licensed under the
[MIT License](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/LICENSE).
