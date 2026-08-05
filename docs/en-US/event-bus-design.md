# EventHorizon.RocketMQ.EventBus design

[Documentation](README.md) | [简体中文](../zh-CN/event-bus-design.md) |
[`ConsumeResult` handling design](consume-result-design.md) | [Serialization design](serialization-design.md) |
[Testing design](testing-design.md)

A strongly typed, dependency-injection-first EventBus layer for
[EventHorizon.RocketMQ](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ), inspired by the EventBus style of
Microsoft's archived [eShopOnContainers](https://github.com/dotnet-architecture/eShopOnContainers) project and its
current [eShop](https://github.com/dotnet/eShop) successor.

The library provides the same integration-event programming model over RocketMQ 5 gRPC and classic Remoting while
keeping the two transports in separate packages.

## Scope

The first release is intentionally small:

- publish strongly typed integration events;
- consume through the underlying gRPC or Remoting Push consumer only;
- deserialize each RocketMQ message once and dispatch one event at a time;
- discover typed handlers by scanning application assemblies;
- resolve handlers from Microsoft dependency injection, with `Scoped` as the default lifetime;
- support the main client's default and named/keyed client registrations;
- start and stop each configured Producer and Push consumer automatically through Generic Host and `IHostedService`;
- write structured publish, consume, and consume-outcome logs through `Microsoft.Extensions.Logging` by default;
- use Newtonsoft.Json by default for wire compatibility;
- allow applications to replace serialization and deserialization through a public interface; and
- target `net8.0` and `net10.0`, matching the underlying client.

The first release publishes ordinary messages only. It does not expose standalone Pull, Simple, LitePush, Admin,
transactional, FIFO, timed/delay, priority, batch, request-reply, SQL92 filtering, or runtime subscribe/unsubscribe APIs.
Classic Remoting Push may use PULL or POP internally for Broker-owned assignments without creating another EventBus
contract. Event delivery remains at least once, so handlers must be idempotent.

## Packages

| Package | Responsibility | Production dependencies |
| --- | --- | --- |
| `EventHorizon.RocketMQ.EventBus` | Public contracts, default Newtonsoft.Json serializer, route table, registration builder, handler discovery, and common dispatch runtime | Microsoft DI abstractions and Newtonsoft.Json |
| `EventHorizon.RocketMQ.Remoting.EventBus` | Classic Remoting Producer and Push-consumer adapter | EventBus Core and `EventHorizon.RocketMQ.Remoting` |
| `EventHorizon.RocketMQ.Grpc.EventBus` | RocketMQ 5 gRPC Producer and Push-consumer adapter | EventBus Core and `EventHorizon.RocketMQ.Grpc` |

```text
                            EventHorizon.RocketMQ.EventBus
                                      ^
                                      |
                +---------------------+---------------------+
                |                                           |
EventHorizon.RocketMQ.Remoting.EventBus       EventHorizon.RocketMQ.Grpc.EventBus
                |                                           |
EventHorizon.RocketMQ.Remoting                EventHorizon.RocketMQ.Grpc
```

The transport adapters do not reference each other. Applications install only the adapter for the RocketMQ protocol
they use.

Transport-specific `ConsumeResult` types do not enter the public EventBus API. The common dispatch path produces an
internal transport-neutral outcome, and each adapter explicitly maps it to the enum defined by its own main-client
package. See the [`ConsumeResult` handling design](consume-result-design.md#package-boundary).

### NuGet distribution

All three projects produce NuGet packages that remain listed and use the same version and release tag. Core is pushed
first, followed immediately by the two adapters because their package metadata depends on that exact Core version.

Each adapter declares the same-version `EventHorizon.RocketMQ.EventBus` package as a normal dependency. Installing an
adapter therefore restores Core transitively. Core is a shared support package rather than the recommended user entry
point; applications install only their selected adapter.

Core is not embedded into either adapter package, because duplicating the same public types across packages would
create assembly and version conflicts when both adapters are referenced. The source projects use `ProjectReference`;
packed adapters express the corresponding NuGet dependency.

### Public namespaces

| Namespace | Public responsibility |
| --- | --- |
| `EventHorizon.RocketMQ.EventBus` | `IEventBusBuilder` plus startup registration extensions |
| `.Abstractions` | `IEventBus` and `IIntegrationEventBusHandler<TIntegrationEvent>` |
| `.Events` | `IntegrationEvent` |
| `.Exceptions` | `EventBusPublishException` |
| `.Serialization` | `IIntegrationEventSerializer` and the default Newtonsoft.Json serializer |

The gRPC and Remoting packages keep their `AddGrpcEventBus` and `AddRemotingEventBus` extensions in their own root
namespaces. Transport-specific implementation types and `ConsumeResult` values stay internal to the adapter boundary.

## Programming model

### Integration events

`IntegrationEvent` is an abstract base class with exactly two routing properties. It does not add eShop's `Id` or
`CreationDate` fields.

```csharp
public abstract class IntegrationEvent
{
    protected IntegrationEvent(string topic, string? tag = null)
    {
        Topic = topic;
        Tag = tag;
    }

    [JsonIgnore]
    public string Topic { get; }

    [JsonIgnore]
    public string? Tag { get; }
}
```

An application event supplies its stable RocketMQ routing metadata from its public parameterless constructor. The
parameterless constructor is also what startup registration uses to discover the route before the Generic Host starts.

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

The routing rule is:

- `Topic` becomes the RocketMQ message topic;
- a non-null `Tag` becomes one literal RocketMQ message tag, while `null` publishes an untagged message; and
- `(Topic, Tag)` uniquely identifies one integration-event type within one EventBus registration.

When several event types use the same topic, startup registration combines their tags into one RocketMQ tag
subscription such as `order-submitted || order-cancelled`. The received topic and tag select the event type before the
payload is deserialized. `Tag` is publish-time event metadata; `FilterExpression` is a consumer-side value generated
from the registered tags. If any route for a topic is untagged, the consumer subscribes to that topic with `*` and the
local route table still distinguishes `null` from every literal Tag. SQL92 expressions are outside the first-release
EventBus contract.

Core deliberately keeps consumer creation behind a transport-owned callback and keeps dispatch independent from the
Push implementation. Classic Remoting POP is an internal receive engine of the same Push consumer, so it uses
`AddRemotingEventBus`, the same bridge handler, and the same `(Topic, Tag)` route table. Applications may request
Broker-owned queue assignment through `RemotingPushConsumerOptions`; each Broker assignment then determines whether
the main client receives with PULL or POP. EventBus does not configure the Broker's topic-and-group request mode and
does not own POP receipts or settlement.

A genuinely different public delivery model, such as gRPC LitePush, requires a separate adapter entry point after the
main client exposes a documented hosted-delivery abstraction. gRPC `LiteTopic` is a different routing concept and must
not be encoded into `Tag`; Lite support requires its own explicit routing contract.

### Handlers

The handler contract is asynchronous, typed, cancellation-aware, and friendly to assembly scanning:

```csharp
public interface IIntegrationEventBusHandler<in TIntegrationEvent>
    where TIntegrationEvent : IntegrationEvent
{
    Task HandleAsync(
        TIntegrationEvent integrationEvent,
        CancellationToken cancellationToken = default);
}
```

Handlers return `Task`, not `ValueTask`. Application handling usually awaits I/O, and `Task.CompletedTask` is already
allocation-free for synchronous completion. This keeps the public application contract straightforward while transport
and Core internals may still use `ValueTask` where an existing API benefits from it.

```csharp
public sealed class OrderSubmittedIntegrationEventHandler(
    OrdersDbContext dbContext,
    ILogger<OrderSubmittedIntegrationEventHandler> logger)
    : IIntegrationEventBusHandler<OrderSubmittedIntegrationEvent>
{
    public async Task HandleAsync(
        OrderSubmittedIntegrationEvent integrationEvent,
        CancellationToken cancellationToken = default)
    {
        // Application behavior belongs here.
    }
}
```

One event type may have multiple handlers. The dispatcher uses one DI scope for each delivered message,
deserializes the event once, and invokes all matching handlers sequentially in that scope. Scoped dependencies are
resolved from the same scope and disposed asynchronously after handling. A message is acknowledged only after every
handler succeeds. If a later handler fails, an earlier successful handler can run again after redelivery, which is why
handlers must be idempotent.

Core does not add an event ID or expose transport message context to application handlers. An event whose side effects
need deduplication should carry its own stable business or event identifier in the JSON payload. Broker message IDs are
useful for diagnostics but are not the application idempotency contract.

Here, an asynchronous DI scope means that the underlying Push consumer calls `CreateAsyncScope()` for each delivery
attempt. The EventBus adapter and all application handlers for that message resolve from the resulting service
provider. When dispatch finishes, `DisposeAsync()` releases scoped services that implement `IAsyncDisposable`, such as
many database contexts. Scope creation, bridge resolution, and disposal are owned by the main client; a lifecycle
exception is handled by its consumer retry path rather than mapped by EventBus. It is a resource-lifetime boundary, not
an additional queue, thread, or concurrency model.

### EventBus and serialization

Publishing follows the modern eShop asynchronous shape:

```csharp
public interface IEventBus
{
    Task PublishAsync(
        IntegrationEvent integrationEvent,
        CancellationToken cancellationToken = default);
}
```

Publishing failures use one Core exception contract:

```csharp
public sealed class EventBusPublishException : Exception
{
    public Type IntegrationEventType { get; }

    public string Topic { get; }

    public string? Tag { get; }

    public string? RegistrationName { get; }

    public string? TransportResult { get; }
}
```

The adapters create this exception; applications normally catch it rather than construct it. Serialization failures,
transport send exceptions, and a completed Remoting send with a non-success `RemotingSendStatus` are wrapped in
`EventBusPublishException`. The original exception remains in `InnerException`; a non-exception transport outcome is
recorded in `TransportResult`. Caller-requested cancellation propagates as `OperationCanceledException` and is not
wrapped. Exception properties never contain the message body.

The default serializer uses Newtonsoft.Json and UTF-8. It serializes the concrete runtime event type and uses
`TypeNameHandling.None`; the consumer supplies the known event type from the startup route table instead of trusting a
`$type` value from the payload.

Serialization is replaceable without replacing either transport adapter:

```csharp
public interface IIntegrationEventSerializer
{
    byte[] Serialize(IntegrationEvent integrationEvent);

    IntegrationEvent Deserialize(
        ReadOnlyMemory<byte> payload,
        Type integrationEventType);
}
```

The default wire format has no envelope or .NET type name. `Topic` and `Tag` are immutable transport metadata and are
excluded from the JSON body.

The complete settings, UTF-8 rules, schema-evolution policy, failure behavior, and custom serializer requirements are
defined in the [serialization design](serialization-design.md).

## Registration and assembly scanning

The API composes with the builders provided by `EventHorizon.RocketMQ` and returns an EventBus builder used for handler
registration. Producer and Push-consumer roles are both optional and are created only when their capability is used:

- a non-null `configureProducer` delegate adds one Producer and exposes `IEventBus` for that registration;
- a null `configureProducer` delegate adds no Producer, no Producer hosted service, and no `IEventBus` service;
- adding the first handler, directly or through assembly scanning, adds one Push consumer; and
- a registration with neither a Producer nor a handler adds no transport role or hosted service.

This permits publisher-only, consumer-only, and combined hosts without opening unused transport connections.

The transport entry points are:

```csharp
IEventBusBuilder AddRemotingEventBus(
    this RemotingRocketMQBuilder builder,
    Action<RemotingPushConsumerOptions>? configureConsumer = null,
    Action<RemotingProducerOptions>? configureProducer = null);

IEventBusBuilder AddGrpcEventBus(
    this GrpcRocketMQBuilder builder,
    Action<GrpcPushConsumerOptions>? configureConsumer = null,
    Action<GrpcProducerOptions>? configureProducer = null);
```

Remoting queue assignment is configured through the existing Push options rather than an EventBus mode:

```csharp
builder.Services
    .AddRocketMQRemoting(options => options.NamesrvAddr = "localhost:9876")
    .AddRemotingEventBus(
        configureConsumer: options =>
        {
            options.GroupName = "ordering-service";
            options.QueueAssignmentMode = RemotingPushQueueAssignmentMode.Broker;
        });
```

`Client`, the main-client default, performs client-side queue allocation and uses PULL. `Broker` is valid for the
EventBus clustering and concurrent-consumption contract; the Broker returns PULL or POP in each assignment according
to its administrative request-mode configuration. Both paths invoke the same EventBus handler. POP uses the main
client's fixed `PopInvisibleDuration` deadline and does not add a lease-renewal loop to EventBus.

The common one-delegate call configures the Push consumer and does not create a Producer. Producer settings such as
send timeout, retry count, message-size limit, and the Remoting producer group use the named `configureProducer`
argument. Passing even an empty non-null Producer delegate enables publishing with the main client's defaults. A
publisher-only service omits `configureConsumer` and registers no handlers:

```csharp
builder.Services
    .AddRocketMQRemoting(options => options.NamesrvAddr = "localhost:9876")
    .AddRemotingEventBus(
        configureProducer: options =>
        {
            options.GroupName = "ordering-producer";
            options.SendMsgTimeout = TimeSpan.FromSeconds(5);
        });
```

When `configureProducer` is non-null, `Add*EventBus` owns the Producer role and registers the matching unkeyed or keyed
`IEventBus`. It does not adopt a Producer independently registered through `AddGrpcProducer` or
`AddRemotingProducer`; the main client's duplicate-role validation rejects that combination during service
registration. When `configureProducer` is null, an independently configured raw Producer may coexist, but it is not
used by EventBus and no `IEventBus` publishing service is exposed for that registration.

### Named registrations

EventBus preserves the registration model of the main client. When publishing is enabled, a default RocketMQ builder
registers an unkeyed `IEventBus`, while a named builder registers `IEventBus` as a keyed service under the same
`RegistrationName`:

```csharp
builder.Services
    .AddRocketMQGrpc("orders", options =>
    {
        options.Endpoint = "http://localhost:8081";
    })
    .AddGrpcEventBus(
        configureConsumer: options =>
        {
            options.GroupName = "ordering-service";
        },
        configureProducer: static _ => { })
    .AddHandlersFromAssemblyOf<OrderingApplicationMarker>();

public sealed class OrderPublisher(
    [FromKeyedServices("orders")] IEventBus eventBus)
{
    // ...
}
```

`GetRequiredKeyedService<IEventBus>("orders")` is the equivalent programmatic resolution API. A consumer-only named
registration still uses the name to isolate its routes and handlers, but it deliberately has no keyed `IEventBus`.

The default registration identity and every string registration name must be unique across both adapters in one
service collection, whether the registration is publisher-only, consumer-only, or combined. Names use ordinal,
case-sensitive equality, matching the main client's string service keys, so `orders` and `Orders` are distinct.
Multiple named gRPC and Remoting EventBus registrations may otherwise coexist, including registrations that target
different clusters.

Each EventBus registration owns an isolated route table, handler registrations, handler lifetimes, serializer choice,
optional Producer, and optional Push consumer. Calling `UseSerializer<TSerializer>()` replaces the serializer only
for the builder on which it is called. One concrete application Handler type belongs to exactly one EventBus
registration in an `IServiceCollection`. Reusing it under another default or named registration, including through the
other protocol adapter, is rejected during service registration.

The main-client packages are external transport boundaries. EventBus adapters depend only on public Handler contracts
and behavior-oriented registration APIs. They never receive, infer, or reproduce a transport Role Key, options name,
Consumer index, or DI descriptor layout. If a necessary main-client capability is absent or defective, open an issue in
the main-client repository with the use case and boundary requirement; do not modify the sibling client repository as
part of an EventBus change.

The first successfully registered application Handler becomes an internal anchor for a consuming registration. The
adapter closes its protocol bridge type over that anchor, for example
`GrpcIntegrationEventBusHandler<OrderSubmittedIntegrationEventHandler>`, and passes the closed type to the main
client's existing public `AddGrpcPushConsumer<TMessageHandler>` or `AddRemotingPushConsumer<TMessageHandler>` API.
This type identity is entirely internal; applications continue to use `AddHandler<THandler>()` or assembly scanning
and do not declare a second marker. The one-registration-per-Handler rule guarantees that the closed bridge type maps
to exactly one Core registration.

Core adds one internal registration marker to the shared `IServiceCollection` for every `Add*EventBus` call. The marker
contains the public registration identity and a private object token. Core compares markers to reject a second default
registration or an ordinal-equal name even when the two calls came from different adapters.

The private token, not the public string name, keys every internal route table, serializer, application Handler, and
dispatch service. `AddHandler` and assembly scanning add keyed Handler descriptors under the owning token;
`UseSerializer<TSerializer>` adds a keyed singleton serializer under that same token. This model prevents unkeyed
application services or equal Handler types in other EventBus registrations from leaking into dispatch:

```text
public default/name key --> IEventBus (only when Producer enabled)
                         --> EventBus registration marker --> private token

main-client generic Handler registration --> protocol bridge<anchor Handler>
                                                    --> Core registration accessor<anchor Handler>
                                                    --> private token
                                                        --> route table
                                                        --> serializer singleton
                                                        `--> keyed application Handlers
```

The closed protocol bridge Handler always uses `ServiceLifetime.Scoped`, independently of application Handler
lifetimes. The main client therefore creates one async DI scope per delivery and resolves the bridge in it. A
Core-owned generic accessor connects the anchor type to the private registration token; the bridge resolves all keyed
application Handlers from the existing scope without creating a nested scope. `Transient`, `Scoped`, and `Singleton`
application Handler semantics are still honored inside the owning registration.

`IEventBusBuilder` exposes the registration identity alongside its service collection:

```csharp
public interface IEventBusBuilder
{
    IServiceCollection Services { get; }

    string? RegistrationName { get; }
}
```

Classic Remoting:

```csharp
var builder = Host.CreateApplicationBuilder(args);

builder.Services
    .AddRocketMQRemoting(options =>
    {
        options.NamesrvAddr = "localhost:9876";
    })
    .AddRemotingEventBus(options =>
    {
        options.GroupName = "ordering-service";
        options.MaxConcurrency = 8;
    })
    .AddHandlersFromAssemblyOf<Program>();

var app = builder.Build();
await app.RunAsync();
```

RocketMQ 5 gRPC:

```csharp
var builder = Host.CreateApplicationBuilder(args);

builder.Services
    .AddRocketMQGrpc(options =>
    {
        options.Endpoint = "http://localhost:8081";
    })
    .AddGrpcEventBus(options =>
    {
        options.GroupName = "ordering-service";
        options.MaxConcurrency = 8;
    })
    .AddHandlersFromAssemblyOf<Program>();

var app = builder.Build();
await app.RunAsync();
```

Two assembly-scanning entry points are available:

```csharp
IEventBusBuilder AddHandlersFromAssemblyOf<TMarker>(
    ServiceLifetime handlerLifetime = ServiceLifetime.Scoped);

IEventBusBuilder AddHandlersFromAssembly(
    Assembly assembly,
    ServiceLifetime handlerLifetime = ServiceLifetime.Scoped);
```

The generic method uses a marker type for convenience. `AddHandlersFromAssembly` supports assemblies obtained from
configuration, plugin discovery, or another runtime source. Both methods use the same scanning behavior and default
handler lifetime.

Assembly scanning:

1. inspect concrete, non-abstract classes that implement one or more closed constructed
   `IIntegrationEventBusHandler<TIntegrationEvent>` interfaces;
2. register each discovered handler with Microsoft DI, using `Scoped` by default;
3. construct each event type twice through its public parameterless constructor to read `Topic` and nullable `Tag`,
   then verify the route metadata is stable;
4. validate duplicate and ambiguous route registrations before the Host starts;
5. group event tags by topic, sort literal tags with ordinal comparison, use ` || ` when all routes are tagged or `*`
   when any route is untagged, and configure the underlying Push consumer subscriptions; and
6. retain a route table used to choose the registered event type for each received topic and tag.

Every deployment using the same consumer group must discover the same event routes. The underlying RocketMQ clients
require all members of one group to use the same topic set and the same generated tag-based `FilterExpression` values.

### Registration order and startup validation

Handler registration is a startup-only operation and must finish before `BuildServiceProvider()` or
`HostApplicationBuilder.Build()`. The first release does not mutate the route table after the application service
provider has been built.

`AddHandlersFromAssemblyOf` and `AddHandlersFromAssembly` perform dynamic discovery only while configuring the service
collection; they are not runtime registration APIs. Default and named EventBus registrations, direct Handlers,
assembly-scanned Handlers, serializer selection, and generated subscriptions must all be complete before Host build.
The first release provides no runtime add/remove, plugin reload, Consumer restart, or distributed subscription-change
coordination. Microsoft DI captures the service descriptors when the provider is built, so later changes to the
`IServiceCollection` do not alter that provider. At runtime, Core materializes an immutable route plan from the
provider's own EventBus registration snapshot. An `IEventBusBuilder` is a configuration object and must not be retained
as a runtime control surface.

Direct `AddHandler<THandler>()` calls preserve call order. Assembly scanning sorts discovered handler types by their
fully qualified type name using ordinal comparison before registration, so reflection enumeration order cannot change
dispatch order between processes. If business behavior depends on a particular order, handlers should be registered
directly; the first release does not expose priority metadata.

Within one EventBus registration, repeated registration of the same event-handler pair with the same lifetime is
idempotent. Assigning the same Handler type conflicting lifetimes within that registration is rejected instead of
making the result depend on registration order. Registering that Handler type with any other default or named EventBus
registration is also rejected. This ownership rule supplies a unique internal anchor for the protocol bridge and makes
cross-registration Handler intent explicit.

Registration fails before the Generic Host starts when any of these conditions is found:

- an event type is abstract, does not derive from `IntegrationEvent`, or has no public parameterless constructor;
- constructing an event fails, its `Topic` is blank, or its non-null `Tag` is blank;
- `Topic` or `Tag` is `*`, or either route value contains the `||` expression operator;
- two different event types claim the same `(Topic, Tag)` route;
- a handler type is abstract, open generic, or does not implement a closed
  `IIntegrationEventBusHandler<TIntegrationEvent>` interface;
- the same handler type is assigned conflicting service lifetimes within one EventBus registration;
- the same handler type is registered with more than one EventBus registration in the service collection;
- a consumer group is missing after at least one handler has been registered; or
- application configuration has already added manual topic subscriptions to the EventBus-owned Push consumer.

The EventBus does not trim or otherwise normalize `Topic` and non-null `Tag` values; values are compared with ordinal
case-sensitive semantics and are sent exactly as declared. A null Tag remains null and is not converted into a
literal `*`. Event constructors must therefore contain stable constants and must not perform I/O or derive routes
from process-specific state.

Sorting literal tags and deterministically selecting `*` for any topic with an untagged route makes each generated
`FilterExpression` deterministic. Reflection enumeration order or handler registration order therefore cannot make
members of the same Consumer Group send different subscription strings.

The EventBus owns all Push-consumer subscriptions. Applications may configure group name, concurrency, retry, timeout,
prefetch, and cache options, but must not call `Subscribe` inside `AddRemotingEventBus` or `AddGrpcEventBus`. The
Remoting adapter rejects broadcasting and forces `ConsumeMessageBatchSize = 1`.

### Single-handler registration

A handler can be registered directly without scanning its entire assembly:

```csharp
IEventBusBuilder AddHandler<THandler>(
    ServiceLifetime handlerLifetime = ServiceLifetime.Scoped);

eventBusBuilder.AddHandler<OrderSubmittedIntegrationEventHandler>();
```

`AddHandler<THandler>()` inspects only `THandler` and registers every closed
`IIntegrationEventBusHandler<TIntegrationEvent>` interface it implements. An implementation that handles several event
types therefore registers all of those event-handler pairs.

The method uses `Scoped` by default; callers can pass `Transient` or `Singleton` explicitly. Registering or scanning the
same event-handler pair more than once is idempotent, so it does not dispatch the same handler twice. A dedicated
trimming or Native AOT registration mechanism can be added later if there is a concrete requirement; it does not expand
this everyday API.

The Remoting adapter forces `ConsumeMessageBatchSize = 1`, so its batch-shaped transport callback hands the
EventBus exactly one message. The lower-level `PullBatchSize` and `PopBatchSize` may remain greater than one for receive
efficiency.
The gRPC Push consumer already invokes its handler once per message. Both adapters therefore deserialize and dispatch
one message per EventBus invocation.

### Custom serializer

The EventBus builder provides a transport-neutral replacement hook:

```csharp
builder.Services
    .AddRocketMQGrpc(options => options.Endpoint = "http://localhost:8081")
    .AddGrpcEventBus(options => options.GroupName = "ordering-service")
    .UseSerializer<MyIntegrationEventSerializer>()
    .AddHandlersFromAssemblyOf<Program>();
```

`MyIntegrationEventSerializer` implements `IIntegrationEventSerializer` and is resolved as a singleton through
dependency injection. Custom serializer implementations must be thread-safe because independent message deliveries can
run concurrently.

### Publishing

```csharp
public sealed class OrderApplicationService(IEventBus eventBus)
{
    public Task SubmitAsync(Guid orderId, decimal total, CancellationToken cancellationToken)
    {
        var integrationEvent = new OrderSubmittedIntegrationEvent
        {
            OrderId = orderId,
            Total = total
        };

        return eventBus.PublishAsync(integrationEvent, cancellationToken);
    }
}
```

Serialization and transport failures propagate from `PublishAsync` as `EventBusPublishException`. The Remoting adapter
must also treat any non-success `RemotingSendStatus` as a failed publish instead of silently discarding that result.

`IEventBus` is registered only when `configureProducer` was supplied for that default or named EventBus registration.
Consumer-only applications therefore fail normal DI resolution if they accidentally request a publisher, rather than
receiving an object whose `PublishAsync` can never work.

The cancellation token is optional. Cancellation stops waiting for the local send operation, but it cannot retract a
message that the Broker may already have accepted. Retrying after an ambiguous cancellation can therefore publish a
duplicate event.

## Generic Host lifecycle

`AddRemotingEventBus` and `AddGrpcEventBus` compose only the configured roles from the underlying client. Those roles
already register protocol-specific `IHostedService` implementations, so a Generic Host:

1. build and validate the event route table;
2. start the Producer when `configureProducer` was supplied;
3. start the Push consumer and its subscriptions when handlers were registered;
4. stop message reception during Host shutdown; and
5. stop and dispose transport resources in reverse order.

Applications running under Generic Host must not call the underlying client `StartAsync` or `StopAsync` methods
manually. Reusing the client's hosted services also avoids a second background loop or duplicate transport lifecycle in
the EventBus adapters.

Every default or named registration has its own optional Producer hosted service and optional Push-consumer hosted
service. A consumer-only registration never constructs or starts a Producer; a publisher-only registration never
constructs or starts a Push consumer.

## Logging

Both adapters use `Microsoft.Extensions.Logging` and emit structured logs by default. Publish and final Consumer
outcome entries include the complete message content in the structured `Payload` field as single-line JSON, except
that Consumer deserialization-failure entries omit the field.

| Event | Default level | Structured fields |
| --- | --- | --- |
| EventBus registration subscriptions materialized | `Information` | Registration name (`<default>` for the default registration), Consumer Group, handler count, subscription count, and ordinal-sorted Topic plus Tag `FilterExpression` entries |
| Publish completed | `Information` | Topic, tag, Broker message ID, duration, and `Payload` |
| Publish failed or returned a non-success result | `Error` | Topic, tag, duration, exception or transport result, and `Payload` when it can be produced |
| Consumer dispatch completed with `Success` | `Information` | Topic, tag, message ID, Broker name, queue ID, queue offset, delivery attempt, duration, outcome, and `Payload` |
| EventBus requested `Retry` after a Handler or dependency failure | `Error` | The same delivery fields, retry outcome, exception when available, and `Payload` |
| Consumer selected `DeadLetter` because the route was unknown | `Error` | The available delivery fields, outcome, and actual-body `Payload` |
| Consumer selected `DeadLetter` because deserialization failed | `Error` | The available delivery fields and outcome; no `Payload` field |

Consume timeouts and delivery-scope lifecycle failures are owned and logged by the main client. They can cause a
transport retry after the EventBus dispatch call has returned or been abandoned, so they are not reported as a new
EventBus outcome.

Logging is registration-local and defaults to enabled with payloads included:

```csharp
eventBusBuilder.ConfigureLogging(options =>
{
    options.Enabled = true;
    options.IncludePayload = false;
});
```

`Enabled = false` suppresses all EventBus logs for that registration, including the subscription summary.
`IncludePayload = false` keeps the other EventBus logs but omits the field and skips payload formatting. Neither switch
changes the underlying RocketMQ client's logs. Both settings are materialized in the service provider's immutable
registration snapshot.

Applications control verbosity with normal category filters. For example, this keeps successful gRPC EventBus
operations visible while suppressing successful Remoting EventBus operations:

```json
{
  "Logging": {
    "LogLevel": {
      "Default": "Information",
      "EventHorizon.RocketMQ.Grpc.EventBus": "Information",
      "EventHorizon.RocketMQ.Remoting.EventBus": "Warning"
    }
  }
}
```

The same filter can be configured in code:

```csharp
builder.Logging.AddFilter(
    "EventHorizon.RocketMQ.Remoting.EventBus",
    LogLevel.Warning);
```

Logger categories use the adapter namespaces, so standard prefix matching applies. Applications remain responsible for
their own handler logs. The full `Payload` field can contain credentials, personal data, or other sensitive application
content; deployments must set appropriate category filters, retention, export, and access controls.

For the default serializer, logging parses the actual UTF-8 JSON body and compacts it; it does not serialize the event a
second time. For a custom serializer, the built-in Newtonsoft.Json serializer creates the logging view when an event
object is available, while the custom serializer still exclusively controls the wire bytes. If no event object is
available because a route is unknown, EventBus logs the actual body. Non-JSON or malformed UTF-8 bodies use the JSON
wrapper `{"encoding":"base64","data":"..."}`. A Consumer deserialization failure never logs its body. If publish
serialization fails before a body exists and the logging view cannot be generated, `Payload` is null. Diagnostic
formatting and logger-provider failures never change publish or consume behavior.

The subscription summary is emitted once per EventBus registration that has a Consumer during each successful Host
start, after route validation and all local `Subscribe` calls complete. It is not emitted once per Handler. It
describes the client's effective subscription configuration; it does not claim that the Broker has acknowledged or
persisted a subscription. The entries use the same deterministic Topic ordering and ordinal-sorted tag expressions
used to configure the consumer. A registration without handlers has no Consumer and emits no subscription summary.

## Delivery and failure semantics

EventBus classifies each completed dispatch attempt with one of these internal outcomes:

| Condition | EventBus outcome |
| --- | --- |
| Route resolves, deserialization succeeds, and every handler succeeds | `Success` |
| A handler or its dependency throws | `Retry` |
| `ConsumeTimeout` elapses | `Retry`, enforced by the underlying Push consumer |
| Deserialization fails or the serializer returns an invalid event | `DeadLetter` |
| No registered event matches the received topic and tag | `DeadLetter` |
| Host shutdown cancels the delivery | Propagate cancellation; do not force a new result |

The protocol adapter maps this classification to its main client's result. Remoting preserves all three values. gRPC
maps both `Retry` and `DeadLetter` to `Failure`; the service moves the message to DLQ only after the consumer group's
retry limit. The [`ConsumeResult` handling design](consume-result-design.md) defines the complete mapping.

Neither adapter provides exactly-once delivery. A retry can overlap a handler that ignored cancellation, and dispatch
to multiple handlers can repeat handlers that already completed. Consumers must make side effects idempotent.

The complete decision order, exception boundaries, multiple-handler behavior, transport settlement, and default log
levels are specified in the [`ConsumeResult` handling design](consume-result-design.md).

## Testing, environments, and samples

The repository includes unit tests, Docker-backed integration tests, runnable Consumer and Web API Publisher samples, and a manual
multi-Broker environment. Integration tests and the manual environment are intentionally independent:

- `tests/it/EventHorizon.RocketMQ.EventBus.IntegrationTestInfrastructure` creates disposable three-Broker topologies
  with Testcontainers and dynamic ports;
- each protocol integration project owns its protocol-specific fixture lifetime and test assertions; and
- `test-environments/rocketmq-multi-broker/compose.yaml` is a separate fixed-port environment for samples, manual
  validation, and issue reproduction.

Both integration fixtures use one NameServer and three independent master Brokers. The gRPC fixture also starts a
cluster-mode Proxy and exposes the Proxy endpoint; its Brokers advertise Docker-network aliases. The Remoting fixture
exposes NameServer and every Broker directly to the test process with host-reachable advertised addresses. Keeping the
fixtures separate avoids pretending those incompatible address models are one topology.

Unit tests cover Core and each adapter without Docker. Compatibility tests reference all three production projects and
verify API symmetry, independent transport enums and mappings, Core-owned generic registration-accessor boundary
isolation, and default/named DI behavior. Each integration suite starts a Producer and Push Consumer in a Generic Host,
concurrently publishes twelve tagged and twelve untagged events, verifies the matching Handler observes each event
exactly once, and confirms all three Brokers stored messages. The Remoting suite also uses a separate Topic and group
to verify Broker-assigned POP through successful `ack` settlement activities; its original workflow retains the
default client-assigned PULL path. Unit tests own the remaining deterministic result-mapping, retry, dead-letter,
named-registration, and lifecycle branches.

Samples mirror the main repository's protocol-first layout. Each adapter has a Web API Publisher and a Generic Host
Consumer sample, so the absence of the unused transport role is visible. Default and `orders` named registrations live
inside each protocol sample, demonstrating keyed `IEventBus` resolution without merging routes, serializers, handlers,
or lifecycles.

The complete project matrix, topology ownership, CI lifecycle, and sample requirements are defined in the
[testing design](testing-design.md).

## Repository layout

The repository uses the following layout:

```text
.
|-- .github/workflows/
|   |-- dotnet-build.yml
|   `-- publish.yml
|-- docs/
|   |-- en-US/
|   |   |-- README.md
|   |   |-- consume-result-design.md
|   |   |-- event-bus-design.md
|   |   |-- serialization-design.md
|   |   `-- testing-design.md
|   `-- zh-CN/
|       |-- README.md
|       |-- consume-result-design.md
|       |-- event-bus-design.md
|       |-- serialization-design.md
|       `-- testing-design.md
|-- samples/
|   |-- README.md
|   |-- README.zh-CN.md
|   |-- grpc/
|   |   |-- Consumer/
|   |   `-- Publisher/
|   `-- remoting/
|       |-- Consumer/
|       `-- Publisher/
|-- src/
|   |-- EventHorizon.RocketMQ.EventBus/
|   |-- EventHorizon.RocketMQ.Grpc.EventBus/
|   `-- EventHorizon.RocketMQ.Remoting.EventBus/
|-- test-environments/
|   |-- README.md
|   |-- README.zh-CN.md
|   `-- rocketmq-multi-broker/
|       |-- compose.yaml
|       |-- README.md
|       `-- README.zh-CN.md
|-- tests/
|   |-- it/
|   |   |-- README.md
|   |   |-- README.zh-CN.md
|   |   |-- EventHorizon.RocketMQ.EventBus.IntegrationTestInfrastructure/
|   |   |-- EventHorizon.RocketMQ.Grpc.EventBus.IntegrationTests/
|   |   `-- EventHorizon.RocketMQ.Remoting.EventBus.IntegrationTests/
|   `-- ut/
|       |-- EventHorizon.RocketMQ.EventBus.Tests/
|       |-- EventHorizon.RocketMQ.EventBus.Compatibility.Tests/
|       |-- EventHorizon.RocketMQ.Grpc.EventBus.Tests/
|       `-- EventHorizon.RocketMQ.Remoting.EventBus.Tests/
|-- .editorconfig
|-- .gitignore
|-- AGENTS.md
|-- EventHorizon.RocketMQ.EventBus.slnx
|-- global.json
|-- LICENSE
|-- README.md
|-- README.zh-CN.md
`-- codecov.yml
```

The repository follows the main client's repository conventions: C# 12, nullable reference types, XML
documentation for public APIs, xUnit v3 unit tests, Docker-backed integration tests for both transports, runnable
Consumer and Web API Publisher samples, format/build/test CI, package publishing, symbols, and bilingual package
READMEs. The gRPC
package README describes its Proxy-only connection path and client-initiated long polling. The Remoting package README
describes NameServer route discovery, direct connections to advertised Brokers, client-initiated PULL/POP long polling,
and the clustering-only consumption model. The Core package README remains transport-neutral and links to both adapters.
This repository uses the MIT License rather than the main client's Apache-2.0 license.

## Design decisions

1. Each `(Topic, Tag)` pair, including a null Tag for untagged messages, maps to exactly one event type. Multiple
   handlers for that event type are supported and run sequentially.
2. `Topic` and nullable `Tag` are immutable transport metadata reconstructed by the event's public parameterless
   constructor; they are not included in the JSON payload. A topic with any untagged route uses a `*` consumer filter.
3. Every concrete integration-event type has a public parameterless constructor. Registration uses it to discover the
   route without attributes, static abstract members, or application services.
4. Handler failures produce the internal `Retry` outcome. Deserialization failures and unknown routes produce the
   internal `DeadLetter` outcome. Remoting can request immediate DLQ delivery; gRPC maps both failure outcomes to
   `Failure` and relies on the service-side retry/DLQ threshold.
5. The EventBus deserializes and dispatches one message per invocation while retaining configurable transport prefetch
   and consumer concurrency. Remoting handler batches are fixed at one.
6. Public names use `EventHorizon.RocketMQ.EventBus`, `IntegrationEvent`, and
   `IIntegrationEventBusHandler<TIntegrationEvent>`.
7. Handler registration defaults to `Scoped`; the optional `ServiceLifetime` parameter also accepts `Singleton` or
   `Transient`. Singleton handlers must be thread-safe.
8. `Add*EventBus` registers a Producer and exposes `IEventBus` only when `configureProducer` is non-null. It adds a Push
   consumer after the first handler registration. Default publishing registrations are unkeyed; named publishing
   registrations expose keyed `IEventBus`. All registrations isolate routes, handlers, lifetimes, serializer, and any
   configured transport roles.
9. All three NuGet packages are listed and use one version and one release tag. Core is the adapters' shared support
   package rather than the recommended user entry point. It is pushed first because both adapters declare it as a
   same-version dependency; adapters are pushed immediately afterward.
10. Classic Remoting EventBus consumption uses clustering only; broadcasting is outside the first release. Its Push
    consumer may use client-owned PULL assignments or Broker-owned PULL/POP assignments without changing EventBus APIs.
11. Both adapters log publish and consume outcomes by default through Microsoft logging. Publish and final Consumer
    outcomes include the full `Payload` as a JSON-formatted structured field; category filters control this potentially
    sensitive output.
12. Both protocol IT suites use disposable three-Broker Testcontainers fixtures. The fixed-port multi-Broker Compose
    environment is independent and exists for samples, manual validation, and reproduction.
13. Each EventBus registration with a successfully started Consumer logs one aggregated `Information` subscription
    summary, not one log per Handler. It contains the complete deterministic Topic and Tag-expression list.
14. Publish failures use the Core `EventBusPublishException`; caller cancellation remains an unwrapped
    `OperationCanceledException`.
15. Each consuming registration closes its protocol bridge type over its first owned application Handler. A Handler
    type cannot belong to another EventBus registration; no public marker or main-client internal identity is used.
16. Assembly scanning is startup-time discovery, not runtime registration. The route plan, Handlers, serializer, and
    subscriptions materialized from one service provider's registration snapshot remain immutable for that provider.

## License

This repository is licensed under the [MIT License](https://opensource.org/license/mit).
