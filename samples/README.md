# EventHorizon.RocketMQ.EventBus samples

[English](README.md) | [Simplified Chinese](README.zh-CN.md)

The Publisher samples are minimal Web APIs; the Consumer samples are .NET Generic Host applications. The underlying
RocketMQ Producer and Push-consumer roles are started and stopped by their existing `IHostedService` registrations;
sample code does not call transport `StartAsync` or `StopAsync` methods.

## Sample projects

| Sample | Workflow |
| --- | --- |
| `grpc/Publisher` | Web API publishing through a RocketMQ 5 Proxy with default and `orders` keyed EventBus registrations |
| `grpc/Consumer` | gRPC client-initiated Push consumption with default and `orders` registrations |
| `remoting/Publisher` | Web API publishing after NameServer route discovery and direct Broker selection, with default and `orders` registrations |
| `remoting/Consumer` | clustered Remoting Push consumption with one-message dispatch and default plus `orders` registrations |

Publisher samples pass a non-null `configureProducer` delegate and therefore expose `IEventBus`. Consumer samples omit
that delegate; they create a Push consumer only after registering the first Handler and cannot resolve `IEventBus`.

Each Publisher uses `[FromKeyedServices]` with the `orders` registration name. Each Consumer has a matching named
consumer group and a separately registered Handler. Every registration owns its routes, Handler lifetimes, serializer,
optional Producer, optional Consumer, and hosted lifecycle. A concrete Handler type is not shared between them.
The named order event uses `eventbus-orders` with the literal `order-submitted-named` Tag, so it does not also reach
the default `order-submitted` Consumer. Registration names are not RocketMQ routing metadata.

Every project owns its own event contracts, as an application normally would. The Publisher exposes endpoints for
`OrderSubmittedIntegrationEvent`, whose route has a literal Tag, and `InventorySnapshotIntegrationEvent`, whose Tag
is null. The matching Consumer directly registers Handlers for both event shapes. An untagged route generates a `*`
subscription for its topic while local dispatch still matches `(Topic, null)` exactly.

## Local RocketMQ

The runnable defaults target the independent
[`test-environments/rocketmq-multi-broker`](../test-environments/rocketmq-multi-broker/README.md) Compose stack. gRPC
projects connect to its Proxy endpoint. Remoting projects query its NameServer and then connect directly to every
advertised Broker endpoint.

Each runnable project contains `appsettings.json`, `README.md`, and `README.zh-CN.md`. Its README gives the exact run
command, protocol topology, configuration defaults, and which optional EventBus roles are present.

## Scope

Samples demonstrate ordinary event publishing, Push consumption, direct Handler registration, structured logging,
and named DI. They do not demonstrate Pull, Simple, POP, LitePush,
FIFO, transactional, delay, priority, batch, request-reply, SQL92, or runtime subscription changes because those APIs
are outside the first-release EventBus contract.
