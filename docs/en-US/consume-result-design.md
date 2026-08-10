# ConsumeResult handling design

[Documentation](README.md) | [简体中文](../zh-CN/consume-result-design.md) |
[EventBus design](event-bus-design.md)

This document defines how the EventBus adapters classify one delivered RocketMQ message and map that classification to
the transport-level `ConsumeResult`. Application handlers do not return `ConsumeResult`; they return `Task`. EventBus
combines route resolution, deserialization, and all handler executions into one internal outcome.

The transport packages intentionally expose different result contracts:

- `EventHorizon.RocketMQ.Grpc.Consumer.ConsumeResult` (gRPC client [`grpc-v0.4.1`](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/grpc-v0.4.1/src/EventHorizon.RocketMQ.Grpc/Consumer/ConsumeResult.cs)) is a sealed record with `Success` and
  `Failure` results plus the `Suspend(TimeSpan)` factory. EventBus uses the regular Push contract and emits only
  `Success` or `Failure`; `Suspend` is a LitePush capability and is never emitted by EventBus.
- `EventHorizon.RocketMQ.Remoting.Consumer.ConsumeResult` (Remoting client [`remoting-v0.6.1`](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/remoting-v0.6.1/src/EventHorizon.RocketMQ.Remoting/Consumer/ConsumeResult.cs)) is an enum with only `Success` and
  `Retry`. EventBus uses normal retry with the default delay and never requests direct dead-letter settlement.

EventBus applies the same route, payload, and handler classification for both protocols, then maps that internal
decision to the result supported by the owning transport. The wire-level disposition is therefore not identical.

## Package boundary

The two `ConsumeResult` declarations are intentionally separate .NET types. Neither is referenced by the public
EventBus abstractions:

```text
Application handler: Task
           |
           v
Internal transport-neutral dispatch outcome
           |
           +-----------------------------+
           |                             |
           v                             v
gRPC adapter switch              Remoting adapter switch
           |                             |
           v                             v
Grpc.Consumer.ConsumeResult      Remoting.Consumer.ConsumeResult
```

The internal outcome has two states, `Success` and `Retry`, and is not a public application contract. Each adapter uses
an explicit `switch` to map those states to its own transport result. A deserialization failure may still produce
`Success` when `SkipDeserializationFailures` is enabled; the accompanying `DeserializationFailed` diagnostic flag lets
logging distinguish that acknowledgement decision from successful application handling. gRPC maps `Retry` to
`Failure`; Remoting maps it to `ConsumeResult.Retry` and leaves
`RemotingPushConsumeContext.DelayLevelWhenNextConsume` at its default `0`. The adapters must not cast by numeric value;
unit tests lock the result mapping so independent main-client evolution cannot silently alter behavior.

This keeps `EventHorizon.RocketMQ.EventBus` free of gRPC and Remoting references and avoids a dependency from either
adapter to the other. An application may reference both packages without creating a type collision inside the already
compiled adapters. Application code that directly imports both transport consumer namespaces must qualify or alias
`ConsumeResult`, just as it would without EventBus.

Default and named EventBus registrations may coexist. A Producer-enabled default registration exposes unkeyed
`IEventBus`; a Producer-enabled named registration exposes keyed `IEventBus` under the main client's registration
name. Consumer-only registrations expose no `IEventBus`, but their result mapping, routes, and handlers remain isolated
by the same registration identity. This remains unrelated to the two transport-specific `ConsumeResult` type
identities.

## Decision table

| Processing condition | Effective result or disposition | Reason |
| --- | --- | --- |
| The route exists, deserialization succeeds, and every registered handler completes successfully | `Success` | The message has been fully processed and can be acknowledged |
| Resolving or running an application handler fails | `Retry` | EventBus treats the application failure as transient and returns its internal retry outcome |
| The main client cannot create the delivery scope, resolve the protocol bridge, or asynchronously dispose the scope | The underlying consumer retries; EventBus returns no result for that failed invocation | The main client owns the delivery scope and maps lifecycle exceptions to its transport retry behavior |
| Handler execution does not finish before the underlying consumer's `ConsumeTimeout` | The underlying consumer retries; any later EventBus result is ignored | Timeout enforcement and settlement belong to the main client |
| No registered route matches the received `(Topic, Tag)` | `Retry` | EventBus no longer requests direct DLQ; the transport applies normal retry and terminal policy |
| Deserialization fails and `SkipDeserializationFailures = true` (default) | `Success` with `DeserializationFailed = true` | Log the malformed delivery and acknowledge it without invoking an application handler |
| Deserialization fails and `SkipDeserializationFailures = false` | `Retry` with `DeserializationFailed = true` | Re-run deserialization on normal transport redelivery without requesting direct DLQ |
| The custom serializer returns `null`, returns a different event type, or otherwise violates the serializer contract | Apply `SkipDeserializationFailures` | Custom serializer failures use the same registration-local policy as the default serializer |
| A route resolves but has no dispatchable handler because the internal registration state is inconsistent | `Retry` | The configuration defect is logged, and EventBus requests normal retry rather than direct DLQ |
| The underlying consumer is stopping and cancels the delivery operation | No adapter result is forced | Cancellation propagates to the consumer so it can perform its normal shutdown and settlement behavior |

`Success` after a deserialization exception is reserved for the explicit default skip policy. It never means that an
application handler ran. Every non-skipped failure becomes ordinary `Retry` on both adapters, and EventBus never
requests direct DLQ placement. The underlying client or service remains free to apply its configured terminal policy
after normal retry progression.

## Processing flow

For each delivered message, the adapter follows this order:

```text
Receive one message
        |
        v
Look up (Topic, Tag) -------------------- missing ------> Retry
        |
       found
        v
Deserialize once ------------------------ invalid ------> Success when skipping (default)
        |                                      |
      valid                                    +-------> Retry when skipping is disabled
        |
        v
Resolve and run handlers sequentially --- exception ----> Retry
        |
 all completed
        v
      Success
```

Route lookup happens before deserialization. The payload never supplies a .NET type name, and the adapter never uses a
payload `$type` value to choose the destination type.

## Multiple handlers

All handlers registered for the selected event type run sequentially in registration order and in the same DI
scope. The result is `Success` only when every handler completes successfully.

If handler 1 succeeds and handler 2 fails, the adapter returns `Retry` for the entire message. On redelivery, handler 1
runs again before handler 2. EventBus does not persist a per-handler checkpoint, so every handler must make its side
effects idempotent.

The first failure stops the current dispatch. Later handlers are not invoked during that attempt.

## Exceptions and cancellation

The following failures are treated by EventBus as handler failures and result in `Retry`:

- a handler constructor or dependency resolution throws;
- `HandleAsync` throws synchronously;
- the returned `Task` faults; or
- `HandleAsync` observes the delivery token and throws `OperationCanceledException` while the consumer is still
  processing the delivery.

The underlying main client creates, resolves, and asynchronously disposes the delivery scope around the EventBus
protocol bridge. If any of those lifecycle operations throws, the bridge does not produce a usable EventBus outcome;
the main consumer catches the exception and applies its transport retry behavior. EventBus does not create a nested
scope and cannot observe a disposal failure after its dispatch call has completed.

`ConsumeTimeout` is enforced by the underlying Push consumer. When it expires, that consumer requests cancellation,
ignores any later successful result, and settles the message for retry. The EventBus cannot forcibly stop handler code.
A handler that ignores cancellation may overlap the redelivered invocation.

Host shutdown is different from a consume timeout. When the consumer's shutdown token is canceled, the adapter does
not convert it into a new `Retry` or `Success` decision. It propagates cancellation and lets the underlying consumer
stop reception and preserve its protocol-specific settlement behavior.

## Deserialization failures

Deserialization covers UTF-8 decoding, JSON parsing, object creation, member conversion, and validation of the returned
event instance. A failure in any of these steps never invokes an application handler and follows the matching adapter-owned
consumer wrapper's `SkipDeserializationFailures` policy. `GrpcEventBusConsumerOptions` and
`RemotingEventBusConsumerOptions` both default this property to `true`:

```csharp
rocketMQBuilder.AddGrpcEventBus(configureConsumer: options =>
{
    options.SkipDeserializationFailures = false;
});
```

The Remoting adapter uses the same property through `AddRemotingEventBus(configureConsumer: ...)`.

| `SkipDeserializationFailures` | Internal result | Log | Transport disposition |
| --- | --- | --- | --- |
| `true` (default) | `Success` with `DeserializationFailed = true` | `Error`; explicitly says the payload was skipped and omits `Payload` | Acknowledge/commit the delivery; no handler runs |
| `false` | `Retry` with `DeserializationFailed = true` | `Error`; explicitly says retry was requested and omits `Payload` | Request normal redelivery; EventBus does not request direct DLQ |

The wrapper option is snapshotted independently for each default or named EventBus registration. An acknowledgement can
still fail or become indeterminate, so a skipped message may be redelivered under the transport's at-least-once contract.

This rule also applies to a custom `IIntegrationEventSerializer`. Implementations are expected to be deterministic,
side-effect free, and thread-safe. A serializer that depends on a transient external service is outside the intended
contract; the EventBus cannot reliably distinguish that failure from an invalid payload.

## Transport settlement

`ConsumeResult` is the adapter-to-client settlement signal; the underlying client performs the actual Broker operation:

| Internal EventBus outcome | gRPC Push consumer | Remoting Push consumer |
| --- | --- | --- |
| `Success` | Returns `Success` and acknowledges the message | Returns `Success` and commits the singleton message |
| `Retry` | Returns `Failure`; the main client schedules redelivery and the service-side retry policy remains authoritative | Returns `Retry` and keeps the context delay at its default `0`, selecting normal delayed redelivery |

The mapping is deliberately uniform at the EventBus boundary. It does not call a direct forwarding operation, set a
Remoting negative-delay sentinel, or expose a SimpleConsumer-style settlement API. A failure that is not skipped always
enters the owning transport's normal retry path.

The Remoting EventBus fixes `ConsumeMessageBatchSize` to `1`, so batch-wide `ConsumeResult` and `AckIndex` rules never
create partial EventBus outcomes. Network prefetch may still retrieve more than one message, but each message is passed
to EventBus dispatch separately.

When the delivery attempt reaches the transport's configured maximum, the underlying client or service may move a
failed delivery to DLQ. This does not change the EventBus classification or log: the transport owns the final retry and
DLQ threshold.

If acknowledgement or retry scheduling fails, the underlying client may redeliver the message. A returned `Success`,
including a deserialization skip, therefore does not provide exactly-once delivery.

## Logging

The adapter records the selected internal outcome with structured fields, including the complete JSON-formatted `Payload`:

| Internal outcome | Default level | Additional data |
| --- | --- | --- |
| `Success` | `Information` | Topic, tag, message ID, Broker name, queue ID, queue offset, delivery attempt, duration, and `Payload` |
| `Retry` | `Error` | The same delivery fields plus `Payload` and the Handler or dependency exception when available |
| `Retry`, unknown route or invalid registration state | `Error` | The available delivery fields, retry outcome, and actual-body `Payload` |
| `Success`, skipped deserialization failure | `Error` | The available delivery fields, `Success` outcome, and explicit skip action; no `Payload` field |
| `Retry`, deserialization failure | `Error` | The available delivery fields, `Retry` outcome, and explicit retry action; no `Payload` field |

Applications can change these effective levels through normal `Microsoft.Extensions.Logging` category filters. The
adapter namespaces are the category prefixes.

For custom serializers, a successfully deserialized event is rendered with the built-in Newtonsoft.Json serializer for
logging. An unknown route logs the actual body, using a Base64 JSON wrapper for non-JSON bytes. A deserialization failure
always omits the message body. The complete field can contain sensitive data, so applications must configure
`EventBusLoggingOptions`, category filters, retention, and log access accordingly.

An EventBus-selected `Retry` is an error because application handlers cannot request it explicitly; EventBus selects it
after a Handler or dependency failure, an unknown route, invalid registration state, or a deserialization failure when
skipping is disabled. A skipped deserialization failure also remains an `Error` log even though its settlement result is
`Success`; the log states that no application handler ran. Consume timeout, delivery-scope lifecycle failure, and
recoverable transport settlement failures belong to the underlying RocketMQ client and follow that client's logging
categories and levels. The EventBus outcome log covers its dispatch call; if later scope disposal fails, the main
client's error and retry handling describe the final delivery disposition. Normal Host-shutdown cancellation does not
produce an EventBus `Retry` log.
