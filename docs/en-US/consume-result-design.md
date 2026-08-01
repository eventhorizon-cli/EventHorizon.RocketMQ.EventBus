# ConsumeResult handling design

[Documentation](README.md) | [Simplified Chinese](../zh-CN/consume-result-design.md) |
[EventBus design](event-bus-design.md)

This document defines how the EventBus adapters choose the transport-level `ConsumeResult` for one delivered
RocketMQ message. Application handlers do not return `ConsumeResult`; they return `Task`. The EventBus combines route
resolution, deserialization, and all handler executions into one final result.

Both transport packages define equivalent values:

- `EventHorizon.RocketMQ.Grpc.Consumer.ConsumeResult`
- `EventHorizon.RocketMQ.Remoting.Consumer.ConsumeResult`

The adapter returns the value from its own transport package. The decision rules are identical for gRPC and Remoting.

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

The internal outcome has the same three semantic states, but it is not a public application contract. Each adapter uses
an explicit `switch` to map those states to its own transport enum. It must not cast by numeric value, because the two
main-client packages can evolve independently. Adapter unit tests verify every mapping so a future enum change cannot
silently alter behavior.

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
| No registered route matches the received `(Topic, Tag)` | `DeadLetter` | Repeating the same message cannot create a missing startup registration |
| The payload cannot be deserialized into the event type selected by the route | `DeadLetter` | The payload is invalid for that route and retry cannot repair it |
| The custom serializer returns `null`, returns a different event type, or otherwise violates the serializer contract | `DeadLetter` | The adapter treats this as an invalid payload/serializer result |
| A route resolves but has no dispatchable handler because the internal registration state is inconsistent | `DeadLetter` | This is a non-transient configuration defect; it is also logged as an error |
| The underlying consumer is stopping and cancels the delivery operation | No adapter result is forced | Cancellation propagates to the consumer so it can perform its normal shutdown and settlement behavior |

The EventBus never returns `Success` after catching an exception. It also does not retry unknown routes or malformed
payloads, because those failures are deterministic for the current deployment.

## Processing flow

For each delivered message, the adapter follows this order:

```text
Receive one message
        |
        v
Look up (Topic, Tag) -------------------- missing ------> DeadLetter
        |
       found
        v
Deserialize once ------------------------ invalid ------> DeadLetter
        |
      valid
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
not convert it into a new `Retry` or `DeadLetter` decision. It propagates cancellation and lets the underlying consumer
stop reception and preserve its protocol-specific settlement behavior.

## Deserialization failures

Deserialization covers UTF-8 decoding, JSON parsing, object creation, member conversion, and validation of the returned
event instance. A failure in any of these steps returns `DeadLetter` without invoking an application handler.

This rule also applies to a custom `IIntegrationEventSerializer`. Implementations are expected to be deterministic,
side-effect free, and thread-safe. A serializer that depends on a transient external service is outside the intended
contract; the EventBus cannot reliably distinguish that failure from an invalid payload.

## Transport settlement

`ConsumeResult` expresses the EventBus decision; the underlying client performs the actual Broker operation:

| EventBus result | gRPC Push consumer | Remoting Push consumer |
| --- | --- | --- |
| `Success` | Acknowledges the message | Commits the singleton message |
| `Retry` | Changes invisibility so the message can be redelivered; the Broker retry policy remains authoritative | Sends the singleton message back for delayed redelivery |
| `DeadLetter` | Forwards the message to the dead-letter queue | Sends the singleton message directly to the dead-letter queue |

The Remoting EventBus fixes `ConsumeMessageBatchSize` to `1`, so batch-wide `ConsumeResult` and `AckIndex` rules never
create partial EventBus outcomes. Network prefetch may still retrieve more than one message, but each message is passed
to EventBus dispatch separately.

When the delivery attempt has reached the transport's configured maximum, the underlying consumer may move a `Retry`
result to the dead-letter queue. This does not change the adapter's decision: EventBus still reports and logs `Retry`,
while the transport owns the final retry/DLQ threshold.

If acknowledgement, retry scheduling, or dead-letter forwarding fails, the underlying client may redeliver the message.
A returned `Success` therefore does not provide exactly-once delivery.

## Logging

The adapter records the selected result with structured fields, including the complete JSON-formatted `Payload`:

| Result | Default level | Additional data |
| --- | --- | --- |
| `Success` | `Information` | Topic, tag, message ID, Broker name, queue ID, queue offset, delivery attempt, duration, and `Payload` |
| EventBus `Retry` | `Error` | The same delivery fields plus `Payload` and the Handler or dependency exception when available |
| `DeadLetter`, unknown route | `Error` | The available delivery fields, outcome, and actual-body `Payload` |
| `DeadLetter`, deserialization failure | `Error` | The available delivery fields and outcome; no `Payload` field |

Applications can change these effective levels through normal `Microsoft.Extensions.Logging` category filters. The
adapter namespaces are the category prefixes.

For custom serializers, a successfully deserialized event is rendered with the built-in Newtonsoft.Json serializer for
logging. An unknown route logs the actual body, using a Base64 JSON wrapper for non-JSON bytes. A deserialization failure
always omits the message body. The complete field can contain sensitive data, so applications must configure
`EventBusLoggingOptions`, category filters, retention, and log access accordingly.

An EventBus-selected `Retry` is an error in the first-release API because application handlers cannot request it
explicitly; EventBus selects it only after a Handler or dependency failure. Consume timeout, delivery-scope lifecycle
failure, and recoverable transport settlement failures belong to the underlying RocketMQ client and follow that
client's logging categories and levels. The EventBus outcome log covers its dispatch call; if later scope disposal
fails, the main client's error and retry handling describe the final delivery disposition. Normal Host-shutdown
cancellation does not produce an EventBus `Retry` log.
