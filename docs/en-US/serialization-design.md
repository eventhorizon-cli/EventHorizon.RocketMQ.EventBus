# Serialization design

[Documentation](README.md) | [简体中文](../zh-CN/serialization-design.md) |
[EventBus design](event-bus-design.md)

This document defines the default JSON wire contract shared by the gRPC and Remoting EventBus adapters. Both adapters
must produce and consume identical message-body bytes for the same integration event.

## Wire format

The default `NewtonsoftJsonIntegrationEventSerializer` serializes the concrete runtime event type to compact JSON and
encodes it as UTF-8 without a byte-order mark. Deserialization uses strict UTF-8 decoding; malformed byte sequences are
an invalid payload and produce `DeadLetter`.

The message body contains only application event data:

```json
{"OrderId":"353c0bcb-7f6d-49a2-8dd7-d144bee5366a","Total":128.50}
```

`Topic` and nullable `Tag` are carried by RocketMQ transport metadata and are excluded through `[JsonIgnore]`. A null
Tag means the published message has no Tag; it is not written into the payload or converted into a literal `*`. The
default format has no envelope, route, assembly-qualified type name, or `$type` discriminator. The registered
`(Topic, Tag)` route is the only source of the destination event type.

## Fixed Newtonsoft.Json behavior

Core creates and owns its serializer settings. It does not read or mutate `JsonConvert.DefaultSettings`. The effective
default behavior is fixed as follows:

| Concern | Default contract |
| --- | --- |
| Type metadata | `TypeNameHandling.None`; payload metadata never selects a .NET type |
| Metadata properties | Ignored as metadata; reference/type metadata is not interpreted |
| Property naming | `DefaultContractResolver`; declared .NET member names are emitted without camel-case conversion |
| Formatting | Compact JSON with `Formatting.None` |
| Text encoding | Strict UTF-8 without BOM |
| Null members | Included |
| Default-valued members | Included |
| Unknown JSON members | Ignored during deserialization |
| Missing JSON members | Leave the .NET member at its constructor/default value |
| Dates | ISO 8601 with round-trip date/time-zone behavior |
| Culture | `InvariantCulture` for culture-sensitive conversions |
| Enums | Newtonsoft.Json's numeric representation unless the event member/type declares its own converter |
| Object references | No reference preservation; reference loops fail serialization |
| Maximum read depth | 64 levels during deserialization |
| Custom converters | None registered globally by EventBus |

These values are part of the first-release wire contract, not incidental process defaults. Changing one requires a
compatibility review, bilingual documentation, and golden-payload tests.

## Object construction and validation

The route table supplies the exact registered event type. Newtonsoft.Json invokes its public parameterless constructor,
which reconstructs `Topic` and nullable `Tag`, and then populates application members. The serializer result must be
non-null and must have exactly the requested registered event type. A different or unrelated type is invalid even if
assignable.

UTF-8 decoding, JSON parsing, constructor execution, member conversion, maximum-read-depth checks, and returned-type
checks all belong to the deserialization phase. Any failure returns `DeadLetter` before an application handler is
invoked, as defined by the [`ConsumeResult` handling design](consume-result-design.md).

On publish, serialization failures are logged at `Error`, wrapped in `EventBusPublishException`, and propagated from
`PublishAsync`; no transport send is attempted. Newtonsoft.Json does not expose a matching `JsonSerializerSettings`
write-depth limit. EventBus therefore does not claim one: reference loops fail, and the selected transport's configured
maximum message size remains the final payload-size boundary.

## Schema evolution

The default settings support additive evolution, not arbitrary schema changes:

- adding an optional member is compatible with older payloads because the member retains its default value;
- removing a member is compatible for readers because unknown JSON members are ignored;
- changing a member name is breaking unless `[JsonProperty]` preserves the old wire name;
- changing a member's JSON shape or incompatible .NET type is breaking;
- changing numeric range or enum representation can be breaking;
- adding a required validation rule can make previously valid payloads invalid; and
- changing `(Topic, Tag)`, including changing between a literal Tag and null, creates a different route and must be
  treated as a messaging-contract migration.

EventBus does not provide a schema registry or built-in schema-version field. An application that needs explicit
version negotiation should add a normal event property such as `SchemaVersion` and keep old readers/writers compatible
during rollout.

Event types shared between services should live in an application-owned contract package that references only
`EventHorizon.RocketMQ.EventBus`. Contract packages should keep golden JSON examples for externally important events.

## Custom serializer

Applications replace both serialization and deserialization by implementing `IIntegrationEventSerializer` and calling
`UseSerializer<TSerializer>()`. The replacement is registered as a private-token-keyed singleton for that EventBus
registration and must be:

- thread-safe under concurrent publish and consume operations;
- deterministic for the same event contract;
- side-effect free and independent of transient external services;
- strict about malformed input and returned event type; and
- able to read every payload written by the corresponding producers during a rolling deployment.

The EventBus still obtains routing exclusively from transport `Topic` and `Tag`; a custom payload cannot override the
registered route. Custom serializers may choose another body format, but every producer and consumer for that route
must deploy compatible implementations together.

Using the same serializer type in two default or named EventBus registrations creates two independent singleton
instances. Dependencies and mutable state therefore remain registration-local; thread safety is still required within
each instance because its registration can publish and consume concurrently.

## Logging representation

The structured `Payload` field is a diagnostic view, not a second wire contract. When the default serializer is active,
EventBus parses and compacts the actual UTF-8 JSON body without serializing the event again. When a custom serializer is
active and an event object is available, EventBus uses its built-in Newtonsoft.Json serializer to produce a readable
single-line JSON view; the custom serializer remains the only writer and reader of the transport bytes.

For an unknown route, logging uses the actual body. Valid JSON is compacted, while non-JSON or malformed UTF-8 is
represented as `{"encoding":"base64","data":"..."}`. A Consumer deserialization failure omits the body entirely. If
publish serialization fails before producing any body and a JSON view cannot be generated, `Payload` is null.
Diagnostic serialization failure falls back to the wire body and never changes the publish or consume result.

`EventBusLoggingOptions.Enabled` and `IncludePayload` both default to `true` and are configured per registration through
`ConfigureLogging`. Disabling payload inclusion skips diagnostic serialization and removes the structured field.

The complete `Payload` can expose credentials, personal data, or other sensitive application content. Applications
must treat EventBus logs as message-data storage and configure category filters, retention, export, and access controls
accordingly.

## Compatibility tests

Core unit tests use fixed payload fixtures to verify property names, compact UTF-8 bytes, null/default handling,
missing and additional fields, maximum read depth, ignored type metadata, process-default isolation, and the exclusion
of `Topic` and `Tag`.

Each adapter's unit suite verifies its default JSON publish and consume paths independently. Those suites also verify
that custom serializer replacement controls transport bytes in both directions while the log field uses the built-in
Newtonsoft.Json diagnostic view. Cross-package compatibility tests cover the adapters' public boundary symmetry and
their independently owned transport result types.
