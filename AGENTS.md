# AGENTS.md

You are an AI coding assistant for this repository.

## Scope and working style

- These instructions apply repository-wide unless a more specific `AGENTS.md` exists below the target path.
- Follow [`.editorconfig`](.editorconfig) and nearby code before introducing a new style or abstraction.
- Keep changes focused, maintainable, and production-ready. Preserve unrelated user changes.
- Once project scaffolding is added, use the SDK selected by `global.json`; do not change the SDK or target-framework
  policy incidentally.

## References

Use the following as the source of truth for detailed, evolving guidance instead of duplicating it here:

- [English documentation index](docs/en-US/README.md) and
  [Simplified Chinese documentation index](docs/zh-CN/README.md).
- [EventBus design](docs/en-US/event-bus-design.md): package ownership, public contracts, routing, serialization,
  dependency injection, Generic Host lifecycle, logging, testing, and release structure.
- [`ConsumeResult` handling design](docs/en-US/consume-result-design.md): common dispatch outcomes, adapter mappings,
  exception classification, retry, dead-letter, cancellation, and transport settlement.
- [Serialization design](docs/en-US/serialization-design.md): default Newtonsoft.Json settings, UTF-8 wire format,
  schema evolution, compatibility tests, and custom serializer requirements.
- [EventHorizon.RocketMQ](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ): the protocol-client APIs and
  transport behavior adapted by this repository.

Read the relevant reference before changing a package boundary, public contract, route rule, serializer behavior,
handler lifetime, dispatch outcome, transport mapping, hosted lifecycle, or test topology.

When a transport behavior is unclear, inspect the matching implementation and tests in `EventHorizon.RocketMQ` before
designing around assumptions. Preserve the main client's established protocol semantics unless this repository has a
documented EventBus-specific reason to differ.

## Architecture constraints

- Production contains exactly three projects:
  `EventHorizon.RocketMQ.EventBus`, `EventHorizon.RocketMQ.Remoting.EventBus`, and
  `EventHorizon.RocketMQ.Grpc.EventBus`.
- `EventHorizon.RocketMQ.EventBus` is the transport-neutral Core. It owns public event, EventBus, handler, serializer,
  and registration contracts plus the default Newtonsoft.Json serializer, route table, handler discovery, and common
  dispatch runtime. It must not reference either protocol client package.
- Each adapter references Core and exactly one matching main-client package. The two adapters must not reference each
  other or share transport-specific public types.
- Treat the main-client packages as external transport boundaries. Adapters may depend only on their documented public
  contracts and behavior-oriented extension points. Never expose, copy, infer, persist, or pass through internal Role
  Keys, options names, Consumer indexes, DI descriptor layouts, or other registration implementation details.
- Keep registration-specific bridge identity inside the EventBus implementation. The first application Handler owned
  by a consuming registration is its internal anchor type; adapters close their protocol bridge Handler over that type
  and use a Core-owned generic registration accessor. Do not expose an extra marker type in the public API.
- If the main client lacks a required capability or appears to have a design defect, open an issue in the
  `EventHorizon.RocketMQ` repository with the use case and boundary requirements. Do not modify the sibling main-client
  repository as part of an EventBus task unless the user separately and explicitly authorizes that main-client change.
- `Grpc.Consumer.ConsumeResult` and `Remoting.Consumer.ConsumeResult` remain separate types. Map the internal common
  outcome to each enum with an explicit switch; never cast by numeric value.
- Keep transport message conversion, Producer integration, Push Consumer options, subscription materialization,
  protocol result mapping, and transport-specific logging in the owning adapter.
- Keep Core consumption registration and dispatch transport-mode-neutral. A future mode gets a separate adapter entry
  point and bridge, such as `AddGrpcLitePushEventBus` or `AddRemotingPopEventBus`, instead of adding a mode switch to
  the first-release APIs. Add such an entry point only after the main client exposes a documented public
  hosted-delivery abstraction; do not build a private queue loop, POP receipt lifecycle, or settlement layer inside
  EventBus. Reuse `(Topic, Tag)` only when the transport mode has the same routing semantics. In particular, do not
  overload `IntegrationEvent.Tag` with gRPC `LiteTopic`; design a separate Lite routing contract when that feature is
  actually added.
- Keep public EventBus registration builders and extension methods at each owning package's project root and root
  namespace, matching the discoverability pattern used by Microsoft hosting and dependency-injection packages. Core
  public contracts use `Abstractions`, `Events`, `Exceptions`, and `Serialization` namespaces. Transport-neutral
  implementation code belongs under `Internal/Registration`, `Internal/Routing`, `Internal/Dispatching`, and
  `Internal/Logging`.
- Organize adapter internals under responsibility-specific `Internal/Consumer`, `Internal/Producer`,
  `Internal/Registration`, and `Internal/Logging` folders and namespaces. Mirror production responsibilities in
  unit-test folders. Keep
  project files and package READMEs at the project root. Do not flatten implementation classes into a project root or
  change public namespaces merely to match physical folders.
- Keep one top-level type per C# file except for nested implementation details and test helpers.
- Before completing a change, review all touched C# for idiomatic .NET conventions: the file name should identify its
  primary type or a narrowly defined group, folders and namespaces should reflect ownership, extension methods should
  read naturally at call sites, nullable annotations and DI/Options usage should follow framework conventions, and
  catch-all files such as `TestSupport.cs` must not be used. Perform this review before formatting and final code
  review.
- Preserve the main client's default and named/keyed registration model. Producer-enabled default builders expose
  unkeyed `IEventBus`; Producer-enabled named builders expose keyed `IEventBus` under the same registration name.
  Consumer-only registrations expose no `IEventBus`. Reject duplicate EventBus identities across both adapters during
  service registration using ordinal, case-sensitive name equality.
- Isolate route tables, handlers, lifetimes, serializers, optional Producers, and optional Push Consumers per EventBus
  registration. One concrete application Handler type may belong to only one EventBus registration in an
  `IServiceCollection`, across default/named identities and both adapters. Reject cross-registration reuse during
  service registration; duplicate registration within the owning EventBus remains idempotent when lifetimes agree.

## Routing and consumption constraints

- `IntegrationEvent.Topic` maps directly to the RocketMQ topic. A non-null `IntegrationEvent.Tag` maps to one literal
  RocketMQ tag; `null` publishes an untagged message. Do not add a second filter abstraction or SQL92 support to the
  first-release EventBus API.
- Treat `(Topic, Tag)` as the case-sensitive, ordinal route key for one event type, including `null` as the exact
  untagged route. Reject blank non-null values, literal `*`, `||`, ambiguous routes, and process-dependent route
  constructors during startup registration.
- Group tags by topic and sort non-null tags with ordinal comparison. Generate deterministic literal-tag expressions
  when every route is tagged. If any route for a topic is untagged, subscribe to that topic with the RocketMQ `*`
  filter and continue routing locally by the received nullable Tag.
- Handler registration and assembly scanning are startup-only and must finish before the application service provider
  is built. A built provider uses its immutable registration snapshot and does not observe later service-collection
  changes. Do not retain an EventBus builder as a runtime control surface or add runtime subscribe/unsubscribe APIs.
- Direct handler registrations retain call order. Assembly scanning uses deterministic type ordering. Within one
  EventBus registration, duplicate event-handler pairs with the same lifetime are idempotent and conflicting lifetimes
  are configuration errors. Reusing a Handler type in another EventBus registration is a configuration error.
- Dispatch one physical RocketMQ message per transport Handler invocation, deserialize it once, and then invoke all
  matching application Handlers sequentially. Transport prefetch and concurrent invocations remain configurable.
  Force Remoting `ConsumeMessageBatchSize` to `1`; do not confuse this with receive `BatchSize`.
- Use Push consumption only. Remoting EventBus consumption is clustering-only. Pull, Simple, POP, LitePush, FIFO,
  transaction, delay, priority, batch-publish, request-reply, SQL92, and dynamic-subscription APIs remain outside the
  first release.
- A message succeeds only after every matching application handler completes. Handler or dependency failures request
  retry. Unknown routes and invalid payloads request dead-letter. Preserve shutdown cancellation for the underlying
  consumer instead of manufacturing a new result.

## Dependency injection and lifecycle

- Compose adapters through `AddRocketMQGrpc`/`AddRocketMQRemoting` and the matching `Add*EventBus` extension. Reuse the
  main client's Producer and Push Consumer roles instead of creating parallel transport lifecycles.
- Register a Producer and the matching unkeyed/keyed `IEventBus` only when `configureProducer` is non-null. Add the Push
  Consumer after the first handler is registered. Consumer-only applications must not construct a Producer, and
  publisher-only applications must not start an empty consumer.
- Use the main client's Generic Host `IHostedService` registrations for startup and shutdown. Do not add a duplicate
  hosted loop or ask applications to call transport `StartAsync`/`StopAsync` manually.
- Use one async DI scope per delivery attempt. Resolve all handlers for that message from the same scope and invoke them
  sequentially. Respect `Scoped`, `Transient`, and `Singleton`; singleton handlers and serializers must be thread-safe.
- Do not resolve scoped application services from the root provider or create a nested scope when the transport handler
  already owns the per-delivery scope.
- Give each EventBus registration a private Core-owned object token. Key its route table, serializer, application
  Handlers, and dispatch services with that token. Keep this token behind the Core-owned generic registration accessor;
  never exchange it for a transport-owned identity or use unkeyed application Handler or serializer registrations for
  dispatch.
- Register each protocol bridge Handler with the main client as `Scoped`, regardless of application Handler lifetimes.
  Close the bridge Handler type over the registration's internal anchor Handler type, resolve the matching Core
  accessor from the async scope created by the main client, and never create a nested scope.

## Serialization and logging

- Newtonsoft.Json is the default serializer. Use UTF-8, serialize the concrete event type, keep
  `TypeNameHandling.None`, and choose the destination type only from the startup route table.
- `Topic` and `Tag` are transport metadata and must not be written into the JSON body. Do not add an envelope or .NET
  type name to the default wire format.
- Keep `IIntegrationEventSerializer` replaceable and transport-neutral. Custom singleton serializers must be
  thread-safe. Do not read or mutate `JsonConvert.DefaultSettings`.
- Wrap serialization failures, transport send exceptions, and Remoting non-success send statuses in the Core
  `EventBusPublishException`. Preserve the original exception as `InnerException`, keep a non-exception transport
  status in `TransportResult`, and never wrap caller-requested `OperationCanceledException`.
- Emit structured publish, consume, and outcome logs through `Microsoft.Extensions.Logging`. Successful operations use
  `Information`; publish failures, `Retry`, and `DeadLetter` use `Error`. Normal Host-shutdown cancellation is not an
  EventBus error.
- After all subscriptions for one EventBus registration are validated and materialized, emit one aggregated
  `Information` summary, never one log per Handler. Include registration name (`<default>` for the default), Consumer
  Group, handler count, subscription count, and the deterministic Topic plus Tag `FilterExpression` list.
  This records local client configuration, not Broker acknowledgement.
- Publish and final Consumer outcome logs include the complete message content in the structured `Payload` field as
  single-line JSON. For the default serializer, normalize the actual UTF-8 JSON body without serializing the event a
  second time. For a custom serializer, use the built-in Newtonsoft.Json serializer when the event object is available;
  if an unknown route provides no event object or diagnostic serialization fails, normalize the actual body or wrap it
  as `{"encoding":"base64","data":"..."}`. Consumer deserialization-failure logs must omit `Payload` entirely.
  Diagnostic formatting and logger-provider failures must never change publish or consume behavior.
- `ConfigureLogging` is registration-local. `EventBusLoggingOptions.Enabled` and `IncludePayload` both default to
  `true`; `Enabled = false` suppresses all EventBus logs for that registration, including its subscription summary,
  while `IncludePayload = false` preserves other logs without formatting or adding the `Payload` field. Main-client
  logs remain outside these switches. Materialize settings as an immutable service-provider snapshot.
- Use adapter namespaces as logger-category prefixes so applications can filter full-payload logs. Documentation must
  warn that these logs can contain credentials, personal data, or other sensitive application content.

## Documentation

- Update documentation when behavior, configuration, public APIs, commands, architecture, setup, package ownership,
  or user-facing functionality changes.
- Keep English and Simplified Chinese README or design-note pairs semantically synchronized. Root READMEs stay concise;
  implementation detail belongs under the matching language folder in `docs`.
- Update `consume-result-design.md` whenever result selection, cancellation, retry, dead-letter, or settlement behavior
  changes. Update `serialization-design.md` whenever the wire contract changes. Update `event-bus-design.md` for all
  other architectural and public-contract decisions.
- Do not change documentation for an internal refactor with no user-visible or architectural effect. State any
  documentation that could not be updated and why.

## C# and dependencies

- Target the frameworks declared by the project files and use only C# 12 language features. Keep nullable annotations,
  cancellation propagation, and `ConfigureAwait(false)` usage correct and consistent with the main client.
- Source files do not require a license header. Do not add or enforce one in `.editorconfig`.
- Write code comments and XML documentation in English. Document every public API type and member; do not suppress
  `CS1591` project-wide.
- Prefer concise modern C# when it clarifies the code. Use primary constructors for straightforward dependency or state
  initialization, but retain explicit constructors when validation, defensive copies, defaults, registration, or
  resource cleanup make the lifecycle clearer.
- Prefer constructor injection and standard Microsoft DI. Add an interface only for a real public replacement or
  testing boundary; do not abstract data objects, options, framework types, or internal implementation details by
  default.
- Keep each class focused on one cohesive responsibility. Extract an internal collaborator when registration,
  dispatch, logging, and mutable state no longer form one clear responsibility; avoid mechanical wrappers that only
  add indirection.
- Prefer base libraries and existing dependencies. Explain any new production dependency and compatibility impact.
  Do not add a repository `NuGet.config` or hard-code NuGet.org.

## Packaging

- All three production projects are packable and share one package version and release tag.
- Publish `EventHorizon.RocketMQ.EventBus` before the two adapters. Each packed adapter has a normal same-version NuGet
  dependency on Core; do not embed the Core assembly in an adapter package.
- Use `ProjectReference` between projects in this repository. Keep package dependency metadata, symbols, XML docs,
  Source Link, and package READMEs consistent with the main repository.
- A transport-neutral event-contract project may reference Core directly. Application services normally reference only
  their selected adapter and receive Core transitively.

## Testing and validation

- Prefer test-driven development for behavior changes. Start with the smallest test that expresses the intended
  contract, confirm that it fails for the expected reason, implement the smallest coherent change, rerun the focused
  test, and finish with the affected complete test project.
- Do not manufacture a failing test for documentation, mechanical configuration, or pure refactors whose behavior is
  already covered. For those changes, run the relevant formatting, link, build, package, or characterization checks.
- Put deterministic isolated tests in `tests/ut`; put Docker-backed behavior in the matching project under `tests/it`;
  keep reusable Testcontainers code in `tests/it/EventHorizon.RocketMQ.EventBus.IntegrationTestInfrastructure`.
- All integration fixtures use three independent master Brokers. Keep gRPC and Remoting fixtures separate because
  Proxy-internal Broker aliases and host-reachable Remoting routes require different address models. The infrastructure
  project must not reference a production project.
- Keep `test-environments/rocketmq-multi-broker` independent from integration-test fixtures. Its fixed-port Compose
  stack exists for samples, manual validation, and issue reproduction; IT owns dynamic disposable containers.
- Unit tests must not require an external RocketMQ installation or network access. Use xUnit v3 and normally strict Moq
  mocks. Use stateful fakes only when mocks would obscure streaming, lifecycle, or concurrency behavior.
- Add compatibility tests that verify both adapters map every internal dispatch outcome to the correct independent
  transport enum. Test route validation, deterministic scanning, duplicate registration, handler ordering, serializer
  replacement, DI lifetime, cancellation, optional role creation, named isolation, subscription-summary logs, logging
  levels, and every `ConsumeResult` branch.
- Integration suites cover Generic Host lifecycle, concurrent tagged and untagged publish/consume success, exact
  routing, Newtonsoft.Json compatibility, and message distribution across three independent Brokers for both real
  transports. Keep retry, dead-letter, malformed-payload, unknown-route, and other deterministic outcome branches in
  unit tests; integration coverage does not replace deterministic unit coverage.
- After C# changes, run the narrowest relevant tests while iterating, then the affected complete test project. Run
  formatting before finishing.

Standard checks from the repository root:

```bash
dotnet format EventHorizon.RocketMQ.EventBus.slnx
dotnet restore EventHorizon.RocketMQ.EventBus.slnx
dotnet build EventHorizon.RocketMQ.EventBus.slnx --no-restore
dotnet test tests/ut/EventHorizon.RocketMQ.EventBus.Tests/EventHorizon.RocketMQ.EventBus.Tests.csproj --no-restore
dotnet test tests/ut/EventHorizon.RocketMQ.Grpc.EventBus.Tests/EventHorizon.RocketMQ.Grpc.EventBus.Tests.csproj --no-restore
dotnet test tests/ut/EventHorizon.RocketMQ.Remoting.EventBus.Tests/EventHorizon.RocketMQ.Remoting.EventBus.Tests.csproj --no-restore
```

For live transport changes, also run the matching integration project. Validate any changed Compose file with
`docker compose -f <environment>/compose.yaml config --quiet`. Clearly state a required command that could not run.
