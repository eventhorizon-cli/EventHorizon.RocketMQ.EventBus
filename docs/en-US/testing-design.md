# Testing, environments, and samples design

[Documentation](README.md) | [简体中文](../zh-CN/testing-design.md) |
[EventBus design](event-bus-design.md)

This document defines the repository's unit-test, integration-test, local-environment, and sample structure. It keeps
transport-independent behavior deterministic while validating each adapter against real RocketMQ processes.

## Project matrix

| Area | Project or directory | Responsibility |
| --- | --- | --- |
| Unit test | `EventHorizon.RocketMQ.EventBus.Tests` | Event contracts, route table, scanning, handler ordering, serializer, dispatch, DI lifetimes, and logging policy |
| Unit test | `EventHorizon.RocketMQ.Grpc.EventBus.Tests` | gRPC registration, message conversion, optional roles, keyed binding, publish results, and `ConsumeResult` mapping |
| Unit test | `EventHorizon.RocketMQ.Remoting.EventBus.Tests` | Remoting registration, singleton-message enforcement, optional roles, keyed binding, send-status handling, and `ConsumeResult` mapping |
| Compatibility test | `EventHorizon.RocketMQ.EventBus.Compatibility.Tests` | Cross-package API symmetry, independent protocol types, package boundaries, and default/named behavior |
| Integration infrastructure | `EventHorizon.RocketMQ.EventBus.IntegrationTestInfrastructure` | Disposable protocol-specific three-Broker Testcontainers fixtures and unique test resources |
| Integration test | `EventHorizon.RocketMQ.Grpc.EventBus.IntegrationTests` | EventBus behavior through a real RocketMQ 5 cluster-mode Proxy |
| Integration test | `EventHorizon.RocketMQ.Remoting.EventBus.IntegrationTests` | EventBus behavior through real NameServer route lookup and direct Broker Remoting connections |
| Manual environment | `test-environments/rocketmq-multi-broker` | Fixed-port Docker Compose stack for samples, manual testing, and issue reproduction |
| Samples | `samples` | Runnable Web API Publisher and Generic Host Consumer workflows for each protocol, including named registrations |

The integration infrastructure is non-packable and does not reference any production project. Each protocol IT
project references only its adapter and the shared test infrastructure. Unit and compatibility tests never require
Docker or network access.

The infrastructure exposes `RocketMQGrpcClusterFixture` and `RocketMQRemotingClusterFixture`. The fixture types stay
separate because their advertised Broker addresses serve different clients.

## Unit-test coverage

Core tests cover:

- `IntegrationEvent` validation and the exact ordinal `(Topic, Tag)` route key;
- public parameterless-constructor discovery and invalid constructor behavior;
- deterministic assembly scanning, direct registration order, per-registration idempotent duplicates, and conflicting
  lifetimes;
- generated Topic subscriptions and ordinal-sorted Tag expressions;
- startup-only registration and proof that a built provider does not observe later service-collection changes;
- immutable runtime route plans and the absence of direct, scanned, serializer, or subscription mutation services;
- reuse of the transport-owned asynchronous scope, sequential handlers, and every internal dispatch outcome;
- fixed Newtonsoft.Json payloads, malformed UTF-8, schema-evolution defaults, and custom serializers; and
- structured log levels and fields, full JSON-formatted payloads, custom-serializer logging views, binary fallback,
  and the one-time subscription summary.

Adapter and compatibility tests cover:

- default unkeyed and named keyed registrations for both protocols;
- several names in one protocol and mixed gRPC/Remoting names in one service collection;
- ordinal, case-sensitive registration names, including independent `orders` and `Orders` keys;
- duplicate registration identities across adapters;
- per-registration routes, handler lifetimes, serializers, Producer, Consumer, and hosted-service isolation;
- `configureProducer: null` registering no Producer, Producer hosted service, or `IEventBus`;
- a non-null Producer delegate registering exactly one Producer and the correct unkeyed/keyed `IEventBus`;
- first-handler creation of exactly one Push consumer and no Consumer for a publisher-only registration;
- a unique closed protocol bridge type anchored by the first Handler owned by each consuming registration, without
  exposing a transport registration identity;
- startup rejection when direct registration or assembly scanning attempts to assign one Handler type to another
  default or named EventBus registration;
- private-token isolation of routes, serializers, and distinct Handler types across registrations;
- a fixed Scoped protocol bridge and exactly one main-client-owned async scope per delivery;
- explicit mapping of every internal outcome to each independently defined transport `ConsumeResult`, including gRPC
  `Retry`/`DeadLetter` convergence on `Failure`;
- Remoting non-success send statuses becoming publish failures; and
- cancellation-token propagation and subscription-summary startup behavior.

## Integration-test topology

Every integration fixture uses three independent master Brokers. This creates real multi-Broker routes and queue
distribution without claiming replication or high availability. Topics are created explicitly on all three Brokers,
and fixture startup does not complete until NameServer reports the full route.

### gRPC fixture

```text
gRPC IT process
      |
      v
cluster-mode Proxy container
      |
      +--> NameServer container
      |
      +--> broker-a container
      +--> broker-b container
      `--> broker-c container
```

The test process connects only to the dynamically mapped Proxy gRPC endpoint. Brokers advertise Docker-network aliases
at fixed internal ports so the standalone Proxy can reach every route. The test process does not query NameServer or
connect directly to Brokers through the production gRPC client.

### Remoting fixture

```text
Remoting IT process
      |
      +--> NameServer container -- route lookup
      |
      +--> broker-a container -- direct Remoting
      +--> broker-b container -- direct Remoting
      `--> broker-c container -- direct Remoting
```

The fixture maps NameServer and all Broker ports dynamically. Each Broker advertises a host-reachable address and its
mapped port, because the production Remoting client follows the returned route and connects directly to that Broker.
The fixture enables Broker-side assignment and POP, but keeps PULL and POP workflows on separate Topics and consumer
groups. No Proxy is required for Remoting IT.

The two fixtures remain separate because a Proxy can resolve Docker aliases that a host process cannot, while
`127.0.0.1` routes suitable for the host cannot identify three peer containers from inside a Proxy. Shared lifecycle
helpers may be extracted, but there is no public mode-switched fixture with invalid members in one mode.

## Current integration coverage

Each protocol suite starts a Generic Host with one EventBus Producer and one Push Consumer. The default workflows
concurrently publish twelve tagged and twelve untagged events to one fixture-created Topic, verify every event ID
reaches only its matching typed Handler exactly once, and confirm all three Brokers stored messages. This exercises
public registration, Host-owned transport lifecycle, the Newtonsoft.Json body path, literal Tag routing, the wildcard
subscription needed for an untagged route, and Remoting's one-message dispatch constraint.

The Remoting suite also runs a separate Broker-assigned POP workflow on its own Topic and consumer group. It verifies
typed EventBus delivery and waits for successful `ack` settlement activities emitted only after real POP `ACK_MESSAGE`
responses. A PULL regression would emit offset `commit`, not `ack`, and the test would fail. Client assignment remains
the default and is covered by the original PULL workflow.

The fixtures create unique Topics and Groups, wait on observable conditions with bounded timeouts, and own all Docker
resources. Deterministic unit tests cover result mapping, malformed payloads, unknown routes, retry classification,
named registrations, and other branches that do not require a Broker.

## Independent Compose environment

`test-environments/rocketmq-multi-broker` is not an IT fixture and is never required by `dotnet test`. It contains one
fixed-port `compose.yaml` with:

- one NameServer;
- three independent master Brokers with separately persisted stores;
- one standalone cluster-mode Proxy exposing its gRPC endpoint;
- one resource-initializer service that creates sample Topics on every Broker; and
- an optional Dashboard for local inspection.

The Compose environment supports both host-side protocols: gRPC samples connect to Proxy, while Remoting samples query
NameServer and then reach every advertised Broker address. Its bilingual README documents host-address overrides,
ports, startup, health checks, resource creation, and destructive volume cleanup.

The Compose files and Testcontainers fixtures may use the same image version and topology terminology, but they do not
share source files, lifecycle state, ports, or persisted data. A sibling checkout of the main client repository is not
required at test or sample runtime.

## Samples

The first release includes these protocol-specific projects:

| Sample | Demonstrates |
| --- | --- |
| `samples/grpc/Publisher` | gRPC Web API Publisher with default and keyed `orders` `IEventBus` registrations, tagged and untagged endpoints, and no Consumer |
| `samples/grpc/Consumer` | gRPC Push consumption with direct Handler registration, tagged and wildcard subscriptions, and a named `orders` consumer |
| `samples/remoting/Publisher` | Remoting Web API Publisher with default and keyed `orders` `IEventBus` registrations, tagged and untagged endpoints, and no Consumer |
| `samples/remoting/Consumer` | clustered Remoting Push consumption, one-message dispatch, tagged and wildcard subscriptions, and a named `orders` consumer |

Each sample has `appsettings.json`, English and Simplified Chinese README files, runnable defaults for the independent
Compose environment, and one visible SDK workflow. Publisher samples use the WebApplication host and Consumer samples
use Generic Host, so configured RocketMQ roles start and stop through their existing `IHostedService` registrations.
NonHost samples are outside the first release.

## CI and validation

The manual and CI checks use the same project boundaries. Changes to public registration, routing, serialization,
result mapping, or lifecycle behavior require focused unit coverage and the affected protocol IT. Documentation-only
work uses link, spelling, and structure checks and does not manufacture a failing behavioral test.

The independent Compose environment is validated with:

```shell
docker compose -f test-environments/rocketmq-multi-broker/compose.yaml config --quiet
```

## GitHub Actions and releases

The repository contains `.github/workflows/dotnet-build.yml` and one unified
`.github/workflows/publish.yml`. The workflow design intentionally differs from the main client repository's separate
protocol-package publish workflows: EventBus has three packages with one shared version and must release them as one
ordered unit.

| Workflow | Trigger | Purpose |
| --- | --- | --- |
| `.github/workflows/dotnet-build.yml` | Pushes and pull requests targeting `main` | Validate formatting, compilation, target frameworks, unit coverage, integration behavior, samples, and Compose syntax. |
| `.github/workflows/publish.yml` | A pushed tag beginning with `v`; the workflow then requires an exact stable tag | Test, pack, publish the three same-version packages in dependency order, retain package artifacts, and create the release. |

### Build workflow

The build workflow restores from `global.json`, verifies `dotnet format` without modifying the checkout, and builds the
complete solution in Release configuration. Its unit-test matrix collects coverage for all four deterministic test
projects: Core, gRPC adapter, Remoting adapter, and Compatibility. It also explicitly validates every target framework
declared by the supported production projects, rather than assuming the default SDK target covers them all.

After deterministic validation succeeds, two independent Docker jobs run the gRPC and Remoting EventBus integration
projects against their own disposable three-Broker Testcontainers topology. The gRPC job uses its cluster-mode Proxy
fixture; the Remoting job uses its NameServer plus host-reachable direct-Broker fixture. These jobs do not start the
fixed-port `test-environments/rocketmq-multi-broker` Compose environment.

The workflow builds every sample project and validates the independent Compose file with
`docker compose -f test-environments/rocketmq-multi-broker/compose.yaml config --quiet`. It uploads the test coverage
and test-result artifacts required by the configured reporting service. Any reporter credential is supplied only from a
GitHub Actions secret; no credential value is committed to a workflow or documentation.

The workflow has the minimum permissions needed for validation, `permissions: contents: read`, and a concurrency group
derived from the workflow and ref that cancels superseded push or pull-request runs. It uses the SDK declared by
`global.json`, disables .NET telemetry and logos, and may cache NuGet packages using a key derived from the SDK and
project or build metadata.

### Unified publish workflow

The unified publish workflow listens to tags beginning with `v`, then validates the full ref name before performing any
restore, pack, push, or release operation. The only accepted form is stable three-part SemVer:

```text
v<major>.<minor>.<patch>
```

The equivalent validation pattern is:

```text
^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$
```

For example, `v1.2.3` is accepted. `v1.2`, `release-v1.2.3`, and every prerelease tag containing a suffix, such as
`v1.2.3-rc.1`, are rejected. A `v*` trigger remains necessary because GitHub Actions tag filters cannot express the
complete stable-SemVer rule; the first workflow step enforces it.

The parsed version is assigned unchanged to all three production packages. The workflow restores, builds, and runs the
unit release-validation tests before packing `EventHorizon.RocketMQ.EventBus`,
`EventHorizon.RocketMQ.Grpc.EventBus`, and `EventHorizon.RocketMQ.Remoting.EventBus` with that one version. It uploads
the produced `.nupkg` and `.snupkg` files as workflow artifacts before publication.

Publication is deliberately ordered:

1. Push the Core package.
2. Push the gRPC and Remoting adapter packages immediately; both declare the same-version Core dependency.
3. Unlist Core from NuGet search while retaining exact-version dependency restore.
4. Create a GitHub Release for the tag only after both adapter pushes succeed; the Release is never marked as a prerelease.

The publish workflow uses a non-cancelling release concurrency group, so two tags cannot interleave publication. It
needs `contents: write` because it creates the GitHub release. One `NUGET_API_KEY` both publishes the three packages
and unlists Core, so its NuGet.org scope must authorize both operations for the corresponding package IDs.
Package-source credentials where needed, and any reporting token are read from GitHub Actions secrets or the execution
environment; no secret value is hard-coded. A missing required secret fails the publish operation before any package
push. NuGet.org still rejects an API key whose package ownership or operation scope is insufficient at the affected
publish or unlist step.
