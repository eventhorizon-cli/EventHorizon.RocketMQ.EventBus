# Integration tests

[English](README.md) | [简体中文](README.zh-CN.md) |
[Testing design](../../docs/en-US/testing-design.md)

`tests/it` contains two protocol test assemblies and one non-test infrastructure library:

| Project | Responsibility |
| --- | --- |
| `EventHorizon.RocketMQ.Grpc.EventBus.IntegrationTests` | Validates EventBus through a real cluster-mode RocketMQ 5 Proxy |
| `EventHorizon.RocketMQ.Remoting.EventBus.IntegrationTests` | Validates EventBus through NameServer discovery and direct Broker Remoting |
| `EventHorizon.RocketMQ.EventBus.IntegrationTestInfrastructure` | Owns disposable three-Broker Testcontainers fixtures and unique test resources |

The infrastructure project references Testcontainers 4.13.0 and xUnit lifecycle abstractions, but no production
project. Each protocol test project references only its matching adapter and the infrastructure project.

## Disposable topologies

Both fixtures start one NameServer and three independent master Broker containers, create a unique test Topic explicitly
on all Brokers, and wait for a complete route. The gRPC fixture additionally starts a standalone cluster-mode Proxy and
exposes only its gRPC endpoint. The Remoting fixture exposes NameServer and every Broker with host-reachable advertised
addresses.

Fixtures use dynamic ports and are created and disposed by the selected xUnit suite. They do not read, start, or share
state with `test-environments/rocketmq-multi-broker`; that Compose environment is for samples and manual work.

## Current coverage

Each protocol suite starts a Generic Host with one EventBus Producer and one Push Consumer, then publishes twelve tagged
and twelve untagged events concurrently. The registered typed Handlers record the event IDs and the test verifies that
each physical event reaches its matching Handler exactly once. Both suites verify that the resulting Topic has messages
on all three Brokers; the Remoting fixture also verifies the complete three-Broker NameServer route before the
direct-Broker EventBus flow starts. This exercises the public EventBus registration API, host-owned
transport lifecycle, Newtonsoft.Json payload path, `Tag` routing, wildcard subscription required by an untagged route,
multi-Broker route distribution, and the Remoting adapter's one-message dispatch contract.

The deterministic unit suites cover result mapping, malformed payloads, unknown routes, retry classification, named
registrations, and other branches that do not need a Broker. Additional broker-behavior scenarios can be added to these
two integration projects without changing the fixture boundary.

## Commands

Docker must be available:

```shell
dotnet test tests/it/EventHorizon.RocketMQ.Grpc.EventBus.IntegrationTests/EventHorizon.RocketMQ.Grpc.EventBus.IntegrationTests.csproj --no-restore
dotnet test tests/it/EventHorizon.RocketMQ.Remoting.EventBus.IntegrationTests/EventHorizon.RocketMQ.Remoting.EventBus.IntegrationTests.csproj --no-restore
```

The suites require a reachable Docker daemon and use unique Topics and Groups, bounded condition waits, and no arbitrary
sleeps in test behavior. See the testing design for the complete scenario matrix and CI ownership.
