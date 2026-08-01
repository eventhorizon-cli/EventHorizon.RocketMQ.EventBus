# RocketMQ multi-Broker environment

[All test environments](../README.md) | [Simplified Chinese](README.zh-CN.md)

This fixed-port Docker Compose environment runs Apache RocketMQ 5.5.0 for the gRPC and classic Remoting EventBus
samples, manual validation, and issue reproduction. It is separate from `tests/it`: integration tests create their own
dynamic-port Testcontainers topology and never depend on this stack.

The stack contains:

- one NameServer at `localhost:9876`;
- three independent asynchronous master Brokers, advertised to host clients as
  `host.docker.internal:10911`, `host.docker.internal:10921`, and `host.docker.internal:10931`;
- one cluster-mode Proxy at `localhost:8080` for Proxy Remoting and `localhost:8081` for gRPC;
- a resource initializer that creates `eventbus-orders` and `eventbus-inventory-snapshots` on every Broker, with three
  readable and writable queues per Broker; and
- RocketMQ Dashboard at `http://localhost:8082`.

This is a three-master routing and partitioning topology, not a replicated high-availability topology. Each Broker
owns separate persistent storage; stopping one Broker makes that Broker's queues and messages temporarily unavailable.

## Architecture

```text
                            +------------------+
gRPC EventBus ------------>| Proxy :8081      |
                            | cluster mode     |
                            +--------+---------+
                                     |
                                     | route lookup and Broker requests
                                     v
Remoting EventBus ---> NameServer :9876
       |                     |
       | follows routes      +--------+---------+---------+
       |                              |         |         |
       +--------------------------> Broker A  Broker B  Broker C
                                      :10911    :10921    :10931
```

The gRPC application connects only to Proxy. The Remoting application gets routes from NameServer and connects directly
to the advertised Broker endpoints. Both adapters use client-initiated receive/long polling; this environment does
not accept a Broker-initiated inbound connection to an application.

## Start and stop

From the repository root, validate and start the stack:

```shell
docker compose -f test-environments/rocketmq-multi-broker/compose.yaml config --quiet
docker compose -f test-environments/rocketmq-multi-broker/compose.yaml up -d --wait
docker compose -f test-environments/rocketmq-multi-broker/compose.yaml ps
```

The `resource-init` service waits for all three Brokers, creates the sample topics and groups, and verifies each topic
route contains every Broker. Its successful completion means the sample resources are ready.

Normal shutdown retains named-volume data:

```shell
docker compose -f test-environments/rocketmq-multi-broker/compose.yaml down --remove-orphans
```

Adding `-v` deletes all message stores and logs. Use it only for an intentional clean reset.

## Addressing

The default advertised Broker host is `host.docker.internal`, with a Docker host-gateway alias for the services. It
works with Docker Desktop and OrbStack. On Linux, set `ROCKETMQ_ADVERTISED_HOST` to an address reachable by both the
host process and the Proxy container before starting Compose.

Do not advertise `localhost` or `127.0.0.1` for this combined topology: inside Proxy those addresses refer to Proxy,
not one of the Brokers.

## Files and license boundary

`compose.yaml` defines the runtime topology. The three Broker templates, `proxy.json`, and `init-resources.sh` provide
the advertised endpoints and initial resources; `compose.host-volumes.yaml` is an optional host-volume variant.

The Compose configuration is authored for this MIT repository. It follows the main client's operator-facing behavior
without copying Apache-2.0 source files from that repository.
