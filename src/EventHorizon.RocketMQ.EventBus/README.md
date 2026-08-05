# EventHorizon.RocketMQ.EventBus

[English](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/src/EventHorizon.RocketMQ.EventBus/README.md) |
[简体中文](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/src/EventHorizon.RocketMQ.EventBus/README.zh-CN.md)

> This is an internal support package for the EventBus protocol adapters. Do not install it directly in applications.

Install the adapter for the RocketMQ protocol used by the service:

```shell
dotnet add package EventHorizon.RocketMQ.Grpc.EventBus
# or
dotnet add package EventHorizon.RocketMQ.Remoting.EventBus
```

The selected adapter brings in this package automatically. It contains the shared event, handler, serializer, routing,
and dispatch contracts, but it does not connect to RocketMQ or provide a standalone client.

For installation, configuration, and usage, see the
[gRPC adapter guide](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/src/EventHorizon.RocketMQ.Grpc.EventBus/README.md)
or the
[Remoting adapter guide](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/src/EventHorizon.RocketMQ.Remoting.EventBus/README.md).

Architecture and package-boundary details are documented in the
[EventBus design](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/docs/en-US/event-bus-design.md).

## License

This package is licensed under the
[MIT License](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/LICENSE).
