# gRPC EventBus publisher

[English](README.md) | [Simplified Chinese](README.zh-CN.md)

This minimal Web API connects to the RocketMQ 5 Proxy at `RocketMQ:GrpcEndpoint`. It enables a default gRPC EventBus
publisher and a keyed `orders` publisher, but creates no Consumer. The application-facing operation is named
`PublishAsync` because it publishes integration events; the adapter owns the underlying RocketMQ Producer role.

Start the [multi-Broker environment](../../../test-environments/rocketmq-multi-broker/README.md), then run:

```bash
dotnet run --project samples/grpc/Publisher
```

Open `http://localhost:5101/swagger` to send a default tagged order (`order-submitted`), a separately tagged order
through the keyed `orders` registration (`order-submitted-named`), or an untagged inventory snapshot. The registration
name selects an EventBus client; the event Topic and Tag select its RocketMQ route:

```bash
curl -X POST http://localhost:5101/events/orders \
  -H 'Content-Type: application/json' \
  -d '{"orderId":"b0f8a391-919b-4c8c-a85b-077e184dca4d","total":42.50}'
```

Override the endpoint with `RocketMQ__GrpcEndpoint`. Publishing and transport failures are logged by the EventBus and
returned as HTTP errors.
