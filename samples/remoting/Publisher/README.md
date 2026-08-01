# Remoting EventBus publisher

[English](README.md) | [Simplified Chinese](README.zh-CN.md)

This minimal Web API discovers routes through `RocketMQ:NamesrvAddr`. It enables a default Remoting EventBus publisher
and a keyed `orders` publisher, but creates no Consumer. The application-facing operation is named `PublishAsync`
because it publishes integration events; the adapter owns the underlying RocketMQ Producer role.

Start the [multi-Broker environment](../../../test-environments/rocketmq-multi-broker/README.md), then run:

```bash
dotnet run --project samples/remoting/Publisher
```

Open `http://localhost:5102/swagger` to send a default tagged order (`order-submitted`), a separately tagged order
through the keyed `orders` registration (`order-submitted-named`), or an untagged inventory snapshot. The registration
name selects an EventBus client; the event Topic and Tag select its RocketMQ route:

```bash
curl -X POST http://localhost:5102/events/inventory-snapshots \
  -H 'Content-Type: application/json' \
  -d '{"warehouse":"shanghai-1"}'
```

Override NameServer with `RocketMQ__NamesrvAddr`. The client connects directly to the Broker addresses returned by
NameServer. Publishing and transport failures are returned as HTTP errors.
