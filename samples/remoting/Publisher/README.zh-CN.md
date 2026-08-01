# Remoting EventBus Publisher

[English](README.md) | [简体中文](README.zh-CN.md)

这个最小 Web API 通过 `RocketMQ:NamesrvAddr` 发现路由。它启用默认 Remoting EventBus Publisher 与 keyed `orders`
Publisher，但不会创建 Consumer。应用层操作名为 `PublishAsync`，因为其职责是发布集成事件；底层 RocketMQ Producer
角色由适配器管理。

先启动[多 Broker 环境](../../../test-environments/rocketmq-multi-broker/README.zh-CN.md)，再运行：

```bash
dotnet run --project samples/remoting/Publisher
```

打开 `http://localhost:5102/swagger`，即可通过默认 registration 发布 `order-submitted` Tag 的订单事件、通过 keyed
`orders` registration 发布 `order-submitted-named` Tag 的独立订单事件，或发布无 Tag 的库存快照。registration name
只选择 EventBus Client；事件的 Topic 与 Tag 才选择 RocketMQ 路由：

```bash
curl -X POST http://localhost:5102/events/inventory-snapshots \
  -H 'Content-Type: application/json' \
  -d '{"warehouse":"shanghai-1"}'
```

可通过 `RocketMQ__NamesrvAddr` 覆盖 NameServer。客户端会直连 NameServer 返回的 Broker 地址。发布与传输失败会作为
HTTP 错误返回。
