# gRPC EventBus Consumer

[English](README.md) | [简体中文](README.zh-CN.md)

该 Generic Host 只创建 gRPC Push Consumer，不配置 Producer。默认 registration 直接注册带 Tag 的订单路由和无 Tag
的库存路由 Handler。订单路由有两个 Handler，用于演示一条 `(Topic, Tag)` 只选择一种事件类型，并可在一次反序列化后
扇出。无 Tag 路由会让对应 Topic 使用 RocketMQ `*` 订阅表达式。

同一进程还创建了名为 `orders` 的 named registration；它使用独立 Consumer Group 和独立订单 Handler。其路由仍使用
同一 Topic，但字面量 Tag 为 `order-submitted-named`；默认路由使用 `order-submitted`。registration name 只隔离
DI/Client，并不是 RocketMQ 路由元数据。

先启动[多 Broker 环境](../../../test-environments/rocketmq-multi-broker/README.zh-CN.md)，再运行：

```bash
dotnet run --project samples/grpc/Consumer
```

可通过 `RocketMQ__GrpcEndpoint` 覆盖 Proxy Endpoint。正常停止进程即可验证 HostedService 关闭流程。
