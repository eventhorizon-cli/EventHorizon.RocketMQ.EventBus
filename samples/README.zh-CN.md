# EventHorizon.RocketMQ.EventBus 示例

[English](README.md) | [简体中文](README.zh-CN.md)

Publisher sample 是最小 Web API；Consumer sample 是 .NET Generic Host 应用。底层 RocketMQ Producer 与 Push
Consumer 角色由现有 `IHostedService` 启停；sample 代码不会手动调用传输层 `StartAsync` 或 `StopAsync`。

## Sample 项目

| Sample | 工作流 |
| --- | --- |
| `grpc/Publisher` | 通过 RocketMQ 5 Proxy 的 Web API 发布，包含默认和 `orders` keyed EventBus registration |
| `grpc/Consumer` | 默认与 `orders` registration 的 gRPC 客户端主动长轮询 Push 消费 |
| `remoting/Publisher` | 通过 NameServer 发现路由、Broker 直连的 Web API 发布，包含默认和 `orders` registration |
| `remoting/Consumer` | 逐条分发的 clustered Remoting Push 消费，包含默认与 `orders` registration |

Publisher samples 传入非 `null` 的 `configureProducer`，因此暴露 `IEventBus`。Consumer samples 省略该委托；它们
只在首次注册 Handler 后创建 Push Consumer，也无法解析 `IEventBus`。

每个 Publisher 都使用 `[FromKeyedServices]` 解析 `orders` registration。每个 Consumer 都有匹配的 named Consumer
Group 和单独注册的 Handler。每个 registration 独占 Route、Handler 生命周期、Serializer、可选 Producer、可选
Consumer 和 Hosted 生命周期；同一个具体 Handler type 不会在它们之间共享。
named 订单事件使用 `eventbus-orders` 的字面量 `order-submitted-named` Tag，因此不会同时投递给默认
`order-submitted` Consumer。registration name 不是 RocketMQ 路由元数据。

每个项目都拥有自己的事件契约，这符合普通应用的边界。Publisher 暴露带字面量 Tag 的
`OrderSubmittedIntegrationEvent` 和 `Tag == null` 的 `InventorySnapshotIntegrationEvent` 端点；对应 Consumer 直接
注册两种事件形状的 Handler。无 Tag 路由会为其 Topic 生成 `*` 订阅，但本地分发仍精确匹配 `(Topic, null)`。

## 本地 RocketMQ

可运行默认值指向独立的
[`test-environments/rocketmq-multi-broker`](../test-environments/rocketmq-multi-broker/README.zh-CN.md) Compose
stack。gRPC 项目连接 Proxy Endpoint；Remoting 项目查询 NameServer，再直连所有已公布的 Broker Endpoint。

每个可运行项目都包含 `appsettings.json`、`README.md` 和 `README.zh-CN.md`。README 说明准确运行命令、协议拓扑、
配置默认值，以及实际启用的可选 EventBus 角色。

## 范围

Samples 演示普通事件发布、Push 消费、直接 Handler 注册、结构化日志和 named DI。
Pull、Simple、POP、LitePush、FIFO、事务、延迟、优先级、批量、请求-响应、SQL92 和运行时订阅变更不属于首版
EventBus 契约，因此不会出现在 samples 中。
