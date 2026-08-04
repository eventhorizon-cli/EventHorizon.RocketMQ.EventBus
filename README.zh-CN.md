# EventHorizon.RocketMQ.EventBus

[![.NET Build](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/actions/workflows/dotnet-build.yml/badge.svg?branch=main)](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/actions/workflows/dotnet-build.yml)
[![NuGet gRPC EventBus](https://img.shields.io/nuget/vpre/EventHorizon.RocketMQ.Grpc.EventBus.svg?label=NuGet%20gRPC%20EventBus)](https://www.nuget.org/packages/EventHorizon.RocketMQ.Grpc.EventBus)
[![NuGet Remoting EventBus](https://img.shields.io/nuget/vpre/EventHorizon.RocketMQ.Remoting.EventBus.svg?label=NuGet%20Remoting%20EventBus)](https://www.nuget.org/packages/EventHorizon.RocketMQ.Remoting.EventBus)
[![Codecov](https://codecov.io/gh/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/graph/badge.svg)](https://codecov.io/gh/eventhorizon-cli/EventHorizon.RocketMQ.EventBus)

[English](README.md) | [简体中文](README.zh-CN.md)

这是一个面向 [EventHorizon.RocketMQ](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ) 的强类型 EventBus
扩展，集成事件的使用方式参考微软 eShop 项目的实践。

它在 RocketMQ 5 gRPC 与 classic Remoting 之上提供统一的应用层模型，同时将两种协议保留在独立 Package 中。

## 范围

首版支持：

- 发布强类型集成事件，并且只通过 Push Consumer 消费；
- 每次投递只路由和反序列化一条物理 RocketMQ 消息；
- 直接注册 Handler，或通过确定性的程序集扫描注册；
- 通过 Microsoft DI 解析 Handler，默认生命周期为 `Scoped`；
- 保留主 Client 的默认注册和 named/keyed 注册模型；
- 通过主 Client 的 `IHostedService` 注册接入 Generic Host；
- 默认使用 Newtonsoft.Json，并允许按 registration 替换序列化器；
- 输出结构化的发布、消费、结果和订阅汇总日志；
- 提供 Unit Tests、协议专用的三 Broker Integration Tests、可运行 Consumer 与 Web API Publisher samples，以及独立的 Compose 环境。

首版不提供独立的 Pull、Simple、LitePush、SQL92、运行时订阅、事务或顺序消息、延迟消息、批量发布、请求-响应和
exactly-once 投递。Classic Remoting Push 在 Broker 分配队列时可以内部使用 PULL 或 POP，不会改变 EventBus API 和
Handler。后续如需增加新的公开投递模型，主 Client 必须先提供合适的 hosted-delivery 抽象。Handler 的业务副作用
必须具备幂等性。

## 安装

| Package | 职责 |
| --- | --- |
| `EventHorizon.RocketMQ.Remoting.EventBus` | classic Remoting Producer 与 Clustering Push Consumer 适配器 |
| `EventHorizon.RocketMQ.Grpc.EventBus` | RocketMQ 5 gRPC Producer 与 Push Consumer 适配器 |

安装实际使用的 RocketMQ 协议适配器即可。两个适配器会传递还原共享的 EventBus 实现，并且不会相互引用。该支持包会
从 NuGet 搜索中取消列出，不作为用户直接安装的入口。

## 使用方式

公开契约位于 `EventHorizon.RocketMQ.EventBus.Abstractions`、`.Events`、`.Exceptions` 和 `.Serialization` 命名空间；
注册扩展位于 Core 或所选适配器的根命名空间。

```csharp
using EventHorizon.RocketMQ.EventBus;
using EventHorizon.RocketMQ.EventBus.Abstractions;
using EventHorizon.RocketMQ.EventBus.Events;
using EventHorizon.RocketMQ.Grpc.EventBus;
```

事件继承 `IntegrationEvent`，并把稳定路由传入基类构造函数：

```csharp
public sealed class OrderSubmittedIntegrationEvent : IntegrationEvent
{
    public OrderSubmittedIntegrationEvent()
        : base("orders", "order-submitted")
    {
    }

    public Guid OrderId { get; init; }
}

public sealed class InventorySnapshotIntegrationEvent : IntegrationEvent
{
    public InventorySnapshotIntegrationEvent()
        : base("inventory-snapshots")
    {
    }

    public int Available { get; init; }
}
```

`Topic` 直接对应 RocketMQ Topic。非 `null` 的 `Tag` 对应一个字面量 Tag；`null` 表示发布不带 Tag 的消息。精确且按序号
比较的 `(Topic, Tag)` 路由只会选择一种事件类型；该事件类型可以注册多个 Handler，在一次反序列化后按顺序执行。
`*` 是 Consumer FilterExpression，不能作为事件 Tag。如果某个 Topic 存在无 Tag 路由，Consumer 会使用 `*` 订阅，
本地路由仍然精确匹配 `(Topic, null)`。两个路由值都不会写入默认 JSON Body。

Handler 使用 `Task`，普通的异步应用代码无需额外约定：

```csharp
public sealed class OrderSubmittedIntegrationEventHandler
    : IIntegrationEventBusHandler<OrderSubmittedIntegrationEvent>
{
    public Task HandleAsync(
        OrderSubmittedIntegrationEvent integrationEvent,
        CancellationToken cancellationToken = default)
    {
        return Task.CompletedTask;
    }
}
```

注册 gRPC EventBus 并扫描应用程序集：

```csharp
builder.Services
    .AddRocketMQGrpc(options => options.Endpoint = "http://localhost:8081")
    .AddGrpcEventBus(
        configureConsumer: options => options.GroupName = "ordering-service",
        configureProducer: static _ => { })
    .AddHandlersFromAssemblyOf<Program>();
```

`AddHandler<THandler>()` 注册一个具体 Handler type。`AddHandlersFromAssemblyOf<TMarker>()` 和
`AddHandlersFromAssembly(assembly)` 在启动阶段发现 Handler；三种方法都接受可选的 `ServiceLifetime`。在一个
`IServiceCollection` 中，同一个具体 Handler type 只能属于一个 EventBus registration，跨协议和跨注册名都不例外。

`configureProducer` 是发布能力开关。省略它时，该 registration 不会创建 Producer、Producer HostedService 或
`IEventBus`。首次注册 Handler 时才会创建 Push Consumer，因此纯发布服务不会创建空 Consumer，纯消费服务也不会创建
Producer。

启用发布能力的 named registration 以相同名称暴露 keyed Publisher：

```csharp
var ordersEventBus = serviceProvider.GetRequiredKeyedService<IEventBus>("orders");
await ordersEventBus.PublishAsync(new OrderSubmittedIntegrationEvent { OrderId = orderId });
```

默认 registration 暴露未键控 `IEventBus`。`PublishAsync` 的 `CancellationToken` 为可选参数；序列化和发送失败使用
`EventBusPublishException`，调用方主动取消仍保持为 `OperationCanceledException`。

## 日志

适配器将成功的发布与消费记录为 `Information`，将发布失败、EventBus 自己选择的 `Retry` 和 `DeadLetter` 记录为
`Error`，并为每个消费 registration 输出一次聚合订阅汇总。发布日志和 Consumer 最终结果日志会在结构化 `Payload`
字段中以单行 JSON 记录完整消息内容。使用自定义序列化器时，只要能获得事件对象，就使用内置 Newtonsoft.Json
生成日志视图；路由未知时，原始 Body 会在需要时使用 Base64 JSON 包装。Consumer 反序列化失败时省略该字段。
日志和 Payload 默认都开启，并可按 registration 配置：

```csharp
eventBusBuilder.ConfigureLogging(options =>
{
    options.Enabled = true;
    options.IncludePayload = false;
});
```

Payload 日志可能包含敏感数据，应用还应配置适当的日志分类过滤、保留周期和访问控制：

```json
{
  "Logging": {
    "LogLevel": {
      "EventHorizon.RocketMQ.Grpc.EventBus": "Information",
      "EventHorizon.RocketMQ.Remoting.EventBus": "Warning"
    }
  }
}
```

## 设计文档

- [English 文档](docs/en-US/)
- [简体中文文档](docs/zh-CN/)

## 许可证

本项目采用 [MIT License](LICENSE)。
