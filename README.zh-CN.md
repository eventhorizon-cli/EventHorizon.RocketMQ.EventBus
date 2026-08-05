# EventHorizon.RocketMQ.EventBus

[![.NET Build](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/actions/workflows/dotnet-build.yml/badge.svg?branch=main)](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/actions/workflows/dotnet-build.yml)
[![NuGet gRPC EventBus](https://img.shields.io/nuget/vpre/EventHorizon.RocketMQ.Grpc.EventBus.svg?label=NuGet%20gRPC%20EventBus)](https://www.nuget.org/packages/EventHorizon.RocketMQ.Grpc.EventBus)
[![NuGet Remoting EventBus](https://img.shields.io/nuget/vpre/EventHorizon.RocketMQ.Remoting.EventBus.svg?label=NuGet%20Remoting%20EventBus)](https://www.nuget.org/packages/EventHorizon.RocketMQ.Remoting.EventBus)
[![Codecov](https://codecov.io/gh/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/graph/badge.svg)](https://codecov.io/gh/eventhorizon-cli/EventHorizon.RocketMQ.EventBus)

[English](README.md) | [简体中文](README.zh-CN.md)

这是 [EventHorizon.RocketMQ](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ) 的强类型 EventBus。RocketMQ 5
gRPC 与 classic Remoting 使用相同的事件、处理器、路由、序列化和托管模型，同时保留彼此独立的协议适配器。

## 选择适配器

| 包 | 适用场景 |
| --- | --- |
| [`EventHorizon.RocketMQ.Grpc.EventBus`](https://www.nuget.org/packages/EventHorizon.RocketMQ.Grpc.EventBus) | 通过 RocketMQ 5 Proxy 使用 gRPC 的服务 |
| [`EventHorizon.RocketMQ.Remoting.EventBus`](https://www.nuget.org/packages/EventHorizon.RocketMQ.Remoting.EventBus) | 通过 NameServer 发现 Broker 并使用 classic Remoting 的服务 |

应用只需安装与所用协议对应的适配器。两个适配器彼此独立，不会额外引入另一种协议的客户端。

## 支持范围

- 强类型事件发布与 Push 消费
- 精确的 `(Topic, Tag)` 路由，包括无 Tag 消息
- 直接注册处理器，或按确定顺序扫描程序集
- Microsoft 依赖注入与 Generic Host 生命周期
- 默认注册和 named/keyed 注册
- 默认使用 Newtonsoft.Json，并可为每个注册项替换序列化器
- 结构化的发布、消费、结果与订阅汇总日志

消息采用至少一次（at-least-once）投递，处理器必须保证业务副作用幂等。当前 EventBus 不提供独立 Pull、SimpleConsumer、
LitePull、LitePush、FIFO、事务、延迟、优先级、批量、请求-响应、SQL92 和运行时订阅 API。

Classic Remoting Push 在 Broker 分配队列时可以内部使用 PULL 或 POP；这一选择不会改变 EventBus API 和处理器契约。

## 快速开始

定义带有固定路由的事件：

```csharp
public sealed class OrderSubmittedIntegrationEvent : IntegrationEvent
{
    public OrderSubmittedIntegrationEvent()
        : base("orders", "order-submitted")
    {
    }

    public Guid OrderId { get; init; }
}
```

实现对应的处理器：

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

注册 gRPC 适配器，并扫描应用程序集：

```csharp
builder.Services
    .AddRocketMQGrpc(options => options.Endpoint = "http://localhost:8081")
    .AddGrpcEventBus(
        configureConsumer: options => options.GroupName = "ordering-service",
        configureProducer: static _ => { })
    .AddHandlersFromAssemblyOf<Program>();
```

如果通过 NameServer 和 classic Remoting 连接，请改用 `AddRocketMQRemoting` 与 `AddRemotingEventBus`。

`configureProducer` 用于启用发布能力并注册 `IEventBus`；纯消费服务可以省略它。注册第一个处理器时才会添加 Push
Consumer，因此纯发布服务不会启动空 Consumer。

通过默认注册发布事件：

```csharp
await eventBus.PublishAsync(
    new OrderSubmittedIntegrationEvent { OrderId = orderId },
    cancellationToken);
```

启用 Producer 的命名注册会用同一个名称暴露 keyed `IEventBus`：

```csharp
var ordersEventBus = serviceProvider.GetRequiredKeyedService<IEventBus>("orders");
```

## 路由与失败处理

`Topic` 直接对应 RocketMQ Topic。非 `null` 的 `Tag` 是一个字面量 Tag；`null` 表示发布无 Tag 消息。在同一个
EventBus 注册项中，区分大小写且按序号比较的 `(Topic, Tag)` 只对应一种事件类型。该事件类型可以注册多个处理器，
消息只反序列化一次，处理器随后按顺序执行。默认 JSON 消息体不包含 `Topic` 和 `Tag`。

序列化和发送失败统一抛出 `EventBusPublishException`；调用方主动取消时仍抛出
`OperationCanceledException`。

消费时，只有全部匹配的处理器都成功完成，消息才算处理成功。处理器失败会请求重试；未知路由和无效 Payload 会
请求死信处理。适配器会根据对应协议客户端的能力映射这些结果。

## 日志

每个注册项默认启用 EventBus 日志和完整 Payload 日志：

```csharp
eventBusBuilder.ConfigureLogging(options =>
{
    options.Enabled = true;
    options.IncludePayload = false;
});
```

Payload 日志可能包含凭据、个人信息或其他敏感内容。部署时应配置适当的日志分类过滤、保留周期和访问控制。

## 文档

- [English documentation](docs/en-US/)
- [简体中文文档](docs/zh-CN/)
- [示例](samples/)

## 许可证

本项目采用 [MIT License](LICENSE)。
