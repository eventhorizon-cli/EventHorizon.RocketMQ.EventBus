# EventHorizon.RocketMQ.Grpc.EventBus

[English](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/src/EventHorizon.RocketMQ.Grpc.EventBus/README.md) |
[简体中文](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/src/EventHorizon.RocketMQ.Grpc.EventBus/README.zh-CN.md)

`EventHorizon.RocketMQ.Grpc.EventBus` 为
[EventHorizon.RocketMQ.Grpc](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ) 提供强类型事件发布与 Push 消费。

应用通过 gRPC 连接 RocketMQ 5 Proxy 时应选择这个适配器。消息采用至少一次（at-least-once）投递，处理器必须保证
业务副作用幂等。

## 安装

```shell
dotnet add package EventHorizon.RocketMQ.Grpc.EventBus
```

该适配器会自动引入所需的 gRPC 客户端和 EventBus 依赖，应用通常只需安装这个包。

当前 EventBus 支持强类型事件发布与 Push 消费，不提供 SimpleConsumer、LitePush、FIFO、事务、延迟、优先级、
批量、请求-响应、SQL92 和运行时订阅 API。

## 连接 RocketMQ

将 `Endpoint` 设置为 RocketMQ 5 Proxy 地址。应用不能把 NameServer 或 Broker 地址用作 gRPC Endpoint。

```csharp
builder.Services.AddRocketMQGrpc(options =>
{
    options.Endpoint = "http://localhost:8081";
});
```

当前适配器要求 RocketMQ 5 Proxy 以集群模式运行，并且能够访问集群中的 NameServer 与 Broker。

## 定义事件与处理器

每个事件都要在公开无参构造函数中声明固定的 RocketMQ 路由：

```csharp
public sealed class OrderSubmittedIntegrationEvent : IntegrationEvent
{
    public OrderSubmittedIntegrationEvent()
        : base("orders", "order-submitted")
    {
    }

    public Guid OrderId { get; init; }
    public decimal Total { get; init; }
}
```

实现强类型异步处理器：

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

`Topic` 直接对应 RocketMQ Topic。非 `null` 的 `Tag` 是一个字面量 Tag；`null` 表示发布无 Tag 消息。在同一个注册项
中，区分大小写且按序号比较的 `(Topic, Tag)` 只对应一种事件类型。默认 JSON 消息体不包含 `Topic` 和 `Tag`。

## 注册 EventBus

下面的配置同时启用发布与消费，并扫描应用程序集中的处理器：

```csharp
var builder = Host.CreateApplicationBuilder(args);

builder.Services
    .AddRocketMQGrpc(options => options.Endpoint = "http://localhost:8081")
    .AddGrpcEventBus(
        configureConsumer: options =>
        {
            options.GroupName = "ordering-service";
            options.MaxConcurrency = 8;
        },
        configureProducer: static _ => { })
    .AddHandlersFromAssemblyOf<Program>();

using var host = builder.Build();
await host.RunAsync();
```

使用 `AddHandler<THandler>()` 可以注册单个处理器；`AddHandlersFromAssemblyOf<TMarker>()` 用于扫描标记类型所在的
程序集；`AddHandlersFromAssembly(assembly)` 用于扫描指定程序集。处理器只能在启动阶段注册，默认生命周期为
`Scoped`，也可以选择 `Transient` 或 `Singleton`。单例处理器必须保证线程安全。

`configureProducer` 用于启用发布能力并注册 `IEventBus`；纯消费服务可以省略它。注册第一个处理器时才会添加 Push
Consumer，因此纯发布服务可以只启用 Producer 而不注册处理器。Generic Host 负责启动和停止已经配置的 RocketMQ
角色。

同时支持命名的 RocketMQ 注册。启用 Producer 的命名 EventBus 会用同一个名称暴露 keyed `IEventBus`：

```csharp
builder.Services
    .AddRocketMQGrpc("orders", options => options.Endpoint = "http://orders-proxy:8081")
    .AddGrpcEventBus(
        configureConsumer: options => options.GroupName = "ordering-service",
        configureProducer: static _ => { })
    .AddHandler<OrderSubmittedIntegrationEventHandler>();

using var host = builder.Build();
var ordersEventBus = host.Services.GetRequiredKeyedService<IEventBus>("orders");
```

## 投递行为

每条消息只反序列化一次。全部匹配的处理器在同一个异步 DI Scope 中按顺序执行；只有所有处理器都成功完成，消息才
算处理成功。

| 情况 | gRPC 结果 |
| --- | --- |
| 路由已知、Payload 有效且所有处理器都成功完成 | `Success` |
| 处理器或应用依赖失败 | `Failure` |
| 路由未知或 Payload 无效 | `Failure` |
| Host 停止并取消投递 | 继续传播取消，不额外生成结果 |

gRPC Push 处理器没有单独的死信返回值。遇到未知路由或无效 Payload 时，EventBus 会记录 `DeadLetter`，但向 gRPC
返回 `Failure`；消息只有达到消费组的重试上限后，才会由 RocketMQ 转入 DLQ。

序列化和发送失败统一抛出 `EventBusPublishException`；调用方主动取消时仍抛出未包装的
`OperationCanceledException`。

## 序列化与日志

默认序列化器使用 Newtonsoft.Json 和 `TypeNameHandling.None`，生成紧凑的 UTF-8 JSON，不添加额外的消息封装或
.NET 类型名。调用 `UseSerializer<TSerializer>()` 可以替换单个 EventBus 注册项的序列化器。

默认启用结构化 EventBus 日志和完整 Payload 日志：

```csharp
eventBusBuilder.ConfigureLogging(options =>
{
    options.Enabled = true;
    options.IncludePayload = false;
});
```

Payload 日志可能包含凭据、个人信息或其他敏感内容。部署时应为
`EventHorizon.RocketMQ.Grpc.EventBus` 配置适当的日志分类过滤、保留周期和访问控制。

## 延伸阅读

- [EventBus 详细设计](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/docs/zh-CN/event-bus-design.md)
- [`ConsumeResult` 处理](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/docs/zh-CN/consume-result-design.md)
- [序列化契约](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/docs/zh-CN/serialization-design.md)
- [可运行示例](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/tree/main/samples)
- [底层 gRPC 客户端使用说明](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/main/src/EventHorizon.RocketMQ.Grpc/README.zh-CN.md)

## 许可证

本包采用
[MIT License](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/LICENSE)。
