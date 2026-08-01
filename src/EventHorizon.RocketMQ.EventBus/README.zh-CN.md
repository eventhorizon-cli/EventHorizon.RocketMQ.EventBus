# EventHorizon.RocketMQ.EventBus

[English](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/src/EventHorizon.RocketMQ.EventBus/README.md) |
[简体中文](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/src/EventHorizon.RocketMQ.EventBus/README.zh-CN.md)

`EventHorizon.RocketMQ.EventBus` 是强类型 EventBus 适配器共用的实现依赖。它拥有公开的事件、Handler、发布器、
序列化器、注册、路由和分发契约，但不引用 gRPC 或 classic Remoting Client Package。该包会从 NuGet 搜索中取消列出，
不作为用户直接安装的入口。

安装一个适配器即可通过传递依赖获得这些类型：

```shell
dotnet add package EventHorizon.RocketMQ.Grpc.EventBus
# 或者
dotnet add package EventHorizon.RocketMQ.Remoting.EventBus
```

## 包边界

```text
应用事件契约
     |
     v
EventHorizon.RocketMQ.EventBus
       ^                 ^
       |                 |
Grpc.EventBus      Remoting.EventBus
       |                 |
RocketMQ 5 Proxy   NameServer 与 Broker
```

Core 不连接 RocketMQ、不管理 Socket，也不添加传输层 `IHostedService`。所选适配器负责消息转换、可选 Producer 和
Push Consumer 角色、传输层消息处置和传输日志；两个适配器不会相互引用。

## 公开契约

| 命名空间 | 公开 API |
| --- | --- |
| `EventHorizon.RocketMQ.EventBus` | `IEventBusBuilder`、`EventBusLoggingOptions`、`ConfigureLogging` 和启动阶段注册扩展 |
| `.Abstractions` | `IEventBus` 和 `IIntegrationEventBusHandler<TIntegrationEvent>` |
| `.Events` | `IntegrationEvent` |
| `.Exceptions` | `EventBusPublishException` |
| `.Serialization` | `IIntegrationEventSerializer` 和默认 Newtonsoft.Json 序列化器 |

`IntegrationEvent` 通过基类构造函数携带不可变的路由元数据：

```csharp
public sealed class OrderSubmittedIntegrationEvent : IntegrationEvent
{
    public OrderSubmittedIntegrationEvent()
        : base("orders", "order-submitted")
    {
    }
}

public sealed class InventorySnapshotIntegrationEvent : IntegrationEvent
{
    public InventorySnapshotIntegrationEvent()
        : base("inventory-snapshots")
    {
    }
}
```

`Topic` 对应 RocketMQ Topic。非 `null` 的 `Tag` 对应一个字面量 Tag；`null` 表示发布无 Tag 消息。区分大小写且按
序号比较的 `(Topic, Tag)` 在一个 registration 内只标识一种事件类型；该事件类型可以注册多个 Handler，在一次
反序列化后按顺序执行。`*` 不是事件 Tag，而是 Consumer FilterExpression。某个 Topic 存在无 Tag 路由时，适配器
使用 `*` 订阅，但 Core 仍精确路由收到的 `(Topic, null)`。`Topic` 和 `Tag` 都不会写入 JSON Body。

Handler 返回 `Task`，每次投递都在一个异步 DI Scope 中按顺序执行：

```csharp
public sealed class OrderSubmittedHandler
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

`AddHandler<THandler>()` 用于注册一个具体 Handler，`AddHandlersFromAssemblyOf<TMarker>()` 扫描 marker 所在程序集，
`AddHandlersFromAssembly(Assembly)` 扫描指定程序集。它们都只能在启动阶段使用，并接受可选 `ServiceLifetime`；默认
值为 `Scoped`。在一个 `IServiceCollection` 中，同一个具体 Handler type 只能属于一个 EventBus registration，包括由
不同适配器拥有的 registration。

## 分发与序列化

Core 对每条物理消息只路由和反序列化一次，然后按顺序调用全部匹配 Handler。Handler 或依赖失败时请求 `Retry`；未知
路由和无效 Payload 请求 `DeadLetter`。投递语义是 at least once，应用副作用必须幂等。

默认序列化器使用严格 UTF-8、无 Envelope、`TypeNameHandling.None` 的 Newtonsoft.Json。它按事件具体类型序列化，
Core 只从启动期 Route Table 选择反序列化目标类型。调用 `UseSerializer<TSerializer>()` 可以为当前 registration
同时替换序列化与反序列化；自定义序列化器必须确定且线程安全。

`IEventBus.PublishAsync` 的 `CancellationToken` 是可选的。某个 registration 只有启用 Producer 后，适配器才会提供
未键控或 keyed 的 `IEventBus`。序列化和发送失败使用 `EventBusPublishException`；请求取消仍保持为
`OperationCanceledException`。

## 边界与后续模式

Core 不使用主 Client 的内部实现。如果主 Client 缺少所需能力或存在设计缺陷，本仓库会在主 Client 仓库提出 issue，
而不是修改主 Client 仓库。当前只支持 Push；以后如支持 LitePush 或 POP，必须通过独立适配器入口，并且主 Client
先公开有文档的 hosted-delivery 抽象，不能把它们做成 Push API 的模式开关。

## 延伸阅读

- [EventBus 详细设计](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/docs/zh-CN/event-bus-design.md)
- [`ConsumeResult` 处理](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/docs/zh-CN/consume-result-design.md)
- [序列化契约](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/docs/zh-CN/serialization-design.md)
- [测试、环境与示例](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/docs/zh-CN/testing-design.md)

## 许可证

本 Package 使用
[MIT License](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/LICENSE)。
