# EventHorizon.RocketMQ.Remoting.EventBus

[English](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/src/EventHorizon.RocketMQ.Remoting.EventBus/README.md) |
[简体中文](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/src/EventHorizon.RocketMQ.Remoting.EventBus/README.zh-CN.md)

`EventHorizon.RocketMQ.Remoting.EventBus` 是
[EventHorizon.RocketMQ.Remoting](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ) 强类型 EventBus 适配器。
它在 classic RocketMQ Remoting Client 上增加集成事件发布和 Push 消费，同时将应用事件契约、路由和序列化保留在
传输无关的 `EventHorizon.RocketMQ.EventBus` 中。

首版只支持普通强类型事件发布和 Push 消费，不提供独立的 Pull、LitePull、FIFO、事务、延迟、优先级、批量、
请求-响应、SQL92 或运行时订阅 API。Remoting EventBus 只使用 Clustering，不支持 Broadcasting。消息投递至少一次，
应用 Handler 必须保证业务副作用具备幂等性。

同一个 Push EventBus 既支持由 Client 分配队列并使用 PULL，也支持由 Broker 分配队列后按 assignment 使用 PULL 或
POP。POP 是内部接收方式，不会增加另一套 EventBus API 或 Handler 契约。

## Package 与依赖

使用以下命令安装 Package：

```shell
dotnet add package EventHorizon.RocketMQ.Remoting.EventBus
```

该 Package 依赖同版本的 `EventHorizon.RocketMQ.EventBus` Core Package 和
`EventHorizon.RocketMQ.Remoting`。Core 会作为传递依赖还原，不会嵌入本 Package，也不作为用户直接安装的入口。Remoting
与 gRPC EventBus 适配器不会相互引用。

适配器通过现有公开 `AddRemotingPushConsumer<TMessageHandler>` API 注册闭合的协议桥接 Handler，其内部泛型
anchor 是该 EventBus registration 的第一个自有应用 Handler。适配器不会检查 Service Descriptor，也不会访问
Client 内部注册身份。

## 连接架构

```text
应用
    |
    v
EventHorizon.RocketMQ.Remoting.EventBus
    |
    v
EventHorizon.RocketMQ.Remoting
    |
    +--> NameServer 路由查询
    |         |
    `---------> 直连已公布地址的 Brokers
```

`NamesrvAddr` 指向一个或多个 NameServer Endpoint。Remoting Client 先从 NameServer 获取路由信息，再直接连接
路由中公布的 Broker 地址。因此，应用所在环境必须能够访问这些公布的 Broker 地址；Proxy 不是 Remoting EventBus
的 Endpoint。

Remoting Push Consumer 不是由 Broker 发起的服务端 Push 协议，而是 Client 发起的长轮询。该适配器会强制设置
`ConsumeMessageBatchSize = 1`，因此每个传输层回调只向 EventBus 投递一条物理消息。`PullBatchSize` 和
`PopBatchSize` 仍可大于 1，以保留接收效率。

`QueueAssignmentMode` 默认是 `RemotingPushQueueAssignmentMode.Client`：由 Client 分配队列并使用 PULL。在 Broker
端已经配置 assignment request mode 的 Clustering 并发消费环境中，可以改为 `Broker`。Broker 返回的每条
assignment 会选择 PULL 或 POP，但 EventBus Handler 不变。使用 POP 时，处理必须在 `PopInvisibleDuration` 内完成；
classic Remoting Push 不会在 Handler 执行期间自动续约 receipt。

## 编程模型

应用事件在公开无参构造函数中声明固定的 RocketMQ 路由元数据，Tag 可选：

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

public sealed class InventorySnapshotIntegrationEvent : IntegrationEvent
{
    public InventorySnapshotIntegrationEvent()
        : base("inventory-snapshots")
    {
    }

    public int Available { get; init; }
}
```

Handler 实现强类型异步契约：

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

`Topic` 直接对应 RocketMQ Topic。非 `null` 的 `Tag` 对应一个字面量 RocketMQ Tag；`null` 表示发布无 Tag 消息。
在一个 EventBus registration 内，区分大小写且按序号比较的 `(Topic, Tag)` 唯一标识一个事件类型。`*` 是 Consumer
`FilterExpression`，不是事件 Tag。某个 Topic 存在无 Tag 路由时，Remoting Consumer 使用 `*` 订阅，但本地分发仍精确
选择 `(Topic, null)`。传输元数据不会写入 JSON Body。

## 注册

下面的默认注册同时启用发布和消费，并扫描应用程序集发现 Handler。默认 Client registration 最终会暴露未键控的
`IEventBus`。

```csharp
var builder = Host.CreateApplicationBuilder(args);

builder.Services
    .AddRocketMQRemoting(options => options.NamesrvAddr = "localhost:9876")
    .AddRemotingEventBus(
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

当一个 Host 需要隔离的多个 Client 时，使用 named main-client registration。启用 Producer 的 named EventBus 会以
同一个名称暴露 keyed `IEventBus`；下面的示例使用直接 Handler 注册，而不是程序集扫描：

```csharp
builder.Services
    .AddRocketMQRemoting("orders", options => options.NamesrvAddr = "orders-nameserver:9876")
    .AddRemotingEventBus(
        configureConsumer: options => options.GroupName = "ordering-service",
        configureProducer: static _ => { })
    .AddHandler<OrderSubmittedIntegrationEventHandler>();

using var host = builder.Build();
var ordersEventBus = host.Services.GetRequiredKeyedService<IEventBus>("orders");
```

`AddHandlersFromAssemblyOf<TMarker>()` 扫描 marker type 所在程序集，`AddHandlersFromAssembly(assembly)` 扫描指定的
`Assembly`，`AddHandler<THandler>()` 注册一个 Handler type。所有注册只能发生在启动阶段，必须在构建
ServiceProvider 或 Host 之前结束。首版没有运行时订阅、退订、Handler 注册或程序集扫描 API。

在同一个 EventBus registration 内，直接注册保留调用顺序，程序集扫描顺序确定。相同生命周期下重复的事件-Handler
组合是幂等的；冲突的生命周期是配置错误。同一个 Service Collection 中，一个 Handler 类型不能加入另一个默认或
named EventBus registration。Handler 默认使用 `Scoped`，也可使用 `Transient` 或 `Singleton`；Singleton Handler
必须线程安全。

## 可选角色与生命周期

`configureProducer` 是发布能力开关：

| 配置 | 注册的角色 |
| --- | --- |
| `configureProducer` 非 `null` | 一个 Remoting Producer；默认 registration 提供未键控 `IEventBus`，named registration 提供 keyed `IEventBus` |
| `configureProducer` 为 `null` | 该 registration 不创建 Producer、Producer HostedService 或 `IEventBus` |
| 首次注册 Handler | 增加一个 Clustering 模式的 Remoting Push Consumer |
| 未注册 Producer 且未注册 Handler | 不增加 EventBus 传输角色或 HostedService |

因此，纯消费服务省略 `configureProducer`；纯发布服务传入非 `null` 的 `configureProducer`，且不注册 Handler。
每个默认或 named EventBus registration 都独立持有 Route Table、Serializer、Handler 注册及生命周期、可选 Producer
和可选 Push Consumer。

适配器复用主 Remoting Client 的 `IHostedService` 注册。Generic Host 负责启动和停止真正的 Producer 与 Push Consumer
角色；使用 Generic Host 的应用不能手动调用底层 Client 的 `StartAsync` 或 `StopAsync`。

## 序列化与分发

默认序列化器是 Newtonsoft.Json。它按事件具体类型生成紧凑 UTF-8 JSON，并使用 `TypeNameHandling.None`；没有
Envelope 或 .NET type name，`Topic` 与 `Tag` 也会从 Body 排除。启动时生成的 Route Table 会在反序列化前选定目标
事件类型。

通过 `UseSerializer<TSerializer>()`，可以为当前 registration 替换为实现 `IIntegrationEventSerializer` 的
Serializer。每个 EventBus registration 都持有一个按私有 token keyed 的 Singleton；两个 registration 即使使用同一个
Serializer 类型，也会创建两个独立实例。自定义 Serializer 必须线程安全、结果确定，并在该事件的 Producer 与
Consumer 之间保持兼容。

每次投递使用一个异步 DI Scope。适配器从同一个 Scope 解析全部匹配 Handler，并按顺序调用它们。只有所有 Handler
都成功完成，消息才算成功。

| 情况 | 分发结果 |
| --- | --- |
| 路由已知、Payload 有效且全部 Handler 成功完成 | `Success` |
| Handler 或应用依赖解析失败 | EventBus 返回 `Retry` |
| 主客户端无法创建或释放投递 Scope，或者消费超时 | 底层 Consumer 重试；EventBus 不制造结果 |
| 路由未知或 Payload 无法反序列化 | `DeadLetter` |
| Host 停止取消本次投递 | 取消继续传给底层 Consumer，不制造新的结果 |

Remoting 适配器会把内部结果显式映射到 `EventHorizon.RocketMQ.Remoting.Consumer.ConsumeResult`，不会在 EventBus
公开 API 中暴露传输层 `ConsumeResult`。

序列化失败、传输发送失败和非成功的 Remoting send status 统一抛出 `EventBusPublishException`。调用方主动取消时，
仍直接抛出未包装的 `OperationCanceledException`。

## 日志

适配器通过 `Microsoft.Extensions.Logging` 写入结构化的发布、消费和结果日志，category prefix 为
`EventHorizon.RocketMQ.Remoting.EventBus`。发布和消费成功使用 `Information`；发布失败、`Retry` 与 `DeadLetter`
使用 `Error`，其中 `Retry` 指 EventBus 因 Handler 或依赖失败选择的结果。消费超时和投递 Scope 生命周期失败由
底层客户端记录。正常 Host 停止引起的取消不是 EventBus 错误。发布日志和 Consumer 最终结果日志会在结构化
`Payload` 字段中以单行 JSON 记录完整消息内容。默认序列化器直接复用实际 JSON Body；使用自定义序列化器时，只要
能获得事件对象，就用内置 Newtonsoft.Json 生成日志视图。无法读取的原始 Body 会记录为
`{"encoding":"base64","data":"..."}`；如果序列化在生成 Body 前就失败，`Payload` 可以为空。Consumer
反序列化失败时会完全省略该字段。

EventBus 日志与 Payload 默认都开启，可分别按 default 或 named registration 配置。`Enabled = false` 会关闭该
registration 的发布、Consumer、结果与订阅汇总日志，但不影响底层 RocketMQ 客户端日志：

```csharp
eventBusBuilder.ConfigureLogging(options =>
{
    options.Enabled = true;
    options.IncludePayload = false;
});
```

完整 Payload 可能包含凭据、个人信息或其他敏感内容。部署时应配置适当的分类过滤、日志保留周期和访问控制。例如：

```json
{
  "Logging": {
    "LogLevel": {
      "EventHorizon.RocketMQ.Remoting.EventBus": "Information"
    }
  }
}
```

每个 Consumer registration 在路由验证完成、并且全部本地订阅已物化后，只会为该 EventBus registration 输出一条
聚合的 `Information` 订阅汇总，不会按每个 Handler 单独输出。它包含注册名（默认 registration 为 `<default>`）、
Consumer Group、Handler 数量、订阅数量，以及确定性排序的 Topic 和 Tag `FilterExpression` 列表。该日志描述
本地 Client 配置，而不是 Broker 确认。

## 延伸阅读

- [EventBus 详细设计](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/docs/zh-CN/event-bus-design.md)
- [`ConsumeResult` 处理设计](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/docs/zh-CN/consume-result-design.md)
- [序列化设计](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/docs/zh-CN/serialization-design.md)
- [测试、环境与示例](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/docs/zh-CN/testing-design.md)
- [底层 Remoting Client Samples](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/tree/main/samples/remoting)

## 许可证

本项目采用 [MIT License](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/LICENSE)。
