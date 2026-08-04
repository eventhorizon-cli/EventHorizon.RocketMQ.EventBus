# EventHorizon.RocketMQ.EventBus 详细设计

[文档目录](README.md) | [English](../en-US/event-bus-design.md) |
[`ConsumeResult` 处理设计](consume-result-design.md) | [序列化设计](serialization-design.md) |
[测试设计](testing-design.md)

这是一个面向 [EventHorizon.RocketMQ](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ) 的强类型、
DI 优先 EventBus 层。整体编程风格参考微软已归档的
[eShopOnContainers](https://github.com/dotnet-architecture/eShopOnContainers) 及其后续项目
[eShop](https://github.com/dotnet/eShop)。

该库在 RocketMQ 5 gRPC 与 Remoting 之上提供一致的集成事件编程模型，同时把两种传输协议保留在相互独立的
包中。

## 范围

首个版本刻意保持精简：

- 发布强类型集成事件；
- 消费端只使用底层 gRPC 或 Remoting Push Consumer；
- 每条 RocketMQ 消息只反序列化一次，并且每次只分发一个事件；
- 通过扫描应用程序集发现强类型 Handler；
- 通过 Microsoft DI 解析 Handler，默认生命周期为 `Scoped`；
- 支持主项目中的默认客户端注册和 named/keyed 客户端注册；
- 通过 Generic Host 和 `IHostedService` 自动启动与停止每个已配置的 Producer 和 Push Consumer；
- 默认通过 `Microsoft.Extensions.Logging` 记录结构化的发布、消费和消费结果日志；
- 默认使用 Newtonsoft.Json，以保证 JSON 兼容性；
- 公开序列化接口，允许应用完全替换序列化与反序列化实现；
- 与底层客户端一致，同时面向 `net8.0` 和 `net10.0`。

首个版本只发布普通消息，不提供独立的 Pull、Simple、LitePush、Admin、事务消息、FIFO、定时/延迟消息、
优先级消息、批量消息、请求-响应、SQL92 过滤或运行时动态订阅/退订 API。Classic Remoting Push 在 Broker
分配队列时可以内部使用 PULL 或 POP，不需要另建 EventBus 契约。消息投递语义仍然是至少一次，因此 Handler
必须具备幂等性。

## 包

| 包 | 职责 | 生产依赖 |
| --- | --- | --- |
| `EventHorizon.RocketMQ.EventBus` | 公开契约、默认 Newtonsoft.Json 序列化器、路由表、注册 Builder、Handler 发现和公共分发运行时 | Microsoft DI abstractions 和 Newtonsoft.Json |
| `EventHorizon.RocketMQ.Remoting.EventBus` | Remoting Producer 与 Push Consumer 适配器 | EventBus Core 和 `EventHorizon.RocketMQ.Remoting` |
| `EventHorizon.RocketMQ.Grpc.EventBus` | RocketMQ 5 gRPC Producer 与 Push Consumer 适配器 | EventBus Core 和 `EventHorizon.RocketMQ.Grpc` |

```text
                            EventHorizon.RocketMQ.EventBus
                                      ^
                                      |
                +---------------------+---------------------+
                |                                           |
EventHorizon.RocketMQ.Remoting.EventBus       EventHorizon.RocketMQ.Grpc.EventBus
                |                                           |
EventHorizon.RocketMQ.Remoting                EventHorizon.RocketMQ.Grpc
```

两个传输适配器不会相互引用。应用只需安装其实际使用的 RocketMQ 协议适配器。

传输层各自定义的 `ConsumeResult` 不会进入 EventBus 公开 API。公共分发流程只产生内部的传输无关结果，再由每个
适配器显式映射到其主项目包中的枚举。详见 [`ConsumeResult` 处理设计](consume-result-design.md#包边界)。

### NuGet 发布方式

三个项目都会生成 NuGet 包，并使用相同的版本号和发布 Tag。发布时先推送作为支持依赖的 Core，随后立即推送两个
适配器；两个适配器推送成功后，再取消列出 Core。

每个适配器都把相同版本的 `EventHorizon.RocketMQ.EventBus` 声明为普通依赖，因此安装适配器时会自动传递安装
Core。应用安装适配器，不直接安装 Core。

Core 会从 NuGet 搜索中取消列出，但仍可通过确切的依赖版本还原。它的程序集不会分别嵌入两个适配器包；否则应用
同时引用两个适配器时，相同公开类型会产生程序集与版本冲突。仓库源码使用 `ProjectReference`，打包后则表现为
对应的 NuGet 依赖。

### 公开命名空间

| 命名空间 | 公开职责 |
| --- | --- |
| `EventHorizon.RocketMQ.EventBus` | `IEventBusBuilder` 与启动阶段注册扩展 |
| `.Abstractions` | `IEventBus` 与 `IIntegrationEventBusHandler<TIntegrationEvent>` |
| `.Events` | `IntegrationEvent` |
| `.Exceptions` | `EventBusPublishException` |
| `.Serialization` | `IIntegrationEventSerializer` 与默认 Newtonsoft.Json 序列化器 |

gRPC 与 Remoting Package 分别在各自根命名空间保留 `AddGrpcEventBus` 和 `AddRemotingEventBus` 扩展。传输层实现类型与
`ConsumeResult` 都留在适配器边界内。

## 编程模型

### 集成事件

`IntegrationEvent` 是一个只有两个路由属性的抽象基类，不会额外加入 eShop 中的 `Id` 或 `CreationDate`。

```csharp
public abstract class IntegrationEvent
{
    protected IntegrationEvent(string topic, string? tag = null)
    {
        Topic = topic;
        Tag = tag;
    }

    [JsonIgnore]
    public string Topic { get; }

    [JsonIgnore]
    public string? Tag { get; }
}
```

应用事件通过公开无参构造函数向基类传入稳定的 RocketMQ 路由信息。启动注册也会使用这个无参构造函数，
在 Generic Host 启动之前读取路由。

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

路由规则如下：

- `Topic` 对应 RocketMQ 消息 Topic；
- 非 `null` 的 `Tag` 对应单个、字面量形式的 RocketMQ 消息 Tag；`null` 表示发布不带 Tag 的消息；
- 在一个 EventBus 注册内，`(Topic, Tag)` 唯一标识一个集成事件类型。

当多个事件类型共用同一个 Topic 时，启动注册会把它们的 Tag 合并为一个 RocketMQ Tag 订阅表达式，例如
`order-submitted || order-cancelled`。收到消息后，先通过 Topic 与 Tag 选择事件类型，再反序列化消息体。`Tag`
是发布阶段的事件元数据；`FilterExpression` 是消费端根据已注册 Tag 自动生成的值。如果某个 Topic 存在无 Tag
路由，Consumer 会为该 Topic 生成 `*` 订阅，本地 Route Table 仍会精确区分 `null` 与所有字面量 Tag。首版
EventBus 契约不支持 SQL92 表达式。

Core 通过传输层自有的回调创建 Consumer，并让分发逻辑独立于 Push 实现。Classic Remoting POP 是同一个 Push
Consumer 的内部接收引擎，因此继续使用 `AddRemotingEventBus`、同一个桥接 Handler 和同一张 `(Topic, Tag)`
路由表。应用可以通过 `RemotingPushConsumerOptions` 请求由 Broker 分配队列；随后，每条 Broker assignment 决定
主 Client 使用 PULL 还是 POP。EventBus 不修改 Broker 上按 Topic 与 Consumer Group 配置的 request mode，也不接管
POP receipt 或消息处置。

如果以后支持 gRPC LitePush 这类公开编程模型不同的投递方式，应等主 Client 提供有文档的 hosted-delivery 抽象后，
再增加独立的适配器入口。gRPC `LiteTopic` 是另一种路由概念，不能塞进 `Tag`；Lite 支持需要单独定义清楚的路由
契约。

### Handler

Handler 接口是异步、强类型、支持取消并适合程序集扫描的：

```csharp
public interface IIntegrationEventBusHandler<in TIntegrationEvent>
    where TIntegrationEvent : IntegrationEvent
{
    Task HandleAsync(
        TIntegrationEvent integrationEvent,
        CancellationToken cancellationToken = default);
}
```

Handler 返回 `Task`，而不是 `ValueTask`。应用 Handler 通常会等待 I/O，而同步完成时 `Task.CompletedTask` 本身没有
额外分配。这样公开的应用契约保持直接，传输层和 Core 内部仍可在已有 API 受益时使用 `ValueTask`。

```csharp
public sealed class OrderSubmittedIntegrationEventHandler(
    OrdersDbContext dbContext,
    ILogger<OrderSubmittedIntegrationEventHandler> logger)
    : IIntegrationEventBusHandler<OrderSubmittedIntegrationEvent>
{
    public async Task HandleAsync(
        OrderSubmittedIntegrationEvent integrationEvent,
        CancellationToken cancellationToken = default)
    {
        // 在这里实现应用业务逻辑。
    }
}
```

同一个事件类型可以注册多个 Handler。分发器为每条消息使用一个 DI 作用域，只反序列化一次事件，然后在同一个
作用域内依次调用所有匹配的 Handler。`Scoped` 依赖都从该作用域解析，并在处理结束后异步释放。
只有全部 Handler 成功后才确认消息。如果后面的 Handler 失败，重投后前面已经成功的 Handler 仍可能再次执行，
因此 Handler 必须具备幂等性。

Core 不会增加事件 ID，也不会向业务 Handler 暴露传输层消息上下文。如果某个事件的副作用需要去重，应在 JSON
消息体中自行携带稳定的业务 ID 或事件 ID。Broker Message ID 可以用于诊断，但不是应用层幂等契约。

这里的异步 DI 作用域，是指底层 Push Consumer 每次尝试投递消息时调用 `CreateAsyncScope()`。EventBus 适配器和
该消息对应的全部业务 Handler 都从这个作用域解析；分发结束后调用 `DisposeAsync()`，异步释放实现了
`IAsyncDisposable` 的 `Scoped` 服务，例如许多数据库上下文。它只是资源生命周期边界，不会额外创建消息队列、
线程或并发模型。Scope 创建、桥接层解析和释放均由主客户端负责；生命周期异常会进入主客户端 Consumer 的重试
路径，而不是由 EventBus 映射结果。

### EventBus 与序列化

发布接口采用现代 eShop 的异步形式：

```csharp
public interface IEventBus
{
    Task PublishAsync(
        IntegrationEvent integrationEvent,
        CancellationToken cancellationToken = default);
}
```

发布失败使用一个 Core 异常契约：

```csharp
public sealed class EventBusPublishException : Exception
{
    public Type IntegrationEventType { get; }

    public string Topic { get; }

    public string? Tag { get; }

    public string? RegistrationName { get; }

    public string? TransportResult { get; }
}
```

该异常由适配器创建，应用通常只捕获而不自行构造。序列化失败、传输发送异常，以及 Remoting 返回的非成功
`RemotingSendStatus` 都会包装成 `EventBusPublishException`。原始异常保留在 `InnerException`；没有异常对象的传输
结果写入 `TransportResult`。调用方主动取消时仍传播原始 `OperationCanceledException`，不会包装。异常属性永远不
包含消息 Body。

默认序列化器使用 Newtonsoft.Json 和 UTF-8，按照事件的实际具体类型序列化，并保持
`TypeNameHandling.None`。消费端根据启动时建立的路由表提供已知事件类型，不信任消息体中的 `$type` 值。

应用可以在不替换传输适配器的情况下完全替换序列化实现：

```csharp
public interface IIntegrationEventSerializer
{
    byte[] Serialize(IntegrationEvent integrationEvent);

    IntegrationEvent Deserialize(
        ReadOnlyMemory<byte> payload,
        Type integrationEventType);
}
```

默认消息格式不包含额外封装或 .NET 类型名称。`Topic` 和 `Tag` 是不可变的传输元数据，不进入 JSON 消息体。

完整的设置、UTF-8 规则、Schema 演进策略、失败行为和自定义序列化器要求见
[序列化设计](serialization-design.md)。

## 注册与程序集扫描

API 会直接衔接 `EventHorizon.RocketMQ` 提供的 Builder，并返回用于注册 Handler 的 EventBus Builder。Producer
和 Push Consumer 都是可选角色，只有实际启用对应能力时才创建：

- `configureProducer` 非 `null` 时注册一个 Producer，并为该注册暴露 `IEventBus`；
- `configureProducer` 为 `null` 时不注册 Producer、Producer HostedService 或 `IEventBus` 服务；
- 首次直接注册 Handler 或通过程序集扫描发现 Handler 时，注册一个 Push Consumer；
- 如果既没有 Producer，也没有 Handler，则不增加任何传输角色或 HostedService。

因此，同一套 API 可以构建纯发布、纯消费或同时发布与消费的 Host，不会打开未使用的传输连接。

两个传输入口定义为：

```csharp
IEventBusBuilder AddRemotingEventBus(
    this RemotingRocketMQBuilder builder,
    Action<RemotingPushConsumerOptions>? configureConsumer = null,
    Action<RemotingProducerOptions>? configureProducer = null);

IEventBusBuilder AddGrpcEventBus(
    this GrpcRocketMQBuilder builder,
    Action<GrpcPushConsumerOptions>? configureConsumer = null,
    Action<GrpcProducerOptions>? configureProducer = null);
```

Remoting 的队列分配通过现有 Push options 配置，不需要增加 EventBus mode：

```csharp
builder.Services
    .AddRocketMQRemoting(options => options.NamesrvAddr = "localhost:9876")
    .AddRemotingEventBus(
        configureConsumer: options =>
        {
            options.GroupName = "ordering-service";
            options.QueueAssignmentMode = RemotingPushQueueAssignmentMode.Broker;
        });
```

主 Client 默认使用 `Client`：由 Client 分配队列，并通过 PULL 接收消息。`Broker` 适用于 EventBus 已支持的
Clustering 并发消费；Broker 根据运维侧 request-mode 配置，在每条 assignment 中返回 PULL 或 POP。两条路径调用
相同的 EventBus Handler。POP 使用主 Client 固定的 `PopInvisibleDuration` 处理期限，EventBus 不会另建租期续约循环。

最常见的单委托调用只配置 Push Consumer，不会创建 Producer。发送超时、发送重试、消息大小限制和 Remoting
Producer Group 等参数通过具名参数 `configureProducer` 配置。即使传入空的非 `null` Producer 委托，也会使用
主客户端默认值启用发布能力。纯发布服务省略 `configureConsumer`，并且不注册 Handler：

```csharp
builder.Services
    .AddRocketMQRemoting(options => options.NamesrvAddr = "localhost:9876")
    .AddRemotingEventBus(
        configureProducer: options =>
        {
            options.GroupName = "ordering-producer";
            options.SendMsgTimeout = TimeSpan.FromSeconds(5);
        });
```

当 `configureProducer` 非 `null` 时，`Add*EventBus` 独占 Producer 角色，并注册匹配的未键控或 keyed
`IEventBus`。它不会接管通过 `AddGrpcProducer` 或 `AddRemotingProducer` 独立注册的 Producer；如果两者同时配置，
主客户端会在服务注册阶段按重复角色报错。当 `configureProducer` 为 `null` 时，独立配置的底层 Producer 可以
共存，但 EventBus 不会使用它，也不会为该注册暴露 `IEventBus` 发布服务。

### Named registration

EventBus 完整保留主项目的注册模型。启用发布能力后，默认 RocketMQ Builder 注册未键控的 `IEventBus`；named
Builder 使用相同的 `RegistrationName` 注册 keyed `IEventBus`：

```csharp
builder.Services
    .AddRocketMQGrpc("orders", options =>
    {
        options.Endpoint = "http://localhost:8081";
    })
    .AddGrpcEventBus(
        configureConsumer: options =>
        {
            options.GroupName = "ordering-service";
        },
        configureProducer: static _ => { })
    .AddHandlersFromAssemblyOf<OrderingApplicationMarker>();

public sealed class OrderPublisher(
    [FromKeyedServices("orders")] IEventBus eventBus)
{
    // ...
}
```

也可以通过 `GetRequiredKeyedService<IEventBus>("orders")` 获取同一个实例。纯消费的 named registration 仍使用
注册名隔离路由和 Handler，但不会注册 keyed `IEventBus`。

在一个 Service Collection 中，无论注册是纯发布、纯消费还是同时具备两种能力，默认注册标识和每个字符串注册名
都必须在两个适配器之间保持唯一。注册名采用区分大小写的序号比较，与主客户端字符串 Service Key 一致，因此
`orders` 和 `Orders` 是两个不同注册。除此之外，可以同时注册多个 named gRPC 和 Remoting EventBus，包括连接
不同 RocketMQ 集群的注册。

每个 EventBus 注册都独占自己的路由表、Handler 注册、Handler 生命周期、序列化器选择、可选 Producer 和可选
Push Consumer。`UseSerializer<TSerializer>()` 只替换当前 Builder 对应的序列化器。在一个
`IServiceCollection` 中，一个具体应用 Handler 类型只属于一个 EventBus registration。无论通过默认或 named
registration，还是另一个协议适配器再次注册，都会在服务注册阶段失败。

主客户端 Package 是 EventBus 之外的传输边界。适配器只依赖其公开 Handler Contract 和面向行为的注册 API，绝不
接收、推断或复刻传输层 Role Key、Options Name、Consumer Index 或 DI Descriptor 布局。如果主 Client 缺少必要能力
或存在设计缺陷，应在主 Client 仓库提出包含使用场景与边界要求的 issue；不能把修改主 Client 仓库作为 EventBus
变更的一部分。

每个消费 registration 中，第一个成功注册的应用 Handler 会成为内部 anchor。适配器用它闭合协议桥接类型，例如
`GrpcIntegrationEventBusHandler<OrderSubmittedIntegrationEventHandler>`，再把闭合类型传给主客户端现有的公开
`AddGrpcPushConsumer<TMessageHandler>` 或 `AddRemotingPushConsumer<TMessageHandler>` API。该类型身份完全属于
内部实现；应用仍只调用 `AddHandler<THandler>()` 或程序集扫描，不需要额外声明 marker。一个 Handler 只能属于一个
registration 的规则保证闭合桥接类型只会对应一个 Core registration。

每次调用 `Add*EventBus` 时，Core 都会在共享 `IServiceCollection` 中增加一个内部 Registration Marker。Marker
包含公开注册标识和一个私有 object token。Core 通过 Marker 检查重复的默认注册或序号比较相等的注册名，因此
即使两次调用来自不同适配器，也会拒绝冲突。

内部 Route Table、Serializer、业务 Handler 和 Dispatch Service 全部使用私有 token 作为 key，而不是公开字符串
注册名。`AddHandler` 和程序集扫描把 Handler Descriptor keyed 到所属 token；`UseSerializer<TSerializer>` 把
Singleton Serializer keyed 到同一个 token。该模型可以避免未键控应用服务或其他 EventBus 注册中的相同 Handler
类型泄漏到当前分发：

```text
公开默认/name key --> IEventBus（仅在启用 Producer 时）
                    --> EventBus Registration Marker --> 私有 token

主客户端泛型 Handler 注册 --> 协议桥接 Handler<anchor Handler>
                                      --> Core registration accessor<anchor Handler>
                                      --> 私有 token
                                          --> Route Table
                                          --> Serializer Singleton
                                          `--> keyed 业务 Handler
```

闭合后的协议桥接 Handler 固定使用 `ServiceLifetime.Scoped`，不跟随业务 Handler 生命周期。主客户端因此会为每次
投递创建一个异步 DI Scope，并在其中解析桥接 Handler。Core 自有的泛型 accessor 把 anchor 类型连接到私有
registration token；桥接层从现有 Scope 解析全部 keyed 业务 Handler，不会创建嵌套 Scope。所属注册内部仍严格
遵守业务 Handler 配置的 `Transient`、`Scoped` 或 `Singleton` 语义。

`IEventBusBuilder` 同时公开注册标识和 Service Collection：

```csharp
public interface IEventBusBuilder
{
    IServiceCollection Services { get; }

    string? RegistrationName { get; }
}
```

Remoting：

```csharp
var builder = Host.CreateApplicationBuilder(args);

builder.Services
    .AddRocketMQRemoting(options =>
    {
        options.NamesrvAddr = "localhost:9876";
    })
    .AddRemotingEventBus(options =>
    {
        options.GroupName = "ordering-service";
        options.MaxConcurrency = 8;
    })
    .AddHandlersFromAssemblyOf<Program>();

var app = builder.Build();
await app.RunAsync();
```

RocketMQ 5 gRPC：

```csharp
var builder = Host.CreateApplicationBuilder(args);

builder.Services
    .AddRocketMQGrpc(options =>
    {
        options.Endpoint = "http://localhost:8081";
    })
    .AddGrpcEventBus(options =>
    {
        options.GroupName = "ordering-service";
        options.MaxConcurrency = 8;
    })
    .AddHandlersFromAssemblyOf<Program>();

var app = builder.Build();
await app.RunAsync();
```

提供两个程序集扫描入口：

```csharp
IEventBusBuilder AddHandlersFromAssemblyOf<TMarker>(
    ServiceLifetime handlerLifetime = ServiceLifetime.Scoped);

IEventBusBuilder AddHandlersFromAssembly(
    Assembly assembly,
    ServiceLifetime handlerLifetime = ServiceLifetime.Scoped);
```

泛型方法通过标记类型方便地定位程序集；`AddHandlersFromAssembly` 支持来自配置、插件发现或其他运行时来源
的程序集。两个方法使用相同的扫描行为和默认 Handler 生命周期。

程序集扫描会：

1. 查找实现一个或多个闭合构造 `IIntegrationEventBusHandler<TIntegrationEvent>` 接口的具体非抽象类；
2. 把每个 Handler 注册到 Microsoft DI，默认生命周期为 `Scoped`；
3. 通过公开无参构造函数构造每种事件两次，读取 `Topic` 和 nullable `Tag`，并验证路由元数据保持稳定；
4. 在 Host 启动前验证重复注册和存在歧义的路由；
5. 按 Topic 合并事件 Tag，使用序号比较排序；全部路由带 Tag 时通过 ` || ` 连接，存在无 Tag 路由时使用 `*`，再配置
   底层 Push Consumer 订阅；
6. 保存路由表，用收到的 Topic 与 Tag 选择对应的已注册事件类型。

同一 Consumer Group 的每个部署必须发现完全相同的事件路由。底层 RocketMQ 客户端要求同组所有成员使用一致的
Topic 集合和自动生成的 Tag `FilterExpression`。

### 注册顺序与启动校验

Handler 注册只允许发生在启动阶段，必须在 `BuildServiceProvider()` 或 `HostApplicationBuilder.Build()` 之前
完成。首版不会在应用的 Service Provider 构建完成后修改路由表。

`AddHandlersFromAssemblyOf` 和 `AddHandlersFromAssembly` 只会在配置 Service Collection 时动态发现类型，并不是
运行时注册 API。默认/named EventBus registration、直接注册或扫描发现的 Handler、Serializer 选择和自动生成的
订阅都必须在 Host Build 前完成。首版不提供运行时增删、Plugin Reload、Consumer 重启或分布式订阅变更协调。
Microsoft DI 会在构建 Provider 时捕获 Service Descriptor，之后再修改 `IServiceCollection` 不会影响这个 Provider。
运行时，Core 会从该 Provider 自己的 EventBus registration 快照生成不可变 Route Plan。`IEventBusBuilder` 只是配置
对象，应用不应把它保留为运行时控制入口。

直接调用 `AddHandler<THandler>()` 时保留调用顺序。程序集扫描会先按 Handler 的完整类型名称进行序号排序，再
完成注册，避免不同进程中的反射枚举顺序改变分发顺序。如果业务逻辑依赖特定顺序，应直接逐个注册 Handler；
首版不提供优先级元数据。

在同一个 EventBus registration 内，使用相同生命周期重复注册同一个事件/Handler 组合是幂等的。如果同一个
Handler 类型在该注册内使用不同生命周期，启动时直接报错，避免最终结果依赖注册顺序。该 Handler 类型如果加入
任何其他默认或 named EventBus registration，也会直接报错。这个所有权规则为协议桥接层提供唯一的内部 anchor，
并让跨 registration 的 Handler 意图保持明确。

出现以下任一情况时，注册过程会在 Generic Host 启动前失败：

- 事件类型是抽象类、没有继承 `IntegrationEvent`，或者没有公开无参构造函数；
- 创建事件实例失败、`Topic` 为空，或者非 `null` 的 `Tag` 为空；
- `Topic` 或 `Tag` 是 `*`，或者任一路由值包含表达式操作符 `||`；
- 两个不同事件类型声明了相同的 `(Topic, Tag)` 路由；
- Handler 类型是抽象类、开放泛型，或者没有实现闭合的
  `IIntegrationEventBusHandler<TIntegrationEvent>` 接口；
- 同一个 Handler 类型在同一 EventBus registration 内配置了相互冲突的生命周期；
- 同一个 Handler 类型被注册到 Service Collection 中的多个 EventBus registration；
- 已经注册至少一个 Handler，但没有配置 Consumer Group；
- 应用在 EventBus 管理的 Push Consumer 上手动添加了 Topic 订阅。

EventBus 不会对 `Topic` 和非 `null` 的 `Tag` 做裁剪或其他标准化；它们使用区分大小写的序号比较，并按声明值
原样发送。`null` Tag 始终保持为 `null`，不会转换成字面量 `*`。因此事件构造函数必须使用稳定常量，不能执行
I/O，也不能根据不同进程的状态动态生成路由。

字面量 Tag 排序，以及存在无 Tag 路由时确定性地选择 `*`，可以保证生成的 `FilterExpression` 完全确定。反射
枚举顺序或 Handler 注册顺序不会导致同一个 Consumer Group 的不同成员生成不同的订阅字符串。

Push Consumer 的全部订阅都由 EventBus 管理。应用可以配置 group name、并发度、重试、超时、预取和缓存参数，
但不能在 `AddRemotingEventBus` 或 `AddGrpcEventBus` 中调用 `Subscribe`。Remoting 适配器会拒绝 `Broadcasting`，
并强制设置 `ConsumeMessageBatchSize = 1`。

### 单个 Handler 注册

应用可以直接注册一个 Handler，而不用扫描它所在的完整程序集：

```csharp
IEventBusBuilder AddHandler<THandler>(
    ServiceLifetime handlerLifetime = ServiceLifetime.Scoped);

eventBusBuilder.AddHandler<OrderSubmittedIntegrationEventHandler>();
```

`AddHandler<THandler>()` 只检查 `THandler`，并注册它实现的全部闭合
`IIntegrationEventBusHandler<TIntegrationEvent>` 接口。因此，一个实现类如果处理多种事件，会注册它声明的全部
事件/Handler 组合。

该方法默认使用 `Scoped`；调用方可以显式传入 `Transient` 或 `Singleton`。重复扫描或注册同一个事件/Handler
组合是幂等的，不会导致同一条消息重复调用同一个 Handler。如果以后出现明确的 trimming 或 Native AOT 需求，
再单独增加对应的注册机制，不为此扩大日常 API。

Remoting 适配器强制设置 `ConsumeMessageBatchSize = 1`，从而把传输层的批量回调约束为 EventBus 每次只处理
一条消息。底层的 `PullBatchSize` 与 `PopBatchSize` 仍可大于 1，以保留接收效率。gRPC Push Consumer 原生就是
每条消息调用一次 Handler。因此两个适配器都保证每次 EventBus 调用只反序列化并分发一条消息。

### 自定义序列化器

EventBus Builder 提供与传输无关的替换入口：

```csharp
builder.Services
    .AddRocketMQGrpc(options => options.Endpoint = "http://localhost:8081")
    .AddGrpcEventBus(options => options.GroupName = "ordering-service")
    .UseSerializer<MyIntegrationEventSerializer>()
    .AddHandlersFromAssemblyOf<Program>();
```

`MyIntegrationEventSerializer` 实现 `IIntegrationEventSerializer`，并通过依赖注入按 Singleton 解析。由于不同
消息可能并发处理，自定义序列化器必须保证线程安全。

### 发布事件

```csharp
public sealed class OrderApplicationService(IEventBus eventBus)
{
    public Task SubmitAsync(Guid orderId, decimal total, CancellationToken cancellationToken)
    {
        var integrationEvent = new OrderSubmittedIntegrationEvent
        {
            OrderId = orderId,
            Total = total
        };

        return eventBus.PublishAsync(integrationEvent, cancellationToken);
    }
}
```

序列化或传输发送失败会通过 `PublishAsync` 以 `EventBusPublishException` 传播。Remoting 适配器还必须把任何
非成功 `RemotingSendStatus` 视为发布失败，不能静默丢弃该结果。

只有对应默认或 named EventBus 注册传入了 `configureProducer`，DI 中才会注册 `IEventBus`。如果纯消费应用误把
发布器作为依赖，标准 DI 解析会直接失败，而不是返回一个永远无法成功执行 `PublishAsync` 的对象。

取消令牌是可选的。取消只会停止等待本地发送操作，无法撤回 Broker 可能已经接收的消息。因此，在发送结果不明确
时重试，仍可能重复发布同一个事件。

## Generic Host 生命周期

`AddRemotingEventBus` 和 `AddGrpcEventBus` 只组合实际配置的底层客户端角色。这些角色已经注册了协议专用
`IHostedService`，因此 Generic Host：

1. 构建并验证事件路由表；
2. 仅在传入 `configureProducer` 时启动 Producer；
3. 仅在已注册 Handler 时启动 Push Consumer 及其订阅；
4. Host 关闭时停止接收新消息；
5. 按相反顺序停止并释放传输资源。

在 Generic Host 中运行的应用不能手动调用底层客户端的 `StartAsync` 或 `StopAsync`。复用主客户端的
HostedService 也可以避免 EventBus 适配器再创建一套重复的后台循环或传输生命周期。

每个默认或 named registration 都有自己可选的 Producer HostedService 和 Push Consumer HostedService。纯消费
注册不会构造或启动 Producer；纯发布注册不会构造或启动 Push Consumer。

## 日志

两个适配器默认使用 `Microsoft.Extensions.Logging` 输出结构化日志。发布日志和 Consumer 最终结果日志会在结构化
`Payload` 字段中，以单行 JSON 记录完整消息内容；Consumer 反序列化失败日志例外，会省略该字段。

| 事件 | 默认级别 | 结构化字段 |
| --- | --- | --- |
| EventBus 注册的全部订阅物化完成 | `Information` | 注册名（默认注册使用 `<default>`）、Consumer Group、Handler 数量、订阅数量，以及按序号排序的 Topic 与 Tag `FilterExpression` 明细 |
| 发布完成 | `Information` | Topic、Tag、Broker Message ID、耗时和 `Payload` |
| 发布失败或返回非成功结果 | `Error` | Topic、Tag、耗时、异常或传输结果，以及可以生成时的 `Payload` |
| Consumer 分发结果为 `Success` | `Information` | Topic、Tag、Message ID、Broker 名称、Queue ID、Queue Offset、投递次数、耗时、结果和 `Payload` |
| Handler 或依赖失败后，EventBus 请求 `Retry` | `Error` | 相同的投递字段、重试结果、可以获得的异常和 `Payload` |
| Consumer 因路由未知选择 `DeadLetter` | `Error` | 可获得的投递字段、结果，以及来自实际 Body 的 `Payload` |
| Consumer 因反序列化失败选择 `DeadLetter` | `Error` | 可获得的投递字段和结果；不包含 `Payload` 字段 |

消费超时和投递 Scope 生命周期失败由主客户端负责处理与记录。它们可能在 EventBus 分发调用已经返回或被放弃后
触发传输层重试，因此不会再生成一个新的 EventBus 结果。

日志按 registration 隔离，默认开启并包含 Payload：

```csharp
eventBusBuilder.ConfigureLogging(options =>
{
    options.Enabled = true;
    options.IncludePayload = false;
});
```

`Enabled = false` 会关闭该 registration 的全部 EventBus 日志，包括订阅汇总。`IncludePayload = false` 保留其他
EventBus 日志，但省略该字段并跳过正文格式化。两个开关都不影响底层 RocketMQ 客户端日志，并且都会物化到 Service
Provider 的不可变 registration 快照中。

应用使用标准日志分类过滤规则控制详细程度。例如，下面的配置保留 gRPC EventBus 成功日志，同时隐藏 Remoting
EventBus 成功日志：

```json
{
  "Logging": {
    "LogLevel": {
      "Default": "Information",
      "EventHorizon.RocketMQ.Grpc.EventBus": "Information",
      "EventHorizon.RocketMQ.Remoting.EventBus": "Warning"
    }
  }
}
```

也可以使用代码配置相同的过滤规则：

```csharp
builder.Logging.AddFilter(
    "EventHorizon.RocketMQ.Remoting.EventBus",
    LogLevel.Warning);
```

Logger category 使用适配器命名空间，因此适用标准的前缀匹配规则。应用仍需自行管理业务 Handler 的日志。完整
`Payload` 可能包含凭据、个人信息或其他敏感应用数据；部署时必须配置合适的分类过滤、保留周期、导出策略和访问
控制。

使用默认序列化器时，日志直接解析实际 UTF-8 JSON Body 并压成单行，不会再次序列化事件。使用自定义序列化器时，
只要能获得事件对象，就用内置 Newtonsoft.Json 生成日志视图；wire bytes 仍完全由自定义序列化器控制。无法获得
事件对象是因为路由未知时，会记录实际 Body。非 JSON 或无效 UTF-8 Body 使用
`{"encoding":"base64","data":"..."}` JSON 包装。Consumer 反序列化失败绝不记录 Body。如果发布序列化在生成
Body 前失败，并且无法生成日志视图，`Payload` 为 null。日志格式化或 Logger Provider 失败不得改变发布或消费行为。

每次 Host 成功启动时，每个包含 Consumer 的 EventBus registration 在路由校验和全部本地 `Subscribe` 调用完成
后只输出一次订阅汇总，不会为每个 Handler 单独输出。该日志描述客户端最终生效的订阅配置，不代表 Broker 已经
确认或持久化订阅。明细使用与 Consumer 配置相同的 Topic 确定性排序和 Tag 序号排序。没有 Handler 的注册不会
创建 Consumer，也不会输出订阅汇总。

## 投递与失败语义

EventBus 会为每次完成的分发尝试给出以下内部结果：

| 情况 | EventBus 结果 |
| --- | --- |
| 路由匹配、反序列化成功且全部 Handler 成功 | `Success` |
| Handler 或它的依赖抛出异常 | `Retry` |
| `ConsumeTimeout` 到期 | `Retry`，由底层 Push Consumer 执行 |
| 反序列化失败，或序列化器返回无效事件 | `DeadLetter` |
| 收到的 Topic 与 Tag 没有匹配事件 | `DeadLetter` |
| Host 停止并取消本次投递 | 继续传播取消，不强制生成新结果 |

协议适配器再把内部分类映射到主 Client 的结果。Remoting 保留三个结果；gRPC 将 `Retry` 和 `DeadLetter` 都映射为
`Failure`，消息只有在达到 Consumer Group 的重试上限后才由服务端转入 DLQ。完整映射见
[`ConsumeResult` 处理设计](consume-result-design.md)。

两个适配器都不提供 exactly-once 投递。重试可能与忽略取消信号的 Handler 重叠；多个 Handler 中已经执行成功的
部分也可能再次执行。消费端必须保证副作用幂等。

完整的判断顺序、异常边界、多 Handler 行为、传输层消息处置和默认日志级别见
[`ConsumeResult` 处理设计](consume-result-design.md)。

## 测试、环境与示例

仓库同时提供单元测试、基于 Docker 的集成测试、可运行的 Consumer 和 Web API Publisher 示例，以及手工使用的多 Broker 环境。
集成测试与手工环境有意保持独立：

- `tests/it/EventHorizon.RocketMQ.EventBus.IntegrationTestInfrastructure` 使用 Testcontainers 和动态端口创建临时
  三 Broker 拓扑；
- 两个协议的集成测试项目分别管理对应 fixture 的生命周期和断言；
- `test-environments/rocketmq-multi-broker/compose.yaml` 是另一套固定端口环境，用于运行 samples、手工验证和
  复现问题。

两个集成测试 fixture 都使用一个 NameServer 和三个相互独立的 Master Broker。gRPC fixture 额外启动
cluster-mode Proxy，只向测试进程公开 Proxy Endpoint，Broker 使用 Docker 网络别名。Remoting fixture 则把
NameServer 和每个 Broker 直接暴露给测试进程，并使用宿主机可达的 Broker 公布地址。两种寻址方式并不兼容，
因此不会用一个带模式分支的 fixture 伪装成同一种拓扑。

Unit Tests 在不使用 Docker 的情况下覆盖 Core 和两个适配器。Compatibility Tests 同时引用三个生产项目，验证
API 对称性、独立的传输层枚举与映射、Core 自有泛型 registration accessor 边界隔离，以及默认/named DI 行为。每个
Integration Test Suite 都在 Generic Host 中启动 Producer 和 Push Consumer，并发发布十二条带 Tag 和十二条无 Tag 的
事件，验证匹配 Handler 对每个事件只观察到一次，并确认三个 Broker 都存储了消息。Remoting Suite 还会使用独立的
Topic 与 Group，通过成功的 `ack` settlement Activity 验证 Broker 分配的 POP；原有流程继续覆盖默认的 Client
分配 PULL。其余确定性的结果映射、Retry、DeadLetter、named registration 和生命周期分支由 Unit Tests 覆盖。

Samples 沿用主项目按协议组织的方式。每个适配器分别提供 Web API Publisher 与 Generic Host Consumer 示例，让未使用
的传输角色确实不存在这一行为保持可见；默认 registration 和 `orders` named registration 放在同一个协议 sample 中，
展示 keyed `IEventBus` 解析如何隔离路由、序列化器、Handler 和生命周期。

完整的项目矩阵、拓扑归属、CI 生命周期和 samples 要求见[测试设计](testing-design.md)。

## 仓库结构

仓库采用以下结构：

```text
.
|-- .github/workflows/
|   |-- dotnet-build.yml
|   `-- publish.yml
|-- docs/
|   |-- en-US/
|   |   |-- README.md
|   |   |-- consume-result-design.md
|   |   |-- event-bus-design.md
|   |   |-- serialization-design.md
|   |   `-- testing-design.md
|   `-- zh-CN/
|       |-- README.md
|       |-- consume-result-design.md
|       |-- event-bus-design.md
|       |-- serialization-design.md
|       `-- testing-design.md
|-- samples/
|   |-- README.md
|   |-- README.zh-CN.md
|   |-- grpc/
|   |   |-- Consumer/
|   |   `-- Publisher/
|   `-- remoting/
|       |-- Consumer/
|       `-- Publisher/
|-- src/
|   |-- EventHorizon.RocketMQ.EventBus/
|   |-- EventHorizon.RocketMQ.Grpc.EventBus/
|   `-- EventHorizon.RocketMQ.Remoting.EventBus/
|-- test-environments/
|   |-- README.md
|   |-- README.zh-CN.md
|   `-- rocketmq-multi-broker/
|       |-- compose.yaml
|       |-- README.md
|       `-- README.zh-CN.md
|-- tests/
|   |-- it/
|   |   |-- README.md
|   |   |-- README.zh-CN.md
|   |   |-- EventHorizon.RocketMQ.EventBus.IntegrationTestInfrastructure/
|   |   |-- EventHorizon.RocketMQ.Grpc.EventBus.IntegrationTests/
|   |   `-- EventHorizon.RocketMQ.Remoting.EventBus.IntegrationTests/
|   `-- ut/
|       |-- EventHorizon.RocketMQ.EventBus.Tests/
|       |-- EventHorizon.RocketMQ.EventBus.Compatibility.Tests/
|       |-- EventHorizon.RocketMQ.Grpc.EventBus.Tests/
|       `-- EventHorizon.RocketMQ.Remoting.EventBus.Tests/
|-- .editorconfig
|-- .gitignore
|-- AGENTS.md
|-- EventHorizon.RocketMQ.EventBus.slnx
|-- global.json
|-- LICENSE
|-- README.md
|-- README.zh-CN.md
`-- codecov.yml
```

仓库遵循主客户端的仓库约定：C# 12、nullable reference types、完整的公开 API XML 文档、xUnit v3 单元
测试、覆盖两种协议的 Docker 集成测试、可运行的 Consumer 和 Web API Publisher 示例、格式检查/构建/测试 CI、NuGet 包发布、
符号包和双语包内 README。gRPC 包内 README 会说明仅通过 Proxy 连接及 Client 发起的长轮询；Remoting 包内 README
会说明 NameServer 路由发现、直连 Broker 公布地址、Client 发起的 PULL/POP 长轮询和仅支持集群消费模式。Core 包内
README 保持协议无关，并链接到两个适配器。本仓库使用 MIT License，而不是主客户端的 Apache-2.0 License。

## 设计决策

1. 每个 `(Topic, Tag)`（包括表示无 Tag 消息的 `null` Tag）只映射一个事件类型；同一事件类型可以注册多个
   Handler，并按顺序执行。
2. `Topic` 和 nullable `Tag` 是不可变的传输元数据，由事件的公开无参构造函数重建，不进入 JSON 消息体。某个
   Topic 只要包含无 Tag 路由，Consumer 就使用 `*` 过滤表达式。
3. 每个具体集成事件类型都必须提供公开无参构造函数。注册过程使用它发现路由，无需 Attribute、static
   abstract 成员或应用服务。
4. Handler 失败时产生内部 `Retry`；反序列化失败和未知路由产生内部 `DeadLetter`。Remoting 可以请求立即进入
   DLQ；gRPC 会把两类失败都映射为 `Failure`，由服务端重试次数与 DLQ 阈值决定最终处置。
5. EventBus 每次调用只反序列化并分发一条消息，同时保留可配置的传输预取和消费并发度；Remoting Handler
   批量回调固定为 1。
6. 公开命名使用 `EventHorizon.RocketMQ.EventBus`、`IntegrationEvent` 和
   `IIntegrationEventBusHandler<TIntegrationEvent>`。
7. Handler 注册默认使用 `Scoped`；可选 `ServiceLifetime` 参数也接受 `Singleton` 或 `Transient`。Singleton
   Handler 必须保证线程安全。
8. 只有 `configureProducer` 非 `null` 时，`Add*EventBus` 才注册 Producer 并暴露 `IEventBus`；首次注册 Handler
   后再增加 Push Consumer。默认发布注册不使用 key，named 发布注册暴露 keyed `IEventBus`。所有注册都会隔离
   路由、Handler、生命周期、序列化器和实际配置的传输角色。
9. 三个 NuGet 包使用同一个版本和同一个发布 Tag。Core 最先推送，作为两个适配器的未列出传递依赖；随后立即推送
   适配器。
10. Remoting EventBus 消费只使用 `Clustering`，首版不支持 `Broadcasting`。同一个 Push Consumer 可以使用 Client
    分配的 PULL，或 Broker 分配的 PULL/POP，不会改变 EventBus API。
11. 两个适配器默认通过 `Microsoft.Extensions.Logging` 记录发布和消费结果；发布和 Consumer 最终结果会用 JSON
    格式的结构化字段记录完整 `Payload`，应用必须用日志分类过滤规则控制这类可能包含敏感信息的输出。
12. 两个协议的 IT 都使用临时的三 Broker Testcontainers fixture。固定端口的多 Broker Compose 环境与 IT 相互
    独立，只服务于 samples、手工验证和问题复现。
13. 每个包含成功启动 Consumer 的 EventBus registration 只输出一条聚合的 `Information` 订阅汇总，不按 Handler
    分别输出；日志包含完整且顺序确定的 Topic 与 Tag 表达式明细。
14. 发布失败统一使用 Core 的 `EventBusPublishException`；调用方取消仍原样传播 `OperationCanceledException`。
15. 每个消费 registration 用其第一个自有应用 Handler 闭合协议桥接类型。一个 Handler 类型不能属于另一个
    EventBus registration；实现不使用公开 marker，也不接收主客户端内部注册身份。
16. 程序集扫描属于启动阶段类型发现，不是运行时动态注册；从一个 Service Provider 的 registration 快照生成后，
    Route Plan、Handler、Serializer 和订阅在该 Provider 生命周期内保持不变。

## 许可证

本仓库采用 [MIT License](https://opensource.org/license/mit)。
