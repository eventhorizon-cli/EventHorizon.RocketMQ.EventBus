# ConsumeResult 处理设计

[文档目录](README.md) | [English](../en-US/consume-result-design.md) |
[EventBus 详细设计](event-bus-design.md)

本文档说明 EventBus 适配器处理一条 RocketMQ 消息时，如何完成内部分类，再映射到传输层的 `ConsumeResult`。
业务 Handler 不返回 `ConsumeResult`，只返回 `Task`。EventBus 会综合路由查找、反序列化以及全部 Handler 的执行
结果，得出一个内部结果。

两个传输包有意使用不同的结果契约：

- `EventHorizon.RocketMQ.Grpc.Consumer.ConsumeResult`（gRPC 客户端 [`grpc-v0.4.1`](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/grpc-v0.4.1/src/EventHorizon.RocketMQ.Grpc/Consumer/ConsumeResult.cs)）是 sealed record，包含 `Success`、`Failure`
  结果以及 `Suspend(TimeSpan)` 工厂方法。EventBus 使用普通 Push 契约，只会发出 `Success` 或 `Failure`；`Suspend`
  是 LitePush 能力，EventBus 不会发出它。
- `EventHorizon.RocketMQ.Remoting.Consumer.ConsumeResult`（Remoting 客户端 [`remoting-v0.6.2`](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ/blob/remoting-v0.6.2/src/EventHorizon.RocketMQ.Remoting/Consumer/ConsumeResult.cs)）是只包含 `Success` 和 `Retry`
  的枚举。EventBus 使用默认延迟执行普通重试，绝不请求直接进入死信队列。

EventBus 对两种协议使用相同的路由、消息体和 Handler 分类规则，再把内部判断映射到所属传输层支持的结果。因此，
两种协议最终执行的消息处置并不完全相同。

## 包边界

两个 `ConsumeResult` 是有意保持独立的 .NET 类型。EventBus 的公开抽象不会引用其中任何一个：

```text
业务 Handler：Task
       |
       v
内部、与传输协议无关的分发结果
       |
       +---------------------------+
       |                           |
       v                           v
gRPC 适配器显式映射             Remoting 适配器显式映射
       |                           |
       v                           v
Grpc.Consumer.ConsumeResult    Remoting.Consumer.ConsumeResult
```

内部结果只有 `Success` 和 `Retry` 两种状态，并不是面向应用的公开契约。两个适配器分别通过显式 `switch` 映射到
自己的传输层结果。启用 `SkipDeserializationFailures` 时，反序列化失败也可能产生 `Success`；附带的
`DeserializationFailed` 诊断标记可以让日志区分“确认消息”和“应用处理成功”。gRPC 将 `Retry` 映射为 `Failure`；
Remoting 将其映射为 `ConsumeResult.Retry`，并保持 `RemotingPushConsumeContext.DelayLevelWhenNextConsume` 的默认值
`0`。适配器不能依赖整数值直接转换；Unit Test 会锁定结果映射，避免主 Client 独立演进时静默改变行为。

这样，`EventHorizon.RocketMQ.EventBus` 不需要引用 gRPC 或 Remoting，两个适配器也不会相互依赖。应用可以同时
引用两个包，不会在已经编译完成的适配器内部产生类型冲突。如果应用自己的代码同时导入两个传输层 Consumer
命名空间，则需要使用完整类型名或 `using` alias；这与是否使用 EventBus 无关。

默认和 named EventBus 注册可以共存。启用 Producer 的默认注册暴露未键控 `IEventBus`；启用 Producer 的 named
registration 使用主项目中的注册名暴露 keyed `IEventBus`。纯消费注册不暴露 `IEventBus`，但仍通过同一个注册标识
隔离结果映射、路由和 Handler。这与两个传输包各自定义的 `ConsumeResult` 类型标识无关。

## 判断表

| 处理情况 | 最终结果或处置 | 原因 |
| --- | --- | --- |
| 路由存在、反序列化成功，而且所有已注册 Handler 都成功完成 | `Success` | 消息已经完整处理，可以确认 |
| 解析或执行应用 Handler 失败 | `Retry` | EventBus 将应用异常视为暂时性故障，并返回内部重试结果 |
| 主客户端创建投递 Scope、解析协议桥接层或异步释放 Scope 失败 | 底层 Consumer 重试；这次失败调用没有 EventBus 返回结果 | 投递 Scope 由主客户端拥有，生命周期异常由主客户端映射到传输层重试行为 |
| Handler 没有在底层 Consumer 的 `ConsumeTimeout` 内完成 | 底层 Consumer 重试；忽略 EventBus 随后产生的结果 | 超时控制和消息处置属于主客户端职责 |
| 收到的 `(Topic, Tag)` 没有匹配的注册路由 | `Retry` | EventBus 不再请求直接进入 DLQ；由传输层执行普通重试和最终处置策略 |
| 反序列化失败且 `SkipDeserializationFailures = true`（默认） | `Success`，并带有 `DeserializationFailed = true` | 记录无效投递并确认消息，不调用业务 Handler |
| 反序列化失败且 `SkipDeserializationFailures = false` | `Retry`，并带有 `DeserializationFailed = true` | 在传输层正常重新投递时再次尝试反序列化，不请求直接进入 DLQ |
| 自定义序列化器返回 `null`、返回了其他事件类型，或者违反序列化接口约定 | 按 `SkipDeserializationFailures` 处理 | 自定义序列化器失败与默认序列化器遵循相同的 registration 级策略 |
| 路由存在，但内部注册状态不一致，找不到可执行的 Handler | `Retry` | 记录配置错误，并请求普通重试，而不是直接进入 DLQ |
| 底层 Consumer 正在停止，并取消本次投递 | 适配器不强制返回结果 | 取消会继续传给 Consumer，由它按协议完成正常停止和消息处置 |

反序列化异常后的 `Success` 只适用于显式的默认跳过策略，并不表示业务 Handler 曾经运行。所有未跳过的失败在两个
适配器上都变成普通 `Retry`，EventBus 绝不请求直接进入 DLQ。底层 Client 或服务端仍可在正常重试进度结束后，按自身配置
执行最终处置策略。

## 处理流程

每条消息按以下顺序处理：

```text
收到一条消息
     |
     v
查找 (Topic, Tag) ------------------- 未找到 ------> Retry
     |
   已找到
     v
反序列化一次 ------------------------- 失败 ------> 默认跳过时 Success
     |                                      \-> 禁用跳过时 Retry
    成功
     v
按顺序解析并执行 Handler ------------- 异常 ------> Retry
     |
  全部完成
     v
   Success
```

路由查找先于反序列化。消息体不携带 .NET 类型名称，适配器也不会通过消息中的 `$type` 值选择目标类型。

## 多个 Handler

同一事件类型的所有 Handler 会在同一个 DI 作用域中按注册顺序执行。只有全部 Handler 成功完成时才返回
`Success`。

如果 Handler 1 成功、Handler 2 失败，适配器会为整条消息返回 `Retry`。重新投递后，Handler 1 会先于 Handler 2
再次执行。EventBus 不保存每个 Handler 的执行进度，因此所有 Handler 的业务副作用都必须具备幂等性。

当前处理在遇到第一个失败后立即停止，本次不会继续调用后面的 Handler。

## 异常与取消

以下情况由 EventBus 按 Handler 失败处理，并返回 `Retry`：

- Handler 构造函数或依赖解析抛出异常；
- `HandleAsync` 同步抛出异常；
- `HandleAsync` 返回的 `Task` 最终失败；
- Consumer 仍在处理本次投递时，`HandleAsync` 响应投递 token 并抛出 `OperationCanceledException`。

外层投递 Scope 由底层主客户端围绕 EventBus 协议桥接层创建、解析并异步释放。如果这些生命周期操作抛出异常，
桥接层不会产生可用的 EventBus 结果；主客户端 Consumer 会捕获异常并执行对应传输协议的重试行为。EventBus 不会
创建嵌套 Scope，也无法观察其分发调用完成后发生的 Scope 释放异常。

`ConsumeTimeout` 由底层 Push Consumer 执行。超时后，它会请求取消、忽略 Handler 随后返回的成功结果，并按
`Retry` 处置消息。EventBus 无法强制终止业务代码；如果 Handler 忽略取消信号，它可能与重新投递后的新调用
同时运行。

Host 停止与消费超时不同。Consumer 的停止 token 被取消时，适配器不会把它转换成新的 `Retry` 或 `Success` 决策，而是
继续传播取消，让底层 Consumer 停止接收消息，并保留相应协议的消息处置逻辑。

## 反序列化失败

反序列化包括 UTF-8 解码、JSON 解析、对象创建、成员类型转换以及返回事件实例的校验。任何一步失败都不会调用业务
Handler，而是按对应适配器自有 Consumer 配置类型的 `SkipDeserializationFailures` 属性处理。
`GrpcEventBusConsumerOptions` 与 `RemotingEventBusConsumerOptions` 的该属性都默认为 `true`：

```csharp
rocketMQBuilder.AddGrpcEventBus(configureConsumer: options =>
{
    options.SkipDeserializationFailures = false;
});
```

Remoting 适配器通过 `AddRemotingEventBus(configureConsumer: ...)` 使用同一个属性。

| `SkipDeserializationFailures` | 内部结果 | 日志 | 传输层处置 |
| --- | --- | --- | --- |
| `true`（默认） | `Success`，并带有 `DeserializationFailed = true` | `Error`；明确说明已跳过消息，并省略 `Payload` | 确认/提交消息；不运行 Handler |
| `false` | `Retry`，并带有 `DeserializationFailed = true` | `Error`；明确说明请求重试，并省略 `Payload` | 请求正常重新投递；EventBus 不请求直接进入 DLQ |

该配置按每个默认或 named EventBus registration 独立快照化。确认仍可能失败或处于不确定状态，因此被跳过的消息仍可能在
至少一次投递语义下重新投递。

这条规则同样适用于自定义 `IIntegrationEventSerializer`。自定义实现应当是确定性的、没有外部副作用且线程
安全。依赖临时外部服务的序列化器不属于预期用法；EventBus 无法可靠区分外部服务故障和无效消息体。

## 传输层如何处置结果

`ConsumeResult` 是适配器发给客户端的处置信号，真正与 Broker 交互的是底层客户端：

| EventBus 内部结果 | gRPC Push Consumer | Remoting Push Consumer |
| --- | --- | --- |
| `Success` | 返回 `Success` 并确认消息 | 返回 `Success` 并提交这一条消息 |
| `Retry` | 返回 `Failure`；主 Client 安排重新投递，服务端重试策略决定最终结果 | 返回 `Retry`，保持上下文延迟默认值 `0`，按正常延迟重新投递 |

这里的映射在 EventBus 边界上保持一致。EventBus 不会调用直接转发操作，不会设置 Remoting 的负延迟哨兵值，也不会
暴露类似 SimpleConsumer 的处置 API。任何未被跳过的失败都会进入所属传输层的普通重试路径。

Remoting EventBus 会固定 `ConsumeMessageBatchSize = 1`，因此批次级 `ConsumeResult` 和 `AckIndex` 规则不会在
EventBus 中产生部分成功结果。网络层仍可一次预取多条消息，但每条消息都会独立进入 EventBus 分发。

当投递次数达到传输层配置的上限时，底层 Client 或服务端可以把失败投递转入 DLQ。这不会改变 EventBus 的分类或
日志；重试次数和最终 DLQ 阈值属于传输层职责。

如果确认或安排重试失败，底层 Client 仍可能重新投递消息。因此，即使返回 `Success`（包括跳过反序列化失败的情况），
也不代表 exactly-once 投递。

## 日志

适配器使用结构化字段记录最终内部结果，其中包括 JSON 格式的完整 `Payload`：

| 内部结果 | 默认级别 | 附加信息 |
| --- | --- | --- |
| `Success` | `Information` | Topic、Tag、Message ID、Broker 名称、Queue ID、Queue Offset、投递次数、耗时和 `Payload` |
| `Retry` | `Error` | 相同的投递字段、`Payload`，以及可以获得的 Handler 或依赖异常 |
| `Retry`，未知路由或无效注册状态 | `Error` | 可以获得的投递字段、重试结果，以及来自实际 Body 的 `Payload` |
| `Success`，跳过反序列化失败 | `Error` | 可以获得的投递字段、`Success` 结果和明确的跳过动作；不包含 `Payload` 字段 |
| `Retry`，反序列化失败 | `Error` | 可以获得的投递字段、`Retry` 结果和明确的重试动作；不包含 `Payload` 字段 |

应用可以通过标准的 `Microsoft.Extensions.Logging` 日志分类过滤规则改变实际输出级别。适配器命名空间就是
日志分类前缀。

使用自定义序列化器时，成功反序列化得到的事件会通过内置 Newtonsoft.Json 生成日志视图。路由未知时记录实际
Body，非 JSON 字节使用 Base64 JSON 包装；反序列化失败时始终省略消息 Body。完整字段可能包含敏感数据，因此应用
必须配置适当的 `EventBusLoggingOptions`、分类过滤、保留周期和日志访问权限。

在首版 API 中，EventBus 自己选择的 `Retry` 属于错误，因为业务 Handler 不能主动请求重试；EventBus 会在 Handler 或
依赖失败、未知路由、无效注册状态，或禁用跳过时反序列化失败后选择它。跳过反序列化失败时，虽然处置结果是 `Success`，
仍会记录 `Error`，并说明没有运行任何业务 Handler。消费超时、投递 Scope 生命周期失败，以及可恢复的传输层处置失败都
属于底层 RocketMQ 客户端职责，并遵循主客户端自己的日志 category 和级别。EventBus 结果日志只描述其分发调用；如果
随后释放 Scope 失败，最终投递处置应以主客户端的错误与重试日志为准。Host 正常停止所触发的取消不会产生 EventBus
`Retry` 错误日志。
