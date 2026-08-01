# ConsumeResult 处理设计

[文档目录](README.md) | [English](../en-US/consume-result-design.md) |
[EventBus 详细设计](event-bus-design.md)

本文档说明 EventBus 适配器处理一条 RocketMQ 消息时，如何选择传输层的 `ConsumeResult`。业务 Handler 不返回
`ConsumeResult`，只返回 `Task`。EventBus 会综合路由查找、反序列化以及全部 Handler 的执行结果，得出一个最终
结果。

两个传输包分别定义了含义一致的类型：

- `EventHorizon.RocketMQ.Grpc.Consumer.ConsumeResult`
- `EventHorizon.RocketMQ.Remoting.Consumer.ConsumeResult`

适配器返回其所属传输包中的类型，但 gRPC 与 Remoting 使用完全相同的判断规则。

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

内部结果包含相同的三种语义，但不是面向应用的公开契约。两个适配器分别通过显式 `switch` 映射到自己的传输层枚举，
不能依赖整数值直接转换，因为主项目的两个包可以独立演进。适配器单元测试会覆盖每个映射，避免以后增加或调整枚举
值时静默改变行为。

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
| 收到的 `(Topic, Tag)` 没有匹配的注册路由 | `DeadLetter` | 重复投递同一消息无法补上缺失的启动注册 |
| 消息体无法反序列化成路由选定的事件类型 | `DeadLetter` | 消息体与路由不匹配，重试无法修复 |
| 自定义序列化器返回 `null`、返回了其他事件类型，或者违反序列化接口约定 | `DeadLetter` | 适配器将其视为无效消息或无效序列化结果 |
| 路由存在，但内部注册状态不一致，找不到可执行的 Handler | `DeadLetter` | 这是不可自动恢复的配置错误，同时会记录错误日志 |
| 底层 Consumer 正在停止，并取消本次投递 | 适配器不强制返回结果 | 取消会继续传给 Consumer，由它按协议完成正常停止和消息处置 |

EventBus 捕获异常后绝不会返回 `Success`。未知路由和无效消息也不会进入重试，因为它们在当前部署中属于确定性
错误。

## 处理流程

每条消息按以下顺序处理：

```text
收到一条消息
     |
     v
查找 (Topic, Tag) ------------------- 未找到 ------> DeadLetter
     |
   已找到
     v
反序列化一次 ------------------------- 失败 ------> DeadLetter
     |
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

Host 停止与消费超时不同。Consumer 的停止 token 被取消时，适配器不会把它转换成新的 `Retry` 或
`DeadLetter` 决策，而是继续传播取消，让底层 Consumer 停止接收消息，并保留相应协议的消息处置逻辑。

## 反序列化失败

反序列化包括 UTF-8 解码、JSON 解析、对象创建、成员类型转换以及返回事件实例的校验。任何一步失败都返回
`DeadLetter`，并且不会调用业务 Handler。

这条规则同样适用于自定义 `IIntegrationEventSerializer`。自定义实现应当是确定性的、没有外部副作用且线程
安全。依赖临时外部服务的序列化器不属于预期用法；EventBus 无法可靠区分外部服务故障和无效消息体。

## 传输层如何处置结果

`ConsumeResult` 表达 EventBus 的处理决定，真正与 Broker 交互的是底层客户端：

| EventBus 结果 | gRPC Push Consumer | Remoting Push Consumer |
| --- | --- | --- |
| `Success` | 确认消息 | 提交这一条消息 |
| `Retry` | 修改不可见时间，使消息可以重新投递；最终策略由 Broker 决定 | 将这一条消息发回 Broker，延迟后重新投递 |
| `DeadLetter` | 将消息转发到死信队列 | 将这一条消息直接发送到死信队列 |

Remoting EventBus 会固定 `ConsumeMessageBatchSize = 1`，因此批次级 `ConsumeResult` 和 `AckIndex` 规则不会在
EventBus 中产生部分成功结果。网络层仍可一次预取多条消息，但每条消息都会独立进入 EventBus 分发。

当投递次数已经达到传输层配置的上限时，底层 Consumer 可以把 EventBus 返回的 `Retry` 最终转入死信队列。
这不会改变适配器的判断：EventBus 仍记录并返回 `Retry`，重试次数和最终 DLQ 阈值由传输层负责。

如果确认、安排重试或转发死信失败，底层客户端仍可能重新投递消息。因此，即使返回 `Success`，也不代表
exactly-once 投递。

## 日志

适配器使用结构化字段记录最终结果，其中包括 JSON 格式的完整 `Payload`：

| 结果 | 默认级别 | 附加信息 |
| --- | --- | --- |
| `Success` | `Information` | Topic、Tag、Message ID、Broker 名称、Queue ID、Queue Offset、投递次数、耗时和 `Payload` |
| EventBus `Retry` | `Error` | 相同的投递字段、`Payload`，以及可以获得的 Handler 或依赖异常 |
| `DeadLetter`，路由未知 | `Error` | 可以获得的投递字段、结果，以及来自实际 Body 的 `Payload` |
| `DeadLetter`，反序列化失败 | `Error` | 可以获得的投递字段和结果；不包含 `Payload` 字段 |

应用可以通过标准的 `Microsoft.Extensions.Logging` 日志分类过滤规则改变实际输出级别。适配器命名空间就是
日志分类前缀。

使用自定义序列化器时，成功反序列化得到的事件会通过内置 Newtonsoft.Json 生成日志视图。路由未知时记录实际
Body，非 JSON 字节使用 Base64 JSON 包装；反序列化失败时始终省略消息 Body。完整字段可能包含敏感数据，因此应用
必须配置适当的 `EventBusLoggingOptions`、分类过滤、保留周期和日志访问权限。

在首版 API 中，EventBus 自己选择的 `Retry` 属于错误，因为业务 Handler 不能主动请求重试；EventBus 只会在
Handler 或依赖失败后选择它。消费超时、投递 Scope 生命周期失败，以及可恢复的传输层处置失败都属于底层
RocketMQ 客户端职责，并遵循主客户端自己的日志 category 和级别。EventBus 结果日志只描述其分发调用；如果随后
释放 Scope 失败，最终投递处置应以主客户端的错误与重试日志为准。Host 正常停止所触发的取消不会产生 EventBus
`Retry` 错误日志。
