# 序列化设计

[文档目录](README.md) | [English](../en-US/serialization-design.md) |
[EventBus 详细设计](event-bus-design.md)

本文档定义 gRPC 与 Remoting EventBus 适配器共同使用的默认 JSON 消息格式。对于同一个集成事件，两个适配器必须
生成并读取完全相同的消息 Body 字节。

## 消息格式

默认 `NewtonsoftJsonIntegrationEventSerializer` 按事件的实际具体类型生成紧凑 JSON，再编码成不带 BOM 的
UTF-8。反序列化使用严格的 UTF-8 解码；无效字节序列属于无效消息，并返回 `DeadLetter`。

消息 Body 只包含应用事件数据：

```json
{"OrderId":"353c0bcb-7f6d-49a2-8dd7-d144bee5366a","Total":128.50}
```

`Topic` 和 nullable `Tag` 通过 RocketMQ 传输元数据携带，并使用 `[JsonIgnore]` 排除在消息 Body 之外。`null`
Tag 表示发布消息没有 Tag，既不会写入 Body，也不会转换为字面量 `*`。默认格式没有额外封装、路由、程序集限定
类型名或 `$type` 判别字段；注册得到的 `(Topic, Tag)` 路由是目标事件类型的唯一来源。

## 固定的 Newtonsoft.Json 行为

Core 创建并独占序列化设置，不读取或修改 `JsonConvert.DefaultSettings`。默认行为固定如下：

| 项目 | 默认契约 |
| --- | --- |
| 类型元数据 | `TypeNameHandling.None`；消息中的元数据不能选择 .NET 类型 |
| 元数据属性 | 不解释类型或对象引用元数据 |
| 属性命名 | 使用 `DefaultContractResolver`，按声明的 .NET 成员名输出，不自动转成 camelCase |
| 格式 | 使用 `Formatting.None` 输出紧凑 JSON |
| 文本编码 | 不带 BOM 的严格 UTF-8 |
| Null 成员 | 保留 |
| 默认值成员 | 保留 |
| 未知 JSON 成员 | 反序列化时忽略 |
| 缺失 JSON 成员 | 保留构造函数或 .NET 默认值 |
| 日期 | 使用 ISO 8601，并保留日期与时区的往返语义 |
| Culture | 涉及 Culture 的转换使用 `InvariantCulture` |
| 枚举 | 默认使用 Newtonsoft.Json 的数字表示；事件成员或类型可以自行声明 Converter |
| 对象引用 | 不保留引用；循环引用会导致序列化失败 |
| 最大读取深度 | 反序列化时限制为 64 层 |
| 自定义 Converter | EventBus 不全局注册任何 Converter |

这些值属于首版消息契约，不是可以随进程环境变化的偶然默认值。修改任何一项都必须完成兼容性评审、同步中英文
文档，并更新固定消息样本测试。

## 对象创建与校验

路由表提供准确的已注册事件类型。Newtonsoft.Json 调用事件的公开无参构造函数重建 `Topic` 和 nullable `Tag`，
再填充应用成员。序列化器返回值不能是 `null`，并且必须与请求的已注册事件类型完全一致；即使其他类型可以赋值
给它，也按无效结果处理。

UTF-8 解码、JSON 解析、构造函数执行、成员类型转换、最大读取深度和返回类型校验都属于反序列化阶段。任何一步
失败都会在调用业务 Handler 之前返回 `DeadLetter`，具体规则见 [`ConsumeResult` 处理设计](consume-result-design.md)。

发布时发生序列化失败，会记录 `Error` 日志、包装成 `EventBusPublishException`，并通过 `PublishAsync` 向调用方传播，
不会尝试调用传输层发送。
Newtonsoft.Json 没有与 `JsonSerializerSettings` 对应的写入深度限制，因此 EventBus 不会承诺不存在的发布侧深度
上限：循环引用会失败，最终 Payload 大小边界由所选传输协议配置的最大消息大小决定。

## Schema 演进

默认设置支持增量演进，但不能让任意 Schema 变更自动兼容：

- 新增可选成员与旧消息兼容，因为缺失成员会保留默认值；
- 删除成员与新 Reader 兼容，因为未知 JSON 成员会被忽略；
- 修改成员名属于破坏性变更，除非通过 `[JsonProperty]` 保留旧的消息字段名；
- 修改成员的 JSON 结构或改成不兼容的 .NET 类型属于破坏性变更；
- 修改数字范围或枚举表示方式可能造成破坏性变更；
- 新增必填校验规则可能让过去的有效消息变成无效消息；
- 修改 `(Topic, Tag)`，包括在字面量 Tag 与 `null` 之间切换，会形成另一条路由，必须按消息契约迁移处理。

EventBus 不提供 Schema Registry，也不内置 Schema Version 字段。需要显式版本协商的应用可以增加普通事件属性，
例如 `SchemaVersion`，并在滚动发布期间保持新旧 Reader/Writer 相互兼容。

跨服务共享的事件类型应放在应用自己的契约包中，该包只引用 `EventHorizon.RocketMQ.EventBus`。对外部行为重要的
事件契约应保存固定 JSON 样本。

## 自定义序列化器

应用实现 `IIntegrationEventSerializer` 并调用 `UseSerializer<TSerializer>()`，即可同时替换序列化与反序列化。
替换实现按所属 EventBus registration 的私有 token 注册为 keyed Singleton，并且必须：

- 支持发布与消费并发调用，保证线程安全；
- 对相同事件契约产生确定性结果；
- 不产生外部副作用，也不依赖临时外部服务；
- 严格处理无效输入和错误的返回事件类型；
- 在滚动发布期间能够读取对应 Producer 写出的全部消息。

EventBus 仍然只通过传输层 `Topic` 和 `Tag` 获取路由，自定义消息 Body 不能覆盖已注册路由。自定义序列化器可以
选择其他 Body 格式，但同一路由的 Producer 与 Consumer 必须一起部署兼容实现。

同一个 Serializer 类型用于两个默认或 named EventBus registration 时，会创建两个独立 Singleton 实例，因此它们
的依赖和可变状态仍保持注册内隔离。每个实例所在注册仍可能并发发布和消费，所以依然必须保证线程安全。

## 日志表示

结构化 `Payload` 字段是诊断视图，不是第二套 wire contract。使用默认序列化器时，EventBus 直接解析并压缩实际
UTF-8 JSON Body，不会再次序列化事件。使用自定义序列化器并且能够获得事件对象时，EventBus 使用内置
Newtonsoft.Json 生成可读的单行 JSON 视图；传输字节仍然只由自定义序列化器负责读写。

路由未知时，日志使用实际 Body。有效 JSON 会压成单行；非 JSON 或无效 UTF-8 会表示为
`{"encoding":"base64","data":"..."}`。Consumer 反序列化失败时完全省略 Body。如果发布序列化在产生 Body 前
就失败，并且无法生成 JSON 视图，`Payload` 为 null。诊断序列化失败时会回退到 wire body，并且绝不能改变发布或
消费结果。

`EventBusLoggingOptions.Enabled` 和 `IncludePayload` 都默认为 `true`，通过 `ConfigureLogging` 按 registration 配置。
关闭 Payload 后不会执行诊断序列化，并且会移除对应结构化字段。

完整 `Payload` 可能暴露凭据、个人信息或其他敏感应用内容。应用必须把 EventBus 日志视为消息数据存储，并配置
适当的分类过滤、保留周期、导出策略和访问控制。

## 兼容性测试

Core 单元测试使用固定消息样本验证属性名、紧凑 UTF-8 字节、Null/默认值、缺失/新增字段、最大读取深度、忽略类型
元数据、进程默认设置隔离，以及排除 `Topic` 和 `Tag`。

两个适配器的 Unit Test 分别验证各自默认 JSON 发布与消费路径，也验证应用替换的自定义序列化器会控制双向传输
字节，而日志字段使用内置 Newtonsoft.Json 诊断视图。跨包 Compatibility Test 验证适配器公开边界的对称性，以及
两个传输层结果类型保持独立。
