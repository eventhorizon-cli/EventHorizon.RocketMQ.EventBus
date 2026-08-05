# EventHorizon.RocketMQ.EventBus

[English](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/src/EventHorizon.RocketMQ.EventBus/README.md) |
[简体中文](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/src/EventHorizon.RocketMQ.EventBus/README.zh-CN.md)

> 这是 EventBus 协议适配器内部共用的支持包，请勿在应用中直接安装。

请根据服务使用的 RocketMQ 协议安装对应适配器：

```shell
dotnet add package EventHorizon.RocketMQ.Grpc.EventBus
# 或者
dotnet add package EventHorizon.RocketMQ.Remoting.EventBus
```

所选适配器会自动引入本包。本包包含共用的事件、处理器、序列化、路由和分发契约，但不会连接 RocketMQ，也不提供
可独立使用的客户端。

安装、配置和使用方式请参阅
[gRPC 适配器说明](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/src/EventHorizon.RocketMQ.Grpc.EventBus/README.zh-CN.md)
或
[Remoting 适配器说明](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/src/EventHorizon.RocketMQ.Remoting.EventBus/README.zh-CN.md)。

架构与包边界的技术细节请参阅
[EventBus 详细设计](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/docs/zh-CN/event-bus-design.md)。

## 许可证

本包采用 [MIT License](https://github.com/eventhorizon-cli/EventHorizon.RocketMQ.EventBus/blob/main/LICENSE)。
