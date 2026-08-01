# 文档

[项目 README](../../README.zh-CN.md) | [English](../en-US/)

- [EventBus 详细设计](event-bus-design.md)
- [`ConsumeResult` 处理设计](consume-result-design.md)
- [序列化设计](serialization-design.md)
- [测试、环境与示例设计](testing-design.md)

EventBus 详细设计是主要规范；`ConsumeResult` 文档单独说明完整的结果判断表，以及 EventBus 处理逻辑与传输层消息
处置之间的边界；序列化文档固定默认 JSON 消息格式及其兼容性规则；测试文档定义 UT/IT 归属、多 Broker 拓扑、
独立 Compose 环境和可运行 samples。
