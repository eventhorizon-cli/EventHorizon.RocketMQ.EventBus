# Documentation

[Project README](../../README.md) | [简体中文](../zh-CN/)

- [EventBus design](event-bus-design.md)
- [`ConsumeResult` handling design](consume-result-design.md)
- [Serialization design](serialization-design.md)
- [Testing, environments, and samples design](testing-design.md)

The EventBus design is the primary specification. The `ConsumeResult` document defines the complete decision table and
the boundary between EventBus handling and transport settlement. The serialization document fixes the default JSON
wire contract and its compatibility rules. The testing document defines UT/IT ownership, multi-Broker topologies,
the independent Compose environment, and runnable samples.
