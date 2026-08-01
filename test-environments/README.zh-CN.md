# 测试环境

[English](README.md) | [简体中文](README.zh-CN.md) |
[测试设计](../docs/zh-CN/testing-design.md)

`test-environments` 提供已实现的固定端口 Docker Compose 环境，用于运行 samples、手工验证和问题复现。

| 环境 | 用途 |
| --- | --- |
| [`rocketmq-multi-broker`](rocketmq-multi-broker/README.zh-CN.md) | 一个 NameServer、三个独立 Master Broker、一个 cluster-mode Proxy、一个资源初始化器，以及两个 EventBus 适配器使用的 sample 资源 |

这些环境与 `tests/it` 相互独立。Integration Tests 通过自己的 Testcontainers fixture 创建动态端口的临时
container，不依赖手工启动 Compose stack。
