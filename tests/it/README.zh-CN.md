# Integration Tests

[English](README.md) | [简体中文](README.zh-CN.md) |
[测试设计](../../docs/zh-CN/testing-design.md)

`tests/it` 包含两个协议测试程序集和一个非测试基础设施库：

| 项目 | 职责 |
| --- | --- |
| `EventHorizon.RocketMQ.Grpc.EventBus.IntegrationTests` | 通过真实 cluster-mode RocketMQ 5 Proxy 验证 EventBus |
| `EventHorizon.RocketMQ.Remoting.EventBus.IntegrationTests` | 通过 NameServer 发现和 Broker Remoting 直连验证 EventBus |
| `EventHorizon.RocketMQ.EventBus.IntegrationTestInfrastructure` | 管理临时三 Broker Testcontainers fixture 和唯一测试资源 |

基础设施项目引用 Testcontainers 4.13.0 与 xUnit 生命周期抽象，但不引用任何生产项目。每个协议测试项目只引用自己的
适配器和基础设施项目。

## 临时拓扑

两个 fixture 都启动一个 NameServer 和三个独立 Master Broker container，在所有 Broker 上显式创建唯一的测试 Topic，
并等待完整路由。gRPC fixture 额外启动一个独立 cluster-mode Proxy，只公开其 gRPC Endpoint；Remoting fixture
则通过宿主机可达的公布地址公开 NameServer 和全部 Broker。

Fixture 使用动态端口，并由选中的 xUnit Suite 创建和释放。它们不读取、启动或共享
`test-environments/rocketmq-multi-broker` 的状态；该 Compose 环境只服务 samples 和手工工作。

## 当前覆盖

每个协议 Suite 都启动一个包含 EventBus Producer 与 Push Consumer 的 Generic Host，然后并发发布十二条带 Tag 和
十二条无 Tag 的事件。已注册的强类型 Handler 会记录事件 ID，测试断言每个物理事件只会到达匹配的 Handler 一次；两组
Suite 都会断言该 Topic 在全部三个 Broker 上都有消息。Remoting fixture 还会在直连 Broker 的 EventBus 流程开始前
确认 NameServer 返回完整的三 Broker 路由。这覆盖公开 EventBus 注册 API、由 Host 管理的传输生命周期、
Newtonsoft.Json Payload 路径、`Tag` 路由、无 Tag 路由要求的通配订阅、多 Broker 路由分布，以及 Remoting 适配器的
一条消息一次分发约束。

不需要真实 Broker 的结果映射、无效 Payload、未知路由、Retry 分类、named registration 等分支由确定性 Unit Test
覆盖。后续可以在不改变 fixture 边界的前提下，为这两个 Integration Test Project 增加更多 Broker 行为场景。

## 命令

运行前必须可访问 Docker：

```shell
dotnet test tests/it/EventHorizon.RocketMQ.Grpc.EventBus.IntegrationTests/EventHorizon.RocketMQ.Grpc.EventBus.IntegrationTests.csproj --no-restore
dotnet test tests/it/EventHorizon.RocketMQ.Remoting.EventBus.IntegrationTests/EventHorizon.RocketMQ.Remoting.EventBus.IntegrationTests.csproj --no-restore
```

测试需要可访问 Docker daemon，使用唯一 Topic/Group、有上限的条件等待，测试行为中不使用任意 sleep。完整场景矩阵和
CI 归属见测试设计。
