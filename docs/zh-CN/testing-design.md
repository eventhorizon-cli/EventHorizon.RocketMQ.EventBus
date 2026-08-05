# 测试、环境与示例设计

[文档目录](README.md) | [English](../en-US/testing-design.md) |
[EventBus 详细设计](event-bus-design.md)

本文档定义仓库中的 Unit Tests、Integration Tests、本地环境和 samples 结构。设计目标是在确定性测试公共行为的
同时，使用真实 RocketMQ 进程验证两个适配器。

## 项目矩阵

| 区域 | 项目或目录 | 职责 |
| --- | --- | --- |
| Unit Test | `EventHorizon.RocketMQ.EventBus.Tests` | 事件契约、路由表、扫描、Handler 顺序、序列化器、分发、DI 生命周期和日志策略 |
| Unit Test | `EventHorizon.RocketMQ.Grpc.EventBus.Tests` | gRPC 注册、消息转换、可选角色、keyed 绑定、发布结果和 `ConsumeResult` 映射 |
| Unit Test | `EventHorizon.RocketMQ.Remoting.EventBus.Tests` | Remoting 注册、单消息约束、可选角色、keyed 绑定、发送状态和 `ConsumeResult` 映射 |
| Compatibility Test | `EventHorizon.RocketMQ.EventBus.Compatibility.Tests` | 跨包 API 对称性、独立协议类型、包边界和默认/named 行为 |
| Integration Test 基础设施 | `EventHorizon.RocketMQ.EventBus.IntegrationTestInfrastructure` | 临时的协议专用三 Broker Testcontainers fixture 和唯一测试资源 |
| Integration Test | `EventHorizon.RocketMQ.Grpc.EventBus.IntegrationTests` | 通过真实 RocketMQ 5 cluster-mode Proxy 验证 EventBus |
| Integration Test | `EventHorizon.RocketMQ.Remoting.EventBus.IntegrationTests` | 通过真实 NameServer 路由发现和 Broker Remoting 直连验证 EventBus |
| 手工环境 | `test-environments/rocketmq-multi-broker` | 固定端口 Docker Compose stack，服务于 samples、手工测试和问题复现 |
| Samples | `samples` | 每种协议各自可运行的 Web API Publisher、Generic Host Consumer，以及 named registration 工作流 |

Integration Test 基础设施不可打包，也不引用任何生产项目。每个协议 IT 项目只引用自己的适配器和公共测试基础设施。
Unit Tests 与 Compatibility Tests 不依赖 Docker 或网络。

基础设施公开 `RocketMQGrpcClusterFixture` 和 `RocketMQRemotingClusterFixture`。两个 fixture 保持分离，因为它们为
不同 Client 提供不同的 Broker 公布地址模型。

## Unit Test 覆盖

Core Tests 覆盖：

- `IntegrationEvent` 校验和精确的序号比较 `(Topic, Tag)` 路由 Key；
- 公开无参构造函数发现，以及无效构造函数行为；
- 确定性程序集扫描、直接注册顺序、注册内幂等重复注册和生命周期冲突；
- 自动生成的 Topic 订阅与按序号排序的 Tag 表达式；
- 只允许在启动阶段注册，并验证已构建的 Service Provider 不会观察到后续 Service Collection 变更；
- 运行时 Route Plan 不可变，并且不存在直接注册、程序集扫描、Serializer 或订阅变更服务；
- 复用传输层拥有的每次投递异步 Scope、Handler 顺序执行，以及全部内部 Dispatch Outcome；
- 固定 Newtonsoft.Json Payload、无效 UTF-8、Schema 演进默认值和自定义序列化器；
- 结构化日志级别与字段、JSON 格式的完整 Payload、自定义序列化器日志视图、二进制回退，以及每次启动只输出一次的
  订阅汇总。

适配器和 Compatibility Tests 覆盖：

- 两种协议的默认未键控注册和 named keyed registration；
- 同一协议中的多个注册名，以及一个 Service Collection 中混合的 gRPC/Remoting 注册名；
- 注册名使用区分大小写的序号比较，包括相互独立的 `orders` 和 `Orders` Key；
- 两个适配器之间重复的 EventBus 注册标识；
- 每个注册的路由、Handler 生命周期、序列化器、Producer、Consumer 和 HostedService 隔离；
- `configureProducer: null` 不注册 Producer、Producer HostedService 或 `IEventBus`；
- 非 `null` Producer 委托只注册一个 Producer 和正确的未键控/keyed `IEventBus`；
- 首次注册 Handler 时只创建一个 Push Consumer，纯发布注册没有 Consumer；
- 每个消费 registration 使用其第一个自有 Handler 闭合出唯一的协议桥接类型，不暴露传输层注册身份；
- 直接注册或程序集扫描尝试把同一 Handler 类型加入另一个默认或 named EventBus registration 时，在启动期失败；
- 使用私有 token 隔离不同 registration 的 Route、Serializer 和不同 Handler 类型；
- 固定使用 Scoped 协议桥接层，并保证每次投递只有一个由主客户端创建的异步 Scope；
- 把每个内部 Outcome 显式映射到两个独立定义的传输层 `ConsumeResult`，包括 gRPC 的 `Retry`、`DeadLetter`
  最终都映射为 `Failure`；
- Remoting 非成功发送状态会转换成发布失败；
- CancellationToken 传播和订阅汇总启动行为。

## Integration Test 拓扑

每个 Integration Test fixture 都使用三个相互独立的 Master Broker。这样可以覆盖真实的多 Broker 路由和队列分布，
但不把它描述成复制或高可用拓扑。所有 Topic 都在三个 Broker 上显式创建；只有 NameServer 返回完整路由后，fixture
启动才算完成。

### gRPC fixture

```text
gRPC IT 进程
    |
    v
cluster-mode Proxy container
    |
    +--> NameServer container
    |
    +--> broker-a container
    +--> broker-b container
    `--> broker-c container
```

测试进程只连接动态映射的 Proxy gRPC Endpoint。Broker 在固定内部端口上公布 Docker 网络别名，使独立 Proxy 可以
访问每条路由。生产 gRPC 客户端不会由测试进程查询 NameServer 或直连 Broker。

### Remoting fixture

```text
Remoting IT 进程
    |
    +--> NameServer container -- 查询路由
    |
    +--> broker-a container -- Remoting 直连
    +--> broker-b container -- Remoting 直连
    `--> broker-c container -- Remoting 直连
```

fixture 动态映射 NameServer 和全部 Broker 端口。每个 Broker 公布宿主机可达的地址和映射端口，因为生产 Remoting
客户端会按返回路由直接连接对应 Broker。fixture 会启用 Broker assignment 与 POP，但 PULL 和 POP 流程使用
独立的 Topic 与 Consumer Group。Remoting IT 不需要 Proxy。

Proxy 可以解析宿主机进程无法使用的 Docker 别名，而适合宿主机的 `127.0.0.1` 路由又无法让 Proxy 区分三个对等
container，因此两个 fixture 保持独立。可以提取公共生命周期 Helper，但不设计一个在部分模式下成员无效的公共
模式化 fixture。

## 当前 Integration Test 覆盖

每个协议 Suite 都启动一个包含 EventBus Producer 和 Push Consumer 的 Generic Host。默认流程向 fixture 创建的
Topic 并发发布十二条带 Tag 和十二条无 Tag 的事件，然后验证每个事件 ID 只会到达其匹配的强类型 Handler 一次，并
确认三个 Broker 都存储了消息。这覆盖公开注册、由 Host 管理的传输生命周期、Newtonsoft.Json Body 路径、字面量
Tag 路由、无 Tag 路由需要的通配订阅，以及 Remoting 一条消息一次的分发约束。

Remoting Suite 还会在独立 Topic 与 Consumer Group 上运行 Broker 分配的 POP 流程。测试除了验证强类型 EventBus
投递，还会等待真实 POP `ACK_MESSAGE` 响应完成后产生的 `ack` settlement Activity。PULL 只会产生 offset `commit`；
如果实现退化为 PULL，测试会超时失败。原有流程继续覆盖默认的 Client assignment + PULL。

Fixture 使用唯一 Topic 与 Group，通过有上限的可观察条件等待，并自行管理全部 Docker 资源。不需要真实 Broker 的
结果映射、无效 Payload、未知路由、Retry 分类、named registration 等分支由确定性 Unit Test 覆盖。

## 独立 Compose 环境

`test-environments/rocketmq-multi-broker` 不是 IT fixture，运行 `dotnet test` 不依赖它。它提供固定端口的
`compose.yaml`，其中包含：

- 一个 NameServer；
- 三个拥有独立持久化 Store 的 Master Broker；
- 一个公开 gRPC Endpoint 的独立 cluster-mode Proxy；
- 一个在所有 Broker 上创建 samples Topic 的 Resource Initializer；
- 一个可选的 Dashboard，用于本地检查。

Compose 环境同时支持宿主机侧的两种协议：gRPC samples 连接 Proxy，Remoting samples 查询 NameServer 后访问
每个已公布的 Broker 地址。双语 README 说明宿主机地址覆盖、端口、启动、健康检查、资源创建和会删除 Volume 数据的
清理命令。

Compose 文件和 Testcontainers fixture 可以使用相同的镜像版本与拓扑术语，但不共享源码、生命周期状态、端口或
持久化数据。运行测试或 samples 不依赖本机存在主项目的兄弟目录。

## Samples

首版包含以下按协议划分的项目：

| Sample | 演示内容 |
| --- | --- |
| `samples/grpc/Publisher` | 默认与 keyed `orders` `IEventBus` registration 的 gRPC Web API Publisher，提供带 Tag 和无 Tag 端点，不创建 Consumer |
| `samples/grpc/Consumer` | gRPC Push 消费、直接 Handler 注册、Tag 和通配订阅，以及 named `orders` Consumer |
| `samples/remoting/Publisher` | 默认与 keyed `orders` `IEventBus` registration 的 Remoting Web API Publisher，提供带 Tag 和无 Tag 端点，不创建 Consumer |
| `samples/remoting/Consumer` | clustered Remoting Push 消费、单消息分发、Tag 和通配订阅，以及 named `orders` Consumer |

每个 sample 都有 `appsettings.json`、中英文 README、适用于独立 Compose 环境的可运行默认值，以及一条清晰可见的
SDK 工作流。Publisher sample 使用 WebApplication Host，Consumer sample 使用 Generic Host，由现有 `IHostedService`
启停配置的 RocketMQ 角色。首版不提供 NonHost samples。

## CI 与验证

手工检查与 CI 使用相同的项目边界。公开注册、路由、序列化、结果映射或生命周期发生变化时，必须增加聚焦的
Unit Test 并运行受影响协议的 IT。纯文档变更执行链接、拼写和结构检查，不为此制造失败的行为测试。

独立 Compose 环境通过以下命令校验：

```shell
docker compose -f test-environments/rocketmq-multi-broker/compose.yaml config --quiet
```

## GitHub Actions 与发布

仓库包含 `.github/workflows/dotnet-build.yml` 和统一的 `.github/workflows/publish.yml`。该 workflow 设计有意不同于
主客户端仓库按协议拆分的发布 workflow：EventBus 有三个使用同一版本的 Package，必须把它们作为一个有序整体发布。

| Workflow | 触发条件 | 目标 |
| --- | --- | --- |
| `.github/workflows/dotnet-build.yml` | 推送到 `main` 和以 `main` 为目标的 Pull Request | 校验 Format、编译、目标框架、Unit Test Coverage、Integration 行为、Samples 和 Compose 语法。 |
| `.github/workflows/publish.yml` | 推送以 `v` 开头的 tag；workflow 会继续要求 tag 完全符合稳定版格式 | 按依赖顺序测试、打包和发布三个同版本 Package，保留 Package Artifact，并创建 Release。 |

### Build workflow

Build workflow 根据 `global.json` Restore，验证 `dotnet format` 且不修改 checkout，并以 Release 配置构建完整
Solution。它的 Unit Test matrix 会为四个确定性测试项目收集 Coverage：Core、gRPC 适配器、Remoting 适配器和
Compatibility。它还会显式验证所有受支持生产项目声明的每个 Target Framework，而不是假定默认 SDK Target 已足够。

确定性检查通过后，两个独立的 Docker Job 分别运行 gRPC 和 Remoting EventBus Integration Project，并各自使用临时
三 Broker Testcontainers 拓扑。gRPC Job 使用 cluster-mode Proxy fixture；Remoting Job 使用 NameServer 与宿主机可达的
直连 Broker fixture。这些 Job 不会启动固定端口的 `test-environments/rocketmq-multi-broker` Compose 环境。

该 workflow 会构建每个 Sample Project，并使用
`docker compose -f test-environments/rocketmq-multi-broker/compose.yaml config --quiet` 校验独立 Compose 文件。它会上传
已配置报告服务需要的 Test Coverage 和 Test Result Artifact。任何报告服务凭据只能通过 GitHub Actions Secret 提供，
不得将凭据值提交到 workflow 或文档中。

该 workflow 使用验证所需的最小权限 `permissions: contents: read`，并使用从 workflow 和 ref 推导的 concurrency group，
以取消已经被更新的 push 或 pull-request run。它使用 `global.json` 声明的 SDK，关闭 .NET telemetry 与 logo，并可以按
SDK 和项目或构建元数据生成的 key 缓存 NuGet Package。

### 统一发布 workflow

统一发布 workflow 监听以 `v` 开头的 tag，然后在执行任何 Restore、Pack、Push 或 Release 操作前验证完整 ref name。
唯一接受的格式是稳定三段 SemVer：

```text
v<major>.<minor>.<patch>
```

等价的校验表达式为：

```text
^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$
```

例如，接受 `v1.2.3`。拒绝 `v1.2`、`release-v1.2.3`，以及所有带 suffix 的 prerelease tag，例如 `v1.2.3-rc.1`。
仍需要使用 `v*` trigger，因为 GitHub Actions 的 tag filter 无法表达完整的稳定 SemVer 规则；workflow 的第一步会强制
执行该规则。

解析出的版本会原样赋予三个生产 Package。workflow 会先 Restore、Build 并运行发布用 Unit Tests，再使用同一个版本打包
`EventHorizon.RocketMQ.EventBus`、`EventHorizon.RocketMQ.Grpc.EventBus` 和
`EventHorizon.RocketMQ.Remoting.EventBus`。它会在发布前把生成的 `.nupkg` 和 `.snupkg` 文件上传为 workflow Artifact。

发布必须按以下顺序进行：

1. 推送 Core 包。
2. 立即推送 gRPC 与 Remoting 适配器包；它们声明了对同版本 Core 的普通依赖。
3. 三个包全部推送成功后，才为 Tag 创建 GitHub Release；该 Release 不标记为 prerelease。

发布工作流使用不会取消运行中发布任务的 release concurrency group，避免两个 Tag 交错发布。工作流创建 GitHub
Release，因此需要 `contents: write`。三个包共用一个 `NUGET_API_KEY`；该密钥必须对三个 Package ID 都拥有发布权限。
NuGet 凭据、按需使用的包源凭据和报告 Token 都从 GitHub Actions Secret 或执行环境读取，不硬编码 Secret。缺少必需
Secret 时，发布操作会在推送任意包前失败；如果密钥缺少 Package 所有权或发布权限，NuGet.org 会在
对应的推送步骤拒绝请求。
