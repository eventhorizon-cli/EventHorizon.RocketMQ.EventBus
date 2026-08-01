# RocketMQ 多 Broker 环境

[全部测试环境](../README.zh-CN.md) | [English](README.md)

这个固定端口 Docker Compose 环境运行 Apache RocketMQ 5.5.0，供 gRPC 和 classic Remoting EventBus samples、手工
验证与问题复现使用。它与 `tests/it` 分离：Integration Tests 自己创建动态端口的 Testcontainers 拓扑，从不依赖此
Compose stack。

该 stack 包含：

- 一个 NameServer，地址为 `localhost:9876`；
- 三个相互独立的异步 Master Broker，对宿主机 Client 公布为
  `host.docker.internal:10911`、`host.docker.internal:10921` 和 `host.docker.internal:10931`；
- 一个 cluster-mode Proxy，Proxy Remoting 使用 `localhost:8080`，gRPC 使用 `localhost:8081`；
- 一个 Resource Initializer，在每个 Broker 上创建 `eventbus-orders` 和 `eventbus-inventory-snapshots`，每个
  Broker 为每个 Topic 配置三个可读和可写队列；
- RocketMQ Dashboard，地址为 `http://localhost:8082`。

这是三 Master 的路由与分区拓扑，不是具备副本的高可用拓扑。每个 Broker 使用独立持久化 Store；停止其中一个 Broker
会使该 Broker 所属队列和消息暂时不可用。

## 架构

```text
                            +------------------+
gRPC EventBus ------------>| Proxy :8081      |
                            | cluster mode     |
                            +--------+---------+
                                     |
                                     | 查询路由并访问 Broker
                                     v
Remoting EventBus ---> NameServer :9876
       |                     |
       | 跟随路由            +--------+---------+---------+
       |                              |         |         |
       +--------------------------> Broker A  Broker B  Broker C
                                      :10911    :10921    :10931
```

gRPC 应用只连接 Proxy。Remoting 应用从 NameServer 获取路由后直接连接已公布 Broker Endpoint。两个适配器都使用
Client 主动发起的接收或长轮询；该环境不接受 Broker 向应用主动建立的入站连接。

## 启动与停止

在仓库根目录校验并启动 stack：

```shell
docker compose -f test-environments/rocketmq-multi-broker/compose.yaml config --quiet
docker compose -f test-environments/rocketmq-multi-broker/compose.yaml up -d --wait
docker compose -f test-environments/rocketmq-multi-broker/compose.yaml ps
```

`resource-init` 服务会等待三个 Broker 全部可用，创建 sample Topic 与 Group，并验证每个 Topic 的路由包含全部
Broker。该服务成功完成后，sample 所需资源即可使用。

普通停止会保留 Named Volume 数据：

```shell
docker compose -f test-environments/rocketmq-multi-broker/compose.yaml down --remove-orphans
```

附加 `-v` 会删除全部消息 Store 和日志，只能用于明确需要全新环境时。

## 地址

默认 Broker 公布主机为 `host.docker.internal`，Compose 服务也配置了 Docker host-gateway alias。它适用于 Docker
Desktop 和 OrbStack。Linux 用户需要在启动 Compose 前把 `ROCKETMQ_ADVERTISED_HOST` 设置为宿主机进程与 Proxy
container 都可访问的地址。

组合拓扑不能把 Broker 公布为 `localhost` 或 `127.0.0.1`：从 Proxy 内部看，这两个地址指向 Proxy 自身，不是任意
Broker。

## 文件与许可证边界

`compose.yaml` 定义运行拓扑；三份 Broker 模板、`proxy.json` 和 `init-resources.sh` 提供 Broker 公布地址与初始资源；
`compose.host-volumes.yaml` 是可选的宿主机 Volume 变体。

Compose 配置为本 MIT 仓库独立编写。它遵循主 Client 面向操作者的行为，但不复制主 Client 仓库中 Apache-2.0 的源码
文件。
