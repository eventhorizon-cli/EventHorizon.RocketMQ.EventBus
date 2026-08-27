# Remoting EventBus Consumer

[English](README.md) | [简体中文](README.zh-CN.md)

该 Generic Host 只创建 Clustering Remoting Push Consumer，不配置 Producer。默认 registration 直接注册带 Tag 的订单
路由和无 Tag 的库存路由。EventBus 强制每次 Handler 调用只处理一条消息，同时保留可配置的接收预取。订单路由有两个
Handler，用于演示一条 `(Topic, Tag)` 只选择一种事件类型，并可在一次反序列化后扇出。

同一进程还创建了名为 `orders` 的 named registration；它使用独立 Consumer Group 和独立订单 Handler。其路由仍使用
同一 Topic，但字面量 Tag 为 `order-submitted-named`；默认路由使用 `order-submitted`。registration name 只隔离
DI/Client，并不是 RocketMQ 路由元数据。

默认 registration 显式使用 `RemotingPushQueueAssignmentMode.Broker`，而 named `orders` registration 保持默认的
`Client` 分配。这是有意为之：队列分配按 Consumer registration 选择，Broker 的消费请求模式则按 `(Topic, Consumer
Group)` 选择。`Broker` 只让 Broker 分配队列，并不直接选择 POP。未配置 POP 请求模式时，本地 stack 会使用 RocketMQ
通常的 PULL 默认值。

先启动[多 Broker 环境](../../../test-environments/rocketmq-multi-broker/README.zh-CN.md)，再运行：

```bash
dotnet run --project samples/remoting/Consumer
```

可通过 `RocketMQ__NamesrvAddr` 覆盖 NameServer。正常停止进程即可验证 HostedService 关闭流程。

## 可选的 Broker-assigned POP

本地 stack 启动后，为默认 Group 消费的两个 Topic 配置 POP，再启动这个 sample：

```shell
docker compose -f test-environments/rocketmq-multi-broker/compose.yaml exec broker-a \
  sh /home/rocketmq/rocketmq-5.5.0/bin/mqadmin setConsumeMode \
  -n nameserver:9876 -c DefaultCluster -t eventbus-orders \
  -g eventbus-remoting-sample -m POP -q 0

docker compose -f test-environments/rocketmq-multi-broker/compose.yaml exec broker-a \
  sh /home/rocketmq/rocketmq-5.5.0/bin/mqadmin setConsumeMode \
  -n nameserver:9876 -c DefaultCluster -t eventbus-inventory-snapshots \
  -g eventbus-remoting-sample -m POP -q 0
```

`-c DefaultCluster` 会把配置应用到 sample stack 的三个 Master。将 `-m POP` 改为 `-m PULL` 即可切回。不要在
`eventbus-remoting-sample` 中混用 `Client` 与 `Broker` 实例；请求模式切换时，旧 receiver 尚未处置的消息可能重新投递，
所以 sample Handler 和生产 Handler 一样都必须保持幂等。
