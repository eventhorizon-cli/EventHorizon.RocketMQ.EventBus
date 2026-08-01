# Test environments

[English](README.md) | [简体中文](README.zh-CN.md) |
[Testing design](../docs/en-US/testing-design.md)

`test-environments` contains implemented fixed-port Docker Compose environments for runnable samples, manual
validation, and issue reproduction.

| Environment | Purpose |
| --- | --- |
| [`rocketmq-multi-broker`](rocketmq-multi-broker/README.md) | One NameServer, three independent master Brokers, one cluster-mode Proxy, a resource initializer, and sample resources for both EventBus adapters |

These environments are independent from `tests/it`. Integration tests create disposable dynamic-port containers
through their own Testcontainers fixtures and do not require a manually started Compose stack.
