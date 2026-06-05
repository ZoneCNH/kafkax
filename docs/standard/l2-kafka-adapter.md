# L2 Kafka Adapter Standard

`kafkax` 的 L2 Kafka adapter/factory 目标是提供受治理的基础设施适配层，而不是业务消息模型或某个 Kafka driver 的公开封装。

## Contract-first 边界

- Public API 必须保持 driver-neutral，不得暴露 `kgo.*`、`kafka-go.*`、`confluent.*` 或其它第三方 Kafka 类型。
- 公共模型名称为 `Producer`、`Consumer`、`Admin`、`TopicSpec`、`Message`、`Header`、`Offset`、`Error`、`Health` 和 `Config`。
- `contracts/l2-kafka-adapter.schema.json` 是 L2 profile contract。
- `contracts/kafkax.config.schema.json`、`contracts/kafkax.message.schema.json`、`contracts/kafkax.topic.schema.json` 和 `contracts/kafkax.metrics.schema.json` 是 Kafka-specific data contracts。
- `contracts/error.schema.json` 和 `contracts/health.schema.json` 继续作为共享 typed error 和 health contracts。

## Gate 期望

| Gate | 当前状态规则 | Evidence 要求 |
| --- | --- | --- |
| `kafka-contract` | required | JSON schema validity 加 `GOWORK=off make contracts`。 |
| `kafka-integration` | driver 和 broker fixture 存在前保持 blocked | 真实 broker 运行输出；broker 不可用必须记录为 blocked，不得写成 passed。 |
| `kafka-fault-injection` | broker fixture 存在前保持 blocked | auth、timeout、rebalance、broker unavailable 和 retry Evidence。 |
| `kafka-metrics-golden` | metrics backend/fixture 存在前保持 blocked | 覆盖 producer、consumer、admin、connection、rebalance、lag 和 DLQ 的 golden metrics。 |
| `kafka-admin-golden` | admin implementation 存在前保持 blocked | topic create/describe/delete 或显式 unsupported-operation Evidence。 |

## 当前切片非目标

本文档不选择 Kafka driver，不新增 broker runtime，不新增 Makefile kafka targets，也不声明 downstream adoption。实现、broker Evidence 和 release Evidence 完整存在前，adoption 必须保持 `not_claimed`。
