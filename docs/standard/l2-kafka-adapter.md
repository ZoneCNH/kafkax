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
| `kafka-contract` | required | `GOWORK=off make kafka-contract` 校验 driver-neutral public API marker、Kafka schema、harness runtime 映射和 testkit/API guard marker；`GOWORK=off make contracts` 继续覆盖 JSON schema validity。 |
| `kafka-integration` | 未提供真实 broker fixture 时保持 `status=gap` 且非零退出；提供 fixture 时必须执行 broker-backed smoke | 真实 broker 运行输出；broker 不可用必须记录为 blocked gap，不得写成 passed。 |
| `kafka-fault-injection` | 未提供真实 broker fixture 时保持 `status=gap` 且非零退出；提供 fixture 时必须执行失败模式验证 | auth、timeout、rebalance、broker unavailable 和 retry Evidence。 |
| `kafka-metrics-golden` | 未提供真实 broker fixture 时保持 `status=gap` 且非零退出；提供 fixture 时必须校验指标脱敏与 label allowlist | 覆盖 producer、consumer、admin、connection、rebalance、lag 和 DLQ 的 golden metrics。 |
| `kafka-admin-golden` | 未提供真实 broker fixture 时保持 `status=gap` 且非零退出；提供 fixture 时必须执行 topic/admin smoke | topic create/describe/delete 或显式 unsupported-operation Evidence。 |

## 当前 driver 切片

当前仓库提供可选的 `pkg/kafkax/kafkago` production driver，基于 `github.com/segmentio/kafka-go` 适配 `pkg/kafkax` 的 driver-neutral public API。Broker-dependent gates 通过 `KAFKAX_BROKER_FIXTURE` 或 `--broker-fixture` 读取真实 broker fixture；fixture 缺失时必须继续输出 `status=gap`，fixture 存在时不得使用 `testkit.FakeKafka` 替代 broker Evidence。Downstream adoption 仍必须由独立 release/adoption Evidence 声明，不能只由本仓库 broker gate 通过推导。
