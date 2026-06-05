# kafkax 基础库总标准

[`kafkax`](https://github.com/ZoneCNH/kafkax) 是 Standard Source、Go Reference Template、generator、Harness 和 Evidence 的统一仓库。当前仓库同时承载面向生成基础库的通用模板，以及 L2 Kafka adapter/factory 的最小 contract surface。

## 范围

- `kafkax` 作为标准权威源，维护模板、generator、Harness、Evidence 和 release gate。
- 生成后的基础库必须保持公共 API、配置、错误、健康检查、metrics、测试和发布规则一致。
- L2 Kafka adapter/factory 必须保持 driver-neutral：public API 不暴露 `kgo.*`、`kafka-go.*`、`confluent.*` 或其它第三方 Kafka 类型。
- 模板和生成库不得依赖 `x.go` 或私有业务模型。

## 公共 API

公共 API 至少覆盖 `Producer`、`Consumer`、`Admin`、`TopicSpec`、`Message`、`Header`、`Offset`、`Error`、`Health` 和 `Config`。交付语义、幂等、重试、DLQ、消费位点和 admin topic 行为必须写入 API 文档和合约，不得只存在于实现注释。

## Contract surfaces

- 通用 config/error/health/metrics contract：`contracts/config.schema.json`、`contracts/error.schema.json`、`contracts/health.schema.json`、`contracts/metrics.md`。
- Kafka L2 contract：`contracts/l2-kafka-adapter.schema.json`。
- Kafka config/message/metrics contract：`contracts/kafkax.config.schema.json`、`contracts/kafkax.message.schema.json`、`contracts/kafkax.metrics.schema.json`。
- 文档入口：`docs/api.md`、`docs/config.md`、`docs/errors.md`、`docs/metrics.md`、`docs/testing.md`、`docs/release.md`。

## Harness and Evidence

Harness 必须用 `GOWORK=off` 运行。`docs-check`、`contracts`、`boundary`、`standard-impact-check` 和 release Evidence 是 DONE with Evidence 的基础。Kafka broker 相关 gates（`kafka-integration`、`kafka-fault-injection`、`kafka-metrics-golden`、`kafka-admin-golden`）没有真实 broker/driver 证据时只能标记 blocked，不能标记 passed。

## Security

配置必须由调用方显式传入；不得隐式读取 `/home/k8s/secrets/env/*` 或其它生产密钥目录。Sanitize 后的配置、错误、日志、Evidence 和 release manifest 不得包含原始凭据、消息值、生产连接串或业务私密数据。
