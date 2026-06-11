# Testkit 测试工具

为 `kafkax` Kafka L2 adapter contract 提供可复用测试夹具、断言和 fake broker-less runtime。

## 契约

- `Config(name string)` 返回带 `Name` 和 `Timeout` 的最小有效配置。
- `RequireNoError(t, err)` 在 `err == nil` 时保持静默，在非空错误时终止当前测试。
- `RequireGolden(t, path, actual)` 读取 golden 文件并比较实际输出；不一致时报告 expected / actual 上下文。
- `FakeKafka` 提供无真实 broker、无第三方 Kafka client 依赖的 producer、consumer 和 admin fixture，用于锁定 public API contract 与 clone isolation。

## 回归覆盖

`fixture_test.go` 锁定 `Config("fixture")` 的字段和 `Validate` 结果，并验证 `RequireNoError(t, nil)` 可用。`golden_test.go` 锁定 golden 断言的匹配路径。`kafka_test.go` 覆盖 fake producer/consumer/admin、round-trip、clone isolation、cancel 和 topic plan/apply 行为。

本包必须保持独立于 `x.go`、业务特定模型、真实生产连接和具体 Kafka client 类型。broker-backed integration 只能由独立 gate 证明，不能用 `FakeKafka` 代替。
