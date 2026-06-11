# ADR-20260604-001: Kafka driver-neutral boundary 和 fake-first gate

- 状态：Accepted for contract-first execution
- 日期：2026-06-05
- 关联目标：`docs/goal/goal.md`、REQ-004、REQ-011

## 背景

`kafkax` 要作为 Kafka L2 adapter/factory 标准推进，但公共 API 不能绑定任意第三方 Kafka client。当前仓库已经有 contract-first 的 `pkg/kafkax` API、JSON schema、文档 gate、`internal/driver` descriptor 边界和 `testkit.FakeKafka` fixture；尚未在本工区出现默认生产 driver 或 broker-backed fixtures。

## 决策

- 在内部实现层建立 `internal/driver` descriptor 边界，用标准 Go 类型表达 driver 能力，不反向依赖公开包。
- 公共 API 的目标语义以 `Producer.Send` / `Producer.SendBatch` / `Producer.Flush`、`Consumer.Poll` / `Consumer.Run` / `Consumer.Commit` / `Consumer.Pause` / `Consumer.Resume`、`Admin.DescribeTopics` / `Admin.PlanTopics` / `Admin.ApplyTopics` 为准。
- `testkit` 必须先提供 fake fixture，用于无 broker、无第三方 Kafka 依赖的 contract tests。broker-backed adapter 只能在 fake gate 通过后进入。
- 公开包、contracts 和 docs 不得暴露 `kgo.*`、`kafka-go.*`、`confluent.*` 或其它第三方 Kafka driver 类型。
- Kafka broker-dependent gates 在没有可重复 broker fixture 之前保持 `blocked`，不得被 docs-only 或 skeleton-only 证据升级。

## 后果

- REQ-004 的第一切片已覆盖 `internal/driver` descriptor 边界、testkit fake fixtures 和公共 API contract tests；完整完成仍需要 production driver 决策、broker gate 状态和 release evidence，单独的 fake-first 证据不足以声明完整目标完成。
- REQ-013/REQ-014 的 release/adoption 证据必须等待 runtime、release manifest、score gate 和 downstream adoption artifacts，不得由 ADR 替代。
- 生产 driver 可以后续选择具体 Kafka client，但选择结果必须隔离在内部 adapter 或可选边界内，不能改变公共 API 或 schema contract。

## 验证要求

- Static/docs gate：`git diff --check`、`GOWORK=off make docs-check`、`GOWORK=off go test ./pkg/kafkax ./contracts`。
- Fake-fixture gate：`GOWORK=off go test ./pkg/kafkax ./internal/driver/... ./testkit ./contracts`，覆盖 `internal/driver` descriptor 边界与 `testkit` fake fixtures，无 broker、无第三方 Kafka client。
- Broker gate：`kafka-integration`、`kafka-fault-injection`、`kafka-metrics-golden`、`kafka-admin-golden` 在 broker fixture 存在前必须保持 blocked。
