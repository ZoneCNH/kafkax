# Kafkax 目标门禁证据 - 2026-06-05

本文档记录 `docs/goal/goal.md` 第一批 contract-first 交付切片的本地验证证据。它不声明完整目标完成；当前只证明公共 API skeleton、Kafka L2 contract、文档锚点和基础治理 gate 已对齐。

## 已通过检查

- `git diff --check`：通过，未发现 whitespace error。
- `GOWORK=off go test ./pkg/kafkax ./contracts`：通过。
- `GOWORK=off make contracts`：通过。
- `GOWORK=off make docs-check`：通过。
- `GOWORK=off make boundary`：通过；未发现 `x.go` 依赖、业务术语越界或模板边界违规。
- `GOWORK=off go test ./...`：通过。
- `GOWORK=off make standard-impact-check`：通过，并重新生成 `release/standard-impact/latest.md`。

## 当前覆盖面

- 公共 API 新增 driver-neutral `Message`、`Header`、`Producer`、`Consumer`、`Admin`、`TopicSpec`、`TopicDescription`、`TopicPlan` 和 Kafka 领域 error kind。
- `contracts/l2-kafka-adapter.schema.json` 绑定 `contracts/kafkax.config.schema.json`、`contracts/kafkax.message.schema.json`、`contracts/kafkax.topic.schema.json`、`contracts/kafkax.metrics.schema.json`、`contracts/error.schema.json` 和 `contracts/health.schema.json`。
- `contracts/contracts_test.go` 校验 message/topic schema 与公开 Go 类型的字段锚点，避免再次漂移到非 canonical schema 名称。
- `docs/api.md`、`docs/standard/l2-kafka-adapter.md`、`docs/standard/harness-gates.md`、`docs/standard/kafkax.md` 和 `docs/release.md` 已统一引用 canonical Kafka L2 contract。

## 未完成项

- 尚未实现 `internal/driver`、driver fake、默认 Kafka driver adapter 或 ADR，因此 REQ-004 仍未完成。
- Producer/Consumer/Admin 仍是公共接口和数据结构 skeleton，尚未实现 goal 中要求的 batch send、poll/handler lifecycle、rebalance、pause/resume、topic mutation、retry 和 close flush 语义。
- Kafka broker-dependent gates 仍应保持 `blocked`，包括 `kafka-integration`、`kafka-fault-injection`、`kafka-metrics-golden` 和 `kafka-admin-golden`。
- 尚未生成并验证 `release/manifest/latest.json` 与 `.sha256`，也未运行 release score 9.8 gate。
- 尚无真实 downstream adoption 证据；`adoption_claim` 不能升级，只能保持 `not_claimed`。
- 尚未产出 retrospective 的 Prompt、Harness/Rule patch 和 New Issue Candidate 全套证据。

## 停止条件

本切片可以作为 contract-first API/文档/静态 gate 的通过证据。完整 `DONE with evidence:` 只能在 `docs/goal/goal.md` 的 release manifest、release score、driver runtime、Kafka-specific gates、downstream adoption 边界和 retrospective 证据全部满足后声明。
