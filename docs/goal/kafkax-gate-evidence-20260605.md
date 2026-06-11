# Kafkax 目标门禁证据 - 2026-06-05

本文档记录 `docs/goal/goal.md` 第一批 contract-first / fake-first 交付切片的本地验证证据。它不声明完整目标完成；当前只证明公共 API、Kafka L2 contract、`internal/driver` descriptor 边界、testkit fake fixture、ADR 决策锚点、文档锚点和基础治理 gate 已对齐。

## 已通过检查

- `git diff --cached --check`：通过，未发现 whitespace error。
- `GOWORK=off GOCACHE=/tmp/kafkax-gocache go test ./pkg/kafkax ./internal/driver ./testkit ./contracts`：通过。
- `GOWORK=off GOCACHE=/tmp/kafkax-gocache go test ./...`：通过。
- `GOWORK=off GOCACHE=/tmp/kafkax-gocache make contracts`：通过。
- `GOWORK=off GOCACHE=/tmp/kafkax-gocache make docs-check`：通过。
- `GOWORK=off GOCACHE=/tmp/kafkax-gocache make kafka-contract`：通过；`goalcli kafka-contract` 输出 `status=passed`，确认 public API、Kafka schema、`.agent/harness/harness.yaml` 运行时 gate 映射、testkit/API guard marker 已存在。
- `GOWORK=off GOCACHE=/tmp/kafkax-gocache go run ./cmd/goalcli kafka-integration`：按预期返回非零，输出 `status=gap`；缺口包括 production Kafka driver、broker-backed Evidence、FakeKafka 不能替代 broker Evidence、未提供 `KAFKAX_BROKER_FIXTURE`。
- `GOWORK=off GOCACHE=/tmp/kafkax-gocache go run ./cmd/goalcli kafka-fault-injection`：按预期返回非零，输出 `status=gap`。
- `GOWORK=off GOCACHE=/tmp/kafkax-gocache go run ./cmd/goalcli kafka-metrics-golden`：按预期返回非零，输出 `status=gap`。
- `GOWORK=off GOCACHE=/tmp/kafkax-gocache go run ./cmd/goalcli kafka-admin-golden`：按预期返回非零，输出 `status=gap`。
- `GOWORK=off GOCACHE=/tmp/kafkax-gocache go test ./cmd/goalcli`：通过，覆盖 Kafka gate CLI 报告与 gap 语义。
- `GOWORK=off GOCACHE=/tmp/kafkax-gocache make command-registry`：通过。
- `GOWORK=off GOCACHE=/tmp/kafkax-gocache make makefile-baseline`：通过。
- `GOWORK=off GOCACHE=/tmp/kafkax-gocache make cli-contract`：通过。
- `GOWORK=off GOCACHE=/tmp/kafkax-gocache make boundary`：通过；未发现 `x.go` 依赖、业务术语越界或模板边界违规。
- `GOWORK=off GOCACHE=/tmp/kafkax-gocache make standard-impact-check`：通过，并重新生成 `release/standard-impact/latest.md`。
- `GOWORK=off GOCACHE=/tmp/kafkax-gocache make downstream-sync-plan`：通过；输出 `downstream_sync_required=true`、`target_count=11`、`adoption_claim=not_claimed`。
- `GOWORK=off GOCACHE=/tmp/kafkax-gocache make score`：通过；`goalcli score --min 9.8` 返回 `value=10`、`threshold=9.8`、`status=passed`。
- `GOWORK=off GOCACHE=/tmp/kafkax-gocache CHECK_STATUS=gap GENERATED_BY=codex-goal-slice make evidence`：通过，生成 ignored `release/manifest/latest.json`；manifest 记录 `checks=gap` 与 `tree_state=dirty`。
- `GOWORK=off GOCACHE=/tmp/kafkax-gocache make release-evidence-hash`：通过，生成 ignored `release/manifest/latest.json.sha256`。
- `GOWORK=off GOCACHE=/tmp/kafkax-gocache make release-evidence-check`：通过，在非 `RELEASE_EVIDENCE_REQUIRE_PASSED=1` 模式验证 manifest 结构。
- `GOWORK=off GOCACHE=/tmp/kafkax-gocache make release-evidence-checksum-check`：通过，校验 `release/manifest/latest.json` 与 `.sha256` 一致。

说明：首次未设置 `GOCACHE` 的 `go test` 只因沙箱内默认 Go build cache 目录只读失败；使用 `/tmp/kafkax-gocache` 后验证通过。

## 当前覆盖面

- 公共 API 新增 driver-neutral `Message`、`Header`、`Producer`、`Consumer`、`Admin`、`TopicSpec`、`TopicDescription`、`TopicPlan`、`TopicApplyResult` 和 Kafka 领域 error kind；`docs/api.md` 记录当前语义为 `Send` / `SendBatch` / `Flush`、`Run` / `Poll` / `Commit` / `Pause` / `Resume`、plural topic planning/apply。
- `internal/driver` 定义不反向依赖公开包的 driver descriptor 边界；`testkit.FakeKafka` 提供无 broker、无第三方 Kafka client 的 fake-first contract fixture。
- `contracts/l2-kafka-adapter.schema.json` 绑定 `contracts/kafkax.config.schema.json`、`contracts/kafkax.message.schema.json`、`contracts/kafkax.topic.schema.json`、`contracts/kafkax.metrics.schema.json`、`contracts/error.schema.json` 和 `contracts/health.schema.json`。
- `contracts/contracts_test.go` 校验 message/topic schema 与公开 Go 类型的字段锚点，避免再次漂移到非 canonical schema 名称。
- `docs/api.md`、`docs/standard/l2-kafka-adapter.md`、`docs/standard/harness-gates.md`、`docs/standard/kafkax.md` 和 `docs/release.md` 已统一引用 canonical Kafka L2 contract。
- `docs/adr/ADR-20260604-001-kafka-driver.md` 记录 driver-neutral boundary、fake-first gate 和 broker gate blocked 规则。

## Kafka L2 gate checklist

- `kafka-contract` / static docs-contract gate：通过；`GOWORK=off GOCACHE=/tmp/kafkax-gocache make kafka-contract` 输出 `status=passed`，并由 `make contracts`、`make docs-check`、`go test ./pkg/kafkax ./internal/driver ./testkit ./contracts` 交叉证明。
- `fake-fixture` / testkit fixture gate：通过；`GOWORK=off go test ./pkg/kafkax ./internal/driver/... ./testkit ./contracts` 覆盖 public API、internal driver descriptor、testkit fixture 和 contracts。
- `broker-driver` gates：可执行但仍为 gap；`kafka-integration`、`kafka-fault-injection`、`kafka-metrics-golden` 和 `kafka-admin-golden` 已有 `Makefile`/`goalcli`/`.agent/harness/harness.yaml` 入口，无 production driver 或 broker fixture 时输出 `status=gap` 且非零退出，不能作为 release usable Evidence。
- `release/adoption` gates：部分 gap evidence 已生成；本地 `release/manifest/latest.json` 与 `.sha256` 已生成并校验，但 manifest 仍是 `checks=gap`、`tree_state=dirty`，不能替代 release-ready、downstream adoption 或 retrospective 全套证据。

## 未完成项

- ADR 已补充，且已实现 `internal/driver` descriptor 边界与 `testkit` fake fixture；REQ-004 仍未完整完成，因为 production driver、broker-backed fixture、broker gate 状态和 release evidence 尚未满足。
- Producer/Consumer/Admin 已具备公共接口、testkit fake fixture 和基础行为测试；尚未实现 goal 中要求的真实 broker 投递、consumer group rebalance、lag/retry/DLQ、exactly-once 或 production driver close flush 语义。
- Kafka broker-dependent gates 已有可执行 gap report，但仍不能通过；`kafka-integration`、`kafka-fault-injection`、`kafka-metrics-golden` 和 `kafka-admin-golden` 必须等 production driver、broker fixture 和 golden Evidence 完成后才能升级。
- 已生成并验证 ignored `release/manifest/latest.json` 与 `.sha256` 的本地 gap artifact；release score 9.8 gate 已通过，但 `checks=gap`、`tree_state=dirty` 和缺少 broker/downstream Evidence 表明它不能替代 release-ready/downstream/retrospective 证据。
- 已生成 downstream sync plan，但尚无真实 downstream adoption 证据；`adoption_claim` 不能升级，只能保持 `not_claimed`。
- 尚未产出 retrospective 的 Prompt、Harness/Rule patch 和 New Issue Candidate 全套证据。

## 停止条件

本切片可以作为 contract-first API/文档/静态 gate 的通过证据。完整 `DONE with evidence:` 只能在 `docs/goal/goal.md` 的 release manifest、release score、driver runtime、Kafka-specific gates、downstream adoption 边界和 retrospective 证据全部满足后声明。
