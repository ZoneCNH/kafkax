# Kafkax L2 Contract-First 切片回顾

## 基本信息

- Retro ID: RETRO-20260605-KAFKAX-L2-CONTRACT
- Goal: `docs/goal/goal.md` REQ-001..REQ-015
- Date: 2026-06-05

## 回顾

### 什么有效

- 先用 driver-neutral public API、`internal/driver` descriptor 和 `testkit.FakeKafka` 锁定 Kafka L2 contract，避免在未选择生产 Kafka client 前把第三方 concrete type 泄漏到公开 API。
- `api_contract_test.go`、`contracts/contracts_test.go` 和 fake fixture 测试能证明当前 slice 的 schema/API 锚点。
- gate evidence 明确区分 static contract 通过与 broker-dependent gates blocked，避免把 mock-only 测试写成真实 broker 证据。
- `kafka-contract` 已成为 `Makefile`/`goalcli`/`.agent/harness/harness.yaml` 可执行入口，broker-dependent gates 已能输出结构化 `gap` report，避免 silent skip。

### 什么失败

- 初始审计文档在后续代码和 gate 已收敛后没有同步更新，留下 `go test ./...`、`docs-check`、score 和 Config 缺口的旧结论。
- `pkg/kafkax/doc.go` 与 `testkit/README.md` 保留通用基础库/模板口径，和 Kafka L2 adapter 目标不一致。
- Kafka-specific broker gate 虽已可执行，但当前只能输出 gap report；没有 production driver、broker fixture 和 golden Evidence 时仍不能进入 release usable 状态。

### 根因分析

- 本目标跨度同时覆盖标准、代码、contract、release 和 downstream adoption，单个切片完成后容易混淆“当前 slice 通过”和“完整 goal 通过”。
- broker runtime、production driver 和 downstream proof 是外部证据面，不能由本地 fake fixture 自动补齐。

## Gate / Rule 缺失分析

- 已补的 Gate: `kafka-contract`、`kafka-integration`、`kafka-fault-injection`、`kafka-metrics-golden`、`kafka-admin-golden` 的可执行 `Makefile`/`goalcli` 入口，以及 `.agent/harness/harness.yaml` 中的 required/extended gate 映射。
- 已补的 Rule: broker-dependent gate 在无 broker fixture 时必须输出 `gap` artifact，不得 silent skip。
- 仍缺失的 Evidence: production Kafka driver、broker fixture、fault injection、metrics golden、admin golden 和 release manifest/checksum。
- 需修复的 Prompt: 执行 `docs/goal/goal.md` 时必须每轮先做 REQ-001..REQ-015 状态对账，再声明切片完成。
- 需新增的 CI: static `kafka-contract` 可进入 PR gate；broker gates 进入 extended/release profile，并允许在无 broker fixture 时生成 `gap` evidence。

## 下轮自动避免方案

- 在 evidence 文档中固定三类状态语义：`passed`、goalcli `gap`/文档 blocked、`not_claimed`，禁止用 “partial” 替代 gate 结果。
- 每次修改 Config/API/contract 后运行 `go test ./pkg/kafkax ./internal/driver ./testkit ./contracts` 与 `make contracts`。
- 完整 release 前必须补 `release/manifest/latest.json`、checksum、downstream sync plan、downstream adoption proof 或 blocked owner。

## Patch 输出

### Prompt Patch (PATCH-PROMPT-20260605-KAFKAX-001)

- 执行 Kafka L2 goal 时，先列 REQ-001..REQ-015 当前状态和缺口；任何 DONE 声明必须引用 gate evidence，且 broker/downstream/release 证据缺失时只能声明当前 slice。

### Harness Patch (PATCH-HARNESS-20260605-KAFKAX-001)

- 新增 `kafka-contract` static gate：校验 public API、Kafka schema、harness runtime gate、testkit/API guard marker；当前 `GOWORK=off GOCACHE=/tmp/kafkax-gocache make kafka-contract` 输出 `status=passed`。
- 新增 broker-dependent gate gap artifact 协议：未设置 broker fixture 或 production driver 缺失时 `kafka-integration`、`kafka-fault-injection`、`kafka-metrics-golden`、`kafka-admin-golden` 输出 `gap` JSON，不得标记 passed。

### Rule Patch (PATCH-RULE-20260605-KAFKAX-001)

- `FakeKafka` 只能证明 public API contract 与 fake fixture 行为，不能作为真实 broker delivery、rebalance、lag、DLQ、metrics golden 或 admin golden 证据。
- Downstream matrix 和 adoption registry 只能作为登记证据；没有 downstream repo commit/gate 输出时，`adoption_claim` 必须保持 `not_claimed`。

### CI Gate Suggestion

- PR: `GOWORK=off GOCACHE=/tmp/kafkax-gocache make kafka-contract`
- Release/extended: `KAFKAX_BROKER_FIXTURE=<path> GOWORK=off make kafka-integration kafka-fault-injection kafka-metrics-golden kafka-admin-golden`

### New Issue Candidates

- ISSUE-KAFKAX-001: 实现 production Kafka driver 并选择 driver dependency，附 dependency impact 与 migration plan。
- ISSUE-KAFKAX-002: 增加 broker fixture 与 Kafka integration/fault/metrics/admin gates。
- ISSUE-KAFKAX-003: 生成 release manifest/checksum 并完成 downstream sync/adoption proof。

## 验收

- [x] Retrospective 已生成
- [x] Prompt Patch 已生成
- [x] Harness Patch 已生成
- [x] Rule Patch 已生成
- [x] New Issue Candidates 已生成
