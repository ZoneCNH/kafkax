# kafkax REQ-001..REQ-015 当前证据审计（2026-06-05）

本文件是 worker-1 对 `docs/goal/goal.md` 中 REQ-001..REQ-015 的初始仓库事实审计，来源为上下文文件 `/home/kafkax/.omx/context/kafkax-l2-standard-factory-20260605T002438Z.md`、目标文件 `docs/goal/goal.md` 与审计时工作树。它不是完成声明；缺口必须保持显式，不得把登记表或模板基线写成已采纳、已集成或已发布。

更新说明：本文保留 2026-06-05 初始审计快照；后续切片已补充 public API、`internal/driver` descriptor 边界、testkit fake fixture 和 topic contract。当前状态以 `docs/goal/kafkax-gate-evidence-20260605.md` 与代码为准；下方“未发现”“未满足”表述是变更前事实，不作为当前完成声明。

## 初始审计结论

- 初始仓库已经具备基础 Go module、治理/合约目录、通用测试夹具、通用 gate 与下游矩阵登记。
- 初始 `pkg/kafkax` 仍是通用模板式 client/config/health/metrics 包，不是 Kafka 适配库 API。
- 初始审计未发现生产/消费/Admin/driver/testkit fake/Kafka 专用 gate/release evidence/downstream proof。
- 第一安全交付切片应保持 contract-first：先补 `pkg/kafkax` 的 driver-neutral public API skeleton 与测试，不引入第三方 Kafka 类型或新依赖。

## 当前切片状态（2026-06-05 收敛后）

- 已补 `pkg/kafkax` driver-neutral public API：`Message`、`Header`、`Producer`、`Consumer`、`Admin`、`TopicSpec`、`TopicPlan`、`TopicApplyResult` 与注入式 `Client` accessor。
- 已补 `internal/driver` descriptor 边界和 `testkit.FakeKafka` fixture，覆盖 producer/consumer/admin fake 行为与 clone isolation。
- 已补 topic contract schema 映射测试、公开 API 第三方 Kafka concrete type 禁止测试、ADR 与 gate evidence。
- 已补可执行 Kafka gate 入口：`kafka-contract` 输出 `status=passed`；`.agent/harness/harness.yaml` 映射 `kafka_contract` required gate 与四个 broker extended gap gates；broker-dependent gates 输出 `status=gap` 且非零退出，明确不能替代 broker-backed Evidence。
- 已通过基础治理 gate：`go test ./...`、`go test ./cmd/goalcli`、`make contracts`、`make docs-check`、`make cli-contract`、`make command-registry`、`make makefile-baseline`、`make boundary`、`make standard-impact-check` 和 `make score`；运行时使用 `GOCACHE=/tmp/kafkax-gocache` 避免沙箱内默认 Go build cache 只读。
- 已生成并校验本地 release manifest/checksum gap artifact；`release/manifest/latest.json` 与 `.sha256` 是 ignored 产物，manifest 使用 `CHECK_STATUS=gap` 且记录 `tree_state=dirty`，不能作为 release-ready Evidence。
- 尚未完成完整目标：production driver、broker-backed Kafka gate、release-ready manifest、downstream adoption proof 和完整 release retrospective 仍保持显式 blocked/not_claimed；`downstream-sync-plan` 已验证同步计划，但输出仍保持 `adoption_claim=not_claimed`。

## REQ 审计矩阵

| ID | 当前状态 | 证据 | 缺口 / 约束 |
| --- | --- | --- | --- |
| REQ-001 | 部分满足 | `docs/downstream-matrix.md:5-17` 明确矩阵不是采纳证据，`kafkax` 是 L2 目标；`.agent/registries/downstream-adoption-status.yaml:85-91` 标记 `not_adopted` / `not_run`；`:127-133` 禁止把登记/缺证据升级为采纳；`GOWORK=off GOCACHE=/tmp/kafkax-gocache make standard-impact-check` 已通过并重新生成 `release/standard-impact/latest.md`；`GOWORK=off GOCACHE=/tmp/kafkax-gocache make downstream-sync-plan` 已通过，输出 `downstream_sync_required=true`、`target_count=11`、`adoption_claim=not_claimed`。 | `docs/standard/layer-governance-rules.md` 表达的是 Standard/Runtime 仓库自身不得依赖 L0/L1/L2 生成库，`docs/downstream-matrix.md` 表达的是生成后的 L2 目标库允许依赖；同步计划已生成，但不构成真实 downstream adoption proof。 |
| REQ-002 | 部分满足 | `go.mod:1-3` 为 `github.com/ZoneCNH/kafkax`、Go 1.23；存在 `.agent/`、`contracts/`、`docs/`、`scripts/`、`release/manifest/template.json`；`pkg/kafkax/doc.go` 与 `testkit/README.md` 已改为 Kafka L2 adapter/testkit 口径；`GOWORK=off GOCACHE=/tmp/kafkax-gocache go test ./...` 与 `make boundary` 已通过。 | `scripts/check_rendered_template.sh` 和 clean status 仍未作为本切片完成证据；当前工作树含 staged delivery changes，因此 clean status 只能在提交/发布前验证。 |
| REQ-003 | 部分满足 | 当前 `pkg/kafkax` 已包含 `Producer`、`Consumer`、`Admin`、`TopicSpec`、`Message`、`Header`、`Offset`、`TopicPlan`、`TopicApplyResult` 等 driver-neutral public types；`pkg/kafkax/api_contract_test.go` 禁止公开 API 暴露第三方 Kafka concrete types。 | 仍需 production driver 与 broker-backed contract 证明；Error、Health、Config 的 Kafka 专用语义仍需继续收敛。 |
| REQ-004 | 部分满足 | `internal/driver/driver.go` 提供 descriptor/capability 边界；`testkit/kafka.go` 和 `testkit/kafka_test.go` 提供 fake driver fixture；`docs/adr/ADR-20260604-001-kafka-driver.md` 固化 fake-first 边界。 | 默认 production driver、具体 Kafka client 选择、broker gate 与 release evidence 尚未完成。 |
| REQ-005 | 部分满足 | `pkg/kafkax/producer.go` 定义 `Producer.Send`、`SendBatch`、`Flush`、`Close` 与结果 clone；`testkit` fake producer 覆盖 send/batch/flush/cancel 行为。 | 仍需 broker delivery、metadata/error classification、close flush 与 metrics 的真实 driver 证据。 |
| REQ-006 | 部分满足 | `pkg/kafkax/consumer.go` 定义 `Consumer.Run`、`Poll`、`Commit`、`Pause`、`Resume`、`Close`；`Client.Consumer` 支持 group/topic/default subscription 注入；`testkit` fake consumer 覆盖 poll/commit/pause/resume/cancel。 | 仍需 broker group/rebalance/lag metric/graceful shutdown 的真实 driver 证据。 |
| REQ-007 | 部分满足 | `pkg/kafkax/admin.go` 定义 `Admin`、`TopicSpec`、topic describe/plan/apply contract；`testkit` fake admin 覆盖 create/update/noop plan/apply。 | ACL、broker metadata、conflict handling 与 broker-backed admin golden 仍未完成。 |
| REQ-008 | 部分满足 | `pkg/kafkax/config.go` 已包含 `Brokers`、`ClientID`、`Security`、`Producer`、`Consumer`、`Admin`、`Retry`、`Observability`，并对密码/token 做 sanitize；`docs/standard/layer-governance-rules.md:24` 禁止基础库读取 `/home/k8s/secrets/env/*`。 | 安全配置仍是 driver-neutral contract surface；TLS/SASL 文件来源、真实 credential provider、broker auth failure gate 和 secret leak golden 仍需真实 driver/broker 或专门 security evidence。 |
| REQ-009 | 部分满足 | `pkg/kafkax/health.go:25-139` 与 `pkg/kafkax/metrics.go:3-12` 有通用 health/metrics contract。 | 缺少 broker 连接状态、producer/consumer/admin readiness、lag、rebalance、DLQ、retry、throttle、structured logging/tracing 语义。 |
| REQ-010 | 未满足 | 未发现 resilience policy、retry/DLQ/circuit-breaker/backoff 等 Kafka 语义；`docs/downstream-matrix.md:17` 当前允许依赖只列 `kernel`、`configx`、`observex`。 | 新增 resiliencx/schedulex 等依赖前必须先做 Standard Impact；本切片不应新增依赖。 |
| REQ-011 | 部分满足 | `testkit/kafka.go` 已提供 `FakeKafka`、fake producer/consumer/admin 与 golden record helper；`testkit/kafka_test.go` 覆盖 round-trip、clone isolation、cancel、admin plan/apply。 | broker-backed shared contract、fault injection、metrics/admin golden gate 仍未完成；真实 Kafka 仍不得作为 unit 默认依赖。 |
| REQ-012 | 部分满足 | `Makefile`、`goalcli` 与 `.agent/harness/harness.yaml` 已提供 `kafka-contract`、`kafka-integration`、`kafka-fault-injection`、`kafka-metrics-golden`、`kafka-admin-golden`；`kafka_contract` 位于 `required_gates`，四个 broker gates 位于 `extended_gates`；`GOWORK=off GOCACHE=/tmp/kafkax-gocache make kafka-contract` 输出 `status=passed`；`GOWORK=off GOCACHE=/tmp/kafkax-gocache go run ./cmd/goalcli kafka-integration` 输出 `status=gap` 且非零退出；`make command-registry`、`make makefile-baseline`、`make cli-contract` 已通过。 | broker-dependent gates 当前只是可执行 gap report；production driver、broker fixture、fault injection、metrics golden 和 admin golden Evidence 完成前不得标记 passed 或 release usable。 |
| REQ-013 | 部分满足 | `release/manifest/template.json` 存在；`CHECK_STATUS=gap GENERATED_BY=codex-goal-slice make evidence`、`make release-evidence-hash`、`make release-evidence-check` 和 `make release-evidence-checksum-check` 已通过；`release/manifest/latest.json` 与 `.sha256` 是 ignored 生成产物，不提交源码历史。 | 当前只是本地 gap manifest：`checks=gap`、`tree_state=dirty`，且没有 Kafka driver/client/broker 版本、broker gate passed 结果或 downstream proof；不能填充真实 release 完成声明。 |
| REQ-014 | 未满足 | `.agent/registries/downstream-adoption-status.yaml:121-133` 明确 `no_proof_based_adoption` 且禁止 `proof_contract_absent != adopted`；`docs/downstream-matrix.md:7` 明确矩阵不是采纳证据。 | 当前 adoption claim 必须保持 `not_claimed` / blocked；没有真实 downstream proof、commit SHA、go.mod replacement 或 gate 输出。 |
| REQ-015 | 部分满足 | `.agent/` 下存在 rules、harness、docs、retrospective 模板和 docker-toolchain retrospective；本切片新增 `.agent/retrospective/kafkax-l2-contract-20260605.md` 记录 Prompt/Harness/Rule patch 和 issue candidates。 | retrospective 仍只是当前 contract-first 切片；完整 release retrospective 需在 production driver、broker gates、release manifest 和 downstream adoption 决策完成后补齐。 |

## 已运行的审计命令

```bash
nl -ba docs/goal/goal.md | sed -n '289,547p'
nl -ba go.mod | sed -n '1,40p'
nl -ba pkg/kafkax/client.go | sed -n '1,140p'
nl -ba pkg/kafkax/config.go | sed -n '1,120p'
nl -ba pkg/kafkax/errors.go | sed -n '1,140p'
nl -ba pkg/kafkax/health.go | sed -n '1,180p'
nl -ba pkg/kafkax/metrics.go | sed -n '1,120p'
nl -ba pkg/kafkax/options.go | sed -n '1,100p'
nl -ba docs/downstream-matrix.md | sed -n '1,40p'
nl -ba .agent/registries/downstream-adoption-status.yaml | sed -n '80,135p'
find internal -maxdepth 3 -type d | sort
find contracts -maxdepth 2 -type f | sort
find release/manifest -maxdepth 1 -type f -print
rg -n "type (Producer|Consumer|Admin|TopicSpec|Header|Offset)\\b|type Message\\b|internal/driver|kgo\\.|kafka-go|confluent|kafka-contract|kafka-integration|fault-injection|metrics-golden|admin-golden" pkg internal contracts testkit Makefile docs .agent --glob '!docs/goal/goal.md' --glob '!AGENTS.md'
```

## 并行探针结论

Subagent `019e952f-0596-77f3-9ffb-81f876b40196` / `Ptolemy` 做了只读复核并确认：当前不能把矩阵登记当作采纳；`pkg/kafkax` 不是 Kafka adapter；generic gate 不能替代 Kafka-specific gate；单个 `Secret` redaction 不能证明完整安全配置；新增 resilience/scheduling dependency 需要先做 Standard Impact。

## 本审计文件验证快照

- `git diff --cached --check`：通过。
- `GOWORK=off GOCACHE=/tmp/kafkax-gocache go test ./pkg/kafkax ./internal/driver ./testkit ./contracts`：通过。
- `GOWORK=off GOCACHE=/tmp/kafkax-gocache go test ./...`：通过。
- `GOWORK=off GOCACHE=/tmp/kafkax-gocache go test ./cmd/goalcli`：通过。
- `GOWORK=off GOCACHE=/tmp/kafkax-gocache make contracts`：通过。
- `GOWORK=off GOCACHE=/tmp/kafkax-gocache make docs-check`：通过。
- `GOWORK=off GOCACHE=/tmp/kafkax-gocache make kafka-contract`：通过，输出 `status=passed`。
- `GOWORK=off GOCACHE=/tmp/kafkax-gocache go run ./cmd/goalcli kafka-integration`：按预期非零退出，输出 `status=gap`。
- `GOWORK=off GOCACHE=/tmp/kafkax-gocache go run ./cmd/goalcli kafka-fault-injection`：按预期非零退出，输出 `status=gap`。
- `GOWORK=off GOCACHE=/tmp/kafkax-gocache go run ./cmd/goalcli kafka-metrics-golden`：按预期非零退出，输出 `status=gap`。
- `GOWORK=off GOCACHE=/tmp/kafkax-gocache go run ./cmd/goalcli kafka-admin-golden`：按预期非零退出，输出 `status=gap`。
- `GOWORK=off GOCACHE=/tmp/kafkax-gocache make command-registry`：通过。
- `GOWORK=off GOCACHE=/tmp/kafkax-gocache make makefile-baseline`：通过。
- `GOWORK=off GOCACHE=/tmp/kafkax-gocache make cli-contract`：通过。
- `GOWORK=off GOCACHE=/tmp/kafkax-gocache make boundary`：通过。
- `GOWORK=off GOCACHE=/tmp/kafkax-gocache make standard-impact-check`：通过。
- `GOWORK=off GOCACHE=/tmp/kafkax-gocache make downstream-sync-plan`：通过，输出 `adoption_claim=not_claimed`。
- `GOWORK=off GOCACHE=/tmp/kafkax-gocache make score`：通过，score `10` >= `9.8`。
- `GOWORK=off GOCACHE=/tmp/kafkax-gocache CHECK_STATUS=gap GENERATED_BY=codex-goal-slice make evidence`：通过，生成 ignored gap manifest。
- `GOWORK=off GOCACHE=/tmp/kafkax-gocache make release-evidence-hash`：通过，生成 ignored checksum。
- `GOWORK=off GOCACHE=/tmp/kafkax-gocache make release-evidence-check`：通过，验证 manifest 结构。
- `GOWORK=off GOCACHE=/tmp/kafkax-gocache make release-evidence-checksum-check`：通过，验证 checksum 一致。

## 下一安全切片

1. 在 `pkg/kafkax` 内先补 contract-first public API skeleton（Producer、Consumer、Admin、Message、Header、Offset、TopicSpec、config/options/error 分类），不引入第三方 Kafka concrete types。
2. 为 public API skeleton 增加编译级/行为级单元测试与文档化契约。
3. 下一切片应实现或选择 production Kafka driver，并为 `kafka-integration`、`kafka-fault-injection`、`kafka-metrics-golden`、`kafka-admin-golden` 提供可重复 broker fixture；在没有 broker/driver 证据前，release/downstream adoption 继续保持 blocked/not_claimed。
