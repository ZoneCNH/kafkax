# kafkax REQ-001..REQ-015 当前证据审计（2026-06-05）

本文件是 worker-1 对 `docs/goal/goal.md` 中 REQ-001..REQ-015 的当前仓库事实审计，来源为上下文文件 `/home/kafkax/.omx/context/kafkax-l2-standard-factory-20260605T002438Z.md`、目标文件 `docs/goal/goal.md` 与当前工作树。它不是完成声明；缺口必须保持显式，不得把登记表或模板基线写成已采纳、已集成或已发布。

## 结论

- 当前仓库已经具备基础 Go module、治理/合约目录、通用测试夹具、通用 gate 与下游矩阵登记。
- 当前 `pkg/kafkax` 仍是通用模板式 client/config/health/metrics 包，不是 Kafka 适配库 API。
- 生产/消费/Admin/driver/testkit fake/Kafka 专用 gate/release evidence/downstream proof 均未满足。
- 第一安全交付切片应保持 contract-first：先补 `pkg/kafkax` 的 driver-neutral public API skeleton 与测试，不引入第三方 Kafka 类型或新依赖。

## REQ 审计矩阵

| ID | 当前状态 | 证据 | 缺口 / 约束 |
| --- | --- | --- | --- |
| REQ-001 | 部分满足 | `docs/downstream-matrix.md:5-17` 明确矩阵不是采纳证据，`kafkax` 是 L2 目标；`.agent/registries/downstream-adoption-status.yaml:85-91` 标记 `not_adopted` / `not_run`；`:127-133` 禁止把登记/缺证据升级为采纳。 | 尚未在本切片运行并落盘 `GOWORK=off make standard-impact-check`、`release/standard-impact/latest.md`、`GOWORK=off make downstream-sync-plan` 的通过证据。`docs/standard/layer-governance-rules.md:20` 写着 `kafkax` 不依赖 `kernel`/L1/L2，和 `docs/downstream-matrix.md:17` 的允许依赖口径存在待决策差异。 |
| REQ-002 | 部分满足 | `go.mod:1-3` 为 `github.com/ZoneCNH/kafkax`、Go 1.23；存在 `.agent/`、`contracts/`、`docs/`、`scripts/`、`release/manifest/template.json`；未发现 `release/manifest/latest.json`。 | `pkg/kafkax/doc.go:1-5` 与 `testkit/README.md:1-15` 仍有“基础库/模板”口径；独立仓库 gate 仍需 `scripts/check_rendered_template.sh`、`GOWORK=off go test ./...`、`make boundary`、clean status 证明。 |
| REQ-003 | 未满足 | `pkg/kafkax/client.go:8-78` 只有 `Client/New/Close`；`pkg/kafkax/config.go:11-40` 是通用 `Name/Timeout/Secret`；仓库搜索未发现 `Producer`、`Consumer`、`Admin`、`TopicSpec`、`Header`、`Offset` 的 public type。 | 需要 driver-neutral public API，覆盖 Producer、Consumer、Admin、TopicSpec、Message、Header、Offset、Error、Health、Config；不得暴露 `kgo.*`、`kafka-go.*`、`confluent.*` 等第三方 Kafka concrete types。 |
| REQ-004 | 未满足 | `internal/` 下没有 `internal/driver/`；仓库搜索未发现 `internal/driver`、`franz` 或 fake driver 实现。 | 需要最小 driver contract、默认 driver 隔离或 ADR、fake driver 测试；public API 不应随 driver 变化。 |
| REQ-005 | 未满足 | `pkg/kafkax/client.go:16-78` 只支持 New/Close；`pkg/kafkax/errors.go:8-20` 是通用错误种类；`pkg/kafkax/metrics.go:3-12` 是 client 级指标。 | 需要 producer send/batch/cancel/metadata/error classification/close flush/metrics contract 与测试。 |
| REQ-006 | 未满足 | 未发现 consumer group、manual commit、rebalance、pause/resume、lag metric public API。 | 需要 Consumer API、handler contract、offset commit、rebalance hook、graceful shutdown 与测试。 |
| REQ-007 | 未满足 | 未发现 `Admin`、`TopicSpec`、topic metadata plan/apply API。 | 需要 admin topic/ACL/metadata contract；默认不得 auto-create topic；需要 dry-run/plan/apply 语义。 |
| REQ-008 | 部分满足 | `pkg/kafkax/config.go:11-40` 有显式 Config、Validate、Sanitize；`docs/standard/layer-governance-rules.md:24` 禁止基础库读取 `/home/k8s/secrets/env/*`。 | 现有 Config 缺少 Brokers、ClientID、Security、Producer、Consumer、Admin、Retry、Observability 等 Kafka 配置；单个 `Secret` 红线不足以证明完整安全配置契约。 |
| REQ-009 | 部分满足 | `pkg/kafkax/health.go:25-139` 与 `pkg/kafkax/metrics.go:3-12` 有通用 health/metrics contract。 | 缺少 broker 连接状态、producer/consumer/admin readiness、lag、rebalance、DLQ、retry、throttle、structured logging/tracing 语义。 |
| REQ-010 | 未满足 | 未发现 resilience policy、retry/DLQ/circuit-breaker/backoff 等 Kafka 语义；`docs/downstream-matrix.md:17` 当前允许依赖只列 `kernel`、`configx`、`observex`。 | 新增 resiliencx/schedulex 等依赖前必须先做 Standard Impact；本切片不应新增依赖。 |
| REQ-011 | 未满足 | `testkit/README.md:1-15` 仅说明通用 Config/RequireNoError/RequireGolden；未发现 fake Kafka runtime 或 producer/consumer shared contract harness。 | 需要 fake producer/consumer/admin、golden record helpers、contract tests；禁止真实 Kafka 作为 unit 默认依赖。 |
| REQ-012 | 部分满足 | `Makefile` 有通用 `test`、`race`、`lint`、`standard-impact-check`、`docs-check`、`security`、`boundary`、`contracts`、`evidence` 等 target。 | 未发现 `kafka-contract`、`kafka-integration`、fault injection、metrics golden、admin golden 等 Kafka 专用 gate；broker integration 无可运行证据时应 blocked。 |
| REQ-013 | 部分满足 | `release/manifest/template.json` 存在，`release/manifest/latest.json` 未发现，符合“不要提交生成 latest”约束。 | 还没有 Kafka driver/client/broker 版本、gate 结果、checksum、release evidence manifest；不能填充真实 release 完成声明。 |
| REQ-014 | 未满足 | `.agent/registries/downstream-adoption-status.yaml:121-133` 明确 `no_proof_based_adoption` 且禁止 `proof_contract_absent != adopted`；`docs/downstream-matrix.md:7` 明确矩阵不是采纳证据。 | 当前 adoption claim 必须保持 `not_claimed` / blocked；没有真实 downstream proof、commit SHA、go.mod replacement 或 gate 输出。 |
| REQ-015 | 部分满足 | `.agent/` 下存在 rules、harness、docs、retrospective 模板和 docker-toolchain retrospective。 | 未发现本次 kafkax REQ 收敛所需的专属 retrospective、prompt patch、harness patch、rule patch 或 issue candidate 证据。 |

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

## 下一安全切片

1. 在 `pkg/kafkax` 内先补 contract-first public API skeleton（Producer、Consumer、Admin、Message、Header、Offset、TopicSpec、config/options/error 分类），不引入第三方 Kafka concrete types。
2. 为 public API skeleton 增加编译级/行为级单元测试与文档化契约。
3. 再补 `internal/driver` 与 `testkit` fake，最后增加 Kafka-specific gate；在没有 broker/driver 证据前，release/downstream adoption 继续保持 blocked/not_claimed。
