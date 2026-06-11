# Gate

## Required Gates

- Format Gate：`GOWORK=off make fmt`
- Static Check Gate：`GOWORK=off make vet`
- Lint Gate：`GOWORK=off make lint`，缺少 `golangci-lint` 时失败
- Unit Test Gate：`GOWORK=off make test`
- Race Test Gate：`GOWORK=off make race`
- Boundary Gate：`GOWORK=off make boundary`
- Secret Gate：`GOWORK=off make security`，必须委托 `goalcli security` 默认完成密钥扫描；仅当 `XLIB_ENABLE_VULNCHECK=1` 且一周窗口到期，或 `XLIB_FORCE_VULNCHECK=1` 时先执行漏洞扫描
- Contract Gate：`GOWORK=off make contracts`
- Docs Gate：`GOWORK=off make docs-check`
- Integration Gate：`GOWORK=off make integration`，默认下游为 `kernel`
- Evidence Gate：`CHECK_STATUS=passed GOWORK=off make evidence`
- Release Gate：`GOWORK=off make release-check`

## Final Gates

- `XLIB_CONTEXT=release_verify GOWORK=off make release-final-check`
- `XLIB_CONTEXT=release_verify GOWORK=off make release-preflight VERSION=<version>`
- `goalcli score --min 9.8`
- kernel downstream smoke：渲染后执行 `GOWORK=off go test ./...`、`make contracts`、`make boundary` 和 release Evidence 校验。

## Extended Gates

- Property Gate：`GOWORK=off make property`
- Fuzz Smoke Gate：`FUZZ_SMOKE_TIME=<duration> GOWORK=off make fuzz-smoke`
- Golden Gate：`GOWORK=off make golden`
- Extended CI Gate：`GOWORK=off make ci-extended`
- Extended Release Gate：`GOWORK=off make release-check-extended`

## Kafka L2 Adapter Gates

Kafka L2 adapter factory 当前只声明 driver-neutral contract surface；没有 approved driver、broker fixture 或 broker runtime Evidence 时，broker-dependent gate 不得升级为通过。

`.agent/harness/harness.yaml` 将 `kafka_contract` 作为 `required_gates` 中的静态 L2 gate，将 `kafka_integration`、`kafka_fault_injection`、`kafka_metrics_golden` 和 `kafka_admin_golden` 放入 `extended_gates`。broker-dependent gates 在 production driver、broker fixture 和 Evidence 缺失时通过 goalcli report 输出 `status=gap`，文档中的 blocked 语义不得被升级为 passed。

| Gate | 当前证据 | 状态语义 |
| --- | --- | --- |
| `kafka-contract` | `contracts/l2-kafka-adapter.schema.json`、Kafka config/message/topic/metrics schema、`docs/standard/l2-kafka-adapter.md`、`.agent/harness/harness.yaml` | 可由静态 docs/schema/contract 检查覆盖 |
| `kafka-integration` | 需要真实 Kafka broker、driver 选择、producer/consumer/admin smoke 和 Evidence artifact | 未实现前必须是 `gap`，不得写成 `passed` |
| `kafka-fault-injection` | 需要 broker outage、retry/backoff、rebalance 和 DLQ 证据 | 未实现前必须是 `gap` |
| `kafka-metrics-golden` | 需要 metrics golden fixture 证明 label allowlist、secret/message-value 不泄露 | 未实现前必须是 `gap` |
| `kafka-admin-golden` | 需要 topic/admin golden fixture 和 broker 版本记录 | 未实现前必须是 `gap` |

## Policy

Required Gates 是 `kafkax` 和所有生成基础库的强制基线。Extended Gates 推荐所有生成基础库实现，并对 storage、messaging、observability 和 security-sensitive 基础库强制执行。Chaos、mutation 和 long soak 等 profile-specific heavy gates 不进入默认 `make ci`。


## Goal v2.9.3 Governance Gates

- P0 Governance Gate：`XLIB_CONTEXT=local_write GOWORK=off make governance-check`，串联 `main-guard`、`worktree-guard`、`evidence-check`、`boundary`、`architecture`、`domain`、`security`、`security-debt`、`contracts`、`docs-check`、`cli-contract`、`issue-registry`、`command-registry`、`makefile-baseline`、`audit-goal`、`rules-consistency-check`、`debt` 和 `traceability-check`。
- P1 Governance Dry Run：`GOWORK=off make p1-governance-check`，验证 `agent-team-contract`、`scope-lock`、`pr-template`、`acceptance-matrix`、`runtime-health`、`upgrade-standard`、`conformance-profile`、`downstream-registry`、`self-healing-skeleton`、`goal-runtime`、`github-governance`、`supply-chain`、`changelog`、`governance-fixture-test`、`autoresearch`、`policy-schema`、`github-settings`、`toolchain`、`evidence-artifacts` 和 `naming` 的本地 dry-run 契约；不读取 GitHub secrets，不写外部路径。
- P2 Runtime Dry Run：`GOWORK=off make p2-runtime-check`，验证 `install-runtime`、`upgrade-runtime`、`release-ready`、`evidence-replay`、`attest-conformance`、`pack-standard`、`pack-gate`、`pack-evidence`、`downstream-baseline`、`downstream-adoption`、`runtime-file-ownership` 和 `execution-context` 的本地 dry-run 契约。
