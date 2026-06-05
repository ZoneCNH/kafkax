# GOAL-20260604-KAFKAX-L2-STANDARD-FACTORY

# kafkax：从零散 Kafka 基础设施封装库升级为 xlib-standard 标准源控制下的 L2 基础设施适配层标准工厂

> 版本：v1.0 Complete Execution Plan  
> 日期：2026-06-04（Asia/Tokyo）  
> 目标对象：`kafka` / `kafkax` 统一升级为 `github.com/ZoneCNH/kafkax`  
> 标准源：`github.com/ZoneCNH/xlib-standard`  
> L0：`github.com/ZoneCNH/kernel`  
> L1：`configx` / `observex` / `testkitx` / `resiliencx` / `schedulex`（其中生产依赖必须以 `xlib-standard` 当前标准矩阵为准；若矩阵未登记，必须先做 Standard Impact）  
> L2：`kafkax`  
> 执行模式：Goal Runtime Prompt v3.1 / Full Mode  
> 完成声明要求：`DONE with evidence:`；没有 Evidence 不允许声称完成。

---

## 0. 当前事实快照

### 0.1 已确认事实

1. `xlib-standard` 当前定位不是普通模板仓库，而是基础库标准与交付运行时仓库，承担五类职责：
   - Standard Source
   - Go Reference Template
   - Generator
   - Harness
   - Evidence Runtime

2. `xlib-standard` 当前下游矩阵已经把 `kafkax` 登记为 L2 目标库：
   - module path：`github.com/ZoneCNH/kafkax`
   - package：`kafkax`
   - layer：L2
   - 当前允许依赖：`kernel`、`configx`、`observex`
   - 当前禁止依赖：业务 topic 设计、业务消息 schema

3. 当前下游采纳状态文件中，`kafkax` 是 `standard_target_declared`，但 `adoption_status = not_adopted`、`evidence_state = not_run`。因此本方案必须把 `kafkax` 当作“已登记目标库，但尚未完成 proof-based adoption”的对象处理。

4. 当前 `xlib-standard` 生成机制以 `scripts/render_template.sh` 为入口，生成后的库必须独立运行 release gate，并生成自己的 `release/manifest/latest.json` Evidence；`latest.json` 不能提交到源码历史。

5. 当前标准强调：
   - 不允许把 registry / patch-only / baseline scan 误升级成 adopted。
   - 不允许导入 `x.go`。
   - 不允许读取或泄露 `/home/k8s/secrets/env/*` 的真实内容。
   - 不允许在没有命令输出和 Evidence artifact 的情况下宣称完成。

### 0.2 需要人工确认或 AutoResearch 的事实

1. `ZoneCNH/kafkax` 仓库是否已创建。若不存在，必须新建；若仅存在 `ZoneCNH/kafka`，应决定是重命名、迁移还是保留为历史占位仓库。
2. `resiliencx`、`schedulex` 是否已被 `xlib-standard` 正式登记为 L1 生产依赖。若当前标准矩阵未登记，`kafkax` 不能直接把它们作为 production dependency；必须先提交 Standard Impact。
3. Kafka Go client 默认实现选择：推荐默认 `franz-go`，但必须通过 ADR 记录取舍，并允许后续替换 internal driver，不泄露第三方类型到公共 API。
4. CI 是否允许启动 Docker / Redpanda / Kafka container 做 integration gate；如果不允许，必须把 broker integration 标记为 extended gate，而 MVA 使用 fake driver + contract tests。

---

## 1. 问题的底层本质

`kafkax` 的真实问题不是“封装一个 Kafka 客户端”，而是：

> 如何把 Kafka 这类高复杂度基础设施适配能力，从一次性、项目内、经验式封装，升级为一个可复制、可验证、可发布、可追责、可下游采纳、可自我改进的 L2 标准工厂单元。

这意味着 `kafkax` 必须同时满足三类约束：

1. **Kafka 语义正确性**：生产、消费、分区、offset、consumer group、rebalance、retry、DLQ、事务、幂等、压缩、认证、TLS、错误分类、观测性和 graceful shutdown 不能靠“薄封装”糊弄。
2. **xlib-standard 工厂约束**：独立仓库、独立版本、独立 Evidence、共享 L0/L1 契约、共享 Harness、共享 Release Gate、共享下游采纳协议。
3. **复利工程约束**：每次实现 `kafkax` 的经验必须反哺 `xlib-standard` 的 generator、contracts、harness、rule patch、prompt patch，使后续 `redisx`、`postgresx`、`taosx`、`ossx`、`clickhousex`、`natsx` 的实现成本下降。

最终目标不是一个“能发消息”的库，而是一个可复制到所有 L2 基础设施适配库的标准工厂样板。

---

## 2. 不可再拆解的基本真理

### 2.1 工程基本真理

1. **没有边界就没有复用**：L2 适配库必须只表达基础设施语义，不能表达业务语义。
2. **没有 Evidence 就没有完成**：文档、计划、PR 描述、口头说明都不是完成证明。
3. **没有独立 release 就没有独立基础库**：`kafkax` 必须可以脱离 `x.go`、脱离业务系统、脱离本地 `go.work` 运行全部 gate。
4. **没有 contract 就没有稳定 API**：Kafka 适配库必须先冻结公共 API、配置契约、错误契约、metrics contract，再写 driver 实现。
5. **没有 Harness 就没有工厂**：只有自动门禁能复制，人工经验不能规模化复制。
6. **没有下游采纳证据就不能说已被采用**：registry 只是目标登记，只有 downstream repo 的当前命令输出和 artifact 才是 proof-based adoption。
7. **没有 self-improving 就没有复利**：每次失败都必须变成 Prompt Patch、Harness Patch、Rule Patch、CI Gate Suggestion 或 New Issue Candidate。

### 2.2 Kafka 基本真理

1. **Kafka 的顺序保证只在同一 topic partition 内成立**，不能承诺跨 partition 全局顺序。
2. **Exactly-once 不是 wrapper 自动提供的魔法**，它依赖 broker、producer 幂等、transactional.id、consumer isolation、offset commit 和业务端幂等处理共同成立。
3. **retry 可能破坏顺序、放大重复、制造流量风暴**，必须受 budget、backoff、DLQ 和 idempotency key 约束。
4. **consumer group rebalance 是正常事件，不是异常事件**，必须显式处理 revoke/assign、flush、commit、shutdown。
5. **offset commit 是一致性边界**，handler 成功前后提交的差异决定 at-most-once / at-least-once 语义。
6. **topic、key、schema 是业务边界**，L2 库不能内置业务 topic 或业务消息结构。
7. **Kafka 连接是长生命周期资源**，必须受 L0 lifecycle / context / shutdown 约束，不得使用隐藏全局 client。

---

## 3. 被误认为真理的常见假设

| 常见假设 | 为什么是错的 | 正确处理 |
|---|---|---|
| “Kafka 封装就是 producer + consumer helper” | 这只是 API 便利层，不是标准工厂 | 先定义 contracts、boundary、harness、evidence，再实现 helper |
| “能发消息就完成了” | 没有 offset、rebalance、error、metrics、shutdown，不可生产使用 | 必须覆盖生产、消费、管理、观测、测试、发布全链路 |
| “registry 里有 kafkax 就表示已采用” | registry 只是目标登记 | 必须有 downstream repo 的 gate 输出和 evidence artifact |
| “用一个 Go Kafka client 就锁定实现” | 第三方类型泄露会绑死公共 API | 第三方库只能在 internal driver 内部出现 |
| “重试越多越稳定” | 重试可能导致重复、延迟雪崩、顺序错乱 | 使用 resiliencx budget、backoff、DLQ、idempotency key |
| “自动建 topic 更方便” | 生产环境 topic 管理通常需要 ACL、分区、保留策略审计 | Admin 能力可选，默认显式 TopicSpec + plan/apply |
| “业务 topic 和 schema 顺手放进库里” | 这会把 L2 污染成 L3 | L2 只提供 generic codec / envelope / headers |
| “release manifest 提交到仓库方便追踪” | 当前标准明确 latest.json 是生成 Evidence artifact，不应提交 | CI 上传 artifact，源码只保留 template 和协议 |
| “main 上直接修很快” | 会破坏可追踪、并发隔离和 rollback | 必须使用 git worktree + feature branch |
| “x.go 需要什么，kafkax 就加什么” | 这会造成反向依赖和业务入侵 | x.go 只能作为下游采纳方，不能定义 L2 公共边界 |

---

## 4. 可以被打破的限制

1. **可以打破“每个库各写一套 CI”的限制**：改成从 `xlib-standard` 生成、继承并按 L2 profile 扩展。
2. **可以打破“Kafka client 类型直接暴露”的限制**：公共 API 只暴露 `kafkax` 自己的稳定类型，第三方实现放入 `internal/driver/*`。
3. **可以打破“只有真实 Kafka 才能测”的限制**：分层测试为 fake contract、golden、property、broker integration、fault injection。
4. **可以打破“文档靠人工更新”的限制**：generator、docs-check、standard-impact-check、downstream-sync-plan 自动检查漂移。
5. **可以打破“失败只是失败”的限制**：失败必须转成 Self-improving patch。
6. **可以打破“L2 适配库只能服务 x.go”的限制**：`kafkax` 必须是独立基础库，可以服务任何 Go 项目；`x.go` 只是下游采纳方。
7. **可以打破“标准一次写完”的限制**：通过 Standard Impact + Rule Patch 把 `kafkax` 经验回流到 `xlib-standard`。

---

## 5. 从零设计的新方案

### 5.1 总体定位

`kafkax` 是 L2 Kafka 基础设施适配层，不是业务事件总线，不是 topic 目录，不是消息 schema 仓库，不是 x.go 专用 SDK。

它的职责：

1. 提供 Kafka producer / consumer / admin 的稳定公共 API。
2. 统一配置加载、脱敏、校验、默认值、secret reference 处理。
3. 统一错误分类、重试、timeout、circuit breaker、bulkhead、rate limit、budget。
4. 统一 metrics、logging、tracing、health、readiness、liveness。
5. 提供可复用 testkit、fake driver、contract test、integration harness。
6. 生成 release Evidence，并支持 downstream proof-based adoption。
7. 把新增经验反哺 `xlib-standard` 的 L2 工厂标准。

### 5.2 分层架构

```text
xlib-standard
  ├─ Standard Source
  ├─ Go Reference Template
  ├─ Generator
  ├─ Harness
  └─ Evidence Runtime
        ↓ generate / govern
L0: kernel
  ├─ error
  ├─ lifecycle
  ├─ context
  ├─ clock
  ├─ shutdown
  └─ validation primitive
        ↓
L1: cross-cutting capabilities
  ├─ configx       显式配置、配置 schema、脱敏、secret reference
  ├─ observex      metrics / logging / tracing / health contracts
  ├─ testkitx      fake runtime / golden / contract / harness helper
  ├─ resiliencx    retry / timeout / circuit breaker / bulkhead / rate limit / budget
  └─ schedulex     可选：后台任务、periodic metadata refresh、maintenance loop
        ↓
L2: kafkax
  ├─ producer
  ├─ consumer
  ├─ admin
  ├─ topic metadata
  ├─ offset policy
  ├─ error classification
  ├─ DLQ / retry policy
  ├─ config / security
  ├─ metrics / tracing / health
  └─ testkit / contracts / evidence
        ↓
L3: consumers
  ├─ x.go
  ├─ market-data-server
  ├─ macro-data-server
  ├─ regime-server
  └─ other applications
```

### 5.3 关键边界

| 层 | 允许做 | 禁止做 |
|---|---|---|
| `xlib-standard` | 定义标准、generator、harness、evidence、L2 profile | 写业务 Kafka topic 或业务 schema |
| `kernel` | 错误、生命周期、context、shutdown、validation | 引入 Kafka client 或业务语义 |
| `configx` | 显式配置、脱敏、secret reference | 隐式读取生产 secret |
| `observex` | metrics/logging/tracing/health 契约 | 业务告警策略 |
| `testkitx` | fake、fixture、golden、contract helper | 真实生产连接 |
| `resiliencx` | retry/timeout/circuit/bulkhead/rate/budget | 业务补偿语义 |
| `kafkax` | Kafka 基础设施适配 | 业务 topic、业务消息 schema、x.go import |
| `x.go` | 调用 `kafkax` 组合业务事件流 | 反向污染 L2 API |

---

## 6. Goal Runtime v3.1 对象模型

### 6.1 Goal

```yaml
goal_id: GOAL-20260604-KAFKAX-L2-STANDARD-FACTORY
title: Upgrade kafka/kafkax into xlib-standard governed L2 infrastructure adapter factory
mode: Full
owner: ZoneCNH
state: INIT
source_of_truth:
  - github.com/ZoneCNH/xlib-standard
  - github.com/ZoneCNH/kernel
  - github.com/ZoneCNH/kafkax
  - github.com/ZoneCNH/kafka   # 若存在，仅作为迁移/重命名前状态
completion_statement_required: DONE with evidence
```

### 6.2 Spec

```yaml
spec_id: SPEC-kafkax-l2-standard-factory-v1.0
requirements:
  - REQ-SPEC-kafkax-l2-standard-factory-v1.0-001: Standard Source alignment
  - REQ-SPEC-kafkax-l2-standard-factory-v1.0-002: Independent repository bootstrap
  - REQ-SPEC-kafkax-l2-standard-factory-v1.0-003: Public API contracts
  - REQ-SPEC-kafkax-l2-standard-factory-v1.0-004: Kafka driver abstraction
  - REQ-SPEC-kafkax-l2-standard-factory-v1.0-005: Producer semantics
  - REQ-SPEC-kafkax-l2-standard-factory-v1.0-006: Consumer semantics
  - REQ-SPEC-kafkax-l2-standard-factory-v1.0-007: Admin and topic metadata
  - REQ-SPEC-kafkax-l2-standard-factory-v1.0-008: Config and secret safety
  - REQ-SPEC-kafkax-l2-standard-factory-v1.0-009: Observability and health
  - REQ-SPEC-kafkax-l2-standard-factory-v1.0-010: Resilience policy
  - REQ-SPEC-kafkax-l2-standard-factory-v1.0-011: Testkit and fake runtime
  - REQ-SPEC-kafkax-l2-standard-factory-v1.0-012: Harness gates
  - REQ-SPEC-kafkax-l2-standard-factory-v1.0-013: Release Evidence
  - REQ-SPEC-kafkax-l2-standard-factory-v1.0-014: Downstream adoption proof
  - REQ-SPEC-kafkax-l2-standard-factory-v1.0-015: Self-improving loop
```

### 6.3 State Machine

```text
INIT
  -> CONTEXT_READY
  -> GOAL_READY
  -> SPEC_READY
  -> DESIGN_READY
  -> PLAN_READY
  -> TASKS_READY
  -> EXECUTING
  -> VERIFYING
  -> REVIEWING
  -> RELEASING
  -> RETROSPECTING
  -> DONE
```

异常状态：

```text
BLOCKED
FAILED
NEEDS_RESEARCH
NEEDS_DECISION
NEEDS_REPLAN
NEEDS_ROLLBACK
NEEDS_HUMAN_APPROVAL
INCONSISTENT_STATE
```

### 6.4 Completion Definition

`DONE` 必须同时满足：

1. `kafkax` 仓库存在并使用 canonical module path：`github.com/ZoneCNH/kafkax`。
2. `kafkax` 从 `xlib-standard` 渲染或等价迁移，并保留 `.agent` / Harness / Evidence Runtime。
3. `kafkax` 公共 API 不泄露第三方 Kafka client 类型。
4. `kafkax` 不导入 `x.go`、不包含业务 topic、不中转业务 schema。
5. Required gates 通过。
6. Kafka-specific gates 通过或以 explicit blocker 记录。
7. `release/manifest/latest.json` 和 `.sha256` 生成并校验。
8. Release score 达到阈值，建议 `>= 9.8`。
9. downstream adoption 不得被虚假升级；只有真实下游仓库 gate 输出才能声明 adopted。
10. Retrospective 输出 Prompt Patch / Harness Patch / Rule Patch / New Issue Candidate。

---

## 7. 需求与验收标准

### REQ-001：Standard Source 对齐

**要求**：`xlib-standard` 必须明确 `kafkax` 是 L2 目标库，并定义其允许依赖、禁止依赖、Evidence 规则和 L2 Kafka profile。

**Acceptance Criteria**：

- AC-001-001：`docs/downstream-matrix.md` 中 `kafkax` module path、package、layer 正确。
- AC-001-002：若引入 `resiliencx` / `schedulex` 为生产依赖，必须先更新标准矩阵和 downstream sync policy。
- AC-001-003：`standard-impact-check` 生成报告，明确是否需要 downstream sync。
- AC-001-004：禁止把 registry state 写成 adoption proof。

**Evidence**：

- `GOWORK=off make standard-impact-check`
- `release/standard-impact/latest.md`
- `GOWORK=off make downstream-sync-plan`

### REQ-002：独立仓库 Bootstrap

**要求**：`kafkax` 必须作为独立仓库、独立 module、独立 release 运行。

**Acceptance Criteria**：

- AC-002-001：仓库 canonical name 为 `ZoneCNH/kafkax`。
- AC-002-002：`go.mod` module 为 `github.com/ZoneCNH/kafkax`。
- AC-002-003：从 `xlib-standard` 生成或迁移后不存在 `templatex`、`baselib-template`、`foundationx` 等残留。
- AC-002-004：`.agent/`、`contracts/`、`docs/`、`scripts/`、`release/manifest/template.json` 存在。
- AC-002-005：不提交 `release/manifest/latest.json`。

**Evidence**：

- `scripts/check_rendered_template.sh`
- `GOWORK=off go test ./...`
- `GOWORK=off make boundary`
- `git status --short`

### REQ-003：公共 API Contract

**要求**：提供 stable、business-agnostic、driver-neutral API。

**Acceptance Criteria**：

- AC-003-001：公共 API 中不存在 `kgo.*`、`kafka-go.*`、`confluent.*` 等第三方类型。
- AC-003-002：API 覆盖 Producer、Consumer、Admin、TopicSpec、Message、Header、Offset、Error、Health、Config。
- AC-003-003：API 文档说明 delivery semantics：at-most-once / at-least-once / transactional optional，不夸大 exactly-once。
- AC-003-004：所有 public config 字段可脱敏、可校验、可序列化为 safe diagnostic view。

**Evidence**：

- `GOWORK=off make contracts`
- `GOWORK=off make docs-check`
- `GOWORK=off make boundary`

### REQ-004：Kafka Driver Abstraction

**要求**：第三方 Kafka client 只能在 internal driver 层使用。

**Acceptance Criteria**：

- AC-004-001：存在 `internal/driver` 接口层。
- AC-004-002：默认实现为 `internal/driver/franz` 或其他 ADR 记录的实现。
- AC-004-003：公共测试可替换 fake driver。
- AC-004-004：更换 driver 不改变公共 API。

**Evidence**：

- `go test ./internal/driver/...`
- `go test ./pkg/kafkax/...`
- ADR：`docs/adr/ADR-20260604-001-kafka-driver.md`

### REQ-005：Producer 语义

**要求**：Producer 支持同步/异步发送、批量发送、partition key、headers、timeout、幂等配置、压缩、错误分类和 metrics。

**Acceptance Criteria**：

- AC-005-001：`Producer.Send(ctx, Message)` 支持 context cancellation。
- AC-005-002：`Producer.SendBatch(ctx, []Message)` 返回逐条结果或聚合错误。
- AC-005-003：默认不吞错；错误必须可分类为 retriable、fatal、timeout、auth、throttle、validation。
- AC-005-004：producer close 必须 flush 或明确返回未完成记录。
- AC-005-005：metrics 覆盖 send count、error count、latency、bytes、throttle、inflight。

**Evidence**：

- unit tests
- fake driver contract tests
- broker integration tests
- metrics golden snapshot

### REQ-006：Consumer 语义

**要求**：Consumer 支持 consumer group、manual commit、handler lifecycle、rebalance、pause/resume、graceful shutdown、lag metrics。

**Acceptance Criteria**：

- AC-006-001：默认推荐 at-least-once：handler 成功后 commit。
- AC-006-002：支持 batch handler 和 single message handler。
- AC-006-003：处理失败可以按 policy retry、skip、DLQ 或 stop。
- AC-006-004：shutdown 时停止拉取、等待 handler、commit 成功处理的 offset、关闭连接。
- AC-006-005：rebalance 事件有 metrics 和日志。

**Evidence**：

- consumer contract tests
- rebalance simulation tests
- integration test：produce -> consume -> commit
- failure test：handler error -> retry / DLQ

### REQ-007：Admin 与 Topic Metadata

**要求**：提供 topic plan、metadata describe、create/delete/alter 的可控 admin 能力。

**Acceptance Criteria**：

- AC-007-001：默认不自动创建业务 topic。
- AC-007-002：`TopicSpec` 包含 partitions、replication factor、retention、cleanup policy、compression、min ISR 等通用配置。
- AC-007-003：`PlanTopics` 只输出差异，不修改 broker。
- AC-007-004：`ApplyTopics` 必须显式调用，并输出 evidence-friendly change summary。

**Evidence**：

- admin unit tests
- topic diff golden tests
- optional broker integration tests

### REQ-008：配置与密钥安全

**要求**：所有配置显式传入，禁止隐式读取生产 secrets。

**Acceptance Criteria**：

- AC-008-001：支持 `Brokers`、`ClientID`、`Security`、`Producer`、`Consumer`、`Admin`、`Retry`、`Observability` 配置。
- AC-008-002：`SafeString()` / `Redacted()` 不泄露 SASL password、token、private key。
- AC-008-003：文档中可以出现 `/home/k8s/secrets/env/*` 作为部署路径名，但不得读取真实内容或写入日志。
- AC-008-004：配置校验失败返回 typed validation error。

**Evidence**：

- secret scan
- config redaction tests
- validation golden tests

### REQ-009：Observability 与 Health

**要求**：通过 `observex` 契约输出 metrics、logs、traces、health/readiness。

**Acceptance Criteria**：

- AC-009-001：Metrics contract 包含 producer、consumer、admin、connection、rebalance、lag、DLQ。
- AC-009-002：Logs 不包含消息 value 默认内容；只记录 topic、partition、offset、key hash、error class。
- AC-009-003：Health 支持 broker metadata check、producer ready、consumer ready。
- AC-009-004：Tracing 支持 inject/extract headers，但不规定业务 trace schema。

**Evidence**：

- metrics contract test
- log redaction test
- health fake + integration test

### REQ-010：Resilience Policy

**要求**：重试、timeout、circuit breaker、bulkhead、rate limit、budget 使用 L1 契约；若 L1 未登记则先 Standard Impact。

**Acceptance Criteria**：

- AC-010-001：Producer retry 受 budget 和 idempotency 约束。
- AC-010-002：Consumer handler retry 不无限阻塞 partition。
- AC-010-003：DLQ policy 显式配置，不默认吞消息。
- AC-010-004：所有 background loop 可停止、可观测、可测试。

**Evidence**：

- resilience policy tests
- fault injection tests
- timeout/cancellation tests

### REQ-011：Testkit

**要求**：提供 `testkit` 支持 fake producer、fake consumer、golden record、contract harness。

**Acceptance Criteria**：

- AC-011-001：下游应用无需真实 Kafka 即可测试业务 handler。
- AC-011-002：fake driver 与真实 driver 共享 contract tests。
- AC-011-003：提供 golden helpers 校验 headers、key、topic、offset、commit 行为。
- AC-011-004：testkit 不连接生产 Kafka。

**Evidence**：

- `go test ./testkit/...`
- examples smoke
- downstream consumer demo test

### REQ-012：Harness Gates

**要求**：继承 `xlib-standard` required gates，并增加 Kafka-specific gates。

**Acceptance Criteria**：

- AC-012-001：Required gates：fmt、vet、lint、test、race、boundary、security、contracts、docs-check、integration、dependency-check、standard-impact-check、score、evidence、release-evidence-check。
- AC-012-002：Kafka gates：kafka-contract、kafka-integration、kafka-fault-injection、kafka-metrics-golden、kafka-admin-golden。
- AC-012-003：broker integration 不可用时必须标记 blocked，不能假装 passed。

**Evidence**：

- Makefile targets
- `.agent/harness/harness.yaml`
- CI logs

### REQ-013：Release Evidence

**要求**：`kafkax` 独立生成 release manifest 和 checksum。

**Acceptance Criteria**：

- AC-013-001：manifest 包含 commit、tree SHA、source digest、contract digest、dependency list、tool versions、gate results、score、workflow artifact。
- AC-013-002：manifest 包含 Kafka client library version、broker integration version、driver implementation name。
- AC-013-003：manifest 与 current source 一致。
- AC-013-004：release-final-check 要求 workspace clean。

**Evidence**：

- `release/manifest/latest.json`
- `release/manifest/latest.json.sha256`
- CI artifact
- `DONE with evidence:`

### REQ-014：Downstream Adoption Proof

**要求**：下游采纳必须 proof-based，不得 registry-only。

**Acceptance Criteria**：

- AC-014-001：采纳证明包含 source repo、source commit、downstream repo、downstream commit、gate outputs、rollback。
- AC-014-002：至少一个下游 demo 或真实 `x.go` integration branch 完成 compile/test。
- AC-014-003：如果未做真实下游采纳，必须写 `adoption_claim=not_claimed`。

**Evidence**：

- `contracts/downstream-adoption-proof.schema.json`
- `release/downstream-adoption/kafkax-*.json`
- downstream CI artifact

### REQ-015：Self-improving Loop

**要求**：每次完成或失败都必须输出复盘补丁。

**Acceptance Criteria**：

- AC-015-001：Retrospective 包含 Prompt Patch、Harness Patch、Rule Patch、CI Gate Suggestion、New Issue Candidate。
- AC-015-002：重复问题不得只写“下次注意”，必须进入机器可检查规则。
- AC-015-003：`xlib-standard` generator 或 harness 的可复用改进必须回流。

**Evidence**：

- `.agent/retrospectives/RETRO-20260604-KAFKAX.md`
- `.agent/patches/PATCH-HARNESS-*.md`
- `.agent/patches/PATCH-RULE-*.md`

---

## 8. 推荐技术设计

### 8.1 仓库结构

```text
kafkax/
  .agent/
    runtime/
    harness/
    evidence/
    traceability/
    retrospectives/
    patches/
  .github/
    workflows/
      ci.yml
      release-check.yml
      security.yml
  cmd/
    # 可选：kafkax doctor / contract CLI，不作为首期必须项
  pkg/
    kafkax/
      client.go
      config.go
      producer.go
      consumer.go
      admin.go
      topic.go
      message.go
      offset.go
      errors.go
      health.go
      metrics.go
      codec.go
      options.go
  internal/
    driver/
      driver.go
      franz/
        client.go
        producer.go
        consumer.go
        admin.go
      fake/
        driver.go
    config/
      redaction.go
      validation.go
    metrics/
      names.go
    contracts/
      validate.go
    testbroker/
      docker.go          # optional integration helper
  testkit/
    fake_producer.go
    fake_consumer.go
    golden.go
    contract.go
    assertions.go
  contracts/
    kafkax.config.schema.json
    kafkax.metrics.schema.json
    kafkax.message.schema.json
    kafkax.downstream-adoption-proof.schema.json
  examples/
    basic-producer/
    basic-consumer/
    consumer-group/
    admin-topic-plan/
    health/
    dlq/
  docs/
    README.md
    api.md
    config.md
    errors.md
    metrics.md
    testing.md
    release.md
    adr/
      ADR-20260604-001-kafka-driver.md
      ADR-20260604-002-delivery-semantics.md
      ADR-20260604-003-no-business-schema.md
  scripts/
    check_boundary.sh
    check_contracts.sh
    run_kafka_integration.sh
    run_fault_injection.sh
    evidence.sh
  release/
    manifest/
      template.json
    standard-impact/
    downstream-sync/
    downstream-adoption/
  Makefile
  go.mod
  README.md
  CHANGELOG.md
  LICENSE
```

### 8.2 公共 API 草案

```go
package kafkax

type Client interface {
    Producer() Producer
    Consumer(group string, topics ...string) (Consumer, error)
    Admin() Admin
    Health(ctx context.Context) HealthStatus
    Close(ctx context.Context) error
}

type Producer interface {
    Send(ctx context.Context, msg Message) (ProduceResult, error)
    SendBatch(ctx context.Context, msgs []Message) (BatchProduceResult, error)
    Flush(ctx context.Context) error
    Close(ctx context.Context) error
}

type Consumer interface {
    Run(ctx context.Context, handler Handler) error
    Poll(ctx context.Context) (RecordBatch, error)
    Commit(ctx context.Context, offsets ...Offset) error
    Pause(ctx context.Context, partitions ...TopicPartition) error
    Resume(ctx context.Context, partitions ...TopicPartition) error
    Close(ctx context.Context) error
}

type Admin interface {
    DescribeTopics(ctx context.Context, topics ...string) ([]TopicDescription, error)
    PlanTopics(ctx context.Context, specs ...TopicSpec) (TopicPlan, error)
    ApplyTopics(ctx context.Context, plan TopicPlan) (TopicApplyResult, error)
    Close(ctx context.Context) error
}

type Message struct {
    Topic     string
    Key       []byte
    Value     []byte
    Headers   []Header
    Timestamp time.Time
}

type Record struct {
    Message
    Partition int32
    Offset    int64
}

type Handler interface {
    Handle(ctx context.Context, record Record) error
}
```

首期不建议把 API 做得过度抽象；核心是：

1. 明确 context 和 Close 语义。
2. 明确 producer / consumer / admin 边界。
3. 不泄露第三方类型。
4. 不承诺业务 schema。
5. 可测试、可观测、可 Evidence。

### 8.3 配置草案

```yaml
kafkax:
  client_id: market-data-dev
  brokers:
    - localhost:9092
  security:
    tls:
      enabled: false
      ca_file_ref: ""
      cert_file_ref: ""
      key_file_ref: ""
    sasl:
      enabled: false
      mechanism: SCRAM-SHA-512
      username_ref: env:KAFKA_USERNAME
      password_ref: env:KAFKA_PASSWORD
  producer:
    required_acks: all
    idempotent: true
    compression: zstd
    max_in_flight: 5
    batch_bytes: 1048576
    linger_ms: 5
    timeout_ms: 10000
  consumer:
    group_id: ""
    topics: []
    auto_commit: false
    start_offset: latest
    max_poll_records: 500
    session_timeout_ms: 45000
    rebalance_timeout_ms: 60000
  admin:
    enabled: true
    timeout_ms: 10000
  resilience:
    retry:
      max_attempts: 3
      backoff: exponential_jitter
      base_delay_ms: 100
      max_delay_ms: 2000
    budget:
      max_retry_ratio: 0.2
    circuit_breaker:
      enabled: true
      failure_threshold: 0.5
  observability:
    metrics_enabled: true
    tracing_enabled: true
    log_record_value: false
```

### 8.4 错误分类

```text
KAFKAX_ERR_VALIDATION
KAFKAX_ERR_CONFIG
KAFKAX_ERR_AUTH
KAFKAX_ERR_TLS
KAFKAX_ERR_BROKER_UNAVAILABLE
KAFKAX_ERR_TIMEOUT
KAFKAX_ERR_CONTEXT_CANCELLED
KAFKAX_ERR_THROTTLED
KAFKAX_ERR_PRODUCE_RETRIABLE
KAFKAX_ERR_PRODUCE_FATAL
KAFKAX_ERR_CONSUME_RETRIABLE
KAFKAX_ERR_CONSUME_FATAL
KAFKAX_ERR_COMMIT_FAILED
KAFKAX_ERR_REBALANCE
KAFKAX_ERR_ADMIN
KAFKAX_ERR_DQL_PUBLISH_FAILED
```

错误对象必须支持：

- `Code()`
- `Temporary()`
- `Retryable()`
- `Fatal()`
- `Unwrap()`
- `SafeFields()`

### 8.5 Metrics Contract

| Metric | Type | Labels | 说明 |
|---|---|---|---|
| `kafkax_producer_records_total` | counter | client_id, topic, result | producer 发送条数 |
| `kafkax_producer_errors_total` | counter | client_id, topic, error_code | producer 错误 |
| `kafkax_producer_latency_seconds` | histogram | client_id, topic | producer 延迟 |
| `kafkax_producer_inflight` | gauge | client_id | inflight 记录数 |
| `kafkax_consumer_records_total` | counter | client_id, group, topic, result | consumer 处理条数 |
| `kafkax_consumer_errors_total` | counter | group, topic, error_code | consumer 错误 |
| `kafkax_consumer_lag` | gauge | group, topic, partition | consumer lag |
| `kafkax_consumer_commit_latency_seconds` | histogram | group, topic | commit 延迟 |
| `kafkax_consumer_rebalances_total` | counter | group, reason | rebalance 次数 |
| `kafkax_admin_requests_total` | counter | operation, result | admin 请求 |
| `kafkax_connection_state` | gauge | broker, state | 连接状态 |
| `kafkax_dlq_records_total` | counter | source_topic, dlq_topic, result | DLQ 记录 |

### 8.6 Health Contract

```yaml
health:
  status: healthy|degraded|unhealthy
  checks:
    broker_metadata:
      status: passed|failed|blocked
      latency_ms: 0
    producer:
      status: passed|failed|disabled
    consumer:
      status: passed|failed|disabled
    admin:
      status: passed|failed|disabled
  safe_details:
    brokers_count: 3
    client_id: redacted-safe
    driver: franz
```

---

## 9. ADR 决策

### ADR-20260604-001：默认 Kafka driver 选择

**候选项**：

1. `franz-go`
2. `segmentio/kafka-go`
3. `confluent-kafka-go`

**推荐**：首期默认 `franz-go`，原因：

- pure Go，无 cgo / librdkafka 部署负担。
- 支持较完整的 Kafka client 能力，包括 producer、consumer、admin、transactions、SASL/TLS、compression、hooks 和 metrics。
- 更适合作为 internal driver；公共 API 不暴露其类型。

**保留约束**：

- 所有 `franz-go` 类型必须停留在 `internal/driver/franz`。
- 如果未来需要 Confluent Cloud / librdkafka 特性，可新增 `internal/driver/confluent`，不破坏公共 API。

### ADR-20260604-002：delivery semantics

**决策**：

- 默认 producer 开启 idempotent 配置。
- 默认 consumer 使用 manual commit。
- 默认 handler 成功后 commit，提供 at-least-once 语义。
- 不默认承诺 exactly-once；只提供 transaction profile 和配置能力。

### ADR-20260604-003：不内置业务 schema

**决策**：

- `kafkax.Message` 只包含 topic、key、value、headers、timestamp。
- 可提供通用 `Codec` 接口，但不提供业务结构体。
- 不定义 `market.kline`、`macro.event`、`regime.signal` 等业务 topic。

### ADR-20260604-004：Admin 默认 plan/apply 分离

**决策**：

- Topic 管理默认先 `PlanTopics`。
- 只有显式调用 `ApplyTopics` 才修改 broker。
- release / CI 中只运行 safe plan，真实 apply 需要 human approval 或 integration broker。

---

## 10. gstack 目标栈

```text
G0: Truth & Context Recovery
    读取 xlib-standard、kernel、kafkax/kafka 当前事实，冻结真实状态，禁止假设已采用。

G1: Standard Source Delta
    在 xlib-standard 中登记/修正 L2 Kafka profile、依赖矩阵、harness gate、evidence schema。

G2: Repository Factory Bootstrap
    使用 xlib-standard generator 生成或迁移 kafkax 独立仓库。

G3: Contract First Design
    冻结 public API、config schema、metrics schema、error taxonomy、topic/admin contract。

G4: Driver Implementation
    internal driver 实现 producer、consumer、admin，默认 franz-go，fake driver 用于测试。

G5: Harness & Evidence
    实现 required gates、Kafka-specific gates、release manifest、score、checksum、CI artifact。

G6: Downstream Adoption
    通过 demo 或 x.go integration branch 证明可采纳，但不把未运行下游写成 adopted。

G7: Self-improving Compound Loop
    把失败、经验、复用能力回流 xlib-standard 的 prompt/harness/rule/generator。
```

---

## 11. superpowers 能力组合

| Superpower | 在本 Goal 中的用途 |
|---|---|
| Goal-Oriented Thinking | 用 Goal v3.1 把目标、需求、验收、证据、发布、复盘串起来 |
| AutoResearch | 验证当前 xlib-standard、kafkax 仓库、Kafka client、CI 能力和依赖版本 |
| Harness Engineering | 把人工规范变成 Makefile / goalcli / CI / contracts gates |
| Compound Engineering | 把 kafkax 的实现经验沉淀为 L2 工厂资产，降低下个 L2 库成本 |
| Self-improving | 每次失败形成 patch，不允许重复同类错误 |
| Agent Teams | 用多个工作树并行推进标准、实现、测试、文档、发布，不在 main 开发 |
| Evidence Protocol | 每个任务、Issue、Release 都有 artifact 和 checksum |
| Downstream Adoption | 通过真实下游 gate 输出证明采纳，不做 registry-only 宣称 |

---

## 12. Harness 设计

### 12.1 继承 xlib-standard Required Gates

```bash
GOWORK=off make fmt
GOWORK=off make vet
GOWORK=off make lint
GOWORK=off make test
GOWORK=off make race
GOWORK=off make boundary
GOWORK=off make security
GOWORK=off make contracts
GOWORK=off make docs-check
GOWORK=off make integration
GOWORK=off make dependency-check
GOWORK=off make standard-impact-check
GOWORK=off make downstream-sync-plan
GOWORK=off go run ./cmd/goalcli score --min 9.8
CHECK_STATUS=passed GOWORK=off make evidence
RELEASE_EVIDENCE_REQUIRE_PASSED=1 GOWORK=off make release-evidence-check
XLIB_CONTEXT=release_verify GOWORK=off make release-final-check
```

### 12.2 新增 Kafka-specific Gates

```bash
GOWORK=off make kafka-contract
GOWORK=off make kafka-metrics-golden
GOWORK=off make kafka-admin-golden
KAFKAX_INTEGRATION=1 GOWORK=off make kafka-integration
KAFKAX_INTEGRATION=1 GOWORK=off make kafka-fault-injection
KAFKAX_BENCH_SMOKE=1 GOWORK=off make kafka-benchmark-smoke
```

### 12.3 Gate 分层

| Gate | 类型 | 阶段 | 必需性 |
|---|---|---|---|
| `kafka-contract` | Semantic + Executable | PR | Required |
| `kafka-metrics-golden` | Executable | PR | Required |
| `kafka-admin-golden` | Executable | PR | Required |
| `kafka-integration` | Hybrid | Release | Required if CI supports broker；否则 blocked with owner |
| `kafka-fault-injection` | Hybrid | Extended | P1 |
| `kafka-benchmark-smoke` | Executable | Extended | P1 |
| `downstream-adoption-proof` | Hybrid | Release | Required only when claiming adoption |

### 12.4 Boundary Gate 必查项

```text
禁止 public API 出现：
- github.com/twmb/franz-go/pkg/kgo
- github.com/segmentio/kafka-go
- github.com/confluentinc/confluent-kafka-go
- github.com/bytechainx/x.go
- github.com/ZoneCNH/x.go
- x.go/internal
- 业务 topic 字面量，例如 market.kline、regime.signal、macro.event

禁止源码、README、测试日志、manifest、PR 描述包含：
- 真实 broker password
- SASL token
- TLS private key
- /home/k8s/secrets/env/* 的真实内容
```

---

## 13. Evidence Protocol

### 13.1 Task 完成声明模板

```text
DONE with evidence:
- scope: task
- id: TASK-GOAL-20260604-KAFKAX-L2-STANDARD-FACTORY-XXX
- branch: goal/kafkax-l2-standard-factory-xxx
- commit: <sha>
- gates:
  - GOWORK=off make test: passed <artifact/log>
  - GOWORK=off make boundary: passed <artifact/log>
  - GOWORK=off make contracts: passed <artifact/log>
- artifacts:
  - <path>: <purpose>
- known gaps:
  - <none or explicit blocker>
```

### 13.2 Release 完成声明模板

```text
DONE with evidence:
- scope: release
- goal: GOAL-20260604-KAFKAX-L2-STANDARD-FACTORY
- repository: github.com/ZoneCNH/kafkax
- branch: main
- tag: v0.1.0
- commit: <sha>
- release manifest: release/manifest/latest.json
- release manifest sha256: <sha256>
- source digest: <manifest.source_digest>
- contract fingerprint: <manifest.contracts.digest>
- dependency list: <manifest.dependencies>
- tool versions: <manifest.tools>
- score: >= 9.8 passed
- workflow artifact:
  - workflow_run_id: <id or local:*>
  - artifact_name: kafkax-release-manifest
  - artifact_url: <url or local path>
- gates:
  - fmt: passed
  - vet: passed
  - lint: passed
  - test: passed
  - race: passed
  - boundary: passed
  - security: passed
  - contracts: passed
  - docs-check: passed
  - integration: passed|blocked with owner
  - kafka-contract: passed
  - kafka-integration: passed|blocked with owner
  - dependency-check: passed
  - standard-impact-check: passed
  - evidence: passed
  - release-evidence-check: passed
  - release-final-check: passed
- downstream adoption:
  - adoption_claim: adopted|not_claimed
  - proof_based_adoption: true|false
  - downstream_repo: <repo or none>
  - downstream_commit: <sha or none>
- known gaps:
  - <none or explicit blocker>
```

### 13.3 Release Manifest 扩展字段建议

```json
{
  "kafkax": {
    "driver": "franz",
    "driver_version": "<go.mod version>",
    "broker_integration": {
      "enabled": true,
      "broker": "redpanda|kafka",
      "broker_version": "<version>",
      "artifact": "release/kafka-integration/latest.json"
    },
    "delivery_semantics": {
      "default_producer_idempotent": true,
      "default_consumer_commit": "manual_after_handler_success",
      "exactly_once_claim": "not_default"
    },
    "contracts": {
      "config_schema": "contracts/kafkax.config.schema.json",
      "metrics_schema": "contracts/kafkax.metrics.schema.json",
      "message_schema": "contracts/kafkax.message.schema.json"
    }
  }
}
```

---

## 14. AutoResearch Protocol

### 14.1 触发条件

出现以下情况必须进入 `NEEDS_RESEARCH`：

1. `kafkax` 仓库是否存在不确定。
2. `kafka` 与 `kafkax` 命名关系不确定。
3. 当前 `xlib-standard` 下游矩阵与记忆中的 L1/L2 分层冲突。
4. `resiliencx` / `schedulex` 未在标准矩阵中登记但实现需要使用。
5. Kafka client 版本、功能、兼容性、license、cgo 要求不确定。
6. CI 是否支持 Docker broker integration 不确定。
7. broker integration 测试失败原因不明确。
8. x.go 下游采纳边界不明确。

### 14.2 Research 输出格式

```yaml
research_id: RESEARCH-20260604-KAFKAX-XXX
question: <待确认问题>
sources:
  - <source>
finding: <结论>
confidence: high|medium|low
impact:
  - spec
  - design
  - plan
  - tasks
decision_needed: true|false
next_action: <task or ADR>
```

---

## 15. 具体执行计划

### Wave 0：Context Recovery 与事实冻结

**目标**：禁止在错误假设上开工。

**任务**：

1. 拉取 `xlib-standard`、`kernel`、`kafka`、`kafkax` 当前状态。
2. 确认 canonical repo：优先 `ZoneCNH/kafkax`。
3. 若仅存在 `ZoneCNH/kafka`：
   - 标记为 legacy/stub。
   - 输出 `MIGRATION-kafka-to-kafkax.md`。
   - 决策：rename、archive、mirror、或重新创建 `kafkax`。
4. 确认 `xlib-standard` 当前 downstream matrix、adoption status、harness gates、evidence protocol。
5. 输出 `CONTEXT-20260604-KAFKAX.md`。

**命令示例**：

```bash
mkdir -p ~/work/zonecnh
cd ~/work/zonecnh

git clone git@github.com:ZoneCNH/xlib-standard.git
git clone git@github.com:ZoneCNH/kernel.git || true
git clone git@github.com:ZoneCNH/kafkax.git || true
git clone git@github.com:ZoneCNH/kafka.git || true

cd xlib-standard
git fetch origin main
git status --short
GOWORK=off make docs-check
GOWORK=off make standard-impact-check
```

**Evidence**：

- `docs/analysis/kafkax-context-20260604.md`
- `release/standard-impact/latest.md`

### Wave 1：Standard Source Delta

**目标**：让 `xlib-standard` 能正式生成和治理 L2 Kafka adapter。

**任务**：

1. 新增或更新 `docs/standard/l2-kafka-adapter.md`。
2. 更新 downstream matrix：确认 `kafkax` 依赖边界。
3. 若要使用 `resiliencx` / `schedulex`：
   - 更新 L1 标准库矩阵。
   - 更新 L2 allowed dependency policy。
   - 更新 Standard Impact。
4. 新增 contracts：
   - `contracts/l2-kafka-adapter.schema.json`
   - `contracts/downstream-adoption-proof.schema.json` 扩展字段。
5. 新增 harness profile：`l2-kafka`。
6. 更新 generator：支持 L2 Kafka profile 生成 `kafkax` skeleton。
7. 更新 docs：Kafka L2 禁止业务 topic / schema。

**Acceptance**：

```bash
GOWORK=off make governance-check
GOWORK=off make p1-governance-check
GOWORK=off make p2-runtime-check
GOWORK=off make docs-check
GOWORK=off make contracts
GOWORK=off make standard-impact-check
GOWORK=off make downstream-sync-plan
GOWORK=off go run ./cmd/goalcli score --min 9.8
```

### Wave 2：kafkax 仓库生成 / 迁移

**目标**：生成或迁移独立 `kafkax` repo。

**首选路径**：新建 `ZoneCNH/kafkax`，从 `xlib-standard` generator 渲染。

```bash
cd ~/work/zonecnh/xlib-standard

git worktree add -b goal/kafkax-standard-source ../wt-xlib-kafkax-standard origin/main
cd ../wt-xlib-kafkax-standard

scripts/render_template.sh \
  --module-name kafkax \
  --module-path github.com/ZoneCNH/kafkax \
  --package-name kafkax \
  --out ../kafkax

cd ../kafkax
git init
git remote add origin git@github.com:ZoneCNH/kafkax.git
git checkout -b goal/kafkax-bootstrap
GOWORK=off go mod tidy
GOWORK=off make release-check
```

**如果只能从 `ZoneCNH/kafka` 迁移**：

1. 创建 `MIGRATION-kafka-to-kafkax.md`。
2. 保留历史 commit，但统一 module path。
3. 若历史内容很少，建议新建 `kafkax`，`kafka` 归档或 README 指向 `kafkax`。
4. 禁止保留 `github.com/ZoneCNH/kafka` 作为长期 canonical module，避免与 Apache Kafka 概念冲突。

### Wave 3：Contract-first API

**目标**：先冻结 API 和 contracts，再实现 driver。

**任务**：

1. 编写 `pkg/kafkax` public types。
2. 编写 config schema。
3. 编写 metrics schema。
4. 编写 error taxonomy。
5. 编写 docs/api.md。
6. 编写 fake driver contract tests。

**命令**：

```bash
GOWORK=off go test ./pkg/kafkax/...
GOWORK=off make contracts
GOWORK=off make docs-check
GOWORK=off make boundary
```

### Wave 4：Driver 实现

**目标**：实现 Kafka driver，但不污染 public API。

**任务**：

1. `internal/driver/driver.go` 定义接口。
2. `internal/driver/franz` 实现 producer。
3. `internal/driver/franz` 实现 consumer。
4. `internal/driver/franz` 实现 admin。
5. `internal/driver/fake` 实现测试驱动。
6. 错误映射。
7. lifecycle 和 close 语义。

**命令**：

```bash
GOWORK=off go test ./internal/driver/...
GOWORK=off go test ./pkg/kafkax/...
GOWORK=off make race
GOWORK=off make boundary
```

### Wave 5：Config / Observability / Resilience

**目标**：将 Kafka adapter 接入 L1 能力。

**任务**：

1. `configx`：显式配置、默认值、redaction、validation。
2. `observex`：metrics/logging/tracing/health。
3. `resiliencx`：producer retry、handler retry、timeout、budget、breaker、bulkhead、rate limit。
4. `schedulex`：可选 metadata refresh / maintenance loop；若未登记，先不作为生产依赖。
5. Secret gate。

**命令**：

```bash
GOWORK=off make test
GOWORK=off make security
GOWORK=off make kafka-metrics-golden
GOWORK=off make docs-check
```

### Wave 6：Testkit 与 Integration

**目标**：让下游不需要真实 Kafka 也能测业务 handler，同时用真实 broker 验证关键路径。

**任务**：

1. `testkit.FakeProducer`
2. `testkit.FakeConsumer`
3. `testkit.ContractSuite`
4. `testkit.GoldenRecord`
5. Redpanda / Kafka integration helper。
6. Fault injection：broker down、timeout、auth failure、handler error、DLQ。

**命令**：

```bash
GOWORK=off go test ./testkit/...
GOWORK=off make kafka-contract
KAFKAX_INTEGRATION=1 GOWORK=off make kafka-integration
KAFKAX_INTEGRATION=1 GOWORK=off make kafka-fault-injection
```

### Wave 7：Release Gate 与 Evidence

**目标**：生成独立 release Evidence。

**命令**：

```bash
GOWORK=off make ci
GOWORK=off make ci-extended
GOWORK=off make dependency-check
GOWORK=off make standard-impact-check
GOWORK=off make downstream-sync-plan
GOWORK=off go run ./cmd/goalcli score --min 9.8
CHECK_STATUS=passed GOWORK=off make evidence
GOWORK=off make release-evidence-hash
RELEASE_EVIDENCE_REQUIRE_PASSED=1 GOWORK=off make release-evidence-check
XLIB_CONTEXT=release_verify GOWORK=off make release-final-check
```

**Release artifact**：

```text
release/manifest/latest.json
release/manifest/latest.json.sha256
release/standard-impact/latest.md
release/downstream-sync/latest.md
release/kafka-integration/latest.json
release/kafka-integration/latest.json.sha256
```

### Wave 8：Downstream Adoption

**目标**：证明 `kafkax` 可被下游采用，但不做虚假 adopted。

**路径 A：Demo downstream**

1. 创建 `examples/downstream-consumer`。
2. 使用 fake driver 验证业务 handler。
3. 使用 integration broker 验证 produce/consume。

**路径 B：x.go integration branch**

1. 在 `x.go` 新建 worktree。
2. 仅作为调用方引入 `kafkax`。
3. 不反向修改 `kafkax` 业务逻辑。
4. 运行 x.go 对应 compile/test gate。
5. 生成 adoption proof。

**adoption proof 示例**：

```yaml
source_repo: github.com/ZoneCNH/kafkax
source_commit: <sha>
downstream_repo: github.com/bytechainx/x.go
downstream_commit: <sha>
mode: proof-based-adoption
gate_outputs:
  - command: GOWORK=off go test ./...
    status: passed
    artifact_path: release/downstream-adoption/xgo-kafkax-test.log
    sha256: <sha256>
rollback:
  owner: ZoneCNH
  commands:
    - go get github.com/ZoneCNH/kafkax@previous
    - revert integration commit
```

### Wave 9：Self-improving

**目标**：把本次 `kafkax` 的经验沉淀到工厂。

**必须输出**：

1. `RETRO-20260604-KAFKAX.md`
2. `PATCH-PROMPT-20260604-KAFKAX.md`
3. `PATCH-HARNESS-20260604-KAFKAX.md`
4. `PATCH-RULE-20260604-KAFKAX.md`
5. `CI-GATE-SUGGESTION-20260604-KAFKAX.md`
6. `NEW-ISSUE-CANDIDATES-20260604-KAFKAX.md`

---

## 16. Issue / PR 拆解

### PR-01：Standard Source Kafka L2 Profile

- Issue：`TASK-GOAL-20260604-KAFKAX-L2-STANDARD-FACTORY-001`
- Repo：`xlib-standard`
- Scope：标准文档、downstream matrix、L2 Kafka profile、Standard Impact
- Gates：docs-check、contracts、standard-impact-check、score
- Evidence：`release/standard-impact/latest.md`

### PR-02：kafkax Repository Bootstrap

- Repo：`kafkax`
- Scope：generator 渲染、module path、package、README、docs、contracts、Makefile、CI
- Gates：release-check、boundary、contracts
- Evidence：bootstrap manifest

### PR-03：Public API + Contracts

- Scope：Producer / Consumer / Admin / Message / Config / Error / Health contracts
- Gates：test、contracts、docs-check、boundary
- Evidence：contract schema digest

### PR-04：Default Driver ADR + Internal Driver Interface

- Scope：ADR、driver interface、fake driver
- Gates：test、boundary
- Evidence：ADR + fake contract tests

### PR-05：Producer Implementation

- Scope：send、send batch、flush、close、idempotent config、metrics
- Gates：test、race、kafka-contract
- Evidence：producer contract logs

### PR-06：Consumer Implementation

- Scope：consumer group、handler、manual commit、rebalance、pause/resume、shutdown
- Gates：test、race、kafka-contract、integration
- Evidence：consumer integration logs

### PR-07：Admin / Topic Plan

- Scope：describe、plan、apply、topic diff golden
- Gates：admin golden、integration
- Evidence：topic plan artifact

### PR-08：Config + Secret Redaction

- Scope：config schema、validation、safe diagnostics、secret gate
- Gates：security、contracts、config tests
- Evidence：redaction golden

### PR-09：Observability + Health

- Scope：metrics, logs, traces, health
- Gates：metrics golden、health test、docs-check
- Evidence：metrics contract artifact

### PR-10：Resilience Policy

- Scope：retry、timeout、budget、circuit breaker、bulkhead、rate limit、DLQ
- Gates：fault injection、race、contracts
- Evidence：fault injection artifact

### PR-11：Testkit

- Scope：fake producer、fake consumer、golden helpers、contract suite
- Gates：testkit tests、examples smoke
- Evidence：testkit contract logs

### PR-12：CI / Release / Evidence Runtime

- Scope：workflows、release manifest extension、checksum、score
- Gates：release-final-check
- Evidence：release manifest artifact

### PR-13：Downstream Adoption Proof

- Scope：demo downstream or x.go branch proof
- Gates：downstream compile/test
- Evidence：adoption proof json + rollback commands

### PR-14：Retrospective + Self-improving Patches

- Scope：retro、prompt patch、harness patch、rule patch、new issue candidates
- Gates：docs-check、traceability check
- Evidence：retrospective artifacts

---

## 17. Traceability Matrix

| Requirement | Acceptance Criteria | Design Section | Task/PR | Test | Evidence | Status |
|---|---|---|---|---|---|---|
| REQ-001 | AC-001-* | §5, §12 | PR-01 | docs/standard-impact | `release/standard-impact/latest.md` | planned |
| REQ-002 | AC-002-* | §8.1 | PR-02 | boundary/release-check | bootstrap manifest | planned |
| REQ-003 | AC-003-* | §8.2 | PR-03 | contracts/docs | schema digest | planned |
| REQ-004 | AC-004-* | §8.2, §9 | PR-04 | driver tests | ADR + logs | planned |
| REQ-005 | AC-005-* | §8.2 | PR-05 | producer contract | producer logs | planned |
| REQ-006 | AC-006-* | §8.2 | PR-06 | consumer integration | consumer logs | planned |
| REQ-007 | AC-007-* | §8.2 | PR-07 | admin golden | topic plan artifact | planned |
| REQ-008 | AC-008-* | §8.3 | PR-08 | security/config | redaction golden | planned |
| REQ-009 | AC-009-* | §8.5, §8.6 | PR-09 | metrics/health | metrics artifact | planned |
| REQ-010 | AC-010-* | §8.4, §12 | PR-10 | fault injection | fault artifact | planned |
| REQ-011 | AC-011-* | §8.1 | PR-11 | testkit tests | testkit logs | planned |
| REQ-012 | AC-012-* | §12 | PR-12 | harness gates | CI logs | planned |
| REQ-013 | AC-013-* | §13 | PR-12 | release checks | manifest + sha | planned |
| REQ-014 | AC-014-* | §15 Wave 8 | PR-13 | downstream test | adoption proof | planned |
| REQ-015 | AC-015-* | §15 Wave 9 | PR-14 | docs/traceability | retro patches | planned |

---

## 18. Risk Register

| Risk ID | 风险 | 严重度 | 触发条件 | 缓解 | Owner |
|---|---|---:|---|---|---|
| RISK-001 | `kafkax` 仓库不存在或命名冲突 | High | clone not found | 创建 canonical `kafkax`，`kafka` 迁移/归档 | release owner |
| RISK-002 | L1 dependency 未被标准矩阵允许 | High | 引入 resiliencx/schedulex | 先做 xlib Standard Impact | standard owner |
| RISK-003 | 第三方 Kafka 类型泄露公共 API | High | public API import kgo/kafka-go | boundary gate fail | API owner |
| RISK-004 | 误称 exactly-once | High | docs/API 出现无条件 EOS | docs-check + ADR | API owner |
| RISK-005 | 业务 topic/schema 污染 L2 | High | 出现 market/regime/macro topic | boundary gate fail | API owner |
| RISK-006 | CI 无法运行 broker integration | Medium | Docker unavailable | 标记 blocked；MVA fake contract；CI 另配 runner | CI owner |
| RISK-007 | 重试导致重复/乱序/雪崩 | High | retry 无 budget | resiliencx budget + DLQ | resilience owner |
| RISK-008 | secret 泄露 | Critical | 日志/manifest/README 泄露 token | security gate + redaction tests | security owner |
| RISK-009 | Release Evidence dirty workspace | High | release-final-check fail | worktree clean / rollback | release owner |
| RISK-010 | downstream adoption 被虚假升级 | High | registry-only marked adopted | adoption proof schema gate | governance owner |
| RISK-011 | Kafka client 依赖策略错误 | Medium | driver feature/compat gap | ADR + AutoResearch + internal driver abstraction | driver owner |
| RISK-012 | testkit 与真实 driver 行为漂移 | Medium | fake tests pass but integration fail | shared contract suite | test owner |

---

## 19. Decision Log

```yaml
- decision_id: DEC-20260604-001
  title: Canonical repo name is kafkax
  status: proposed
  rationale: avoid generic kafka name and align xlib-standard downstream matrix

- decision_id: DEC-20260604-002
  title: Public API must not expose Kafka client implementation types
  status: accepted
  rationale: maintain driver replaceability and standard-source control

- decision_id: DEC-20260604-003
  title: Default consumer semantics are at-least-once with manual commit
  status: accepted
  rationale: safer default; exactly-once requires explicit transactional profile

- decision_id: DEC-20260604-004
  title: Default driver is franz-go behind internal adapter
  status: proposed
  rationale: pure Go and broad Kafka feature coverage; still replaceable

- decision_id: DEC-20260604-005
  title: Topic admin uses plan/apply split
  status: accepted
  rationale: production topic mutation needs explicit approval and evidence
```

---

## 20. Rollback Protocol

### 20.1 Standard Source Rollback

```bash
cd xlib-standard
git worktree add -b rollback/kafkax-standard ../wt-rollback-kafkax-standard origin/main
# revert PR commits or reset branch before merge
git revert <standard-source-commit>
GOWORK=off make release-check
```

### 20.2 kafkax Repo Rollback

```bash
cd kafkax
git checkout main
git pull --ff-only
git checkout -b rollback/kafkax-v0.1.0

go get github.com/ZoneCNH/kafkax@<previous-version>
# or revert broken commits
git revert <bad-commit>
GOWORK=off make release-check
```

### 20.3 Downstream Rollback

```bash
cd x.go
# revert adoption commit or pin previous version
go get github.com/ZoneCNH/kafkax@<previous-good-tag>
GOWORK=off go test ./...
```

Rollback Evidence 必须包含：

- bad commit
- rollback commit
- affected release/tag
- gate outputs
- downstream impact
- known gaps

---

## 21. Human Approval Gates

| Gate | 需要人工批准的原因 |
|---|---|
| 创建或重命名 `ZoneCNH/kafkax` 仓库 | 影响 canonical identity |
| 归档或重定向 `ZoneCNH/kafka` | 影响历史使用者 |
| 把 `resiliencx` / `schedulex` 加入 L2 production dependency | 修改标准矩阵 |
| 选择默认 Kafka driver | 影响长期维护成本 |
| 开启真实 broker integration CI | 影响 CI 成本和安全边界 |
| 发布 v0.1.0 tag | 产生可被下游依赖的版本 |
| 声称 downstream adopted | 必须审查 proof-based evidence |

---

## 22. Failure Budget

| 类型 | Budget | 超限动作 |
|---|---:|---|
| P0 boundary/security failure | 0 | BLOCKED，必须先修 |
| Required gate skipped | 0 | BLOCKED，不得 release |
| Kafka integration unavailable | 1 explicit blocker | 可 MVA，但 release 必须记录 owner/原因 |
| Race failure | 0 | BLOCKED |
| Contract drift | 0 | BLOCKED |
| Docs link/name drift | 0 | BLOCKED |
| Downstream adoption missing | 允许 not_claimed | 不得写 adopted |
| Score below 9.8 | 0 | 不得 release-final |

---

## 23. 最小可行行动 MVA

MVA 不是“完整 Kafka 生产库”，而是“证明 `kafkax` 可以进入标准工厂轨道”的最小闭环。

### MVA 必须完成

1. 冻结当前事实：`kafkax` 是否存在、`kafka` 是否为 legacy、xlib matrix 当前状态。
2. 在 `xlib-standard` 输出 `kafkax` L2 Standard Impact。
3. 生成或创建 `github.com/ZoneCNH/kafkax` 仓库骨架。
4. `go.mod`、package、README、docs、contracts、Makefile、CI、`.agent` 正确。
5. `pkg/kafkax` 提供 public API stub + fake driver。
6. `contracts`、`boundary`、`docs-check`、`test`、`security` 通过。
7. `release/manifest/latest.json` 本地生成并校验。
8. 明确 `adoption_claim=not_claimed`，避免虚假下游采纳。
9. 生成下一阶段 issue 列表和 traceability matrix。

### MVA 不必须完成

1. 不必须完成真实 Kafka broker integration。
2. 不必须完成 exactly-once transaction profile。
3. 不必须完成 x.go 真实采纳。
4. 不必须完成完整 benchmark。
5. 不必须支持所有 SASL/OAuth 组合。

### MVA 完成命令

```bash
GOWORK=off make fmt
GOWORK=off make vet
GOWORK=off make test
GOWORK=off make boundary
GOWORK=off make security
GOWORK=off make contracts
GOWORK=off make docs-check
GOWORK=off make standard-impact-check
GOWORK=off go run ./cmd/goalcli score --min 9.8
CHECK_STATUS=passed GOWORK=off make evidence
RELEASE_EVIDENCE_REQUIRE_PASSED=1 GOWORK=off make release-evidence-check
```

---

## 24. 1 天行动计划

### 目标

建立可执行上下文，完成标准影响分析和 `kafkax` MVA 骨架。

### 步骤

1. 创建工作目录和 worktree。
2. 核对 `xlib-standard`、`kernel`、`kafka`、`kafkax` 仓库状态。
3. 决策 canonical repo：`kafkax`。
4. 在 `xlib-standard` 创建 `goal/kafkax-l2-standard-source` 分支。
5. 输出 `docs/standard/l2-kafka-adapter.md` 初稿。
6. 若 `resiliencx` / `schedulex` 未登记，只写成 planned/optional，不直接引入 production dependency。
7. 使用 generator 生成 `kafkax` repo。
8. 写 public API stub、config schema、metrics schema、error taxonomy。
9. 实现 fake driver 和 contract smoke tests。
10. 跑 MVA gates。
11. 生成 Evidence。
12. 输出 issue/PR 队列。

### 1 天验收

```text
DONE with evidence:
- xlib standard impact generated
- kafkax repo skeleton generated
- public API stub compiled
- fake driver tests passed
- boundary/security/contracts/docs-check passed
- local release manifest generated and verified
- downstream adoption not claimed
```

---

## 25. 7 天行动计划

### Day 1：MVA

完成 §24。

### Day 2：Producer + Config

1. 实现 default driver producer。
2. 实现 Send / SendBatch / Flush / Close。
3. 实现 config validation + redaction。
4. producer metrics。
5. producer unit + fake contract。

### Day 3：Consumer

1. 实现 consumer group。
2. handler runner。
3. manual commit。
4. shutdown。
5. rebalance metrics。
6. consumer contract tests。

### Day 4：Admin + Topic Plan

1. DescribeTopics。
2. PlanTopics。
3. ApplyTopics optional。
4. topic golden tests。
5. admin docs。

### Day 5：Observability + Resilience

1. observex metrics/logging/tracing/health。
2. resiliencx retry/budget/timeout。
3. DLQ policy。
4. fault tests。

### Day 6：Integration + CI

1. Broker integration helper。
2. produce-consume-commit integration。
3. admin integration。
4. CI artifact。
5. release manifest extension。

### Day 7：Release Candidate

1. Full gates。
2. Release score。
3. `v0.1.0-rc.1` tag candidate。
4. Downstream demo proof。
5. Retrospective patches。

---

## 26. 30 天行动计划

### Week 1：标准工厂化闭环

- 完成 `kafkax` v0.1.0 MVA + producer/consumer/admin 基础能力。
- `xlib-standard` 增加 L2 Kafka profile。
- `kafkax` 独立 release evidence。

### Week 2：生产可用语义

- 完整 resilience policy。
- fault injection。
- DLQ。
- rebalance / shutdown 强化。
- metrics dashboard examples。
- security hardening。

### Week 3：下游采纳

- 在 demo downstream 或 x.go integration branch 采纳。
- 生成 proof-based adoption。
- 不满足证据则继续 `not_claimed`。
- 输出 rollback plan。

### Week 4：复利扩张

- 把 `kafkax` L2 profile 抽象为 `l2-adapter-factory-profile`。
- 复用到 `natsx`、`redisx`、`postgresx`。
- 优化 generator。
- 优化 goalcli gates。
- 建立 L2 factory scorecard。

---

## 27. 衡量指标

### 27.1 工厂治理指标

| 指标 | 目标 |
|---|---:|
| release score | >= 9.8 |
| P0 boundary violations | 0 |
| secret violations | 0 |
| required gate skipped | 0 |
| dirty release-final | 0 |
| false adoption claim | 0 |
| traceability coverage | 100% |
| requirements with tests | 100% |
| requirements with evidence | 100% |

### 27.2 工程质量指标

| 指标 | 目标 |
|---|---:|
| public API third-party type leakage | 0 |
| business topic/schema occurrences | 0 |
| unit test pass rate | 100% |
| race test pass | 100% |
| config redaction coverage | 100% sensitive fields |
| metrics contract coverage | producer/consumer/admin/health/DLQ |
| examples smoke | passed |

### 27.3 Kafka 运行指标

| 指标 | 目标/用途 |
|---|---|
| producer p95 latency | 性能基线 |
| producer error rate | 稳定性 |
| producer throttle count | broker 压力 |
| consumer lag | 消费健康 |
| rebalance count/duration | consumer group 稳定性 |
| commit latency/error | offset 一致性 |
| DLQ records | 失败处理质量 |
| retry budget usage | resilience 是否失控 |
| connection reconnect count | 网络稳定性 |

### 27.4 复利指标

| 指标 | 目标 |
|---|---:|
| 新 L2 库生成时间 | 下降 |
| 标准同步缺口 | 下降 |
| 失败到 patch 转化率 | 100% |
| 下游 adoption proof 自动化比例 | 上升 |
| 重复问题复发率 | 下降 |

---

## 28. 迭代优化机制

### 28.1 每次 PR 后

1. 更新 traceability matrix。
2. 更新 risk register。
3. 检查是否有新 ADR。
4. 运行 affected gates。
5. 生成 task evidence。
6. 若失败，输出 Failure Evidence。

### 28.2 每次 Release 后

1. 生成 release manifest + checksum。
2. 汇总 gate outputs。
3. 更新 downstream adoption status。
4. 生成 retrospective。
5. 输出 Prompt Patch / Harness Patch / Rule Patch。
6. 新增 issue candidates。

### 28.3 每次 Downstream Adoption 后

1. 验证 source commit 和 downstream commit。
2. 记录 downstream gate outputs。
3. 记录 rollback commands。
4. 标记 proof-based adoption。
5. 若只是 demo 或 local contract，保持 `adoption_claim=not_claimed`。

### 28.4 Self-improving Patch 模板

```yaml
patch_id: PATCH-HARNESS-20260604-KAFKAX-001
source_goal: GOAL-20260604-KAFKAX-L2-STANDARD-FACTORY
trigger: <failure or improvement>
problem: <what failed>
root_cause: <why existing harness missed it>
patch:
  type: prompt|harness|rule|ci|generator
  target: <file or gate>
  change: <specific change>
verification:
  command: <gate command>
  expected: passed
reuse:
  applies_to:
    - redisx
    - postgresx
    - taosx
    - natsx
```

---

## 29. Release 分级

### v0.1.0 MVA

- Repo skeleton
- API contracts
- fake driver
- config/metrics/error schema
- basic producer/consumer stubs
- required gates
- local release manifest
- no downstream adoption claim

### v0.2.0 Runtime Basic

- real producer
- real consumer
- real admin describe/plan
- basic observability
- integration tests

### v0.3.0 Production Semantics

- resilience policy
- DLQ
- rebalance robustness
- fault injection
- release-final clean

### v0.4.0 Downstream Adoption

- x.go or demo downstream proof
- rollback protocol tested
- adoption proof schema passed

### v1.0.0 Stable

- API freeze
- compatibility policy
- full broker matrix
- production deployment docs
- multiple downstream adoption proofs
- self-improving patches merged into xlib-standard

---

## 30. 执行命令总表

### 初始化

```bash
mkdir -p ~/work/zonecnh
cd ~/work/zonecnh

git clone git@github.com:ZoneCNH/xlib-standard.git
git clone git@github.com:ZoneCNH/kernel.git || true
git clone git@github.com:ZoneCNH/kafkax.git || true
git clone git@github.com:ZoneCNH/kafka.git || true
```

### xlib-standard 标准源分支

```bash
cd ~/work/zonecnh/xlib-standard
git fetch origin main
git worktree add -b goal/kafkax-l2-standard-source ../wt-xlib-kafkax-l2-standard-source origin/main
cd ../wt-xlib-kafkax-l2-standard-source
make install-hooks
make doctor-hooks
GOWORK=off make docs-check
```

### 生成 kafkax

```bash
cd ~/work/zonecnh/wt-xlib-kafkax-l2-standard-source
scripts/render_template.sh \
  --module-name kafkax \
  --module-path github.com/ZoneCNH/kafkax \
  --package-name kafkax \
  --out ../kafkax
```

### kafkax 验证

```bash
cd ~/work/zonecnh/kafkax
make install-hooks
make doctor-hooks
GOWORK=off go mod tidy
GOWORK=off make fmt
GOWORK=off make vet
GOWORK=off make test
GOWORK=off make race
GOWORK=off make boundary
GOWORK=off make security
GOWORK=off make contracts
GOWORK=off make docs-check
GOWORK=off make kafka-contract
GOWORK=off make kafka-metrics-golden
GOWORK=off make kafka-admin-golden
GOWORK=off make dependency-check
GOWORK=off make standard-impact-check
GOWORK=off go run ./cmd/goalcli score --min 9.8
CHECK_STATUS=passed GOWORK=off make evidence
RELEASE_EVIDENCE_REQUIRE_PASSED=1 GOWORK=off make release-evidence-check
```

### Release final

```bash
XLIB_CONTEXT=release_verify GOWORK=off make release-final-check
```

### Broker integration（如果 CI/本地支持）

```bash
KAFKAX_INTEGRATION=1 GOWORK=off make kafka-integration
KAFKAX_INTEGRATION=1 GOWORK=off make kafka-fault-injection
```

---

## 31. 文件交付清单

### xlib-standard 侧

```text
docs/standard/l2-kafka-adapter.md
docs/downstream-matrix.md
docs/downstream-sync-policy.md
docs/standard/harness-gates.md
docs/standard/evidence-protocol.md
contracts/l2-kafka-adapter.schema.json
.agent/harness/harness.yaml
.agent/registries/downstream-adoption-status.yaml
release/standard-impact/latest.md   # generated, not committed unless standard policy allows
```

### kafkax 侧

```text
README.md
CHANGELOG.md
go.mod
Makefile
pkg/kafkax/*.go
internal/driver/**/*.go
internal/config/*.go
internal/metrics/*.go
testkit/*.go
contracts/kafkax.config.schema.json
contracts/kafkax.metrics.schema.json
contracts/kafkax.message.schema.json
contracts/downstream-adoption-proof.schema.json
docs/api.md
docs/config.md
docs/errors.md
docs/metrics.md
docs/testing.md
docs/release.md
docs/adr/ADR-20260604-001-kafka-driver.md
docs/adr/ADR-20260604-002-delivery-semantics.md
docs/adr/ADR-20260604-003-no-business-schema.md
examples/basic-producer/**
examples/basic-consumer/**
examples/consumer-group/**
examples/admin-topic-plan/**
examples/health/**
scripts/run_kafka_integration.sh
scripts/run_fault_injection.sh
release/manifest/template.json
release/manifest/latest.json          # generated, not committed
release/manifest/latest.json.sha256   # generated, not committed
.agent/traceability/traceability-matrix.md
.agent/traceability/risk-register.md
.agent/traceability/decision-log.md
.agent/retrospectives/RETRO-20260604-KAFKAX.md
.agent/patches/PATCH-HARNESS-20260604-KAFKAX.md
.agent/patches/PATCH-RULE-20260604-KAFKAX.md
.agent/patches/PATCH-PROMPT-20260604-KAFKAX.md
```

---

## 32. 验收清单

### 32.1 P0 必过

- [ ] 不在 main 直接开发。
- [ ] 使用 git worktree。
- [ ] 不导入 `x.go`。
- [ ] 不包含业务 topic。
- [ ] 不包含业务 message schema。
- [ ] 不泄露 Kafka secrets。
- [ ] Public API 不暴露第三方 Kafka client 类型。
- [ ] Required gate 未被 skip。
- [ ] Evidence artifact 已生成。
- [ ] `latest.json` 未提交。
- [ ] downstream adoption 未被虚假升级。

### 32.2 P1 应过

- [ ] Producer contract 完整。
- [ ] Consumer contract 完整。
- [ ] Admin topic plan 完整。
- [ ] Metrics golden 完整。
- [ ] Health check 完整。
- [ ] Config redaction 完整。
- [ ] Fake driver 与真实 driver 共享 contract suite。
- [ ] Broker integration 通过或有 explicit blocker。

### 32.3 P2 增强

- [ ] Fault injection。
- [ ] Benchmark smoke。
- [ ] Transactional producer profile。
- [ ] SASL/TLS 组合 matrix。
- [ ] Redpanda / Apache Kafka / MSK compatibility notes。
- [ ] x.go downstream proof。

---

## 33. 最终推荐路径

### 推荐结论

最优路径不是直接在现有 `kafka` 仓库里补代码，而是执行以下顺序：

```text
先标准源，后仓库；
先契约，后实现；
先 fake contract，后真实 broker；
先 Evidence，后 release；
先 not_claimed，后 proof-based adopted；
先 kafkax 打样，后 L2 工厂复制。
```

### 推荐执行顺序

1. **冻结事实**：确认 `kafkax` canonical repo 和 `kafka` legacy 状态。
2. **更新标准源**：在 `xlib-standard` 增加 L2 Kafka profile 和必要 Standard Impact。
3. **生成仓库**：从 `xlib-standard` 渲染 `github.com/ZoneCNH/kafkax`。
4. **契约优先**：先实现 API/schema/error/metrics/testkit fake。
5. **默认 driver**：采用 `franz-go` 作为 internal driver，公共 API 不泄露实现。
6. **语义正确**：producer/consumer/admin 按 Kafka 真实语义实现，不夸大 exactly-once。
7. **接入 L1**：configx、observex、testkitx、resiliencx 受标准矩阵约束接入。
8. **Evidence release**：所有 gate 通过后生成 manifest + checksum。
9. **下游采纳**：只在真实下游 gate 输出存在时声明 adopted。
10. **复利回流**：把 `kafkax` 的新增 gates、contracts、lessons learned 回写 `xlib-standard`。

---

## 34. 参考源与依据

本方案依据以下当前标准源和 Kafka 客户端公开资料整理：

1. `xlib-standard` README：`https://github.com/ZoneCNH/xlib-standard/blob/main/README.md`
2. `xlib-standard` downstream matrix：`https://github.com/ZoneCNH/xlib-standard/blob/main/docs/downstream-matrix.md`
3. `xlib-standard` downstream adoption status：`https://github.com/ZoneCNH/xlib-standard/blob/main/.agent/registries/downstream-adoption-status.yaml`
4. `xlib-standard` harness gates：`https://github.com/ZoneCNH/xlib-standard/blob/main/docs/standard/harness-gates.md`
5. `xlib-standard` evidence protocol：`https://github.com/ZoneCNH/xlib-standard/blob/main/docs/standard/evidence-protocol.md`
6. `xlib-standard` generation docs：`https://github.com/ZoneCNH/xlib-standard/blob/main/docs/generation.md`
7. `franz-go`：`https://github.com/twmb/franz-go`
8. `segmentio/kafka-go`：`https://github.com/segmentio/kafka-go`
9. `confluent-kafka-go`：`https://docs.confluent.io/kafka-clients/go/current/overview.html`

---

## 35. 最终一句话

`kafkax` 的目标不是成为“Kafka helper”，而是成为 `xlib-standard` 标准源控制下第一个真正完成 L2 基础设施适配层闭环的 Kafka 工厂样板：独立仓库、独立 release、独立 Evidence、共享 L0/L1 契约、可下游采纳、可回滚、可复盘、可复制、可复利增长。
