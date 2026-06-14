# kafkax

`kafkax` 是 FoundationX 的 **L2 Kafka adapter 基础库**，为 FoundationX 量化交易系统提供 driver-neutral 的 Kafka 生产、消费和集群管理能力。

kafkax 遵循 xlib-standard 的治理协议，但不是标准源、不是 generator、不是模板仓库。

仓库地址：[`kafkax`](https://github.com/ZoneCNH/kafkax)。module path：`github.com/ZoneCNH/kafkax`。

## 架构定位

```text
L0 基座： kernel（生命周期、依赖注入）
L1 共享： configx、observex、testkitx、resiliencx、schedulex
L2 适配： kafkax ← 本仓库
L3 应用： x.go（编排层，仅消费 L2 API）
```

kafkax 作为 L2 适配层，只表达 Kafka 基础设施语义，不表达业务语义。禁止依赖 `x.go` 或任何业务 topic/schema。

## API 概览

### Producer

```go
type Producer interface {
    Send(context.Context, Message, ...ProduceOption) (ProduceResult, error)
    SendBatch(context.Context, []Message, ...ProduceOption) (BatchProduceResult, error)
    Flush(context.Context) error
    Close(context.Context) error
}
```

- 支持单条发送 `Send` 和批量发送 `SendBatch`
- `Flush` 显式刷新内部缓冲区，确保消息已提交到 broker
- 通过 `ProduceOption` 注入 per-message headers

### Consumer

```go
type Consumer interface {
    Run(context.Context, Handler) error
    Poll(context.Context) (RecordBatch, error)
    Commit(context.Context, ...Offset) error
    Pause(context.Context, ...TopicPartition) error
    Resume(context.Context, ...TopicPartition) error
    Close(context.Context) error
}
```

- `Run` 驱动 consumer loop，将每条 record 分发给 `Handler`，支持 rebalance 回调
- `Poll` 提供手动拉取模式，调用方自行控制拉取节奏
- `Commit` 显式提交 offset
- `Pause` / `Resume` 控制分区消费流控
- 支持 `OffsetResetPolicy`：`earliest` / `latest` / `none`

### Admin

```go
type Admin interface {
    DescribeTopics(context.Context, ...string) ([]TopicDescription, error)
    PlanTopics(context.Context, ...TopicSpec) (TopicPlan, error)
    ApplyTopics(context.Context, TopicPlan) (TopicApplyResult, error)
    Close(context.Context) error
}
```

- `DescribeTopics` 查询 topic 元数据
- `PlanTopics` 对比期望 spec 与集群现状，输出 diff plan
- `ApplyTopics` 按 plan 执行 topic 创建/修改

### 通用类型

```go
type Message struct {
    Topic     string
    Key       []byte
    Value     []byte
    Headers   []Header
    Timestamp time.Time
    Partition Partition
    Offset    Offset
}

type Config struct {
    Name          string
    Timeout       time.Duration
    Brokers       []string
    ClientID      string
    Security      SecurityConfig
    Producer      ProducerConfig
    Consumer      ConsumerConfig
    Admin         AdminConfig
    Retry         RetryConfig
    Observability ObservabilityConfig
}
```

全部公开类型提供不可变 `Clone()` 方法。所有读操作返回副本，不暴露内部引用。

## Kafka 语义

### Producer 确认策略

Producer 通过 `ProducerConfig.RequiredAcks` 控制确认策略：

| RequiredAcks | 语义 | 风险 |
|---|---|---|
| 0 | 不等待 broker 确认 | 消息可能丢失，无重试 |
| 1 | 等待 leader 确认 | leader 故障时可能丢失 |
| -1 (all) | 等待所有 ISR 确认 | 最高持久性保证 |

默认推荐 `RequiredAcks = -1`（all），搭配 `MinInSyncReplicas >= 2` 使用。

### 投递保证

kafkax 不宣称 exactly-once。投递保证取决于 handler 与 offset commit 的时序：

- **at-most-once**：先 commit offset，再处理消息。handler 崩溃时消息丢失。
- **at-least-once**（默认）：先处理消息，再 commit offset。handler 崩溃时消息重复投递。

调用方必须在 handler 中实现幂等处理以应对 at-least-once 的重复消息。Exactly-once 需要 broker 事务（`transactional.id`）、producer 幂等和 consumer 隔离级别共同配合，不属于基础 adapter 的默认语义。

### Consumer Group Rebalance

Consumer group rebalance 是正常运维事件，不是异常。kafkax 的 `Consumer.Run` 在 rebalance 时执行：

1. **partition revoked**：停止当前分区消费，刷新内部缓冲区
2. **partition assigned**：从最后提交的 offset 恢复消费

Handler 必须在 revoke 信号到达后尽快返回，避免 rebalance 超时导致重复消费。

### Offset Commit 时机

- `Consumer.Run` 模式（自动提交）：每条 record 处理成功后自动 commit。handler 返回 error 时跳过 commit，record 将在下次 poll 重新投递。
- `Consumer.Poll` 模式（手动提交）：调用方通过 `Commit` 显式提交 offset，自行决定提交粒度。

### Handler Panic 行为

Consumer handler 中发生的 panic 被 kafkax 捕获并转换为 `ErrorKindConsume` 错误。panic 不会导致 consumer 进程退出，但该条 record 不会被 commit。捕获 panic 后 consumer loop 继续运行，panic 记录通过 `Metrics` 上报。

### 重试

Producer 重试通过 `RetryConfig` 控制：

```go
type RetryConfig struct {
    MaxAttempts int           // 最大重试次数，0 表示不重试
    Backoff     time.Duration // 重试间隔
}
```

重试仅适用于 `Retryable` 错误（网络超时、leader 选举、broker 不可用）。非可重试错误（序列化失败、消息过大、认证失败）直接返回，不重试。

重试可能破坏顺序、放大消息重复。对顺序敏感的场景必须在 handler 中通过 idempotency key 去重。

### Retry Topic / Dead Letter Topic

kafkax 本身不内置 retry topic 或 dead letter topic（DLT）机制。这些是消费端的业务策略，由调用方在 handler 中实现：

- **retry topic**：handler 捕获 `Retryable` 错误后，将消息重新投递到 delay queue topic
- **DLT**：handler 重试耗尽后，将消息写入 dead letter topic 并 commit offset，避免卡住分区消费

kafkax 在公开 API 中保留 headers 和 `Retryable` 错误标记，为上层实现 retry/DLT 策略提供基础。

## Driver-Neutral 设计

kafkax 公开 API 不暴露任何第三方 Kafka client 具体类型。生产驱动 `kafkago.Driver`（`pkg/kafkax/kafkago/driver.go`）基于 `segmentio/kafka-go` 实现 `kafkax.Producer`、`kafkax.ConsumerFactory` 和 `kafkax.Admin`，通过 `ClientOptions()` 返回 `kafkax.Option` 注入到 `kafkax.Client`：

```go
d, _ := kafkago.New(cfg)
client, _ := kafkax.New(ctx, cfg, d.ClientOptions()...)
```

调用方只需依赖 `pkg/kafkax` 的公开接口。`Driver` 接口抽象定义在 `internal/driver/driver.go`（Capability/Descriptor），用于 driver 注册和自我描述。fake driver 位于 `testkit/`，无需真实 broker 即可锁定 API contract 行为。

## 配置

```go
type ProducerConfig struct {
    RequiredAcks    int           // 0 / 1 / -1
    Compression     CompressionCodec
    Idempotence     bool
    MaxRetries      int
    RetryBackoff    time.Duration
    BatchSize       int
    Linger          time.Duration
    BufferMemory    int64
    MaxInFlight     int
    RequestTimeout  time.Duration
    DeliveryTimeout time.Duration
}

type ConsumerConfig struct {
    SessionTimeout    time.Duration
    HeartbeatInterval time.Duration
    MaxPollRecords    int
    FetchMinBytes     int
    FetchMaxBytes     int
    FetchMaxWait      time.Duration
    MaxRetries        int
    RetryBackoff      time.Duration
    IsolationLevel    IsolationLevel
    AutoCommit        bool
}

type AdminConfig struct {
    Timeout          time.Duration
    MaxRetries       int
    RetryBackoff     time.Duration
    RequestTimeout   time.Duration
}
```

完整配置支持 TLS、SASL 认证。`Config.Sanitize()` 返回脱敏副本，secret 字段替换为 `[REDACTED]`。

## 健康检查

```go
func (c *Client) HealthCheck(ctx context.Context) HealthStatus
```

返回 `healthy` / `degraded` / `unhealthy` 三级状态，包含延迟和元数据。对上层 `observex` 的 health endpoint 集成友好。

## Metrics

kafkax 通过 `Metrics` 接口上报以下指标：

| 类别 | 指标 |
|---|---|
| Client | `client_created_total`、`client_closed_total`、`client_errors_total`、`client_health_status`、`client_health_latency_ms`、`client_requests_total`、`client_request_duration_seconds`、`client_retries_total`、`client_inflight` |
| Producer | `producer_messages_total`、`producer_errors_total`、`producer_latency_seconds` |
| Consumer | `consumer_messages_total`、`consumer_errors_total`、`consumer_lag`、`consumer_commits_total` |
| Admin | `admin_operations_total`、`admin_errors_total` |

`Metrics` 接口与 `observex` 的 Prometheus bridge 兼容。测试场景使用 `NoopMetrics` 静默丢弃。

## 错误模型

```go
type Error struct {
    Kind      ErrorKind // config / validation / connection / unavailable / timeout / auth / conflict / rate_limit / produce / consume / commit / admin / driver / internal
    Op        string
    Message   string
    Cause     error
    Retryable bool
}
```

每个错误都携带 `Kind` 分类和 `Retryable` 标记，上层可以根据错误类型执行不同的恢复策略（重试、换 broker、告警、放弃）。

## 非目标

- 不依赖 `x.go`，也不把 `x.go` 作为构建前提。
- 不包含业务 topic、业务消息 schema 或业务 repository。
- 不隐式读取生产密钥，不把 `/home/k8s/secrets/env/*` 的内容写入源码、README、测试日志、manifest 或 PR 描述。
- 不创建隐藏全局客户端、不可关闭后台进程。
- 不在公开 API 中泄露第三方 Kafka client 具体类型。

## 项目结构

- `pkg/kafkax`：公开 API（Producer、Consumer、Admin、Config、Message、Error、Metrics、HealthCheck）
- `pkg/kafkax/kafkago/`：具体 driver 实现
- `internal/`：内部辅助（validation、sanitize、driver 接口定义）
- `testkit/`：可复用测试夹具、fake Kafka runtime、golden 断言
- `contracts/`：JSON schema 和 metrics contract
- `docs/`：规格、设计、API、配置、测试、发布文档
- `scripts/`：gate 与 evidence 脚本
- `.agent/`：Goal Runtime v3.1 工件、evidence、评审、发布、回滚和复盘模板
- `release/manifest/`：release manifest 模板；`latest.json` 由 release gate 生成并作为 evidence artifact 保存

## 文档入口

- [Goal 执行计划](docs/goal/goal.md)：kafkax L2 标准工厂完整执行计划
- [仓库角色](docs/standard/repository-roles.md)：区分 `xlib-standard`、`kernel`、L1/L2 基础库和 `x.go`
- [模块边界](docs/standard/module-boundary.md)：定义标准、模板、generator、harness、evidence 与下游库边界
- [下游矩阵](docs/downstream-matrix.md)：列出 `kernel` 与所有目标库的 module path、package、layer、允许依赖和禁止依赖
- [下游同步策略](docs/downstream-sync-policy.md)：定义上游变更如何同步到 `kernel`、L1/L2 基础库
- [x.go 集成边界](docs/xgo-integration-boundary.md)：说明 `x.go` 只能作为调用方组合层，基础库不得反向依赖
- [Harness gate](docs/standard/harness-gates.md)：required、extended、generator、docs、score 和 final gate 命令
- [Evidence 协议](docs/standard/evidence-protocol.md)：`DONE with evidence:` 和 release manifest 要求
- [测试策略](docs/testing.md)：单元、示例 smoke、release quality 和 release manifest fixture 隔离要求
- [安全与密钥策略](docs/standard/security-and-secret-policy.md)：secret scan、`govulncheck` 和 agent runtime 目录排除边界
- [供应链与 Evidence](docs/supply-chain.md)：workflow action SHA pinning、`govulncheck` 固定版本、release manifest 和 CI artifact 对齐
- [发布](docs/release.md)：`release-check`、manifest 字段和 evidence 规则
- [Driver ADR](docs/adr/)：Kafka driver 实现选型决策记录

## 命令

本地运行完整 gate 前默认需要安装 `golangci-lint`；`make security` 默认只运行 secret scan，不访问漏洞库。只有 `XLIB_ENABLE_VULNCHECK=1` 且一周窗口到期、状态缺失，或 `XLIB_FORCE_VULNCHECK=1` 时才执行 `govulncheck ./...`。缺少默认必需工具，或漏洞扫描到期/强制执行时缺少 `govulncheck`，相关 gate 必须失败。

### 首次 clone 必跑

```bash
make install-hooks   # 启用 .githooks 本地 P0 防线
make doctor-hooks    # 验证 core.hooksPath=.githooks 已生效
make sync-main       # 拉取并 fast-forward 本地 main
```

### 标准 gate

```bash
make ci
make ci-extended
GOWORK=off make dependency-check
GOWORK=off make standard-impact-check
GOWORK=off make docs-check
XLIB_CONTEXT=release_verify GOWORK=off make release-check
XLIB_CONTEXT=release_verify GOWORK=off make release-final-check
make evidence
```

`release-check` 依赖 `dependency-check`、`standard-impact-check` 和 `docs-check`，用于在生成 evidence 前确认依赖漂移自动化、标准影响报告、文档入口、下游同步策略、链接、模板占位符、当前命名、关键文本和 release manifest 协议没有漂移。

Release gate 执行 `GOWORK=off go run ./cmd/goalcli score --min 9.8`。所有发布、验证命令默认使用 `GOWORK=off`，避免父级或本地 `go.work` 改写 module 解析。

## Evidence

完成需要 release manifest 和 CI evidence。`release/manifest/latest.json` 是生成产物，不提交到源码历史；对应的 `release/manifest/latest.json.sha256` 也是生成产物，两者都必须保持在 `.gitignore` 中。

最终完成声明必须包含 `DONE with evidence:`。

Full Goal Runtime v3.1 位于 [.agent](.agent/)。

## Smoke 覆盖

`go test ./...` 覆盖公开包、`internal/`、`contracts/`、`testkit/`。fake Kafka fixture 锁定 producer/consumer/admin contract 行为，不依赖真实 broker。

Broker-backed integration 只能由独立 extended gate 证明，不能由 fake driver 代替。
