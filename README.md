# kafkax

Release version: v1.1.1

`kafkax` 是 FoundationX 的 **L2 Kafka adapter 基础库**，为 FoundationX 量化交易系统提供 driver-neutral 的 Kafka 生产、消费和集群管理能力。

kafkax 遵循 xlib-standard 的治理协议，但不是标准源、不是 generator、不是模板仓库。

仓库地址：[`kafkax`](https://github.com/ZoneCNH/kafkax)。module path：`github.com/ZoneCNH/kafkax`。

本文是 API 参考与语义说明——覆盖 Producer/Consumer/Admin 接口、配置、Kafka 语义和错误模型。

- 权威规范：[docs/standard/l2-kafka-adapter.md](docs/standard/l2-kafka-adapter.md)（contract、边界、合规要求）
- 执行计划与证据：[docs/goal/goal.md](docs/goal/goal.md)（REQ-001..REQ-015 完成度追踪）
- 设计决策：[docs/adr/](docs/adr/)（driver 选型等关键决策记录）

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
- `Flush` 显式刷新 producer 内部缓冲区，确保缓冲消息已发送
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

- `Run` 驱动 consumer loop，每条 record 交给 `Handler` 处理；driver 内部处理 rebalance（partition revoke/assign）
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
```

Message、Record、RecordBatch、Subscription、TopicSpec、TopicDescription、TopicPlan、TopicApplyResult、BatchProduceResult 均提供不可变 `Clone()` 方法。完整 Config 结构体见[配置](#配置)章节。

## Kafka 语义

### Producer 确认策略

Producer 通过 `ProducerConfig.RequiredAcks` 控制确认策略：

| RequiredAcks | 语义 | 风险 |
|---|---|---|
| 0 (unset) | kafkax 默认等待所有 ISR 确认 | Go 零值按生产安全默认处理 |
| 1 | 等待 leader 确认 | leader 故障时可能丢失 |
| -1 (all) | 等待所有 ISR 确认 | 最高持久性保证 |

默认推荐保持零值或显式设置 `RequiredAcks = -1`（all），搭配 topic 侧 `TopicSpec.MinInSyncReplicas >= 2` 使用。kafkax 不把零值映射为 no-ack，因为生产适配器必须默认保留持久化确认。

### 投递保证

kafkax 不宣称 exactly-once。投递保证取决于 handler 与 offset commit 的时序：

- **at-most-once**：先 commit offset，再处理消息。handler 崩溃时消息丢失。
- **at-least-once**（默认）：先处理消息，再 commit offset。handler 崩溃时消息重复投递。

调用方必须在 handler 中实现幂等处理以应对 at-least-once 的重复消息。Exactly-once 需要 broker 事务（`transactional.id`）、producer 幂等和 consumer 隔离级别共同配合，不属于基础 adapter 的默认语义。

### Consumer Group Rebalance

Consumer group rebalance 是正常运维事件，不是异常。rebalance 时 producer 应 flush，consumer 应尽快完成当前批处理并 commit offset，避免 rebalance 超时导致重复消费。

kafkax 公开 API 不暴露 rebalance 回调。底层 driver（如 `segmentio/kafka-go` Reader）在 rebalance 时内部处理 partition 分配和 offset 恢复，上层 handler 只需保证每条 record 处理是幂等的。

### Offset Commit 时机

- `Consumer.Run` 模式：每轮 Poll→Handle→Commit。handler 返回 error 时 `Run` 退出，未 commit 的 record 在 consumer 重启后从上次提交位置重新投递。
- `Consumer.Poll` 模式（手动提交）：调用方自行控制 Poll 和 Commit 的节奏与粒度。

### Handler Panic 行为

`Consumer.Run` 不捕获 handler panic。handler 中发生的 panic 会传播到调用方 goroutine。调用方应在 handler 内部自行 recover，或在外层 goroutine 中设置 recover 保护。

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

- **retry topic**：handler 捕获 `Retryable` 错误后，将消息重新投递到 delay queue topic，例如 `<topic>.retry.<attempt>`，并保留原始 topic、partition、offset、attempt、next-at 等 headers
- **DLT**：handler 重试耗尽后，将消息写入 dead letter topic，例如 `<topic>.dlt`，记录最终错误和原始 offset，然后 commit offset，避免卡住分区消费

生产调用方应把 retry/DLT topic 作为显式 topic 资源纳入 `AdminClient.EnsureTopic`/IaC 管理，并让业务 handler 负责幂等去重。kafkax 在公开 API 中保留 headers 和 `Retryable` 错误标记，为上层实现 retry/DLT 策略提供基础。

## Driver-Neutral 设计

kafkax 公开 API 不暴露任何第三方 Kafka client 具体类型。生产驱动 `kafkago.Driver`（`pkg/kafkax/kafkago/driver.go`）基于 `segmentio/kafka-go` 实现 `kafkax.Producer`、`kafkax.ConsumerFactory` 和 `kafkax.Admin`，通过 `ClientOptions()` 返回 `kafkax.Option` 注入到 `kafkax.Client`：

```go
d, _ := kafkago.New(cfg)
client, _ := kafkax.New(ctx, cfg, d.ClientOptions()...)
```

调用方只需依赖 `pkg/kafkax` 的公开接口。`Driver` 接口抽象定义在 `internal/driver/driver.go`（Capability/Descriptor），用于 driver 注册和自我描述。

### Fake Driver（testkit）

`testkit/kafka.go` 提供无 broker、无第三方依赖的 fake Kafka runtime，用于锁定 API contract 行为：

```go
// 完整 fake —— 同一 KafkaFake 内的 producer/consumer/admin 共享状态
fk := testkit.FakeKafka()
producer, _ := fk.Producer(ctx)
consumer, _ := fk.Consumer(ctx)
admin, _    := fk.Admin(ctx)

// 快捷独立 fake
producer, _ := testkit.FakeProducer(ctx)
consumer, _ := testkit.FakeConsumer(ctx)
admin, _    := testkit.FakeAdmin(ctx)

// Golden 测试数据
msg := testkit.GoldenRecord("my-topic")
```

`KafkaFake` 内部维护 record store 和 topic registry，fake producer 写入后 fake consumer 可 poll 到相同 record，支持 round-trip 验证。fake consumer 支持 Pause/Resume 流控，fake admin 支持 PlanTopics（diff）和 ApplyTopics（CRUD）。`GoldenRecord` 提供稳定的测试数据。

Broker-backed integration 只能由独立 extended gate 证明，不能由 fake driver 代替。

## 配置

```go
type Config struct {
    Name          string
    Timeout       time.Duration
    Secret        string
    Brokers       []string
    ClientID      string
    Security      SecurityConfig
    Producer      ProducerConfig
    Consumer      ConsumerConfig
    Admin         AdminConfig
    Retry         RetryConfig
    Observability ObservabilityConfig
}

type SecurityConfig struct {
    Protocol SecurityProtocol // plaintext / tls / sasl
    Username string
    Password string
    Token    string
}

type ProducerConfig struct {
    RequiredAcks int  // 0(unset => all) / 1 / -1(all)
    Idempotent   bool
    BatchBytes   int
}

type ConsumerConfig struct {
	GroupID           string
	SessionTimeout    time.Duration
	HeartbeatInterval time.Duration
	MaxPollRecords    int
	StartOffset       OffsetResetPolicy // earliest / latest / none
}

type AdminConfig struct {
    Timeout time.Duration
    DryRun  bool
}

type RetryConfig struct {
    MaxAttempts int
    Backoff     time.Duration
}
```

`Config.Sanitize()` 返回脱敏副本，Password/Token 字段替换为 `[REDACTED]`。`Config.Validate()` 校验 Name 非空、数值字段非负。

## 健康检查

```go
func (c *Client) HealthCheck(ctx context.Context) HealthStatus
```

返回 `healthy` / `degraded` / `unhealthy` 三级状态，包含延迟（`LatencyMs`）和诊断元数据。

## Metrics

kafkax 通过 `Metrics` 接口上报以下指标：

| 类别 | 指标 |
|---|---|
| Client | `client_created_total`、`client_closed_total`、`client_errors_total`、`client_health_status`、`client_health_latency_ms`、`client_requests_total`、`client_request_duration_seconds`、`client_retries_total`、`client_inflight` |
| Producer | `producer_messages_total`、`producer_errors_total`、`producer_latency_seconds` |
| Consumer | `consumer_messages_total`、`consumer_errors_total`、`consumer_lag`、`consumer_commits_total` |
| Admin | `admin_operations_total`、`admin_errors_total` |

测试场景使用 `NoopMetrics` 静默丢弃。生产环境由调用方注入实现（如 `observex` 适配）。

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
- 不隐式读取生产密钥；生产凭证通过 `Config.Security` 显式注入，不在源码、日志或 artifact 中泄露。
- 不创建隐藏全局客户端、不可关闭后台进程。
- 不在公开 API 中泄露第三方 Kafka client 具体类型。

## 项目结构

- `pkg/kafkax`：公开 API（Producer、Consumer、Admin、Config、Message、Error、Metrics、HealthCheck）
- `pkg/kafkax/kafkago/`：`segmentio/kafka-go` 生产驱动（`kafkago.Driver`、producer、consumer、admin）
- `internal/`：内部辅助（validation、sanitize、driver 接口抽象、release quality scoring、goal runtime）
- `testkit/`：可复用测试夹具、`KafkaFake` fake runtime、golden 断言
- `contracts/`：JSON schema 和 metrics contract
- `docs/`：API、配置、测试、goal 执行计划、ADR 等文档
- `scripts/`：CI gate 与 evidence 脚本
- `cmd/goalcli/`：goal 治理 CLI（score、schema check、traceability、audit）
- `.agent/`：治理自动化配置（harness、rules、policies、traceability matrix）
- `release/`：release manifest、standard impact 报告、evidence artifact

## 命令

```bash
make ci          # 标准 gate（lint、test、contracts、boundary）
make ci-extended # 扩展 gate（含 extended test suite）
make evidence    # 生成 release evidence
```

完整 gate 链见 [Makefile](Makefile)。

## 测试

`go test ./...` 覆盖公开包、`internal/`、`contracts/`、`testkit/`。fake Kafka fixture（`testkit.KafkaFake`）锁定 producer/consumer/admin contract 行为，不依赖真实 broker。

Broker-backed integration 只能由独立 extended gate 证明，不能由 fake driver 代替。
