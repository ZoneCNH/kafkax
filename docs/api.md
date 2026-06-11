# API 模板

## 占位符

- `kafkax`：生成的仓库名称。
- `github.com/ZoneCNH/kafkax`：生成的 Go module 路径。
- `kafkax`：生成的包名。

## 公共 API

- `Config`：由用户显式提供的配置。
- `Brokers` / `ClientID` / `Security` / `Producer` / `Consumer` / `Admin` / `Retry` / `Observability`：Kafka L2 adapter 的显式配置面。当前 contract-first / fake-first 切片不绑定任何第三方 Kafka driver，也不在公开 API 中暴露 driver concrete type。
- `Validate`：拒绝无效配置，并返回 `ErrorKindValidation`。
- `Sanitize`：在日志或 Evidence 采集前屏蔽敏感值。
- `New`：基于显式配置创建客户端；拒绝 `nil`、canceled 和 expired context；成功时记录 `client_created_total`。
- `Close`：释放资源，并且必须幂等；成功首次关闭时记录 `client_closed_total`。
- `HealthCheck`：报告客户端健康状态，JSON 字段必须匹配 `contracts/health.schema.json`；当本次检查的 context deadline 预算短于 `Config.Timeout` 时返回 `degraded`。
- `Error`：稳定 error contract，支持 `errors.Is` / `errors.As` 和 `IsKind`。
- `ErrorKindProduce` / `ErrorKindConsume` / `ErrorKindCommit` / `ErrorKindAdmin` / `ErrorKindDriver`：Kafka 领域稳定错误分类，用于 producer、consumer、offset commit、admin 和 driver adapter 边界。
- `NewError` / `WrapError`：创建或包装稳定错误，包装时必须保留 cause。
- `Metrics`：注入式指标钩子；指标名必须匹配 `contracts/metrics.md`。
- `Version`：发布版本。

## Kafka L2 合约面

- `Message` / `Header` / `Partition` / `Offset`：公开消息模型；`Clone` 必须隔离 `Key`、`Value` 和 header value，避免调用方和 driver adapter 共享可变 byte slice。
- `Producer`：以 `Send(ctx, Message, ...ProduceOption) (ProduceResult, error)`、`SendBatch(ctx, []Message, ...ProduceOption) (BatchProduceResult, error)` 和 `Flush(ctx) error` 暴露同步投递、批量投递与关闭前 flush；实现可以映射任意 Kafka driver，但公开签名不得包含第三方 Kafka 类型。
- `Consumer`：以 `Run`、`Poll`、`Commit`、`Pause`、`Resume` 和 `Close` 暴露消费、handler lifecycle、offset commit 与背压控制；`Subscription` 是 driver-neutral 订阅配置模型，使用标准 `OffsetResetPolicy`。当前切片尚未实现 broker-backed subscribe lifecycle、rebalance hook 或 lag 采集。
- `Admin`：以 `DescribeTopics`、`PlanTopics`、`ApplyTopics` 和 `Close` 暴露批量 topic 规划与应用；`TopicSpec`、`TopicDescription`、`TopicPlan` 和 `TopicApplyResult` 使用可序列化的标准结构，并显式表达 no-op、create、update、conflict、timeout 和 unsupported operation。
- `contracts/kafkax.message.schema.json` 与 `contracts/kafkax.topic.schema.json` 是当前 Kafka 消息和 topic spec 的机器契约锚点。

生成的基础库不得依赖 `x.go`。

## L2 Kafka adapter API 契约

当 `kafkax` 作为 L2 Kafka adapter/factory 标准推进时，公共 API 必须额外覆盖：

- `Producer`：通过 `Send`、`SendBatch` 和 `Flush` 发送消息，公开交付语义、重试/幂等约束、batch 局部失败和交付结果。
- `Consumer`：通过 `Poll` 和 `Run` 订阅/消费消息，通过 `Commit`、`Pause`、`Resume` 管理 offset、背压、rebalance 和 handler 失败语义。
- `Admin`：通过 plural topic planning/apply 管理 topic/metadata，公开 unsupported operation、timeout、conflict 和 auth error。
- `TopicSpec`：driver-neutral topic 声明。
- `Message`、`Header`、`Offset`：匹配 `contracts/kafkax.message.schema.json` 的 driver-neutral 数据模型。
- `Error`、`Health`、`Config`：继续使用稳定 typed error、health 和 config contract。

公共 API 不得暴露 `kgo.*`、`kafka-go.*`、`confluent.*` 或其它第三方 Kafka driver 类型；driver 只能存在于内部实现或适配器边界。`docs/adr/ADR-20260604-001-kafka-driver.md` 记录 driver-neutral boundary 和 fake-first gate 的决策。当前第一切片的消息和 topic 契约由 `contracts/kafkax.message.schema.json`、`contracts/kafkax.topic.schema.json`、`contracts/kafkax.config.schema.json`、`contracts/error.schema.json` 和 `contracts/kafkax.metrics.schema.json` 约束。

## 当前证据边界

- 本文档描述 Kafka L2 adapter 的目标 API 语义；当前仓库代码已完成 public API、`internal/driver` descriptor 边界、`testkit.FakeKafka` fixture 和 topic schema 的第一切片，尚未完成 production driver、broker-backed driver、broker-dependent gates 或 release/downstream adoption 证据。
- 任何完整目标完成声明必须同时提供 public API contract tests、`internal/driver` 边界、testkit fake fixtures、broker-dependent gate 状态和 release/adoption 证据；缺少 broker、release 或 adoption 证据时只能声明对应 fake-first/contract-first 切片通过。

## 生成对齐

使用 `scripts/render_template.sh` 生成具体基础库时，公共包目录会从 `pkg/kafkax` 移动到 `pkg/kafkax`，代码 imports、文档占位符和 module path 会同步替换。
