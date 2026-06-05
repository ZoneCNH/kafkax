# API 模板

## 占位符

- `kafkax`：生成的仓库名称。
- `github.com/ZoneCNH/kafkax`：生成的 Go module 路径。
- `kafkax`：生成的包名。

## 公共 API

- `Config`：由用户显式提供的配置。
- `Brokers` / `ClientID` / `Security` / `Producer` / `Consumer` / `Admin` / `Retry` / `Observability`：Kafka L2 adapter 的显式配置面。当前 contract-first skeleton 不绑定任何第三方 Kafka driver，也不在公开 API 中暴露 driver concrete type。
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
- `Producer`：以 `Produce(ctx, Message, ...ProduceOption) (ProduceResult, error)` 暴露投递能力；实现可以映射任意 Kafka driver，但公开签名不得包含第三方 Kafka 类型。
- `Consumer`：以 `Subscribe`、`Receive`、`Commit` 和 `Close` 暴露消费与 offset commit 能力；`Subscription` 使用标准 `OffsetResetPolicy`。
- `Admin`：以 `DescribeTopic`、`PlanTopic`、`ApplyTopic` 和 `Close` 暴露 topic 管理能力；`TopicSpec`、`TopicDescription` 和 `TopicPlan` 使用可序列化的标准结构。
- `contracts/kafkax.message.schema.json` 与 `contracts/kafkax.topic.schema.json` 是当前 Kafka 消息和 topic spec 的机器契约锚点。

生成的基础库不得依赖 `x.go`。

## L2 Kafka adapter API 契约

当 `kafkax` 作为 L2 Kafka adapter/factory 标准推进时，公共 API 必须额外覆盖：

- `Producer`：发送消息，公开交付语义、重试/幂等约束和交付结果。
- `Consumer`：订阅和消费消息，公开 offset、commit、rebalance 和 handler 失败语义。
- `Admin`：topic/metadata 管理，公开 unsupported operation、timeout、conflict 和 auth error。
- `TopicSpec`：driver-neutral topic 声明。
- `Message`、`Header`、`Offset`：匹配 `contracts/kafkax.message.schema.json` 的 driver-neutral 数据模型。
- `Error`、`Health`、`Config`：继续使用稳定 typed error、health 和 config contract。

公共 API 不得暴露 `kgo.*`、`kafka-go.*`、`confluent.*` 或其它第三方 Kafka driver 类型；driver 只能存在于内部实现或适配器边界。当前第一切片的消息和 topic 契约由 `contracts/kafkax.message.schema.json`、`contracts/kafkax.topic.schema.json`、`contracts/kafkax.config.schema.json`、`contracts/error.schema.json` 和 `contracts/kafkax.metrics.schema.json` 约束。

## 生成对齐

使用 `scripts/render_template.sh` 生成具体基础库时，公共包目录会从 `pkg/kafkax` 移动到 `pkg/kafkax`，代码 imports、文档占位符和 module path 会同步替换。
