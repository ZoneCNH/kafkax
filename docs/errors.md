# 错误模板

## 占位符

- `kafkax`
- `kafkax`

## 错误类型

| `ErrorKind` | 字符串 | 典型场景 | Retryable |
| --- | --- | --- | --- |
| `ErrorKindConfig` | `config` | 配置来源或配置装载失败。 | 否 |
| `ErrorKindValidation` | `validation` | 配置字段缺失、格式非法、调用参数非法。 | 否 |
| `ErrorKindConnection` | `connection` | 连接建立失败。 | 通常是 |
| `ErrorKindUnavailable` | `unavailable` | context canceled、依赖暂不可用。 | 视场景 |
| `ErrorKindTimeout` | `timeout` | context deadline exceeded 或外部超时。 | 是 |
| `ErrorKindAuth` | `auth` | 认证、授权失败。 | 否 |
| `ErrorKindConflict` | `conflict` | 幂等冲突、资源状态冲突。 | 否 |
| `ErrorKindRateLimit` | `rate_limit` | 限流或配额耗尽。 | 是 |
| `ErrorKindInternal` | `internal` | 未分类内部错误。 | 否 |

## 约束

- 公共错误必须使用 `Error`、`NewError` 或 `WrapError` 表达稳定 contract。
- 包装错误必须保留 cause，使调用方可以使用 `errors.Is` / `errors.As`。
- 调用方按 `IsKind(err, ErrorKind...)` 做分支判断，不依赖错误字符串。
- 错误可以安全纳入 Evidence，但不得包含原始凭据、生产连接串或业务私密数据。
- 生成的库不得使用 `x.go` 业务模型。

## Kafka L2 error mapping

L2 Kafka adapter/factory 只能把 driver/private error 映射到稳定 `ErrorKind`，不能把第三方 driver error 类型暴露为 public API。

| Kafka 场景 | Public kind | 说明 |
| --- | --- | --- |
| brokers/client_id/security 配置缺失或非法 | `validation` / `config` | 配置字段错误使用 `validation`；配置来源错误使用 `config`。 |
| broker 连接失败、metadata 拉取失败 | `connection` | 通常 retryable，必须保留脱敏 cause。 |
| broker 暂不可用、consumer group 暂不可用 | `unavailable` | 可按场景 retry。 |
| send/poll/admin deadline exceeded | `timeout` | 必须支持 `errors.Is` / `errors.As` 访问 wrapped cause。 |
| SASL/TLS/authz 失败 | `auth` | 不得泄露用户名、密码、证书或连接串。 |
| topic 已存在、offset/commit/resource state 冲突 | `conflict` | 调用方按 kind 分支。 |
| quota/throttle | `rate_limit` | retry/backoff contract 必须记录到 metrics。 |
| 未分类 driver failure | `internal` | 不得包含消息 value、raw key 或业务 payload。 |

错误日志、Evidence 和 release manifest 默认不得包含 Kafka message value；需要诊断 payload 时必须另走受控私有证据通道，本仓库 contract 不定义该通道。
