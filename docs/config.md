# 配置模板

## 占位符

- `kafkax`
- `github.com/ZoneCNH/kafkax`
- `kafkax`

## 规则

- 配置必须由调用方显式传入。
- 不得隐式读取生产密钥目录。
- `Config` 必须支持 `Validate` 和 `Sanitize`。
- `Validate` 必须对空配置名和负数 timeout 返回 `ErrorKindValidation`。
- `contracts/config.schema.json` 的 `name`、`timeout_ms` 和 `secret` 必须与 `Config.Name`、`Config.Timeout` 和 `Config.Secret` 保持映射一致。
- 脱敏后的配置可以安全用于日志、Evidence 和发布说明。

生成的库可以在文档中说明由调用方拥有的配置层执行显式加载，然后只接收生成后的 `Config`。

本模板不得依赖 `x.go`。

## Kafka L2 配置 contract

L2 Kafka adapter/factory 的配置 schema 是 `contracts/kafkax.config.schema.json`。它必须保持可序列化、可校验、可脱敏，并覆盖以下显式字段组：

- `brokers`：调用方显式传入的 broker 地址列表，至少 1 项。
- `client_id`：调用方显式传入的 client identity。
- `security`：只允许 secret reference（例如 `username_ref`、`password_ref`、TLS material ref），不得保存原始 secret 值。
- `producer`：acks、idempotent、delivery timeout、batch 和 compression 语义。
- `consumer`：group、offset reset、poll/session timeout 语义。
- `admin`：admin request timeout 和 topic creation policy。
- `retry`：attempt/backoff policy。
- `observability`：metrics namespace、trace headers，以及固定为 `false` 的 `log_message_value`。

该 contract 不授权库隐式读取生产 secret 目录；调用方可以在更外层从 `/home/k8s/secrets/env/*` 读取并解析，但进入 `kafkax` 的只能是已显式构造、可脱敏的 `Config`。
