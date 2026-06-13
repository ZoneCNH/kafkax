# 可观测性模板

## 占位符

- `kafkax`
- `kafkax`

## 指标

使用 `contracts/metrics.md` 中的 metrics contract。模板内置的最小指标包括：

- `client_created_total`
- `client_closed_total`
- `client_errors_total`
- `client_health_status`
- `client_health_latency_ms`
- `client_requests_total`
- `client_request_duration_seconds`
- `client_retries_total`
- `client_inflight`

生命周期指标由 `New`、`Close` 和 `HealthCheck` 直接记录；请求、耗时、重试和 inflight 指标作为生成具体库后的扩展 contract。

Kafka L2 adapter/factory 的扩展指标由 `docs/metrics.md` 和 `contracts/kafkax.metrics.schema.json` 约束，至少覆盖 producer、consumer、admin、connection、rebalance、lag 和 DLQ。`kafka-metrics-golden` 必须用真实 broker fixture 证明 label allowlist 与脱敏；未配置 fixture 时只能记录 blocked/gap，不能记录 passed。

## 健康检查

持有资源的客户端必须暴露 `HealthCheck(context.Context)`。返回值必须使用 `contracts/health.schema.json` 中的字段名：

- `name`
- `status`
- `message`
- `checked_at`
- `latency_ms`
- `metadata`

`status` 只能是 `healthy`、`degraded` 或 `unhealthy`。未初始化、已关闭、`nil` context、canceled context 都必须返回 `unhealthy`。已初始化且未关闭的 client 如果本次检查的 context deadline 预算短于 `Config.Timeout`，必须返回 `degraded`，并继续记录 `client_health_status` 和 `client_health_latency_ms`，其中 `status` label 为 `degraded`。

## 日志

只能记录脱敏配置。不得记录原始凭据或生产连接材料。

Kafka 日志默认不得记录 message value、raw key、生产连接串、SASL/TLS material 或业务私密字段；trace headers 可以传播调用方拥有的上下文，但 `kafkax` 不定义业务 trace schema。

本模板不得依赖 `x.go`。
