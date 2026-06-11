# Metrics Contract

标准指标用于描述 `pkg/kafkax` 暴露给调用方的最小可观测面。实现可以接入任意 metrics 后端，但指标名、类型和标签语义必须保持兼容。

| 指标 | 类型 | 标签 | 说明 |
| --- | --- | --- | --- |
| `client_created_total` | counter | `name` | 成功创建 client 的次数。 |
| `client_closed_total` | counter | `name` | 成功关闭 client 的次数；重复关闭不重复计数。 |
| `client_errors_total` | counter | `op`, `kind` | client 生命周期错误次数，`kind` 必须来自 error contract。 |
| `client_health_status` | gauge | `name`, `status` | 健康状态数值，healthy 为 `1`，其他状态为 `0`。 |
| `client_health_latency_ms` | histogram | `name`, `status` | 单次健康检查耗时，单位为毫秒。 |
| `client_requests_total` | counter | `operation`, `status` | 调用方扩展请求计数。 |
| `client_request_duration_seconds` | histogram | `operation`, `status` | 调用方扩展请求耗时，单位为秒。 |
| `client_retries_total` | counter | `operation`, `kind` | 调用方扩展重试计数。 |
| `client_inflight` | gauge | `operation` | 调用方扩展并发中的请求数。 |
| `producer_messages_total` | counter | `topic`, `status` | producer 投递消息计数，`status` 区分成功、失败和重试后成功。 |
| `producer_errors_total` | counter | `topic`, `kind` | producer 错误计数，`kind` 必须来自 error contract。 |
| `producer_latency_seconds` | histogram | `topic`, `status` | producer 单次投递耗时，单位为秒。 |
| `consumer_messages_total` | counter | `topic`, `group`, `status` | consumer 接收消息计数。 |
| `consumer_errors_total` | counter | `topic`, `group`, `kind` | consumer 错误计数，`kind` 必须来自 error contract。 |
| `consumer_lag` | gauge | `topic`, `group`, `partition` | consumer 分区 lag。 |
| `consumer_commits_total` | counter | `topic`, `group`, `status` | consumer offset commit 计数。 |
| `admin_operations_total` | counter | `operation`, `topic`, `status` | admin topic 操作计数。 |
| `admin_errors_total` | counter | `operation`, `topic`, `kind` | admin 错误计数，`kind` 必须来自 error contract。 |
