# Kafka Metrics Contract

`kafkax` keeps the existing lifecycle metrics in `contracts/metrics.md` and adds the L2 Kafka metric catalog shape in `contracts/kafkax.metrics.schema.json`.

## Required Kafka metric families

| Component | Example metric | Type | Required labels | Notes |
| --- | --- | --- | --- | --- |
| producer | `kafkax_producer_records_total` | counter | `client_id`, `topic`, `result` | Delivery attempts and outcomes. |
| consumer | `kafkax_consumer_records_total` | counter | `client_id`, `group_id`, `topic`, `result` | Poll/handler outcomes without message value labels. |
| admin | `kafkax_admin_operations_total` | counter | `client_id`, `operation`, `result` | Topic and metadata operations. |
| connection | `kafkax_connection_state` | gauge | `client_id`, `broker`, `state` | Broker connectivity state. |
| rebalance | `kafkax_consumer_rebalances_total` | counter | `client_id`, `group_id`, `result` | Consumer group rebalance outcomes. |
| lag | `kafkax_consumer_lag` | gauge | `client_id`, `group_id`, `topic`, `partition` | Consumer lag by partition. |
| dlq | `kafkax_dlq_records_total` | counter | `client_id`, `topic`, `result` | Dead-letter publish outcomes. |

Histograms may add `_duration_seconds` variants for producer send, consumer poll, admin operation, and health check latency. Backends may translate these names to their native exposition format, but the semantic labels must stay stable.

## Safety rules

- Logs and metric labels must not include message values, raw keys, credentials, production connection strings, or business identifiers by default.
- `observability.log_message_value` is fixed to `false` in `contracts/kafkax.config.schema.json` until a separate security review changes the contract.
- Tracing may propagate caller-owned headers, but `kafkax` does not define a business trace schema.
- Broker-dependent metrics golden evidence requires a real driver and broker fixture. When no fixture is configured the gate must report `status=gap`; when a fixture is configured the gate must prove label allowlist behavior and credential/message-value redaction.
