# L2 Kafka Adapter Standard

`kafkax` 的 L2 Kafka adapter/factory 目标是提供受治理的基础设施适配层，而不是业务消息模型或某个 Kafka driver 的公开封装。

## Contract-first boundaries

- Public API must be driver-neutral and must not expose `kgo.*`, `kafka-go.*`, `confluent.*`, or other third-party Kafka types.
- Public model names are `Producer`, `Consumer`, `Admin`, `TopicSpec`, `Message`, `Header`, `Offset`, `Error`, `Health`, and `Config`.
- `contracts/l2-kafka-adapter.schema.json` is the L2 profile contract.
- `contracts/kafkax.config.schema.json`, `contracts/kafkax.message.schema.json`, and `contracts/kafkax.metrics.schema.json` are the minimum Kafka-specific data contracts.
- `contracts/error.schema.json` and `contracts/health.schema.json` remain the shared typed error and health contracts.

## Gate expectations

| Gate | Current status rule | Evidence requirement |
| --- | --- | --- |
| `kafka-contract` | required | JSON schema validity plus `GOWORK=off make contracts`. |
| `kafka-integration` | blocked until driver and broker fixture exist | Real broker run output; unavailable broker must be reported blocked, not passed. |
| `kafka-fault-injection` | blocked until broker fixture exists | Auth, timeout, rebalance, broker unavailable, and retry evidence. |
| `kafka-metrics-golden` | blocked until metrics backend/fixture exists | Golden metrics covering producer, consumer, admin, connection, rebalance, lag, and DLQ. |
| `kafka-admin-golden` | blocked until admin implementation exists | Topic create/describe/delete or explicit unsupported-operation evidence. |

## Non-goals for this contract slice

This document does not select a Kafka driver, add broker runtime, add Makefile kafka targets, or claim downstream adoption. Adoption remains `not_claimed` until implementation, broker evidence, and release evidence exist.
