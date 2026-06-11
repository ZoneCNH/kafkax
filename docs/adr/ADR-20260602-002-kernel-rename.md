# ADR-20260602-002: 默认下游名迁移为 kernel

## 状态

Accepted

## 决策

默认代表下游从历史示例 `foundationx` 迁移为 `kernel`。`kernel` 是 generator、integration smoke、downstream matrix 和 release Evidence 的默认下游锚点；`foundationx` 仅保留为迁移兼容扫描项。

## 约束

- 新文档和 gate 不得把 `foundationx` 描述为默认下游。
- `kernel`、L1、L2 和私有 L3 消费方的同步状态必须通过 downstream matrix、standard-impact 和 release Evidence 表达。
- Kafka L2 adapter factory 未具备 broker Evidence 前，不得把 downstream adoption 记录为完成。

## 后果

Generator、docs-check、integration 和 release manifest 必须以 `kernel` 作为默认代表下游，避免旧命名重新进入交付路径。
