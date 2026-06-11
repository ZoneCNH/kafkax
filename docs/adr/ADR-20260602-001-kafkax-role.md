# ADR-20260602-001: kafkax 统一仓库角色

## 状态

Accepted

## 决策

`github.com/ZoneCNH/kafkax` 同时承担 Standard Source、Go Reference Template、Generator、Harness 和 Evidence Runtime 五类职责。标准文本、模板实现、generator、gate 和 release Evidence 必须在同一套可验证工件中演进，避免标准与实现分叉。

## 约束

- 公共标准和模板不得依赖 `x.go`。
- L2 基础库只能暴露本仓库定义的 driver-neutral public API。
- 任何 release claim 必须由 docs-check、contracts、standard-impact 和 release Evidence 支撑。

## 后果

仓库变更如果影响公共 API、config、error、health、metrics 或 release manifest schema，必须同步更新标准文档、contract schema 和 gate Evidence。
