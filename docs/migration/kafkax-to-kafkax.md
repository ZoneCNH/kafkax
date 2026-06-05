# kafkax 身份迁移指南

本指南记录旧 `kafkax` 叙事到当前 `kafkax` Standard Source / Go Reference Template / Generator / Harness / Evidence Runtime 身份的迁移边界。旧名只允许作为历史兼容语境出现，不得重新成为模块路径、包名、release Evidence 或下游 adoption 的主身份。

## 迁移规则

- 新标准源 URL 固定为 `https://github.com/ZoneCNH/kafkax`。
- 默认代表下游为 `kernel`，旧 `foundationx` 只作为迁移扫描项。
- 文档、模板和 generator 输出必须使用当前 module path、package name 和 release Evidence 术语。
- Kafka L2 adapter factory 的公共契约必须保持 driver-neutral；迁移不得引入 `x.go` 依赖或第三方 Kafka driver 类型到 public API。
- 没有 broker runtime Evidence 时，下游 adoption 与 broker-dependent Kafka gates 只能记录为 blocked/not_claimed，不得记录为 passed。

## 验证

迁移相关变更至少运行：

```bash
GOWORK=off make docs-check
GOWORK=off make standard-impact-check
GOWORK=off make contracts
```
