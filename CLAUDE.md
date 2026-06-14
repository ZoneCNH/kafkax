# CLAUDE.md

> 完整的贡献指南、测试规范、提交规范和 Agent 协作约定见 [AGENTS.md](AGENTS.md)。

## 语言规则（全局强制）

1. **回答语言**：所有对话回复默认使用中文，除非用户明确要求使用其他语言。
2. **文档语言**：所有仓库文档（README、docs/、.agent/、contracts/*.md、变更日志、发布说明、PR 描述、Issue、贡献指南）默认使用中文叙述。
3. **代码注释**：Go 源码中的注释（包括函数文档注释、行内注释、TODO/FIXME）默认使用中文。导出符号的 godoc 注释若面向外部消费者可保留英文，内部代码一律中文。
4. **保留原文的例外**：代码标识符、命令、路径、包名、Go module 路径、外部专有名词（Agent、Harness、manifest、schema、CI、PR、Issue）、协议固定短语和 git 提交标题保留项目惯用原文。
5. **提交信息**：提交正文（body）和 trailer 使用中文；提交标题（subject line）保留英文以兼容工具链。

## 编辑前基线确认（2026-06-14 复盘）

> 基于 ZoneCNH/ZoneCNH#340 来自 kafkax README 修复 session 的复盘规则。本仓库是 Go 源码仓库，规则在此落地。

- **编辑前先 `git log --oneline -5`，然后 `Read` 确认目标文件当前内容**——禁止假设文件仍是自己记忆中的状态。
- **对任何 README 或文档中的代码事实声称，用 `grep` 或 `head` 对照源码验证后再提交。** 典型场景：声称"Config 有 X 字段"→ `grep "type ProducerConfig" pkg/kafkax/config.go`；声称"handler panic 被捕获"→ `grep "recover" pkg/kafkax/kafkago/consumer.go`；声称"文档有内容"→ `head docs/api.md`。
- **先列验证清单，再列变更清单**——先确定需要 grep 什么，验证完再按变更清单编辑。
- **校验命令不计入成本控制**——`grep`/`head`/`git log` 调用成本可忽略，但不执行导致的返工成本极高（本 session：跳过数次 grep → $49.52 无效编辑）。
