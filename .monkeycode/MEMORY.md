# 用户指令记忆

本文件记录了用户的指令、偏好和教导，用于在未来的交互中提供参考。

## 格式

### 用户指令条目
用户指令条目应遵循以下格式：

[用户指令摘要]
- Date: [YYYY-MM-DD]
- Context: [提及的场景或时间]
- Instructions:
  - [用户教导或指示的内容，逐行描述]

### 项目知识条目
Agent 在任务执行过程中发现的条目应遵循以下格式：

[项目知识摘要]
- Date: [YYYY-MM-DD]
- Context: Agent 在执行 [具体任务描述] 时发现
- Category: [代码结构|代码模式|代码生成|构建方法|测试方法|依赖关系|环境配置]
- Instructions:
  - [具体的知识点，逐行描述]

## 去重策略
- 添加新条目前，检查是否存在相似或相同的指令
- 若发现重复，跳过新条目或与已有条目合并
- 合并时，更新上下文或日期信息
- 这有助于避免冗余条目，保持记忆文件整洁

## 条目

[项目知识摘要]
- Date: 2026-04-25
- Context: Agent 在增强 geminidb-rust 项目测试覆盖率时发现
- Category: 测试方法
- Instructions:
  - 项目使用 Rust 的标准测试框架 `#[test]`
  - 测试文件位于 `crates/engine/tests/` 和各模块的 `mod.rs` 中的 `#[cfg(test)]` 模块
  - 运行所有测试: `cargo test`
  - 运行特定模块测试: `cargo test --lib <module_name>` 或 `cargo test --test <test_file_name>`
  - 基准测试位于 `crates/engine/benches/` 目录

[项目知识摘要]
- Date: 2026-04-25
- Context: Agent 在修复编译错误和警告时发现
- Category: 代码质量
- Instructions:
  - 项目使用 `cargo clippy` 进行代码质量检查
  - 应用 clippy 修复: `cargo clippy --fix --lib -p openGemini-engine --allow-dirty`
  - 修复未使用的变量警告时，使用 `_` 前缀或移除 `mut`

[项目知识摘要]
- Date: 2026-04-25
- Context: Agent 在推送代码到 GitHub 时发现
- Category: 环境配置
- Instructions:
  - GitHub token 配置在 remote URL 中 (已隐藏)
  - 分支命名格式: `YYMMDD-feat-xxxxx-xxxx-xxxx` (如 `260425-feat-enhance-compaction-shard`)
  - 使用 `git push origin <branch> -o merge_request.create` 可自动创建 MR

[项目知识摘要]
- Date: 2026-04-25
- Context: Agent 在更新 README 文档时发现
- Category: 项目结构
- Instructions:
  - 项目名: geminidb-rust (openGemini Rust 版本)
  - Cargo 包名: `openGemini-engine`
  - 主要模块: engine, http, influxql, line_protocol, index, memtable, tssp, wal, compaction, raft, shard, schema, metaclient, snapshot, tiered_storage, bloom
  - 测试框架: Rust 标准测试框架 + tempfile 用于测试目录管理
