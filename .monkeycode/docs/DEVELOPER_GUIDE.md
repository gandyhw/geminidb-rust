# 开发指南

## 环境要求

### 必需工具

| 工具 | 版本要求 | 说明 |
|------|----------|------|
| Rust | 1.75+ | 使用 stable 频道 |
| Cargo | 最新 | Rust 包管理器 |
| Git | 任意版本 | 版本控制 |

### 可选工具

| 工具 | 版本要求 | 说明 |
|------|----------|------|
| rustfmt | 最新 | 代码格式化 |
| clippy | 最新 | 代码 lint |
| cargo-hack | 最新 | 依赖检查 |

## 快速开始

### 1. 克隆项目

```bash
git clone https://github.com/your-org/openGemini-rs.git
cd openGemini-rs
```

### 2. 构建项目

```bash
cargo build --release
```

### 3. 运行测试

```bash
cargo test
```

### 4. 运行示例

```bash
cargo run --example basic_write
```

## 项目结构说明

```
openGemini-rs/
├── Cargo.toml              # Workspace 根配置
├── crates/
│   └── engine/             # 核心存储引擎 crate
│       ├── Cargo.toml
│       └── src/
│           ├── lib.rs      # 模块入口
│           ├── config.rs  # 配置定义
│           ├── error.rs   # 错误定义
│           ├── wal/       # WAL 模块
│           ├── memtable/  # MemTable 模块
│           ├── tssp/      # TSSP 文件模块
│           ├── compaction/# 压缩合并模块
│           └── index/     # 索引模块
├── examples/               # 示例代码
├── tests/                  # 集成测试
└── benches/                # 性能基准测试
```

## 开发规范

### 命名规范

| 类型 | 规范 | 示例 |
|------|------|------|
| 模块 | 蛇形命名 | `memtable`, `wal` |
| 结构体 | 帕斯卡命名 | `MemTable`, `TsspFile` |
| 函数 | 蛇形命名 | `write_batch`, `flush` |
| 变量 | 蛇形命名 | `file_id`, `max_size` |
| 常量 | 全大写蛇形 | `MAX_MEMTABLE_SIZE` |
| 枚举变体 | 帕斯卡命名 | `FieldValue::Integer` |

### 代码风格

- 使用 `rustfmt` 自动格式化
- 行宽限制 100 字符
- 使用 4 空格缩进
- 函数不超过 100 行
- 结构体要有文档注释

### 提交规范

提交信息格式：

```
<type>(<scope>): <subject>

<body>

<footer>
```

Type 类型：
- `feat`: 新功能
- `fix`: 修复 bug
- `docs`: 文档变更
- `style`: 代码格式（不影响功能）
- `refactor`: 重构
- `test`: 测试
- `chore`: 构建/工具变更

示例：

```
feat(memtable): add auto flush when memory exceeds threshold

- add should_flush method
- add flush callback mechanism

Closes #123
```

## 常见任务

### 添加新压缩算法

1. 在 `compression.rs` 中实现 `Compressor` trait
2. 在 `CompAlgorithm` 枚举中添加新变体
3. 添加对应的编解码单元测试
4. 更新配置文件支持新算法

### 添加新索引类型

1. 在 `index/` 目录下创建新模块
2. 实现 `Index` trait
3. 在 `TsspReader` 中集成新索引
4. 添加集成测试

### 调试存储问题

```bash
# 启用 debug 日志
RUST_LOG=debug cargo run --example basic_write

# 查看 WAL 内容
cargo run --bin wal_inspect -- <wal_dir>

# 导出 TSSP 文件统计
cargo run --bin tssp_stats -- <tssp_dir>
```

## 构建与发布

### 开发构建

```bash
# Debug 构建
cargo build

# Release 构建
cargo build --release

# 仅构建 engine crate
cargo build -p openGemini-engine
```

### 运行测试

```bash
# 运行所有测试
cargo test

# 运行特定模块测试
cargo test -p openGemini-engine memtable

# 运行带日志的测试
RUST_LOG=debug cargo test
```

### 代码检查

```bash
# 格式化代码
cargo fmt

# 运行 clippy
cargo clippy -- -D warnings

# 检查依赖
cargo audit
```

### 发布

```bash
# 版本 bump（需要 cargo-release）
cargo release <version>

# 发布到 crates.io
cargo publish -p openGemini-engine
```