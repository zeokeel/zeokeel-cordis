# Cordis（Rust 内核）

**纯 Rust 的可插拔内核**：实现 Cordis 的「时空可组合」上下文范式——Context 树提供作用域，
插件运行在 fiber 上，所有副作用可按逆序完整撤销，依赖声明驱动插件的激活与停用。

> 这是**内核**，不是框架：只提供组合原语与生命周期语义。模块加载、热更新、权限、沙箱、
> 进程/WASM 隔离都属于使用它的宿主（embedder）。

- Rust **1.85+**（edition 2021）· MIT · 零 `unsafe`（`#![forbid(unsafe_code)]`）
- 通用、无产品逻辑、静态链接、无 ABI/FFI/动态加载
- **v0.3.x 功能冻结**：不再新增语义能力，只接受 bugfix、安全性修复与文档修正

---

## 设计思想（摘要）

Cordis 把「动态组合」拆成两个正交维度：

| 维度 | 含义 | 内核中的落地 |
| --- | --- | --- |
| **时间可组合性**（revertible effects）| 组件的每个副作用都有逆操作，卸载时按逆序执行，系统回到组件加载前 | 每 fiber 一条有序 effect 列表；`Disposer`/`AsyncDisposer` 单次幂等；激活失败回滚；cancel-safe 卸载 |
| **空间可组合性**（reactive coeffects）| 组件声明自己依赖哪些上下文，上下文变化驱动组件的激活/停用/重算 | `Plugin::inject` 依赖门控、`Pending` 状态、服务变化触发刷新、provider epoch |
| **统一 Context**（context paradigm）| effect 与 coeffect 都经过同一个上下文，作用域隔离使不同组件互不干扰 | `Context` 树、`extend`/`isolate`/`isolate_labeled`、按作用域隔离的服务与事件 |

组件（插件 + fiber）是动态组合的单位：激活是事务（失败全回滚），生命周期操作互斥，
子插件属于父 fiber（级联卸载）。详细推导与 12 条设计不变量见 [`docs/DESIGN.md`](docs/DESIGN.md)。

### 论文来源

> **A Programming Paradigm for Spatiotemporal Composability**
> Yifan Shi（北京大学 / DeepSeek-AI）、Wei Zhang（北京大学）、Tianyi Cui（DeepSeek-AI）
> arXiv:2608.25512 [cs.PL]，2026-08-26 — <https://arxiv.org/abs/2608.25512>

本项目是论文思想的独立纯 Rust 实现（与 Cordis 官方 TypeScript 实现语义对齐，差异见
[`spec/CORDIS-CORE.md`](spec/CORDIS-CORE.md) §5）。

```bibtex
@misc{shi2026cordis,
  title         = {A Programming Paradigm for Spatiotemporal Composability},
  author        = {Shi, Yifan and Zhang, Wei and Cui, Tianyi},
  year          = {2026},
  eprint        = {2608.25512},
  archivePrefix = {arXiv},
  primaryClass  = {cs.PL},
  url           = {https://arxiv.org/abs/2608.25512},
}
```

---

## 快速开始

```toml
[dependencies]
cordis = { git = "https://github.com/zeokeel/Cordis", tag = "v0.3.0" }
serde_json = "1"   # 插件配置是 serde_json::Value
```

最小插件（完整可运行版本：[`examples/hello.rs`](examples/hello.rs)）：

```rust
use std::sync::Arc;
use cordis::{cleanup, Context, Plugin, Result};
use serde_json::Value;

struct Greeter;

impl Plugin for Greeter {
    fn name(&self) -> &'static str { "greeter" }

    fn apply(&self, ctx: Context, config: Value) -> Result<()> {
        let who = config["name"].as_str().unwrap_or("world").to_string();
        println!("hello, {who}");
        // 清理逻辑归 fiber 所有：卸载时逆序、恰好一次执行。
        ctx.effect(move || vec![cleanup(move || println!("bye, {who}"))])?;
        Ok(())
    }
}

fn main() -> Result<()> {
    let root = Context::root();
    let fiber = root.plugin(Arc::new(Greeter), serde_json::json!({ "name": "Cordis" }))?;
    fiber.dispose()?;
    root.dispose_all()?; // 宿主退出时的标准关闭路径
    Ok(())
}
```

运行示例：`cargo run --example hello`（另有 `services`、`events`、`async_plugin`）。

### 核心 API 一览

| 概念 | 入口 |
| --- | --- |
| Context / 作用域 | `Context::root`、`extend`、`isolate`、`isolate_labeled`、`intercept` |
| 插件 | `Plugin::{name, inject, config_schema, apply}`、`ctx.plugin` / `plugin_typed` |
| 异步插件 | `AsyncPlugin::apply_async`、`ctx.plugin_async`、`*_async` 生命周期 API |
| 服务 | `ServiceKey<T>`、`ctx.provide/get/require`、`*_any`（trait object 服务）|
| Effect | `ctx.effect`、`effect_async`、`effect_future`、`effect_stream` |
| 事件 | `ctx.on/once/on_async`、`emit/bail`、`parallel_async/serial_async`、`waterfall` |
| fiber 生命周期 | `Fiber::{state, restart, update, replace_plugin, dispose, dispose_async}` |
| 注册表 | `Registry::{size, fibers, replace, dispose_all, is_idle, when_idle}` |
| 关闭 | `Context::dispose_all()`（推荐）、`when_idle()` |

类型与签名细节以 rustdoc 为准：`cargo doc --open`。

---

## 内核 / 宿主边界

**内核负责**：context/作用域、服务注入、fiber 生命周期、effect 所有权与回滚、事件、
配置 schema、日志、静态插件注册表。

**宿主负责**：模块加载器与热更新、定时器/事件循环、权限与审计、资源配额策略、
线程/fd 计量、进程隔离（L3）或 WASM 沙箱（L4）、UI/产品契约。

可选 feature `supervision` 提供 L2 监督原语（能力自省、per-fiber 配额、心跳与 watchdog
观测），**默认关闭**——核心内核不含任何监督策略：

```toml
cordis = { git = "https://github.com/zeokeel/Cordis", tag = "v0.3.0", features = ["supervision"] }
```

详见 [`docs/KERNEL-ISOLATION.md`](docs/KERNEL-ISOLATION.md)。

---

## 文档与规范

| 文档 | 内容 |
| --- | --- |
| [`docs/DESIGN.md`](docs/DESIGN.md) | 设计思想、论文概念映射、12 条不变量、Rust 设计决策 |
| [`docs/GUIDE.md`](docs/GUIDE.md) | 使用指南：服务、依赖注入、effect、事件、异步、隔离、热替换、关闭、测试 |
| [`docs/KERNEL-ISOLATION.md`](docs/KERNEL-ISOLATION.md) | 故障边界、L1–L4 隔离模型、编译期强制、L2 API |
| [`spec/CORDIS-CORE.md`](spec/CORDIS-CORE.md) | 规范性语义与编号用例（CORE-001…CORE-051，`tests/` 一一对应）|
| [`docs/CORDIS-RUST.md`](docs/CORDIS-RUST.md) | Rust 移植笔记（与 TS 实现的差异、panic 策略）|
| [`docs/dev/HARDENING-LOG.md`](docs/dev/HARDENING-LOG.md) | 开发记录：缺陷清单、逐轮修复与对抗测试日志（非用户文档）|

---

## 质量与严格构建

- **311 个测试**（默认构建；启用 `--features supervision` 时 **317 个**）：
  `cargo test` / `cargo test --features supervision`
- `cargo fmt --check && cargo clippy --all-targets -- -D warnings`（两种 feature 组合都跑）
- `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps`
- 编译器强制 `panic = "unwind"`（`panic=abort` 直接编译失败，CI 反证）；
  release 开启 `overflow-checks`、`codegen-units=1`、thin LTO
- CI：`rust` 作业（fmt/clippy/test/doc/release build/panic=abort 反证）+ `cargo audit`（RustSec）

---

## 已知限制（精简版）

- **未 dispose 的 fiber 会保留其图**：退出时调用 `dispose_all()`（dispose+drop 路径实测 0 泄漏）。
- **同步生命周期是阻塞边界**：异步宿主请用 `*_async` API 或安装 `BlockingExecutor`。
- **取消卸载最多丢失在飞的那一个 cleanup future**（Rust Future drop 语义）；其余会重试。
- **无 ABI / 动态加载**：插件是静态链接的 `Arc<dyn Plugin>`；动态插件需宿主进程/WASM 隔离。
- **进程内不可遏制**：`panic=abort` 构建已被编译期拒绝；栈溢出、OOM、依赖中的 UB、死循环、
  外部 kill 只能靠 L3/L4 隔离（见隔离文档）。
- **事件作用域过滤 = isolate 映射相等**（非子树）；无类型化事件表。
- **无抢占**：watchdog/取消是协作式的；内核不会强杀插件线程。

完整清单与实测数据见 [`docs/KERNEL-ISOLATION.md`](docs/KERNEL-ISOLATION.md)。

---

## 状态与许可证

- 当前版本 **v0.3.0**（CORE-001…CORE-051）；v0.3.x 为功能冻结线。
- MIT，见 [LICENSE](LICENSE)；论文版权归原作者。

维护仓库：<https://github.com/zeokeel/Cordis>（提交作者 `zeokeel-dev`）。
