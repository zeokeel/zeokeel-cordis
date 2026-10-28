# Cordis（Rust 内核）

**纯 Rust 的可插拔内核**：实现 Cordis 的「时空可组合」上下文范式——Context 树提供作用域，
插件运行在 fiber 上，所有副作用可按逆序完整撤销，依赖声明驱动插件的激活与停用。

> 这是**内核**，不是框架：只提供组合原语与生命周期语义。模块加载、热更新、权限、沙箱、
> 进程/WASM 隔离都属于使用它的宿主（embedder）。

[![CI](https://github.com/zeokeel/zeokeel-cordis/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/zeokeel/zeokeel-cordis/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/zeokeel-cordis.svg)](https://crates.io/crates/zeokeel-cordis)
[![docs.rs](https://img.shields.io/docsrs/zeokeel-cordis.svg)](https://docs.rs/zeokeel-cordis)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Rust 1.85+](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](#快速开始)
[![cargo audit](https://img.shields.io/badge/security-cargo--audit-brightgreen.svg)](https://github.com/zeokeel/zeokeel-cordis/actions/workflows/ci.yml)

- Rust **1.85+**（edition 2021）· MIT · 零 `unsafe`（`#![forbid(unsafe_code)]`）
- 通用、无产品逻辑、静态链接、无 ABI/FFI/动态加载
- **v0.4.x 语义冻结**：不再新增语义能力，只接受 bugfix、安全性修复与文档修正

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

```bash
cargo add zeokeel-cordis
```

```toml
[dependencies]
zeokeel-cordis = "0.4.3"
serde_json = "1"   # 插件配置是 serde_json::Value
```

> crates.io 包名 `zeokeel-cordis` 与库目标名 `zeokeel_cordis` 一致：代码里统一
> `use zeokeel_cordis::...`。
> 需要跟仓库最新未发布代码时，可换成 git 依赖：
> `zeokeel-cordis = { git = "https://github.com/zeokeel/zeokeel-cordis", tag = "v0.4.3" }`。

最小插件（完整可运行版本：[`examples/hello.rs`](examples/hello.rs)）：

```rust
use std::sync::Arc;
use zeokeel_cordis::{cleanup, Context, Plugin, Result};
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
| 日志 | `ctx.logger()`、`ctx.log_exporter(TracingExporter)`（默认无导出器，见下）|
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
zeokeel-cordis = { version = "0.4.3", features = ["supervision"] }
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
| [`docs/dev/HARDENING-LOG.md`](docs/dev/HARDENING-LOG.md) | 历史开发记录（v0.3 时代）：缺陷清单与逐轮加固日志；历史快照，非当前行为（取消语义见 0.4.2 A-9）|

---

## 质量与严格构建

- **全量测试**：`cargo test`（默认构建）与 `cargo test --features supervision` 两套都必须全绿；
  0.4.3 实测：默认构建 **404 passed / 0 failed**（含 7 个 doc-tests）、
  `--features supervision` **415 passed / 0 failed**（0.4.2 为 365 / 371，0.4.1 基线
  311 / 317）。本版扩展了
  `tests/regression_core.rs` / `tests/regression_semantics.rs` 与 R3-A1/R3-A4/C1–C5/S1–S3/R1
  审计回归用例（深链迭代卸载、动态卸载记账、when_idle 注册帧、配额活资源等）
- **0.4.3 加固**：R3-A4 动态卸载父 fiber 记账无界泄漏（60k churn 60001 effects → 10k churn
  峰值 1，RSS 基本持平（实测 +≈1 MB））、R3-A1 深依赖链迭代卸载（栈 O(1)；2 MiB 线程
  depth 800 在 **0.4.3 debug+release 均通过**，0.4.2 对照 debug 约 480 层即 abort、release
  约 2200 层才 abort）、R3-A3 注册帧内
  `when_idle` 自锁修复、S1 supervision 配额按活资源记账、C2 Plugin 元数据 panic 遏制、C3 嵌套
  effect 元数据 ghost、S2 logger 非法 level clamp、S3 `AccessorWriteRejected` 等；0.4.2 的
  21 项修复见 [CHANGELOG](CHANGELOG.md)
- `cargo fmt --check`、两种 feature 配置的 `cargo clippy --all-targets -- -D warnings`、
  `cargo test`、`RUSTDOCFLAGS="-D warnings" cargo doc --no-deps`（默认与 `--all-features`）
- 编译器强制 `panic = "unwind"`（`panic=abort` 直接编译失败，CI 反证）；
  release 开启 `overflow-checks`、`codegen-units=1`、thin LTO
- **CI 实际行为**（`.github/workflows/ci.yml`）：
  - `rust` 作业（stable）：fmt、clippy ×2、test ×2、doc ×2、examples 构建 + 冒烟运行、
    真实 `cargo package` verify、release 构建、`panic=abort` 反证；
  - `msrv` 作业（**Rust 1.85.0**）：test ×2 与 `doc --all-features`（**不跑 clippy**——
    lint 集合随 stable 漂移，MSRV 只需 build/test/doc 通过）；
  - `cargo audit`（RustSec）供应链漏洞门禁；
  - build/test/clippy/doc/package/publish 类 cargo 命令统一带 **`--locked`**。
- **可复现构建**：0.4.3 起仓库提交 `Cargo.lock`（已从 `.gitignore` 移除，随 0.4.3 提交），
  CI 与发布流水线统一 `--locked`；库下游使用自己的 lockfile，不受影响。
- **发布**（`.github/workflows/release.yml`）：tag 驱动；校验 **tag 版本 == `Cargo.toml`
  version** 后重跑 fmt / clippy / test / doc / examples 门禁，再 `cargo package` →
  `cargo publish --dry-run` → `cargo publish`；发布成功后把 `.crate` 的 SHA-256 附到对应
  GitHub Release（该步失败不影响 crates.io 发布结果）。本地等价命令与完整流程见
  [CONTRIBUTING.md](CONTRIBUTING.md)。

---

## 已知限制（精简版）

- **未 dispose 的 fiber 会保留其图**：退出时调用 `dispose_all()`（dispose+drop 路径实测 0 泄漏）。
- **默认不安装日志导出器**：根上下文只有 `Logger` 服务和 1000 条 ring buffer；不调用
  `ctx.log_exporter(Arc::new(TracingExporter))?` 时 `ctx.logger().info(..)` 这类记录不会
  打印到终端、也不会进入 `tracing`（内核自身的 `tracing` 诊断日志是另一条独立通道）。
- **同步生命周期是阻塞边界**：异步宿主请用 `*_async` API 或安装 `BlockingExecutor`。
- **取消卸载的语义（v0.4.2，按粒度区分）**：取消 `refresh_async`/`dispose_async`（future
  被 drop）会把 unload **做完**——同步 cleanup 立即执行、异步 cleanup 转入 deferred 由下一次
  dispose 收尾、fiber 落到 `Pending`/`Disposed` 并通知 idle（不再卡 `Unloading`/假 `Active`）；
  在飞的那一个 async cleanup future 不可恢复。`AsyncDisposer` 句柄层面的取消则恢复未启动
  cleanup、由下一次 dispose 重试。
- **无 ABI / 动态加载**：插件是静态链接的 `Arc<dyn Plugin>`；动态插件需宿主进程/WASM 隔离。
- **进程内不可遏制**：`panic=abort` 构建已被编译期拒绝；栈溢出、OOM、依赖中的 UB、死循环、
  外部 kill 只能靠 L3/L4 隔离（见隔离文档）。
- **`when_idle()` 只排除调用线程自身的注册帧（0.4.3）**：`internal/plugin` create hook 内
  调用不再自锁，外部 waiter 仍等待整个注册/激活窗口；`is_idle()` 仍严格。跨线程环形注册
  依赖（互相等待对方注册窗口）仍可能互等，宿主需避免。
- **supervision 配额按活资源计数（0.4.3）**：effects/listeners/child fibers 释放（含 `once`
  自动移除、并发/卸载路径）后立即归还配额槽，超限判定基于活资源而非历史累计。
- **深依赖链卸载已支持（0.4.3）**：迭代级联、栈 O(1)。2 MiB 线程 depth 800 在 0.4.3
  **debug 与 release 均通过**；0.4.2 对照为 debug 约 480 层即 abort、release 约 2200 层才
  abort。`MAX_PLUGIN_DEPTH = 256` 仍只限制插件**加载**深度。
- **事件作用域过滤 = isolate 映射相等**（非子树）；无类型化事件表。
- **无抢占**：watchdog/取消是协作式的；内核不会强杀插件线程。

完整清单与实测数据见 [`docs/KERNEL-ISOLATION.md`](docs/KERNEL-ISOLATION.md)。

## 参与项目

| 文档 | 内容 |
| --- | --- |
| [CONTRIBUTING.md](CONTRIBUTING.md) | 开发环境、测试命令、提交规范、PR 清单、发布流程 |
| [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) | 社区行为准则（Contributor Covenant v2.1）|
| [SECURITY.md](SECURITY.md) | 漏洞私有报告流程、威胁模型与隔离边界 |
| [CHANGELOG.md](CHANGELOG.md) | 版本变更记录（Keep a Changelog）|
| [`.github/ISSUE_TEMPLATE/`](.github/ISSUE_TEMPLATE) | Bug / 功能请求模板 |
| [`.github/pull_request_template.md`](.github/pull_request_template.md) | PR 检查清单 |

**项目标签**：Cargo keywords —
`cordis`、`plugin`、`plugin-system`、`dependency-injection`、`kernel`；
GitHub topics —
`rust`、`cordis`、`plugin-system`、`dependency-injection`、`kernel`、`composability`。

> 安全漏洞请**不要**公开报告，按 [SECURITY.md](SECURITY.md) 使用 GitHub Security Advisories。

---

## 状态与许可证

- 当前版本 **v0.4.3**（CORE-001…CORE-051；0.4.2/0.4.3 均为修复/加固补丁，未新增 CORE
  用例）；v0.4.x 为语义冻结线。
- MIT，见 [LICENSE](LICENSE)；论文版权归原作者。

维护仓库：<https://github.com/zeokeel/zeokeel-cordis>（提交作者 `zeokeel-dev`）。
