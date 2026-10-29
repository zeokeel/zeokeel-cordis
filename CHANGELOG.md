# Changelog

本文件格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，
版本号遵循 [Semantic Versioning](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### Fixed

- **XT-01：`FiberData::drop` 不再用默认 thread-park `block_on` 排空 async cleanup**。此前在
  tokio `current_thread` 等单线程 runtime 上，取消 `dispose_async()` 后 drop 最后一个 `Fiber`
  句柄会死等 reactor 上的 deferred cleanup（永久挂起）。现在 Drop 只执行同步 cleanup；只有在
  embedder 通过 `exec::set_blocking_executor` 显式安装执行器时才排空异步 cleanup，否则跳过并记录
  `tracing::error!`（资源可能泄漏，但不会挂死线程）。回归：
  `regression_core::xt01_dropping_a_fiber_with_deferred_reactor_cleanups_does_not_hang`。
- **XT-03/XT-08：`on_lifecycle` 事件改为锁内迁移快照 + 每 fiber 单调 `seq`**。此前
  `LifecycleEvent.to` 在 hook 执行时现读 `fiber.state()`，并发/重入迁移下会出现
  `from == to`、`Pending -> Disposed` 之类的不自洽事件。现在 `transition()` 在状态锁内
  捕获 `(from, to, seq)`，`LifecycleEvent` 新增 `seq: u64`；并发投递仍可能交错，按
  `(fiber_id, seq)` 排序即可还原迁移链。回归：
  `regression_core::xt03_lifecycle_event_carries_the_transition_snapshot` /
  `xt03_lifecycle_events_sorted_by_seq_form_a_valid_chain`。
- **XT-09：`LogLevel::from_u8` 对 >3 的值 clamp 到 `Error`**（此前映射到最宽的
  `Debug`，与配置路径 `{"level":99}` 的 fail-closed clamp 相反）。回归：
  `logger::xt09_from_u8_clamps_unknown_levels_to_error`。

### Docs

- `README.md` / `docs/CORDIS-RUST.md` 记录 Fiber Drop 的 XT-01 语义与 trade-off。
- `spec/CORDIS-CORE.md` 明确 `Registry::replace`/`replace_async` 的 activation 失败语义（XT-05）：
  `Ok` 不代表 `Active`，失败被 containment 到 fiber（`Failed`），旧实现/服务不会自动恢复；
  只有 lifecycle 拒绝（`LifecycleBusy`）或 validation 错误才走完整回滚。
- `spec/CORDIS-CORE.md` / `docs/CORDIS-RUST.md` 记录 `on_async*` 绕过 `internal/listener`
  hook、async lane 无 `once`/`global` 的设计边界（XT-06）。
- `spec/CORDIS-CORE.md` / `docs/GUIDE.md` 记录 `ctx.set` 每次写留一个逆 effect 的线性成本
  （XT-07：高频写入建议原地修改服务，`set` 留给配置式变更）。

## [0.5.0] - 2026-10-29

**内核 API 收敛 + 新扩展点 + 性能闭环（minor）**：本版按 SemVer 的 `0.y` 规则做了一次
**破坏性公共 API 收敛**（内部类型退出公共契约、计时 API 改为毫秒 + 注入时钟），同时新增
可注入时钟、生命周期广播、结构化插件错误三个通用扩展点，并用完整反向依赖索引消除 0.4.x
的 fanout/级联性能回归。门禁：默认 `cargo test` **446 passed / 0 failed / 0 ignored**、
`--features supervision` **461 passed / 0 failed / 0 ignored**（另含 Miri、跨平台、
coverage、deny、actionlint、SBOM 与属性测试，见 `Docs`/`Verification`）。

### Performance

- **反向依赖索引（S1–S4）完整实现**：内核维护「provider → 依赖方」的反向索引，并在
  refresh/卸载/级联路径复用它，消除全表扫描。索引是**纯内部实现**——不新增、不改变任何
  公共 API 或语义；**显式 `refresh()` 仍做全量重新评估（含 availability gate 翻转）**，
  内部 `refresh_dependents` 走反向索引快路径，语义等价（E 确认无回归）。实测（相对 0.4.x）：
  - 5000-fiber fanout：`create + dispose` **−54%**、`dispose` **−74%**；
  - 1500 层深依赖链级联卸载：cascade **−97%**、`dispose` **−90%**；
  - 1000-fiber fanout：`create + dispose` **−50%**；
  - 0.4.3 记录的 5000-fiber `create`/`dispose_all` 慢 25–35% 的回归由此消除。

### Changed

- **【破坏性】内部类型退出公共契约**：`fiber::FiberRef`、`fiber::FiberData`（及其 `pub`
  字段）、`registry::PluginKind`、`registry::IdleOrRecoverableWait` 全部改为 `pub(crate)`；
  `Registry::counter()` 也改为 `pub(crate)`。Migration：用 `Fiber`/`Context`/`Registry` 的
  公开方法（`Fiber::state/get_effects/name/id/parent`、`Registry::size/get/keys/fibers` 等）
  代替内部结构访问；`PluginKind` 不再公开——直接对 `Plugin` / `AsyncPlugin` trait 编程，
  runtime 元数据用 `PluginRuntime::{name,key,fibers,fiber_count,is_empty}`。
- **【破坏性】`Fiber::last_activity() -> Instant` 移除**，替代为
  `Fiber::last_activity_ms() -> u64`（毫秒，来自树注入的 `Clock`，**`supervision` feature**）。
  Migration：要 Duration 用 `Fiber::idle_for()`（同 feature），要时间戳用
  `last_activity_ms()`；`Instant` 不再出现在公共 API。
- **Q11 行为修正：`FiberQuota::max_effects` 只统计用户 effect**：内核内部 effect（root 的
  logger provide effect、每个动态加载子 fiber 的 ownership effect 等）不再占用
  `max_effects` 配额；`max_effects(1)` 仍可正常加载插件，只限制宿主/插件通过
  `ctx.effect*` 注册的 effect。此前文档中的「`max_effects` 建议 ≥2」限制随之取消。
- **Logger 时间源可注入**：`LogRecord::ts_unix_ms` 默认来自 `SystemClock`；`ctx.logger()`
  门面在**创建时快照**当前树 `Clock`（切换后需重新获取 facade），裸 `LoggerService` API
  始终用 `SystemClock`（见 Added），因此测试/回放可得到确定性时间戳。
- **`Ok` 语义澄清（v0.5.0）**：`restart`/`update`/`replace_plugin`（含 async 变体）返回
  `Ok` 只表示操作完成，**不代表 fiber 处于 `Active`**；reload/激活失败会被 containment 到
  fiber 层（`Failed`），结构化错误保留在 `Fiber::error_detail()`。请用
  `state()`/`error_detail()` 判定结果；validation/veto/lifecycle 错误仍返回 `Err`。
- **`InternalBus` 所有权语义明确**：`Events::internal()`/`InternalBus` 是 **advanced/unstable**
  surface——注册的回调由**调用者**拥有、不随 fiber 卸载移除，须显式 `Disposer` 释放；常规
  插件继续用 fiber-owned 的 `Context::on_internal_*`。`Context::on_lifecycle` 是建立在
  `internal/status` raw hook 上的通用封装（非 fiber 绑定）。

### Added

- **可注入时钟**：`Clock` trait（`now_unix_ms` / `now_monotonic_ms`）与默认 `SystemClock`
  （wall time = Unix 毫秒，单调时间 = 进程内单调原点，实例互换不重置）；`Context::clock()`
  读取当前时钟，`Context::set_clock(Arc<dyn Clock>)` **只能在 root fiber 上的 context 设置**
  （root-derived scope 共享同一树 clock 槽），应在 root 创建后、加载任何插件前一次性设置
  （**不要在运行期切换**）；只影响**新**观测（已记录的时间戳不重写），单调源不得在运行期跳变。
  `ctx.logger()` facade 创建时快照 clock（切换后重新获取），裸 `LoggerService` 用 `SystemClock`。
- **生命周期广播**：`LifecycleEvent { fiber_id, name, from, to }`、`LifecycleListener`、
  `Context::on_lifecycle(listener) -> Result<Disposer>`——每次 fiber 状态迁移触发一次，多个
  订阅者独立运行，listener panic 被遏制并记录；订阅**不是 fiber-owned**，返回的 `Disposer`
  是唯一所有者——**drop `Disposer` 等同 dispose 退订**（单所有者安全网）；要跨安装 fiber 的
  生命周期继续监听，请保存 `Disposer` 并在需要时显式 dispose（适合宿主级 metrics/关闭协调）。
- **结构化插件错误**：`CordisError::Plugin { plugin, kind, message }` 与
  `CordisError::plugin(plugin, kind, message)` 构造函数；插件可在 `apply`/`apply_async` 中
  返回业务分类错误（如 `"config"`/`"network"`/`"auth"`），内核在激活、
  `Fiber::await_settled`、restart/update、`Registry::replace` 中原样传播。
  `Fiber::error_detail() -> Option<CordisError>` 读取保留的结构化错误（含 `Plugin` 变体）。
- `Fiber::last_activity_ms()` 公开（原 `last_activity` 的毫秒替代，见 Changed；**`supervision`
  feature**，与 `Fiber::{heartbeat,idle_for}`、`Registry::stalled_fibers()` 一样默认构建不可用）。
- **`IdleWait` crate 根 re-export**：`Registry::when_idle()` 的返回 future 现在可直接写作
  `zeokeel_cordis::IdleWait<'_>`（公共 API 清单补入；`Context::when_idle()` 是 async 便捷封装）。

### Docs

- 新增/更新 `docs/KERNEL-BOUNDARY.md`（v3 护栏，11 tests = 4 真实断言 + 7 自测；
  **API 极简性/公开 API 策略**节）；
  `CONTRIBUTING.md` 增加 **API 增长判据**（通用性 + 公共面 cap + 破坏性收敛走 minor + migration）；
  `README.md`/`docs/GUIDE.md`/`docs/KERNEL-ISOLATION.md`/`docs/CORDIS-RUST.md` 同步 0.5.0
  API/语义；`spec/CORDIS-CORE.md` 同步 §2.3/§2.6/§2.7/§3/§4/§6 的 0.5.0 语义/API 面与测试
  引用（**不新增 CORE ID**，仍 51 个用例）。
- **零遗留策略**：文档中所有面向未发布版本的未完成项表述已清除；物理或语言层不可消除的
  限制统一改称**设计边界（物理/语言层）**并在 `KERNEL-ISOLATION.md` 集中说明。

### Verification

- **跨平台 CI**：新增 `cross-platform` 作业，在 **windows-latest / macos-latest** 上跑
  default + supervision 测试与 kernel boundary（Ubuntu 继续作为主门禁）。
- **覆盖率门禁**：`coverage` 作业（`cargo llvm-cov --all-features`）以 **fail-under-lines
  85.5** 为下限，实测基线 **86.57%** 行覆盖。
- **Miri**：`miri` 作业 nightly + `-Zmiri-disable-isolation`：`cargo miri test --lib`
  **28/28 UB-clean**（`-Zmiri-ignore-leaks`，单测脚手架允许泄漏）；
  `cargo miri test --test miri_leaks` **1/1 leak-clean**。
- **cargo-deny**：`deny.toml` 启用 advisories（yanked deny、RUSTSEC）/licenses（仅宽松
  OSI 许可）/bans（多版本告警）/sources（仅 crates.io）；CI `deny` 作业执行 `cargo deny check`；
  发布流水线同样执行 `cargo deny` 与 unfinished-markers 检查。
- **actionlint**：CI `actionlint` 作业用 **pinned docker image `rhysd/actionlint:1.7.12`** 对
  `.github/workflows/*.yml` 做静态检查。
- **MSRV clippy**：`msrv-clippy` 作业在 Rust **1.85.0** 上跑两种 feature 配置的 clippy，
  显式允许两种版本级 lint 差异（`mutex_integer`、`needless_lifetimes`）。
- **SBOM（可复现）**：发布流水线生成 **CycloneDX 1.5** JSON（`scripts/sbom.py`），
  **两次生成并 `cmp` 比对**以确保可复现，然后作为 GitHub Release 资产上传（与 `.crate`
  SHA-256 一起）。
- **属性测试**：新增 `tests/property.rs` 确定性状态机随机测试（SplitMix64 固定种子 +
  watchdog），覆盖 load/dispose/refresh/update/replace/dispose_all 与 provider/consumer
  组合（1 个用例）；`tests/api_surface.rs`（19 个公共 API 契约用例）、`tests/miri_leaks.rs`
  （1 个泄漏场景）一并纳入 `cargo test` 门禁。
- **零遗留检查**：文档与 CHANGELOG 中不存在未完成项表述；历史 0.4.x 条目只保留历史事实，
  其中由本版解决的部分已标注「0.5.0 已实现」。

## [0.4.4] - 2026-10-29

**内核边界 + 取消 recovery 收尾补丁**：不新增语义能力，也不新增/重命名任何 CORE 用例。
本版把「内核 vs 插件层 / 宿主」的可执行边界（文档 + CI 护栏）固化，并修复 S7——取消
recovery 时级联根帧的 deferred cleanup 可能延后到显式 dispose 的 P2 边界。反向依赖索引
由 0.5.0 的 S1–S4 实现（见 [0.5.0] Performance）。默认 `cargo test`（含 7 个 doc-tests）
**415 passed / 0 failed**、`--features supervision` **426 passed / 0 failed**（以发布前
门禁最终实测为准；0.4.3 为 404/415，本版为 +S7 1 个 + `kernel_boundary` 10 个）。

### Fixed

- **S7：取消 recovery 的 root 帧 deferred cleanup 收尾**：恢复在级联根帧上被取消时，fiber
  可能已落到目标态（`Pending`），但 `CancelSafeDrain` 恢复出的剩余 async cleanup 留在
  `deferred_async`；旧行为下 `when_idle()` 会先报 idle，要等下一次显式
  `dispose`/`dispose_all` 才执行。修复为：
  - `ReparkGuard` 的 re-park 条件放宽为
    `state == Unloading || (state != Disposed && !deferred_async.is_empty())`；
  - `Registry::take_parked_unloads` 把上述两类帧重新纳入**可恢复集合**（`Disposed` 帧的
    deferred cleanup 仍按 A-9 留给显式重试）；
  - `when_idle()` 的返回条件改为 `idle && !has_parked_unloads()`。
  于是恢复出的 cleanup 在 `when_idle()` 返回前**恰好执行一次**（不重复、不静默延后）。
  回归：`regression_core::s7_cancelled_recovery_drains_root_deferred_cleanup`。

### Docs

- 新增 [`docs/KERNEL-BOUNDARY.md`](docs/KERNEL-BOUNDARY.md)：内核 / 插件层 / 宿主三层职责、
  **绝不属于内核**清单、依赖方向（`src/**` 仅允许 `serde_json` / `tracing` /
  `futures-util`）、扩展点、三条判据与常见误判。
- 新增 **v3 架构护栏** `tests/kernel_boundary.rs`（10 个测试 = 4 个真实断言 + 6 个自测）：
  - 依赖：解析 `[dependencies]` 平铺 / `[dependencies.X]` 表 /
    `[target.'cfg(...)'.dependencies(.X)]`（含 `[target.'cfg(...)'.dependencies]` 平铺
    section），把 `package = "..."` / 单引号 `package = 'tokio'` 重命名解析为**真实包名**；运行时依赖 ⊆ `{serde_json, tracing,
    futures-util}`；禁 `[build-dependencies]`、`build =`/`build.rs`、`src/bin`；`[lib] path`
    只能是 `src/lib.rs`；`[features]` 仅 `default`/`supervision` 且不得 `dep:` 引用依赖；
  - 源码：递归扫描 `src/**`，注释用词法器剥离（**字符串保留**）、`use` 分组展开、全空白
    归一化；禁 `std::fs/net/process/env/os/path/io`、套接字/`File::open`/`OpenOptions`/
    `Command::new`、`thread::spawn`/`thread::Builder`（保留 `park`/`current` 等 executor
    原语）、tokio/async-std/smol/reqwest/hyper/sqlx、`env!`/`option_env!`/`include!`/
    `include_str!`/`include_bytes!`；
  - 业务词：代码 + 字符串按**标识符分词**（`payment_gateway`/`PaymentService` 均命中）+
    文件名 stem；**公开文档**（`///`/`//!`/`/**...*/`/`#[doc]`）单独用 doc 词表扫描
    （允许 http/server/client），普通注释与 `#[cfg(test)]` 模块不参与代码词扫描；
  - 契约：注释剥离后真实存在 `#![forbid(unsafe_code)]`；公共 API 上限 **420**（含
    `pub use`/`pub mod`，tripwire）；
  - 6 个自测证明能抓：表依赖/`package =` 重命名、`[target.'cfg(...)'.dependencies]` 平铺
    section 与单引号 `package = 'tokio'` 重命名、`use` 分组+空格归一化路径、
    snake_case/CamelCase 分词、doc 注释收集、注释伪造 `#![forbid(unsafe_code)]` 与
    字符串 `//` 误剥。
  CI 的 `kernel boundary` 步骤（`cargo test --locked --test kernel_boundary`）强制执行；
  **PR 必须通过**，不得 `#[ignore]`/弱化断言绕过。
- `README.md` / `CONTRIBUTING.md` / `docs/DESIGN.md` 同步边界分层与判据；
  `spec/CORDIS-CORE.md` 仅加 editorial note 指向边界文档（**不新增 CORE ID**）。

### Notes

- **反向依赖索引（性能）**：0.4.4 未包含；由 **0.5.0** 的 S1–S4 完整实现，并消除 0.4.3
  报告的 5000-fiber fanout `create`/`dispose_all` 回归（见 [0.5.0] Performance）。

## [0.4.3] - 2026-10-29

**审计加固补丁**：不新增语义能力，也不新增/重命名任何 CORE 用例；修复第三轮对抗审计确认的
缺陷（动态卸载记账泄漏、深依赖链卸载栈溢出、注册帧内 `when_idle` 自锁、supervision 配额
记账、Plugin 元数据回调 panic 外泄、嵌套 effect 元数据残留等），并同步文档与工程流水线。
默认 `cargo test`（含 7 个 doc-tests）**404 passed / 0 failed**、`--features supervision`
**415 passed / 0 failed**。

### Fixed

- **R3-A4 动态加载父 fiber 记账无界泄漏**：频繁 `ctx.plugin`/子 fiber dispose 时，父 fiber 的
  ownership effect 永不移除——60k churn 累积 **60001** 个 effect、RSS **+54.7 MB**。改为子 fiber
  自身 dispose 时 `detach_child_ownership()` 从父 fiber 的 effect 表精确摘除对应 ownership
  effect（连同嵌套 effect 节点与弱子列表，加锁顺序保持 parent-fiber → children → node），并在
  父 effect 表超过阈值时**摊还 prune** 已 disposed 的 ownership effect（只 prune
  `owner_child` 标记条目，绝不误删活跃 effect）。10k churn 后峰值 1 个 ownership
  effect、RSS **基本持平**（10k churn 实测约 **+996 KB / <1 MB**）。
- **R3-A1 深依赖链卸载栈溢出**：卸载走 `take_effects → retire_unready → unload` 相互递归，
  调用栈深度≈依赖链层数。0.4.2 对照：2 MiB 线程 debug 构建约 **480 层**即栈溢出 abort
  （depth 800 必失败），release 构建约 **2200 层**才 abort（depth 800 可过）。改为**两阶段
  保序迭代级联**：先按 provider-before-dependent 顺序 *begin* 所有传递不可用 fiber（标记
  `Unloading`、撤销服务、取走 effects），再按逆序 *drain*（等价于 dependents-before-provider
  的清理顺序）；ownership 子树用显式 DFS 栈迭代收集，不再递归。0.4.3 在 2 MiB 线程
  depth 800 下 **debug 与 release 均通过**（远超实际 `MAX_PLUGIN_DEPTH = 256` 限制的加载
  链）；加载深度上限不变。修复 R1 卸载栈回归的根因
  （ownership effect 在 drain 中再次递归 dispose 子 fiber），并修复 c16/rt21/r5_05–r5_07/
  executor 相关回归用例。
- **R3-A3 / C4 `when_idle()` 在自身注册帧内自锁**：`ctx.plugin` 的「注册→激活」窗口计入
  in-flight（FIX-11b）后，从 `internal/plugin` create hook 内调用 `when_idle()` 会等待自己的
  注册（0.4.2 的可观察变化）。改为在 Registry 上按**创建线程**记录 in-flight 注册计数
  （跨 `.await`/executor 迁移安全），`when_idle()` 只排除调用线程**自己的**注册：hook 内
  4 ms 级返回，外部 waiter 的 FIX-11b 语义不变；`is_idle()` 保持严格（对该窗口对其它线程
  仍非 idle）。跨线程环形注册依赖（A 注册等 B、B 注册等 A）仍可能互等——文档保留该边界。
- **F1/F4/F5：取消的 async 级联自动、可重试地恢复 parked `Unloading` 帧**：取消
  `dispose_async`/`refresh_async` 时非根 cascade 帧可能短暂停在 `Unloading`。
  - **F1 恢复语义**：`when_idle()`、`refresh_async()`、`dispose_all()` 以及根 fiber 的
    `dispose()`/`dispose_async()` 重试都会自动恢复这些帧到**原级联目标**（依赖方
    `Pending`、ownership 子 `Disposed`），未启动的 deferred async cleanup 在恢复时
    **恰好执行一次**；恢复幂等、去重、有界（按并发 parked 帧数），取消 unwind 中或另一
    unload 进行中时延迟到下一次根操作。若取消恰好发生在某个 async cleanup 正在执行时，
    该在飞 future 不可恢复（A-9 既有边界），但**不会留下永久 `Unloading`**；恢复前
    `is_idle()` 仍为 `false`（严格）。
  - **F4 恢复状态安全**：单恢复者闩锁改为 **RAII guard**（`RecoveryGuard`，取消/panic 都
    不会让闩锁残留、后续恢复永远跳过）；恢复批量取帧用 **`ReparkGuard`/`RecoveryBatch`**，
    恢复在批量中途被取消/失败时把未处理或在飞帧**重新 park**，不丢帧。
  - **F5 等待期持续恢复**：`when_idle()` 在 **`Context::when_idle()` 与裸
    `Registry::when_idle()` 公共 API 两侧**都是 recovery-aware 的等待循环——等待期间会
    持续重试恢复（被 idle 或 parked-recoverable 唤醒，有界并带兜底），不会因等待中才出现
    的 parked 帧永久挂起。**同步 `refresh()` 不恢复 parked 帧**（避免在调用线程同步 drain
    reactor-backed cleanup 而死锁）；恢复只由 async/可等待入口（`when_idle()`、
    `refresh_async()`、`dispose_all()`）以及根 dispose 重试承担。
  回归：`regression_core::f1_retried_root_dispose_async_recovers_parked_cascade`、
  `f1_dispose_all_recovers_parked_cascade`、`f1_refresh_async_recovers_parked_cascade`、
  `f4_cancelled_recovery_releases_the_latch_for_the_next_when_idle`、
  `f4_cancelled_refresh_recovery_is_retried_by_when_idle`、
  `f5_when_idle_recovers_parked_frames_that_appear_while_waiting`、
  `f5_repeated_cancel_then_when_idle_does_not_hang`、
  `f5_raw_registry_when_idle_recovers_parked_cascade`、
  `f5_when_idle_sees_parked_frames_parked_after_entry`；
  以及 `regression_semantics::f1_when_idle_recovers_parked_cascade_after_cancel`、
  `f4_cancelled_recovery_does_not_latch_the_next_when_idle`、
  `f5_when_idle_waits_for_active_unload_then_recovers_parked_frames`、
  `f5_when_idle_recovers_parked_frames_that_appear_while_waiting`、
  `f5_twenty_cancel_and_when_idle_rounds_do_not_hang`。
- **S1 supervision 配额按活资源记账**：`max_effects` 被已释放的 effect 死条目长期占用；
  listener 配额只在用户 disposer 释放（`once` 自动移除、并发 dispose、fiber 卸载会漏槽或
  双扣）；child 只数 fiber 列表长度。改为：`push_effect` 时清理 dead 条目（配额只计活
  effects）；listener 用一次性 `ListenerSlot`（用户 disposer、`once` 自动移除、fiber 卸载
  三路**恰好一次**释放，`OnceListenerRelease` 在调用结束后兜底）；child 配额按 live 弱引用
  统计。释放后立即归还配额槽，不再需要超配。
- **C2 Plugin 元数据回调 panic 遏制**：`Plugin::{name,key,inject,config_schema}`（及
  `AsyncPlugin` 对应方法）是 embedder 代码，内核在创建/替换/重启/诊断/依赖解析时调用；
  panic 原会穿出 `Result` API 或毒化内核锁。现在各自 `catch_unwind` 并**确定性降级**：
  `name()` → `"<panicked plugin>"`、`key()` → `PluginKey::Named("<panicked plugin>")`
  （所有坏 key 共享一个 runtime）、`inject()` → `&[]`（无依赖门控）、`config_schema()` →
  `None`（接受原配置、不校验），均以 `error!` 记录；不再外泄 panic。
- **C3 嵌套 effect 元数据 ghost 清理**：用户提前 dispose 的嵌套 effect 会永久残留在父 effect
  的 `EffectMeta.children` 中（`get_effects()` 树不 prune）。树快照改为按 fiber 的 live 节点
  集过滤，已 dispose 的嵌套 effect 不再出现在诊断树里。
- **S2 logger 非法 level 值 fail-open**：`{"level": 99}` 等未知数字被 `from_u8` 映射到最 verbose
  的 `Debug`，反而放大敏感记录。改为未知数字 **clamp 到 `Error`**（字符串级别行为不变）。
- **S3 accessor setter 拒绝缺少独立错误**：计算 accessor 的 setter 返回 `false`（owner 检视
  新值后拒绝）原报 `ServiceWriteForbidden`（被误读为所有权错误）。新增
  `CordisError::AccessorWriteRejected(name)`，与 `ServiceWriteForbidden`（别的 fiber 拥有）、
  `AccessorReadOnly`（accessor 只读）区分；`ctx.set`/`set_any` 都返回新变体。
- **C4 注册归属表**：见 R3-A3——per-thread `registration_owners` 计数，guard 跨线程 drop 仍
  回收正确归属；`when_idle` 只减自身计数。
- **C5 错误变体**：`CordisError::AccessorWriteRejected` 补齐 `Display` 与文档；`non_exhaustive`
  保证下游已有 `match` 无需改动。
- **R1 卸载栈回归根因**：ownership effect 在父 fiber drain 时会再次递归 dispose 子 fiber，
  使深层/深链卸载栈持续增长；两阶段迭代级联 + 子自 dispose 时 detach ownership 后不再递归。

### Changed

- `Registry::when_idle()` 的等待判据改为「全局 in-flight 注册 − 调用线程自身注册」，`is_idle()`
  仍严格（不排除任何注册）。
- 卸载级联改为两阶段迭代实现：栈 O(1)，drain 顺序仍为 dependents-before-provider；级联轮数
  有 `fiber_count + 2` 的固定点上界，超限的剩余依赖方保持 Active 并可被 retry/refresh 拾取。
- supervision 配额按**活资源**计数（effects/listeners/child fibers 释放即归还槽）；`once`
  监听器的槽在自动移除时释放。
- `Plugin` 元数据回调 panic 一律 contained 降级（见 Fixed C2），不再从公共 API 外泄。
- `CordisError::AccessorWriteRejected(String)` 为新增变体（`#[non_exhaustive]`）。
- 取消级联的 parked `Unloading` 帧由 recovery-aware 入口自动恢复（F1/F4/F5）：
  `when_idle()`（Context 与裸 Registry API 均为等待期持续恢复）、`refresh_async()`、
  `dispose_all()`、根 `dispose()`/`dispose_async()` 重试；**同步 `refresh()` 不恢复**
  （避免同步 drain reactor cleanup 死锁）。恢复目标是原级联目标、deferred async cleanup
  恰好一次；单恢复者闩锁为 RAII、批量取帧用 ReparkGuard 保证中途取消不丢帧。
- **工程/流水线**：`Cargo.lock` 入库（从 `.gitignore` 移除）且 CI/发布命令统一 `--locked`；
  CI 新增独立 **MSRV 1.85 作业**（仅 test/doc，不带 clippy）、examples 构建 + 冒烟运行、
  真实 `cargo package` verify；release workflow 要求从 tag 触发并校验 **tag 版本 ==
  `Cargo.toml` version**，依次 `cargo package --locked` → `cargo publish --dry-run` →
  `cargo publish`，发布成功后把 `.crate` 的 SHA-256 上传到 GitHub Release。

### Compatibility

- **深依赖链卸载限制移除**：数百~数千层的依赖链现在可以正常卸载（迭代级联，栈 O(1)）。
  0.4.3 在 2 MiB 线程 depth 800 下 **debug 与 release 均通过**；0.4.2 对照为 debug 约 480
  层即 abort、release 约 2200 层才 abort。`MAX_PLUGIN_DEPTH = 256` 仍只限制**插件加载深度**，
  与卸载深度无关。此前文档中的「极深依赖链 dispose 会递归爆栈（0.4.1 同型）」限制不再成立。
- **`when_idle()` 在自身注册帧内不再死锁**：0.4.1 在该窗口立即返回全部 idle（语义过宽）、
  0.4.2 会等待自己而死锁；0.4.3 排除**调用线程自身**的 in-flight 注册后正常返回，外部
  waiter 仍按 FIX-11b 等待整个注册/激活窗口。`is_idle()` 语义不变（严格）。跨线程环形注册
  依赖仍可能死锁（文档注明）。
- **Plugin 元数据 panic 从「外泄/毒化锁」变为「contained 降级」**：坏插件的行为变为可观测的
  占位名/空依赖/无 schema，而不是 panic 穿出 `ctx.plugin` 或锁中毒；这是 Rust 内核契约的
  明确化（TS 参考会抛异常），受影响插件本身仍是坏的，建议修复其回调。
- **supervision 配额语义修正**：配额现在按活资源计数——卸载/`once` 自动移除/并发释放后
  不再占槽，因此超限判定的可触发点与 0.4.2 不同（同等上限下现在允许更多历史累积注册）。
- **logger 非法数字 level 改为 clamp 到 `Error`**：依赖 `{"level": 99}` 放大日志的配置现在
  会被收紧到 Error（fail-closed）。
- **`CordisError::AccessorWriteRejected`**：新增变体（`non_exhaustive`，不影响既有 `match`）；
  依赖旧 `ServiceWriteForbidden` 判别 accessor 拒绝的代码需改为匹配新变体。
- **Cargo.lock / `--locked`**：仓库与 CI 现在使用锁文件构建；crates.io 下游不受影响，但从
  git 依赖本仓库时需要提交/接受其 lockfile 约束。

### Docs

- `README.md`：版本/安装/测试数字更新（0.4.3；404/415）；已知限制删除深依赖链栈溢出；
  `when_idle`/quota 语义更新；质量章节保留 CI/MSRV/`--locked`/release 流程描述。
- `docs/KERNEL-ISOLATION.md`：深依赖链限制改写为迭代级联 + 2 MiB/depth-800 的 debug+release
  对照（0.4.2 debug≈480 层 abort）；`when_idle` 新语义与
  跨线程环依赖边界；panic 矩阵新增 Plugin metadata callbacks（contained）行；quota 活资源语义。
- `docs/CORDIS-RUST.md`：新增 0.4.3 加固条目（R3-A4/R3-A1/R3-A3、S1、C2/C3、S2/S3、C4/C5、
  R1）与 TS 对照说明；§4 边界更新（移除深链、更新 when_idle、补 AccessorWriteRejected/quota/
  logger）。
- `spec/CORDIS-CORE.md`：状态升 v0.4.3；§2.3/§2.6/§2.7 与 §6 补 0.4.3 语义；CORE-047/049/
  005/006/041 等行补新回归测试名（不新增 CORE ID）。
- `docs/GUIDE.md`：`when_idle` 新语义、quota 活资源、metadata panic 契约、深链支持。
- `CONTRIBUTING.md`：CI/发布流程与 `Cargo.lock` 策略（0.4.3 起）；版本冻结线更新。

### Known limitations

- **跨线程环形注册依赖**：`when_idle()` 只排除调用线程自身的 in-flight 注册；若两个线程互相
  等待对方的注册窗口，仍可能互等（内核不检测环依赖）——宿主需避免同步环依赖。
- **C2 降级 key 的定位限制**：所有 `Plugin::key()` panic 的插件会共享同一个
  `PluginKey::Named("<panicked plugin>")` runtime，因此 `keys`/`replace`/`delete` 只把它们
  当作一个条目、**无法单独定位**其中某个坏插件。这是对已损坏插件的最佳努力隔离（非受支持
  配置），宿主应修复其 `key()`。
- **性能回归（0.4.3 记录，0.5.0 已消除）**：5000 fiber fanout 场景下 create /
  `dispose_all` 比 0.4.2 慢约 **25–35%**（1k fiber 更快或持平，RSS 与正确性无回归）。
  **0.5.0** 的反向依赖索引（S1–S4）已消除该回归：fanout 5k create+dispose −54%、
  dispose −74%（见 [0.5.0] Performance）。
- **取消 recovery 时少数 restored cleanup 延后（0.4.3 已知边界，P2）**：当一次 recovery
  正在处理 parked 帧、且在某个作为级联根的 fiber 上被取消时，该 fiber 完成到 `Pending` 后，
  其 `CancelSafeDrain` 恢复的剩余 async cleanup 可能被留在 `deferred_async`，直到下一次显式
  `dispose`/`dispose_all` 才执行（不会重复执行，也不会永久 `Unloading`/挂起；`when_idle`
  可能先行报 idle）。**已在 0.4.4（S7）修复**：恢复路径把这类帧重新纳入可恢复集合，
  `when_idle()` 返回前会先执行完恢复出的 cleanup（见 0.4.4 Fixed）。
- 其余进程内不可遏制边界不变：插件自身递归导致的栈溢出、`panic="abort"`、OOM/allocator
  abort、`unsafe` UB、`process::abort/exit`、死循环/死锁、外部 kill（见
  `docs/KERNEL-ISOLATION.md`）。
- Plugin 元数据 panic 的降级是 best-effort 隔离：坏插件会以占位名/无依赖/无 schema 运行，
  语义上仍是坏的，请修复其回调。

## [0.4.2] - 2026-10-28

**修复/加固补丁**：不新增语义能力，也不新增/重命名任何 CORE 用例；修复独立复核确认的
21 项生命周期与语义缺陷，以及审计阶段追加的并发/遏制类修复（D-1、D-2 等），并把此前未
明说的边界写入 `spec/` 与 `docs/`。每项修复都有回归
测试：新增 `tests/regression_core.rs`（生命周期/registry）与 `tests/regression_semantics.rs`
（注入/配置/reflect/waterfall），个别需要内部状态的用例放在 `src/fiber.rs` 的 unit test；
默认 `cargo test` 与 `--features supervision` 两套全绿。

### Fixed

生命周期与并发（P0/P1）：

- **refresh 闩锁无 RAII 复位**：一次 cleanup panic 穿出 `refresh_all` 后，依赖刷新永久瘫痪
  （consumer 永远 Pending、`is_idle()` 仍为 true、无日志）。改为 `RefreshGuard`（sync/async
  都由 Drop 复位；CAS 失败仍置 dirty），refresh/re-provide 均可恢复。
- **refresh/retire 路径的卸载 panic 无遏制**：`refresh_all`、`refresh_all_async`、
  `retire_unready` 里对依赖方的 `unload_fiber` 直接传播 panic——顺序触发即可让 provider
  永久卡在 `Unloading`、`when_idle` 永不返回（复核新增缺陷）。改为 `unload_contained`：
  记录日志并继续推进，provider 一定到达目标态；用户直接 `dispose()` 仍按 CORE-030 重抛。
- **`update`/`update_async` 早退分支绕过 finalize**：`Disposed`/`Unloading`/
  `AsyncPluginNeedsAsync`/ctx 丢失/校验失败/否决等早退在 `finalize_requested_dispose`
  之前 return，父 sweep 打标过的子 fiber 会永久 `Active`（孤儿 + 服务/监听器悬挂）。
  新增 `FinalizeGuard`，覆盖全部返回路径（含 `?` 与 panic）。
- **`sweep_children_sync/async` 无逐子 panic 遏制**：改为逐子 `catch_unwind` 并继续 sweep，
  合并 drain panic，在父 fiber 落目标态后再重抛首个 payload；激活失败回滚与陈旧激活清理
  只记日志。
- **激活 effect 的 epoch 打标不精确**：并发 restart 时旧激活的 effect 会打上"当前"epoch，
  `discard_stale_activation` 清不掉而留下过期尾巴。改为 thread-local 激活帧（同步
  push/pop；异步用包装 future 逐 poll），跨 executor 迁移安全。
- **`restart`/`restart_async` 直写状态**：绕过 `transition`，`internal/status` 看不到
  `Active -> Unloading` 且出现无来源的 `Unloading -> Pending`。改走 `transition`。
- **`Registry::replace` 回滚不完整**：失败时只回滚绑定，已 reload 的 fiber 停在新实现
  （新旧混合态）。改为把已切换的 fiber 用旧 plugin 重新 reload 并恢复绑定；回滚
  自身失败返回 `CordisError::Rollback`（消息含原始错误与回滚错误）。
- **`Fiber::replace_plugin`/`replace_plugin_async` 生命周期缺口**：swap+rebind 前缀不抢
  lifecycle 槽、root 特例失败残留临时 runtime/fiber。改为 `begin_lifecycle` +
  `begin_operation`；失败恢复旧 plugin/runtime 并清理 root 特例，`registry.size()/keys()`
  与操作前一致。
- **`dispose_all` 丢弃 panic payload**：同时出现 `Err` 与 panic 时 Err 优先返回、panic 被
  吞且无日志。改为 panic 优先：error 记 `tracing::error!`，尝试完全部 fiber 后
  `resume_unwind`。
- **`Drop for FiberData` 不清理事件 owner**：drop 时通过 `ctx` 移除事件 owner 条目
  （ctx 已断开则跳过），作为兜底。
- **`is_idle` 漏算注册/激活窗口**：`plugin_with_key`/`plugin_async*` 从 fiber 注册到激活完成
  之间不算 in-flight，`when_idle()` 可能在插件尚未激活（甚至 `internal/plugin` create hook
  尚未返回）时提前返回。改为 Registry 级 `begin_registration()` guard 覆盖「create（含
  `internal/plugin` hook）→ 激活」整个窗口，并保留 fiber 级 `begin_operation` 覆盖激活本身；
  `when_idle` 现在要等窗口结束才返回。
- **D-2：replace 的 reload panic 不再穿出 `Result` API**：`Registry::replace` /
  `replace_async` / `Fiber::replace_plugin(_async)` 的 reload 会 drain 旧实现的 cleanup
  （embedder 代码）；原先 cleanup panic 会在 runtime 已 re-key 之后 unwind 穿出 API（半迁移）。
  现在 reload 包 `catch_unwind`，panic 转成
  `Err(CordisError::PluginFailed("<op>: reload panicked: …"))` 并走完整回滚（已 reload 的
  fiber 用旧实现重新加载）；只有回滚自身失败才返回 `Rollback`。调用方不再收到 panic。
- **D-1：并发 replace/dispose 后 registry 泄漏**：`replace`/`replace_plugin`/`dispose` 并发
  竞争时旧 runtime 可能残留（`registry.keys()` 出现幽灵条目）。修复 rebind/unregister/
  rollback 路径的竞争窗口并自愈已 dispose 的 registration；新增并发回归测试，断言并发
  结束后 registry 必然清空、无 live fiber 且 `is_idle()`（Added tests）。
- **审计轮次追加修复（独立复核发现：含 0.4.1 同源既有缺陷与 0.4.2 修复引入的回归；跨生命周期/registry/语义/配置类别）**：
  - **A-1：stale 异步激活污染新激活**——被新 epoch 取代的旧 attempt 醒来时，取消守卫会把
    新 attempt 的 effects/listeners 回滚、写入伪 error 并置 Failed，最终 `Active` 却
    `await_settled()==Err`。守卫改为 epoch 精确回滚（只 drain 本 attempt 的 effects）并在
    `still_current` 复核后才落态；stale attempt 不再污染 winner。
  - **A-6：回滚目标含并发 Disposed fiber**——replace 失败回滚把已 Disposed 的失败 fiber 也当
    回滚目标，返回误导性错误（实际消息格式
    `Rollback("original error: …; rollback error: …")`）。修复：回滚循环内对
    `state()==Disposed` 的 fiber 跳过（`rollback_targets` 仍返回索引，过滤在循环里做），随后
    heal 孤儿 registration 并 prune 空 runtime。
  - **A-10：取消的异步激活残留子 fiber**——取消激活回滚只清理本 epoch effects，`ctx.plugin`
    创建的子 fiber 会留在 `Active`（父 Failed、`is_idle=true` 看不见）。回滚后补
    `remove_owner` + 受控 `sweep_children_sync`（panic 展开路径非阻塞 request）。
  - **B-3：provider 锁中毒 → 服务永久不可注入**——`swap_plugin` 在持 `FiberData` 锁时调用
    embedder 的 `Plugin::inject()`，其 panic 会毒化锁，旧宽容门把它当失活使服务永久不可注入。
    现在 `inject()` 在锁前求值；`provider_is_active` 对中毒锁按可用宽容（与 `None` 一致）。
  - **B-4'：默认值与声明类型冲突（schema authoring 错误）**——此前只在 inject 后报
    "field must be string, got number" 的误导错误；现在 `validate` 直接给出
    `default value for \`name\` must be <kind>, got <kind>` 且不注入该默认值（`merge` 不做
    authoring 校验，仍按原样物化）。
  - **R2-1：async 自 join 死锁**——cleanup 内 `await` 自己的 `dispose()` 会等自己（0.4.1 同
    源）；现在与 `dispose_blocking` 共用 drain-frame 判据短路。

语义与边界（P1/P2）：

- **provider-ACTIVE 注入门（对齐 TS）**：`ServiceEntry` 记录 provider fiber 的
  `Weak<Mutex<FiberData>>`（`None`/已 drop 视为可用，兼容手工条目），新增严格判定
  `is_available_for_injection`（可用 + provider 为 `Active`）；
  `dependencies_ready`/`dependencies_satisfied` 与 `Context::service_available` 改用严格
  查询 `lookup_for_injection`，`get`/`get_named`/`get_any` 保持非严格（TS 属性读取语义）。
  依赖方不再对着 `Loading` provider 激活；provider 返回 `Err` 时 consumer 的 `apply` 为 0 次。
- **`ctx.set`/`set_any` 类型校验**：`ServiceEntry` 增加 `type_id`（typed 用
  `TypeId::of::<T>()`、`*_any` 用 `TypeId::of::<ArcWrap<T>>()`）；写入前用
  `service_type_id` 比对，不匹配返回 `CordisError::ServiceTypeMismatch(name)` 且原值不变；
  `write_service` 终端再校验一次（defence in depth）；`internal/set` hook 只能否决、
  不能替换值（落实 DESIGN I5）。
- **`update` 顺序 / veto / `no_save`**：改为 `internal/config` → validate →
  `internal/update`，仅 waterfall 终端 store raw 并 restart（且至多一次）；纯 veto
  （不调 next）返回 `Ok(())`、不 restart、raw 不变；replacement 配置先校验再作为新 raw；
  `no_save` 按 TS 参考只作为传给持久化 hook 的提示透传（`update(config, noSave)`），
  成功的 update 始终更新 in-memory raw config 并重启，不因 `no_save` 跳过存储。新增
  additive 入口 `Fiber::update_with_no_save(config, no_save)` /
  `update_async_with_no_save(config, no_save)`（现有 `update`/`update_async` 等价
  `no_save = false`，行为不变）。
- **`AsyncDisposer::dispose_blocking` 不 join**：原先见 `is_disposed()` 即早退（约 300ns
  返回而 cleanup 仍在跑）。改为 `block_on_unit(boxed(dispose()))`，真正等待在飞清理完成。
- **S-1：取消的 async teardown 现在可重试（`AsyncDisposer` 层面）**：`AsyncDisposer::dispose`
  的 future 被 drop
  时未启动的 cleanup 会被恢复，但 FIX-15 早期实现把取消也标记为 `done`，`dispose_blocking()`
  会误早退导致这些 cleanup **永久丢失**（仅在飞的那个 future 本就不可恢复）。改为 teardown
  归属令牌（`claimed`）+ 取消态：下一次 `dispose()` / `dispose_blocking()` 会重新取得
  teardown 并按原逆序重试恢复的 cleanup；`Fiber::dispose()` 同步安全网同样重试。
- **S-2：cleanup 内对自己的 disposer 调 `dispose_blocking()` 不再自等死锁**：FIX-15 早期
  实现会把调用线程记作 joiner 而等待自己。现在用**调用栈 drain frame** 判据（thread-local
  记录「当前 poll 栈是否正在 poll 该 disposer 的 cleanup」；线程 id 判据会误伤同线程独立
  joiner）在自 join 时**立即返回**（与 0.4.1 行为一致）；R2-1 把同一判据用于 async
  `dispose()` 自 await；跨线程/其它 disposer 仍真正阻塞到 teardown 完成。
- **A-9：取消的 fiber unload 现在会「做完」而不是恢复旧状态**：`dispose_async` /
  `refresh_async` 的 future 被 drop 时，`CancelSafeUnload::drop` 不再把 effects 放回并把
  fiber 回退到旧状态（0.4.2 早期实现会留下半拆解；0.4.1 则可能卡 `Unloading`/丢未启动
  cleanup）。改为完成 teardown：同步 cleanup **立即执行**，异步 cleanup 进入 fiber 的
  `deferred_async`、由下一次 dispose/drain 收尾，`finish_unload` 落到目标态
  （重载/依赖触发 → `Pending`，dispose → `Disposed`）并 `remove_owner`/notify idle——不再出现
  永久 `Unloading` 或假 `Active` 半拆解态；在飞的 async cleanup future 仍不可恢复。
- **async waterfall panic 被吞**：`Events::waterfall_async` 把中间件 panic 记日志后降级为
  `Value::Null`（fail-open，与文档"re-raise"矛盾）。改为记录后 `resume_unwind`，与同步
  lane 一致（fail-closed）。
- **`null` 配置默认值**：`JsonConfigSchema::default_value(name, Value::Null)` 原先把该字段
  推断为 `Object`，`validate({})` 必然失败。改为 **`null` 默认值不声明类型、是否注入取决于
  声明的类型**（不新增 `JsonType` 变体，保持 0.4.1 的枚举形状）：未声明类型 → 缺省注入
  `null` 且不做类型检查；已显式 `property(name, JsonType::…)` → 缺省**不注入**（可选字段
  保持 absent，调用方提供值时按声明类型校验，`required` 则要求显式给值）；String 等既有
  非 null 默认值行为不变。
- **accessor panic 返回占位值**：`reflect::Accessor::read` panic 时返回 `Arc<()>` 占位，
  untyped `get_named` 得到 `Some`。改为 `Option<ServiceValue>`：panic 记录后 `None`，
  `get`/`get_any`/`get_named`/`accessor_value` 一致返回 `None`。
- **`MAX_PLUGIN_DEPTH` 数错深度**：原先沿 context 链计数，300 层 `intercept()`/
  `extend_meta()`（0 个嵌套 plugin）就会让单次 `plugin` 被拒。改为沿 fiber 父链计数
  （root fiber 计 1 层，从 root context 加载的 plugin 起始 depth=1，之后每层嵌套 +1，
  约 254 层嵌套后拒绝）；300 层 context 链不再消耗预算，递归加载仍被拒。
- **`ctx.inject` 身份粒度**：所有 inject 共享 `PluginKey::Named("injected")`，3 个 inject
  在 registry 里只有 1 个 runtime，`replace(Named("injected"))` 会重绑全部。改为
  `PluginKey::Type(TypeId::of::<Injected<F>>())` 分键（`name()` 仍返回 `"injected"` 供诊断）。
- **注入/查询语义一致性（FIX-21）**：`service_available` 与 inject 统一走严格注入查询；
  accessor 与 `internal/get` 虚拟服务只对 `get` 可见、不参与 inject（与 TS 一致），并把
  该既定语义写入 `spec/` 与 `docs/`。

### Changed

- `CordisError` 新增 `ServiceTypeMismatch(String)` 与 `Rollback(String)`（`CordisError` 是
  `#[non_exhaustive]`，下游已有的 `match` 无需改动即可编译）。
- `Fiber::replace_plugin`/`replace_plugin_async` 现在占用 fiber 生命周期槽（并发调用可能
  得到 `LifecycleBusy`）。
- `registry.replace` 的失败回滚覆盖已 reload 的 fiber；回滚失败返回 `Rollback`。
- replace/replace_plugin 的 reload panic 策略与 reload 返回 `Err` 统一：捕获后走同一回滚
  路径（不再 unwind 穿出 `Result` API）。
- 新增 additive API：`Fiber::update_with_no_save(config, no_save)` /
  `update_async_with_no_save(config, no_save)`；现有 `update`/`update_async` 保持
  `no_save = false` 语义。`no_save` 按 TS 语义仅作持久化提示透传，内核不因它跳过
  in-memory raw config 提交。
- 内部结构调整（不属公开 API）：`src/fiber.rs` 新增 `begin_operation`/
  `current_activation_epoch`/`unload_contained`，`src/registry.rs` 新增
  `retire_unready_contained`，`ServiceEntry` 增加 `provider`/`type_id`，
  `reflect::Accessor::read` 返回 `Option<ServiceValue>`。

### Compatibility

- **async waterfall panic 现在 re-raise**：`waterfall_async` 的中间件 panic 不再被吞成
  `Value::Null`，而是向上抛出（与同步 `waterfall` 一致）；直接调用方需自行
  `catch_unwind`，在 `apply_async` 内则表现为激活失败并回滚。
- **`ctx.set`/`set_any` 类型不匹配返回 `Err(ServiceTypeMismatch)`**：以前会静默覆盖成功；
  依赖"用错误类型覆盖服务"的代码会开始报错（这是 DESIGN I5 要求的行为）。
- **`ctx.inject` 改为按插件类型分键**：`registry.size()` 会把每个 inject 计为独立 runtime；
  `replace(Named("injected"))` 不再一次重绑所有 inject。
- **注入门要求 provider 为 `Active`**：provider 在 `apply` 内 provide 后若继续耗时工作，
  consumer 会等 provider `Active` 后才激活；provider `apply` 失败时 consumer 从不 apply。
  `get`/`get_named`/`get_any` 读取路径不受影响。
- **`dispose_all` panic 优先**：同时出现 `Err` 与 panic 时调用方收到 panic（error 只记日志）。
- **panic 的 accessor 现在读作 absent**：untyped 通道（`get_named`/`get_any`）返回
  `None`，不再返回占位 `Arc<()>`；typed `get` 此前已因 downcast 失败返回 `None`，两者一致。
- **`MAX_PLUGIN_DEPTH` 口径改为 fiber 深度**：深层 context 链不再消耗加载深度配额。
- **`update` 的 waterfall 顺序与 veto 语义**：现在严格按 `internal/config` → schema 校验
  → `internal/update` 执行；hook 不调 `next` 的纯 veto 返回 `Ok(())` **且不 restart、不改
  raw config**（0.4.1 会在 veto 前就替换 raw）；成功的 update 始终提交新的 raw config
  （调用方传入值或 hook 校验通过的 replacement），与 TS 参考一致。新增 additive 入口
  `Fiber::update_with_no_save` / `update_async_with_no_save`（仅新增方法，现有
  `update`/`update_async` 行为不变）。
- **取消级联恢复语义（F1/F4/F5，可观察变化）**：取消 `dispose_async`/`refresh_async` 后
  不再可能永久滞留 `Unloading`——`when_idle()`（含裸 `Registry::when_idle()` 公共 API）
  在等待期会持续重试恢复，`refresh_async()`/`dispose_all()`/根 dispose 重试也会恢复；
  恢复中途取消/panic 不会残留闩锁或丢帧（RAII + ReparkGuard）。**同步 `refresh()` 不做
  恢复**（这是刻意设计：它可能在 reactor 线程上被调用，同步 drain async cleanup 会死锁）。
  取消撞在飞 async cleanup 时该 future 仍不可恢复（A-9）。
- **`AsyncDisposer::dispose_blocking` 现在真正 join**：若另一个调用者正在 drain，它会阻塞到
  该 teardown 完成（0.4.1 会立即返回）；同步关闭路径可能因此耗时更长。它还会**重试被取消
  teardown 恢复的未启动 cleanup**（S-1；0.4.1 的 `dispose_blocking` 会误早退并永久丢失）；
  而在**同一个 disposer 的 cleanup 内**调用它 `dispose_blocking()` 会**立即返回**（自 join
  短路，与 0.4.1 一致——0.4.2 早期实现会死锁，本版已修正）。
- **取消 fiber unload 现在会完成 teardown（A-9，可观察变化）**：0.4.1 取消
  `dispose_async`/`refresh_async` 会把未启动 cleanup 放回并把 fiber 留在 `Unloading`
  （或丢掉部分清理）；0.4.2 早期实现则回退到旧状态形成假 `Active` 半拆解。0.4.2 正式版起
  取消会**完成**：同步 cleanup 立即执行、异步 cleanup 转入 deferred 由下一次 dispose/drain
  完成、fiber 落到 `Pending`/`Disposed` 并 `remove_owner`/notify idle。注意 `AsyncDisposer`
  句柄层面仍是「恢复未启动 cleanup、下次 dispose 重试」（S-1），两种粒度不要混淆。
- **replace / replace_plugin 的 reload panic 现在返回错误而不是 panic**：0.4.1 中旧实现的
  cleanup panic 会从 `Registry::replace`/`Fiber::replace_plugin` 穿出；现在调用方收到
  `Err(CordisError::PluginFailed("… reload panicked: …"))`，必要时执行回滚（回滚失败才
  `Rollback`）。用 `catch_unwind` 包裹 replace 探测该故障的代码需改为读错误。
- **`is_idle`/`when_idle` 现在覆盖「注册 → 激活」窗口**：插件创建与激活期间不算 idle；
  在 `internal/plugin` create hook 内**同步**调用 `when_idle()` 会等待自己（0.4.1 会立即
  返回）——不要在 create hook 里同步等 idle。同一窗口还存在两条边界：create hook 内的
  `fiber.await_settled()` 会立即 `Ok(())`（fiber 仍是 `Pending`），且
  `stalled_fibers()` 看不到注册窗口。
- **`null` 默认值的注入行为**：`default_value(name, Value::Null)` 不再让 `validate({})`
  失败；未声明类型的键缺省注入 `null`，而显式声明了 `property(name, JsonType::…)` 的键
  缺省保持 **absent**（可选字段；0.4.1 会直接校验失败）。
- **`restart`/`restart_async` 现在发 `internal/status` 的 `Active -> Unloading`**：
  status 观察者会比 0.4.1 多收到这一条事件（此前缺失）。
- 无公开 API 删除或签名破坏（`ServiceEntry` 等内部结构不属于公开 API）。

### Docs

- `spec/CORDIS-CORE.md`：勘误 isolate label 的 FNV-1a 说法（实际是 `Registry` 的
  label→计数器映射，见 `src/registry.rs::labeled_scope_id`）、waterfall 无 `Bail`
  （veto = 不调 `next`）、`EventOptions` 补 `once`、CORE-006/016 测试名
  `service_gate_controls_dependents` → `service_check_gates_dependents`；新增/修订
  v0.4.2 语义条目（async waterfall fail-closed、provider-ACTIVE 注入门、set 类型校验、
  update raw/veto 与 `no_save` 提示、additive `update_with_no_save`、refresh RAII、
  dispose_blocking join、`null` 默认值不声明类型且按声明类型决定是否注入、
  accessor panic→None、inject 身份粒度、`MAX_PLUGIN_DEPTH` 按 fiber 深度、完整 replace
  回滚、`dispose_all` panic 优先）；版本状态升到 v0.4.2。
- `docs/CORDIS-RUST.md`：新增 §2.3「Patched in v0.4.2」；修正 update 顺序、provider 门、
  async waterfall、accessor、`MAX_PLUGIN_DEPTH`、inject key、`dispose_blocking` 描述；
  panic 表补 refresh/retire 与 children sweep 行，waterfall 行明确 sync/async 都 re-raise。
- `docs/GUIDE.md`：日志默认无 exporter（需 `ctx.log_exporter(TracingExporter)`）、
  `intercept` 只影响 service/logger 的 `resolve_config`、`null` 默认值示例、错误表补
  `ServiceTypeMismatch`/`Rollback`、update/veto/no_save（含 `update_with_no_save`）与
  关闭语义、严格注入门、注册窗口内等待 `when_idle`/`await_settled` 的边界警告、
  `refresh` 有界不动点、registry 查询快照语义、深依赖链分层 dispose 建议、replace reload
  panic 说明、取消 teardown 重试（S-1）、自 join 短路（S-2）与取消 fiber unload 的
  「完成/延迟 async」语义（A-9，两种粒度的区分）。
- `docs/KERNEL-ISOLATION.md`：waterfall panic 承诺与实现统一（sync/async 都 re-raise，
  internal hook 仍 contained）；生命周期互斥与版本标注更新；新增注册窗口使用契约
  （create hook 内同步等 `when_idle` 会自我死锁、`await_settled`/`stalled_fibers` 的边界）；
  新增 v0.4.2 panic 遏制矩阵（含 replace/replace_plugin 的 reload panic → `PluginFailed` →
  回滚）与「极深依赖链 unload 递归栈溢出」的已知限制（含实测栈深与分层 dispose 建议）。
- `docs/CORDIS-RUST.md`：补充 replace/reload panic 转错误（D-2）、`refresh` 有界不动点
  （64 轮 + 正/逆序推进差异）、registry 快照非原子（诊断语义）与深依赖链 teardown 边界。
- `spec/CORDIS-CORE.md`：§2.6 replace 的 reload panic 遏制、registry 快照诊断语义；
  §2.8 `refresh`/`refresh_async` 有界不动点说明。
- `docs/DESIGN.md`：I5 注明由 `ctx.set` 的类型校验落实。
- `README.md`：安装改为 crates.io `zeokeel-cordis = "0.4.2"` / `use zeokeel_cordis`；
  质量章节改为测试口径说明 + 0.4.2 加固摘要；补充"默认无日志导出器"说明。

### Known limitations

- **极深依赖链的 dispose 递归栈溢出（既有边界，0.4.1 同型，本版未修）**：依赖链 unload 走
  `take_effects → retire_unready → unload` 相互递归，栈深度约等于依赖链层数；2 MiB worker
  栈实测 debug 约 480 层、release 约 2200 层即 abort（栈溢出不可捕获）。数百~数千层的依赖
  链请分层/分批 dispose（先拆子树、逐批 `dispose()`，或拆成多棵浅树）。
- 其余见 README「已知限制」与 `docs/KERNEL-ISOLATION.md`；本版未改变进程内不可遏制的故障
  边界（`panic=abort`、栈溢出、OOM、UB、死循环等）。

## [0.4.1] - 2026-10-28

### Fixed

- `src/l2.rs` 模块文档里的 `Fiber::heartbeat` 内联链接在 `supervision` feature 下无法解析
  （0.4.0 的 docs.rs 页面把它渲染成纯文本）；改为 `crate::Fiber::heartbeat`。
- CI 与 release workflow 增加 `cargo doc --no-deps --all-features` 且
  `RUSTDOCFLAGS=-D warnings` 的闸门，与 docs.rs 实际使用的构建配置（`all-features`）
  保持一致，避免只有默认 feature 配置通过文档检查。

## [0.4.0] - 2026-10-28

### Changed（破坏性）

- **crate 名与导入路径统一**：库目标名由 `cordis` 改为 `zeokeel_cordis`，与包名
  `zeokeel-cordis` 及项目名一致。用户侧代码由 `use cordis::...` 改为
  `use zeokeel_cordis::...`。这是 0.4.0 唯一的破坏性变更（0.3.0 发布后无下游依赖，
  实际影响为零）。
- 日志 `target` 前缀随 crate 名统一为 `zeokeel_cordis::*`（`fiber`、`internal`、
  `config`、`events`、`context`、`logger`、`reflect`、`containment`），方便按 crate
  名过滤日志。
- docs.rs 改用全部 feature 构建（`[package.metadata.docs.rs] all-features = true`），
  `supervision` feature 的 L2 监督 API 也会出现在在线文档
  <https://docs.rs/zeokeel-cordis> 中。

### Docs

- README、`docs/GUIDE.md`、`docs/CORDIS-RUST.md` 的安装与导入示例同步为新名称；
  README 增加 crates.io / docs.rs 徽章。

## [0.3.0] - 2026-10-28

### Added

- 原地插件替换：`Registry::replace` / `replace_async`、`Fiber::replace_plugin(_async)`
  （CORE-048）。
- 关闭路径 `Context::dispose_all()` / `Registry::dispose_all()`（CORE-051）。
- 静默检测 `is_idle` / `when_idle`（CORE-049）与协作取消
  `Fiber::cancel` / `Context::checkpoint`（CORE-050）。
- 可选 `supervision` feature（默认关闭）：`IsolationCapabilities` 能力自省、
  per-fiber `FiberQuota`（effects/listeners/child fibers）、`heartbeat` 与
  `stalled_fibers` watchdog 观测。
- 严格构建配置：`panic=unwind` 编译期 guard、release `overflow-checks`、
  `codegen-units=1` + thin LTO；CI 增加 `cargo audit` 与 Dependabot。

### Fixed

- `internal/update` hook 替换出的配置在 `update` 返回前用 schema 校验；非法时回滚且不重启。
- scope id 命名空间冲突（`isolate_labeled` 与 `isolate` 共享同一分配器，R17）。
- `dispose_all` 遇清理 panic 不再中断，尝试全部 fiber 后抛出首个错误。
- 子 fiber 归属、取消安全卸载、生命周期互斥（CORE-044…047）的对抗性回归修复。

### Docs

- 中文 README、设计文档（DESIGN）、使用指南（GUIDE）、规范 CORE-001…051。
- 缺陷清单与逐轮修复/对抗记录移至 `docs/dev/HARDENING-LOG.md`。

### Known limitations

见 README「已知限制」与 `docs/KERNEL-ISOLATION.md`。

## [0.2.0] - 2026-10-24

### Added

- 异步插件：`AsyncPlugin`、`plugin_async`、`*_async` 生命周期 API；
  `BlockingExecutor` 钩子与 `exec::block_on` / `yield_time` 辅助。
- panic 隔离与失败回滚：激活失败、清理、监听器、内部 hook、schema、导出器的 panic
  被逐个遏制。
- `ServiceKey<?Sized>` 与 `*_any` 服务 API（trait object 服务）。
- 根 Context ↔ FiberData 强引用环修复。

## [0.1.0] - 2026-10-20

### Added

- 初始内核：`Context` 与作用域（`extend`/`isolate`）、`ServiceKey` 服务注册与依赖注入、
  fiber 状态机、effect 逆序清理、事件（emit/bail/parallel/serial/waterfall）、
  插件 registry 与配置 schema、日志服务。

[Unreleased]: https://github.com/zeokeel/zeokeel-cordis/compare/v0.5.0...HEAD
[0.5.0]: https://github.com/zeokeel/zeokeel-cordis/releases/tag/v0.5.0
[0.4.4]: https://github.com/zeokeel/zeokeel-cordis/releases/tag/v0.4.4
[0.4.3]: https://github.com/zeokeel/zeokeel-cordis/releases/tag/v0.4.3
[0.4.2]: https://github.com/zeokeel/zeokeel-cordis/releases/tag/v0.4.2
[0.4.1]: https://github.com/zeokeel/zeokeel-cordis/releases/tag/v0.4.1
[0.4.0]: https://github.com/zeokeel/zeokeel-cordis/releases/tag/v0.4.0
[0.3.0]: https://github.com/zeokeel/zeokeel-cordis/releases/tag/v0.3.0
[0.2.0]: https://github.com/zeokeel/zeokeel-cordis/releases/tag/v0.2.0
[0.1.0]: https://github.com/zeokeel/zeokeel-cordis/releases/tag/v0.1.0
