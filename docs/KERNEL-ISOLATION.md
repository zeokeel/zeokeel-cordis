# 内核隔离保证（Kernel isolation guarantees）

> **范围声明**：本 crate 是**纯 Rust、进程内、静态链接**的 Cordis 内核。
> 它不提供也**不会**引入 ABI/FFI、cdylib/dylib 动态加载、进程隔离宿主或 WASM 运行时；
> 下面 L2–L4 只是写给 embedder 的**建议方案**（在 crate 之外实现），不是本 crate 的
> 实现范围。内核自身只承诺 §3 所列的 in-process fault containment。

> 适用版本：v0.4.4（v0.2.0 基线 + v0.3.0 加固 + v0.4.2/0.4.3/0.4.4 的遏制与语义修正）。
> 本文回答一个问题：**"插件崩溃会不会拖垮内核？"** 答案分两半——**可捕获的 panic
> 不会；Rust 进程内不可恢复的故障会**。本文给出实测证据、我们据此作出的承诺，以及
> 要做到"Linux 内核级"所必需的外层隔离方案。

## 1. 结论（一句话）

Cordis 在进程内提供的是 **unwinding panic 的故障遏制（fault containment）**，不是沙箱
（sandbox）。在 `panic = "unwind"` 且插件不主动 abort/不越界（见 §4）的前提下，一个插件
的 panic 只会让它自己的 fiber 回滚/失败，其它插件与内核继续运行（**waterfall 中间件是
唯一例外**：按 fail-closed 语义中止链并重抛给调用者，见 §3 第 5 条）；但 `panic="abort"`、
栈溢出、OOM、`process::abort()`、插件 `unsafe` UB、死循环/死锁、线程/fd 泄漏等
**进程内无法隔离**，会杀死或卡死整个进程。要把不可信插件做成"Linux 模块级"的可靠性，
必须使用**进程隔离（L3）**或 **WASM 沙箱（L4）**。

## 2. 实测故障模式（v0.2.0 探针，`target/scratch/redteam-isolation/`）

| # | 场景 | 结果 | 内核状态 | 进程内可遏制？ |
|---|---|---|---|---|
| 1 | `panic="abort"` 构建 + 插件 `panic!` | SIGABRT 134 | 进程死亡 | 否（catch_unwind 失效）|
| 2 | `panic=unwind` + `apply` panic | exit 0，fiber `Failed` | 内核存活，新插件正常 | **是** |
| 3/4 | 无限递归（主栈 / 64 KiB 线程） | SIGABRT 134（"has overflowed its stack"）| 进程死亡 | 否 |
| 5 | 200k 深 context 链 Drop | 134 栈溢出（Drop 中）| 进程死亡 | 否 |
| 6 | 递归 `ctx.plugin` 5000 层 | 134 | 进程死亡 | 否 |
| 7 | `ulimit -v` 下 OOM | 134 allocator abort | 进程死亡 | 否 |
| 8 | 死循环 | 挂起，外部 SIGKILL 137 | 无线程可安全杀死 | 否 |
| 9 | 互斥锁死锁 | 挂起 137 | 同上 | 否 |
| 10/11 | 插件 `process::abort()` / `exit(0)` | 134 / 0 | 进程死亡或假成功 | 否 |
| 12 | 插件 `name()` 在内核锁内 panic（v0.2）| panic 被吞，但锁中毒 | `state()` 谎报、dispose 失败、清理泄漏 | 修复中：已把用户代码移出锁 + poison 策略待统一 |
| 13 | `BlockingExecutor` 在 unwind 中 panic | abort 134（双 panic）| 进程死亡 | 否 |
| 14 | async listener panic（`parallel_async`，v0.2）| panic 逃逸 API，listener 永久 panic | 内核可用但 dispatch 坏 | 修复中：async lane 已加遏制 |
| 15 | cleanup panic（v0.2）| dispose 重抛前未收尾 | fiber 卡 `Unloading`，ctx 不释放 | 修复中：先 `finish_unload` 再重抛 |
| 16 | 插件 `unsafe` 越界写 | SIGSEGV 139 | 进程死亡 | 否（UB 不可遏制）|
| 17 | 插件泄漏 200 线程 / 1004 fd | 无追踪、`dispose()` 不回收 | 资源泄漏 | 否（进程内无配额）|

> 数据来源：`target/scratch/redteam-isolation/REPORT.md` 与 `run.log`（exit code：
> 134=abort/栈溢出/OOM，137=外部 SIGKILL，139=UB SIGSEGV，0=可捕获）。
> 表中第 12–15 行记录的是 v0.2.0 当时的在途状态；v0.3.0+ 的遏制策略以 §3 为准
> （v0.4.2 进一步把 sync/async waterfall 的 panic 策略统一为 fail-closed re-raise）。

## 3. 我们能承诺什么（写进 README/spec 的措辞）

**能承诺**（L1，`panic=unwind`，插件遵守 §4 契约）：

1. 内核**调用**的插件回调（`apply`/`apply_async`、effect body 之外的 cleanup、
   listener、internal hook、exporter、accessor、schema，以及 v0.4.3 起 Plugin 元数据回调
   `name`/`key`/`inject`/`config_schema`）中的 unwinding panic 被遏制：该调用失败/被跳过
   并记录日志，不会 unwind 穿出内核公共 API（元数据回调按 C2 规则确定性降级）。
2. 激活失败（Err 或 panic）**回滚**该 fiber 已注册的 effect/service/listener，fiber 进入
   `Failed`，不污染依赖方（provider 的 service 随回滚移除）。
3. 一个 fiber 的失败/处置不改变其它 fiber 的状态；dispose 的清理按逆序全量执行，清理
   panic 在**收尾完成后**才重抛给定调用者。0.4.2 起，由 refresh/retire 路径触发的
   依赖方卸载会把 panic **记录并遏制**（provider 不会卡在 `Unloading`），而直接调用
   `Fiber::dispose()` 仍按上述契约重抛。
4. 当 `panic=unwind` 时，**插件代码中的 `panic!` 不杀死进程**（测试用例：`hardening::
   panicking_apply_is_isolated_and_rolled_back`、`containment::*`）。
5. **waterfall 中间件是唯一的例外**：其 panic 会中止整条中间件链（终端行为不执行）
   并**重抛给调用者**（fail-closed，绝不"panic 后放行"）；sync 与 async lane 一致
   （0.4.2 起 async 也 re-raise，不再吞成 `Value::Null`）。在激活路径内调用时，它表现为
   该 fiber 的激活失败并按第 2 条回滚。
6. **替换/重载的 reload panic 被遏制为错误**（v0.4.2）：`Registry::replace` /
   `replace_async` / `Fiber::replace_plugin(_async)` 的 reload 会 drain 旧实现的 cleanup
   （embedder 代码）。cleanup panic 现在被捕获并转成
   `Err(CordisError::PluginFailed("<op>: reload panicked: …"))`，随后执行完整回滚
   （`Registry` 路径把已 reload 的 fiber 用旧实现重新加载，单 fiber 路径恢复旧
   plugin/runtime）；`Registry` 路径上只有**回滚自身失败**才返回
   `CordisError::Rollback`。调用方不再收到 panic。
7. **取消语义按粒度区分（v0.4.2：S-1/S-2 + A-9）**：
   - **`AsyncDisposer` 句柄**（单个 effect 的 async cleanup 批次）：`AsyncDisposer::dispose`
     的 future 被 drop 时，未启动的 cleanup 会被恢复；下一次 `dispose()` /
     `dispose_blocking()`（含 `Fiber::dispose()` 的同步安全网）会重新取得 teardown 并
     **按原逆序重试**（S-1）。同一 disposer 的 cleanup 内调用它自己的
     `dispose_blocking()` 会**立即返回**（自 join 短路，避免自等死锁；0.4.1 同行为），
     而跨线程/其它 disposer 的调用仍会阻塞到 teardown 真正结束（S-2）；在 cleanup 内 `await` 自己的 `AsyncDisposer::dispose` 同样短路（R2-1，drain-frame 判据，不会误伤同线程的独立 joiner）。
   - **fiber 级 unload**（`dispose_async` / `refresh_async`）：future 被 drop 时现在会
     **把 unload 做完**而不是恢复旧状态（A-9）：同步 cleanup 立即执行，异步 cleanup 进入
     fiber 的 deferred 列表、由下一次 dispose/drain 收尾，`finish_unload` 落到目标态
     （重载/依赖触发的 unload → `Pending`，dispose → `Disposed`）并 `remove_owner`/notify idle
     ——不会再出现永久 `Unloading` 或假 `Active` 的半拆解态。
8. **深依赖链卸载不增长调用栈（v0.4.3，R3-A1）**：卸载改为**两阶段保序迭代级联**——先按
   provider-before-dependent 顺序 *begin* 所有传递不可用 fiber（标记 `Unloading`、撤服务、
   取 effects），再逆序 *drain*；ownership 子树用显式 DFS 栈收集。调用栈 O(1)，
   清理顺序仍是 dependents-before-provider。2 MiB 线程 depth 800：**0.4.3 debug 与
   release 均通过**；0.4.2 对照为 debug 约 **480 层**即 abort（depth 800 必失败）、release
   约 2200 层才 abort（depth 800 可过）。实际受 `MAX_PLUGIN_DEPTH = 256` 限制的加载链远小于
   此。此前「极深依赖链 dispose 递归栈溢出」的已知限制不再成立。

**panic 遏制矩阵（v0.4.3）**

| 边界 | 行为 |
|---|---|
| `apply`/`apply_async` panic | 遏制：激活失败 + 回滚，fiber `Failed`（CORE-028/029） |
| 直接调用 `dispose`/`dispose_async` 时的 cleanup panic | 收尾完成后重抛首个 panic（CORE-030/011） |
| 由 refresh/retire 路径触发的依赖方 unload panic | 记录并遏制；provider 仍到达目标态（0.4.2） |
| 父 fiber sweep 子 fiber 时的 panic | 逐子遏制、继续 sweep，父落目标态后重抛首个 payload（0.4.2） |
| `Registry::replace` / `replace_plugin` 的 reload panic | 遏制为 `Err(PluginFailed("… reload panicked: …"))` + 回滚；回滚失败才 `Rollback`（0.4.2） |
| Plugin 元数据回调（`name`/`key`/`inject`/`config_schema`）| 遏制并确定性降级（v0.4.3，C2）：`name`→`"<panicked plugin>"`、`key`→`Named("<panicked plugin>")`、`inject`→`&[]`、`config_schema`→`None`，均记录日志；不再穿出 `ctx.plugin`/replace。注意：坏 key 插件共享同一个 runtime，`keys`/`replace`/`delete` 无法单独定位（best-effort，非受支持配置）|
| listener / internal hook / exporter / accessor / schema / availability check | 记录后遏制，各自回退（继续 dispatch、跳过、视为 absent/unavailable 等） |
| waterfall 中间件 panic | **中止整条链并重抛**（fail-closed，唯一例外；两 lane 一致） |
| `dispose_all` 同时有 Err 与 panic | panic 优先：记录 error 后重抛首个 payload（0.4.2） |
| `Drop` 中的 cleanup panic | 记录/尽力清理；unwind 期间的双 panic 会 abort（见下） |

**不能承诺**（进程内物理上做不到）：

- `panic=abort` / `-C panic=abort` 构建：任何 panic 直接 abort；`catch_unwind` 失效。
- 栈溢出（插件自身递归/深 Drop）、OOM/allocator abort、`process::abort/exit`、SIGSEGV
  （插件 `unsafe` UB）、双重 panic（Drop 中 panic）。注意：**依赖链卸载本身已不再是栈溢出
  来源**（v0.4.3 改为两阶段迭代级联，见 §3 承诺与 §5.5）。
- 死循环、死锁、线程/fd/内存无上限泄漏——没有安全的方式在进程内终止 Rust 线程。
- 插件 `unsafe` 代码的内存破坏：同一地址空间内无法隔离。

## 4. 使用契约（embedder 必须遵守）

- **必须**用 `panic = "unwind"` 构建（release 默认；不要在 `[profile.release]` 里设
  `panic = "abort"`）；WASM/`wasm32-unknown-unknown` 默认 abort，此时 §3 的承诺不成立。
- 不要在内核调用栈里调用 `process::abort()/exit()`；不要用会 SIGSEGV 的 `unsafe`（或把
  这类插件放到 L3/L4）。
- 不要在插件里无限阻塞；需要长任务时用可取消的任务 + 心跳（见 §6 路线图）。
- `Fiber::dispose()` 是阻塞边：async 宿主必须安装 `BlockingExecutor`（或只用
  `dispose_async()`），否则 current_thread runtime 会死锁（已知限制）。
  `AsyncDisposer::dispose_blocking()` 会 join 在飞 teardown，并在 teardown 被取消后
  **重试**恢复的未启动 cleanup（S-1）；但在该 disposer **自己的 cleanup 内**调用它
  `dispose_blocking()` 会立即返回（自 join 短路，S-2，避免自等死锁）。
- 同一 fiber 的 `dispose/restart/update` 由内核**互斥**（v0.3.0+）：跨线程并发会得到
  `CordisError::LifecycleBusy`，宿主无需自己加锁，但需要决定重试/串行化策略。
- **`when_idle()` 只排除调用线程自身的注册帧（v0.4.3，R3-A3/C4）**：注册→激活窗口计入
  in-flight（FIX-11b），但从 `internal/plugin` create hook 内调用 `when_idle()` 时，内核按
  **创建线程**记录注册归属并只排除调用者自己的注册——因此 hook 内可以等待（4 ms 级返回），
  不再自锁；其它线程/外部 waiter 仍按 FIX-11b 等待整个窗口；`is_idle()` 保持严格（不排除
  任何注册）。**边界**：跨线程环形注册依赖（A 注册等 B、B 注册等 A）仍可能互等，内核不检测。
- **取消的 async 级联自动、可重试地恢复（F1/F4/F5，v0.4.3）**：取消
  `dispose_async`/`refresh_async` 可能把非根 dependent 短暂 park 在 `Unloading`。
  - 恢复入口：`when_idle()`（**`Context::when_idle()` 与裸 `Registry::when_idle()` 公共
    API 两侧都是等待期持续恢复**）、`refresh_async()`、`dispose_all()`、以及根 fiber 的
    `dispose()`/`dispose_async()` 重试；恢复到原级联目标（依赖方 `Pending`、ownership 子
    `Disposed`），未启动的 deferred async cleanup 恰好执行一次。**同步 `refresh()` 刻意
    不恢复**（可能在 reactor 线程调用，同步 drain async cleanup 会死锁，改为跳过
    `Unloading` 帧）。
  - 安全性（F4）：单恢复者闩锁为 **RAII guard**（取消/panic 不残留）；恢复批量取帧用
    **`ReparkGuard`/`RecoveryBatch`**，中途取消把未处理/在飞帧重新 park（不丢帧）。
  - 有界与延迟：恢复幂等、去重、有界（按并发 parked 帧数）；取消 unwind 中或另一 unload
    进行中时延迟到下一次根操作。
  - 取消边界：若取消恰好撞上正在执行的 async cleanup，该在飞 future 不可恢复（A-9 既有
    边界），但不会留下永久 `Unloading`；恢复前 `is_idle()` 仍为 `false`（严格）。
- **注册窗口的其它边界（v0.4.2/0.4.3 不变）**：create hook 内新 fiber 仍是 `Pending` 且没有 fiber 级
  in-flight 操作，因此 `fiber.await_settled()` 会**立即返回 `Ok(())`**；
  `Registry::stalled_fibers()`/`Context::stalled_fibers()` 也**看不到**注册窗口（只检查
  fiber 级 `pending_ops` 与 `Loading`/`Unloading`）。不要把"注册中"误判为"已稳定/已卡死"。

## 5. 分层隔离（要做到"Linux 内核级"必须叠加）

| 层 | 机制 | 买 到什么 | 成本 |
|---|---|---|---|
| **L1** 进程内遏制（本 crate 已提供）| unwind 边界 + 回滚 + Drop 规则 + 无锁内用户代码 | 可捕获 panic 不扩散；故障归因到 fiber | 0 |
| **L2** 线程级监督 | 每插件/每组一个监督者：心跳、deadline、协作取消（checkpoint）、资源计量 | 定位、降级、优雅重启；**不能**杀线程 | 低-中 |
| **L3** 进程隔离（推荐给不可信插件）| 每插件组一个子进程 + IPC：生命周期/配置/declarative effects 走 RPC；SIGKILL + rlimit/cgroup/seccomp；崩溃报告/自动重启 | abort/SEGV/UB/OOM/死循环全部可恢复；真正的"插件崩溃不拖垮内核" | 高（IPC、序列化、内存、延迟）|
| **L4** WASM 沙箱 | wasmtime/wasmer + fuel/时间/内存上限、能力导入（fs/net 白名单） | 第三方代码的安全边界；无 `unsafe` | 高（宿主接口、性能、无 std 线程）|

> 判断标准：如果插件来自**可信仓库、同源代码** → L1+L2 足够；如果插件是**第三方/可被
> 攻破** → 必须 L3 或 L4。内核本身不应假装是沙箱。

## 5.5 编译期强制与"最严格构建"（v0.3.0+）

编译器能强制的是**可判定的前置条件**，不是运行时资源/停机问题。本项目已把能强制的部分全部
变成**编译错误或 CI 硬门禁**：

| 威胁 | 能否编译期解决 | 本项目的处理 | 残余 |
| --- | --- | --- | --- |
| `panic="abort"` 构建 | ✅ 可判定 | `#[cfg(panic = "abort")] compile_error!`；CI 每次反证 `RUSTFLAGS="-C panic=abort" cargo check` 必须失败 | 无（构建层禁止）|
| 内核自身 `unsafe` UB | ✅ 可判定（本 crate）| crate 级 `#![forbid(unsafe_code)]` | 依赖/宿主代码的 unsafe 不在覆盖范围（用 cargo-audit/geiger 做供应链审计）|
| 整数溢出（静默回绕）| ✅ 可判定 | release `overflow-checks = true` → 溢出 panic，可被 containment 观测 | 不是"消除崩溃"，而是"把静默错误变成可观测错误" |
| 异步死锁（持锁跨 await）| ⚠️ 常见模式可 lint | clippy `await_holding_lock = deny`、`mutex_integer = deny`，CI `-D warnings` | 通用死锁不可判定，仍需 L2 watchdog/超时 |
| 栈溢出（插件自身递归）| ❌ 不可判定 | 最大插件**加载**深度 `MAX_PLUGIN_DEPTH=256`（0.4.2 起按 fiber 父链计数，root 级 plugin 起始 depth=1，约 254 层嵌套后拒绝）；运行时的卸载已迭代化（下条），但插件自身递归/深 Drop 仍可能爆栈 | 需 L3/L4 |
| 深依赖链 unload 栈溢出 | ✅ 已修复（v0.4.3）| 两阶段保序迭代级联（先 begin 后逆序 drain，ownership 子树显式 DFS 栈）：栈 O(1)。2 MiB 线程 depth 800 在 0.4.3 **debug+release 均通过**；0.4.2 对照 debug 约 480 层即 abort、release 约 2200 层才 abort。`MAX_PLUGIN_DEPTH` 仍只限制加载深度 | 剩余风险仅为插件自身递归（L3/L4）|
| 深依赖链卸载顺序 | ✅ 已保证 | begin 阶段 provider-before-dependent、drain 阶段逆序，等价 dependents-before-provider；级联轮数有 `fiber_count + 2` 固定点上界，超限的依赖方保持 Active 可 retry/refresh（R3-A1） | 无 |
| OOM / 分配器 abort | ❌ 不可判定 | 无编译期方案（Rust 分配错误默认 abort，不能 unwind）| 需 L3 cgroup/rlimit 或 L4 内存上限 |
| 死锁 / 无限循环 | ❌ 停机问题 | 协作 checkpoint + `when_idle` + 宿主 watchdog（L2）| 内核不能强杀线程 |
| 外部 kill / SEGV / UB | ❌ | 进程边界外处理 | L3/L4 |

最严格构建配置（`Cargo.toml` 已内置）：

```toml
[profile.dev]
panic = "unwind"

[profile.release]
panic = "unwind"
overflow-checks = true
codegen-units = 1
lto = "thin"
```

CI 额外执行：

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
cargo build --release
RUSTFLAGS="-C panic=abort" cargo check --lib   # 必须失败
```

> 结论：纯 Rust 内核可以把"panic 策略 / unsafe / 溢出 / 常见锁误用"变成**编译期或 CI 期的硬错误**，
> 但无法把栈溢出、OOM、死锁、无限循环、依赖 UB 变成编译错误。要做"插件崩溃绝不拖垮内核"，
> 这些仍然必须在 L3 进程隔离或 L4 WASM 里解决。

## 5.6 L2 可选 API（`supervision` feature，默认关闭，v0.3.0+）

| 能力 | API | 说明 |
|---|---|---|
| 能力自省 | `IsolationCapabilities::detect()` | panic 策略 / 遏制能力 / unsafe / 进程 & WASM（恒 false，属 embedder）|
| 资源配额 | `FiberQuota` + `Context::set_quota` / `Registry::set_quota` | effects / listeners / child fibers 上限，超限 `QuotaExceeded` 并回滚；**按活资源计数**（v0.4.3，S1）：effects 释放后清死条目、listener 槽在用户 disposer/`once` 自动移除/fiber 卸载任一路径恰好释放一次、child 按 live 弱引用统计。**最小可用值：`max_effects` 建议 ≥2**——内核自身占用内部 effect（root 的 logger provide effect、每个动态加载子 fiber 的 ownership effect），配额统计的正是**含内部 ownership effect 的 live effects**；`max_effects = 1` 会让内核自身无法加载插件 |
| 心跳 | `Fiber::heartbeat()` / `Context::heartbeat()` | 长任务上报活动 |
| watchdog 观测 | `Registry::stalled_fibers(timeout)` / `Context::stalled_fibers(timeout)` | 返回 `StalledFiber { fiber, idle_for }`，**不强杀**；只看 fiber 级 in-flight，**不覆盖**注册窗口（v0.4.2）|
| 静默等待 | `Registry::when_idle()` / `Context::when_idle()` | 关闭/健康检查；覆盖注册→激活窗口（FIX-11b），v0.4.3 起**排除调用线程自身的注册**（create hook 内可安全等待，不自锁）；**两侧公共 API 均 recovery-aware，等待期持续重试恢复被取消级联 park 的 `Unloading` 帧**（F1/F5；RAII 闩锁 + ReparkGuard 不丢帧，F4）；同步 `refresh()` 刻意不恢复；`is_idle()` 仍严格；跨线程环形注册依赖仍可能互等 |
| 协作取消 | `Fiber::cancel()` + `Context::checkpoint()` | supervisor 可先 cancel 再重启/隔离 |

CI 另加 `cargo audit`（RustSec）作业，对依赖做供应链漏洞门禁。

## 6. 支持 L2/L3 所需的 kernel API（路线图）

1. ~~启动时断言 `panic=unwind`~~ → **已升级为编译期强制**（v0.3.0+，见 §5.5）；
   ✅ `IsolationCapabilities::detect()` 已提供运行时自省。
2. ✅ 观测：`when_idle()`、`Fiber::last_activity()`/`idle_for()`、`Fiber::heartbeat()`、
   `Registry::stalled_fibers(timeout)` 已提供（§5.6）。
3. 协作取消：`Context::checkpoint() -> Result<(), Cancelled>`、`Fiber::cancel()`、
   `dispose_cancelling(Duration)`。
4. 类型化失败 + `on_fiber_failure(FailureKind::{Err,Panic,AbortedImpossible})` 供 supervisor 决策。
5. 锁纪律：注册时缓存 `Plugin::name/key`，任何用户代码不在内核锁内执行；v0.4.3 起元数据回调
   的 panic 也被遏制并降级（C2，`name`→占位名、`inject`→`[]`、`schema`→`None`）。`Fiber::poisoned()`。
6. 收尾保证：清理 panic 也必须到达 `Disposed/Failed` 并释放 ctx（v0.3 已做）。
7. `BlockingExecutor::block_on -> Result<(), _>`：不允许 panic；文档化双 panic abort。
8. ✅ 部分：per-fiber 配额（effects/listeners/child fibers）已实现；`spawn_guarded`/`open_guarded`
   等 IO/线程级计量仍需 embedder 提供（内核不跟踪线程/fd）。
9. `PluginManifest`：`{name, inject, config_schema, isolation: Thread|Process|Wasm, limits,
   capabilities}`；进程隔离用 RPC 替代 `Arc<dyn Plugin>`。
10. 崩溃报告钩子 + 自动重启策略（L2/L3）。

## 7. 复现与验证

- 隔离探针：`target/scratch/redteam-isolation/`（`iso` = unwind 构建、`aborter` = abort 构建，
  `run.sh` 逐个场景，`timeout` 作 watch-dog；该目录是 gitignored 的本地探针产物）。
- 内核内遏制测试：`tests/containment.rs`（11 个边界用例）、`tests/hardening.rs`
  （CORE-028…039）、v0.3 新增的退化测试，v0.4.2/v0.4.3 的加固回归套件
  `tests/regression_core.rs` / `tests/regression_semantics.rs` / `tests/l2_defenses.rs`，
  以及 v0.4.4 的架构护栏 `tests/kernel_boundary.rs`（10 个测试 = 4 个真实断言 + 6 个自测）
  （部分需要内部状态的用例以 `src/fiber.rs` 内的 unit test 断言，见 CHANGELOG）。
  深依赖链回归：`regression_core::r1_deep_plugin_chain_activates_on_a_2mib_thread`
  （2 MiB 线程上的深层 plugin 链激活 + 迭代卸载；加载深度受 `MAX_PLUGIN_DEPTH = 256` 限制）
  与 `regression_core::c1_deep_chain_dispose_is_iterative_and_ordered`（**1500 层**依赖链、
  默认测试线程，验证 dependents-before-provider 的迭代卸载顺序）。显式的「2 MiB 线程 /
  depth 800」对照验证来自轮 4 探针：0.4.3 debug+release 均 PASS，0.4.2 对照 debug 约 480
  层即 abort。
- 结论复跑：`cargo test`（默认构建）与 `cargo test --features supervision`
  （启用 L2 API 的全部用例）；两套都应全绿（v0.4.4：415 / 426）。

## 8. 与 Linux 内核类比（为什么这个答案是诚实的）

Linux 内核**不隔离模块**：一个有 bug 的模块（空指针、死锁、栈溢出）会让整机 oops/panic；
"插件崩溃不影响内核"这件事在内核里也是靠**进程边界**（用户态）+ 硬件隔离（MMU）实现的，
而不是靠内核模块机制。同理，Rust 的 `catch_unwind` 只能处理 **unwinding panic** 这一种
故障；abort/UB/OOM/栈溢出/挂起必须靠地址空间（进程）或沙箱（WASM）边界。Cordis 的定位
是"可逆副作用 + 反应式依赖"的**内核**，不是"安全沙箱"；沙箱能力应由 L3/L4 提供。
