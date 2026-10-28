# 内核隔离保证（Kernel isolation guarantees）

> **范围声明**：本 crate 是**纯 Rust、进程内、静态链接**的 Cordis 内核。
> 它不提供也**不会**引入 ABI/FFI、cdylib/dylib 动态加载、进程隔离宿主或 WASM 运行时；
> 下面 L2–L4 只是写给 embedder 的**建议方案**（在 crate 之外实现），不是本 crate 的
> 实现范围。内核自身只承诺 §3 所列的 in-process fault containment。

> 适用版本：v0.2.0 基线 + `fix/kernel-hardening-v0.3` 分支的 fault-containment 修复。
> 本文回答一个问题：**"插件崩溃会不会拖垮内核？"** 答案分两半——**可捕获的 panic
> 不会；Rust 进程内不可恢复的故障会**。本文给出实测证据、我们据此作出的承诺，以及
> 要做到"Linux 内核级"所必需的外层隔离方案。

## 1. 结论（一句话）

Cordis 在进程内提供的是 **unwinding panic 的故障遏制（fault containment）**，不是沙箱
（sandbox）。在 `panic = "unwind"` 且插件不主动 abort/不越界（见 §4）的前提下，一个插件
的 panic 只会让它自己的 fiber 回滚/失败，其它插件与内核继续运行；但 `panic="abort"`、
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

## 3. 我们能承诺什么（写进 README/spec 的措辞）

**能承诺**（L1，`panic=unwind`，插件遵守 §4 契约）：

1. 内核**调用**的插件回调（`apply`/`apply_async`、effect body 之外的 cleanup、
   listener、waterfall、internal hook、exporter、accessor、schema）中的 unwinding panic
   被遏制：该调用失败/被跳过并记录日志，不会 unwind 穿出内核公共 API。
2. 激活失败（Err 或 panic）**回滚**该 fiber 已注册的 effect/service/listener，fiber 进入
   `Failed`，不污染依赖方（provider 的 service 随回滚移除）。
3. 一个 fiber 的失败/处置不改变其它 fiber 的状态；dispose 的清理按逆序全量执行，清理
   panic 在**收尾完成后**才重抛给定调用者。
4. 当 `panic=unwind` 时，**插件代码中的 `panic!` 不杀死进程**（测试用例：`hardening::
   panicking_apply_is_isolated_and_rolled_back`、`containment::*`）。

**不能承诺**（进程内物理上做不到）：

- `panic=abort` / `-C panic=abort` 构建：任何 panic 直接 abort；`catch_unwind` 失效。
- 栈溢出（递归/深 Drop）、OOM/allocator abort、`process::abort/exit`、SIGSEGV（插件
  `unsafe` UB）、双重 panic（Drop 中 panic）。
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
- 同一 fiber 的 `dispose/restart/update` 不要并发调用（v0.2 未串行化；见路线图）。

## 5. 分层隔离（要做到"Linux 内核级"必须叠加）

| 层 | 机制 | 买 到什么 | 成本 |
|---|---|---|---|
| **L1** 进程内遏制（本 crate 已提供）| unwind 边界 + 回滚 + Drop 规则 + 无锁内用户代码 | 可捕获 panic 不扩散；故障归因到 fiber | 0 |
| **L2** 线程级监督 | 每插件/每组一个监督者：心跳、deadline、协作取消（checkpoint）、资源计量 | 定位、降级、优雅重启；**不能**杀线程 | 低-中 |
| **L3** 进程隔离（推荐给不可信插件）| 每插件组一个子进程 + IPC：生命周期/配置/declarative effects 走 RPC；SIGKILL + rlimit/cgroup/seccomp；崩溃报告/自动重启 | abort/SEGV/UB/OOM/死循环全部可恢复；真正的"插件崩溃不拖垮内核" | 高（IPC、序列化、内存、延迟）|
| **L4** WASM 沙箱 | wasmtime/wasmer + fuel/时间/内存上限、能力导入（fs/net 白名单） | 第三方代码的安全边界；无 `unsafe` | 高（宿主接口、性能、无 std 线程）|

> 判断标准：如果插件来自**可信仓库、同源代码** → L1+L2 足够；如果插件是**第三方/可被
> 攻破** → 必须 L3 或 L4。内核本身不应假装是沙箱。

## 6. 支持 L2/L3 所需的 kernel API（路线图）

1. `IsolationCapabilities`：启动时断言 `panic=unwind`；`kernel.can_contain_panics()`。
2. 观测：`Registry::when_idle(Duration)`（整树静默）、per-fiber `last_activity()`/`heartbeat()`。
3. 协作取消：`Context::checkpoint() -> Result<(), Cancelled>`、`Fiber::cancel()`、
   `dispose_cancelling(Duration)`。
4. 类型化失败 + `on_fiber_failure(FailureKind::{Err,Panic,AbortedImpossible})` 供 supervisor 决策。
5. 锁纪律：注册时缓存 `Plugin::name/key`，任何用户代码不在内核锁内执行；`Fiber::poisoned()`。
6. 收尾保证：清理 panic 也必须到达 `Disposed/Failed` 并释放 ctx（v0.3 已做）。
7. `BlockingExecutor::block_on -> Result<(), _>`：不允许 panic；文档化双 panic abort。
8. 资源计量：`ctx.spawn_guarded`/`open_guarded`/定时器统一走 `ResourceMeter`；per-fiber 配额
   （effects/listeners/threads/fds）+ 超限拒绝加载。
9. `PluginManifest`：`{name, inject, config_schema, isolation: Thread|Process|Wasm, limits,
   capabilities}`；进程隔离用 RPC 替代 `Arc<dyn Plugin>`。
10. 崩溃报告钩子 + 自动重启策略（L2/L3）。

## 7. 复现与验证

- 隔离探针：`target/scratch/redteam-isolation/`（`iso` = unwind 构建、`aborter` = abort 构建，
  `run.sh` 逐个场景，`timeout` 作 watch-dog）。
- 内核内遏制测试：`tests/containment.rs`（11 边界）、`tests/hardening.rs`
  （CORE-028..039）、以及 v0.3 新增的退化测试。
- 结论复跑：`CARGO_HOME=/data/Cordis/target/scratch/cargo-home cargo test --workspace`。

## 8. 与 Linux 内核类比（为什么这个答案是诚实的）

Linux 内核**不隔离模块**：一个有 bug 的模块（空指针、死锁、栈溢出）会让整机 oops/panic；
"插件崩溃不影响内核"这件事在内核里也是靠**进程边界**（用户态）+ 硬件隔离（MMU）实现的，
而不是靠内核模块机制。同理，Rust 的 `catch_unwind` 只能处理 **unwinding panic** 这一种
故障；abort/UB/OOM/栈溢出/挂起必须靠地址空间（进程）或沙箱（WASM）边界。Cordis 的定位
是"可逆副作用 + 反应式依赖"的**内核**，不是"安全沙箱"；沙箱能力应由 L3/L4 提供。
