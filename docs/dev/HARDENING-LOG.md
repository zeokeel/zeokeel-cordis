# Cordis-Rust 内核：缺陷总清单与线 K 评审

> 状态：分析报告（非规范）。基线 = **tag v0.1.0 / HEAD `f5ce84f`**（冻结快照
> `/data/Cordis/target/scratch`，15 个 src 文件 sha256 与 `git show HEAD:` 一致）。
> 参考物：论文 *A Programming Paradigm for Spatiotemporal Composability*
> (arXiv:2608.25512)、DSH 官方文档 `reference/cordis-*` / `subsystems/{core,scope}`、
> DSH vendor 的 TypeScript Cordis (`vendor/cordis`, 2713 行)。
> 平台实测：`cargo test --workspace` = **91 passed**（与 README 一致）；并发探针
> `/data/Cordis/target/scratch/{probe-own,probe-async,parity-probe}` 可复现。
> 工作区 `rust/` 在 16:48–17:00 期间被另一条会话并发修改（见 §7），本文以 HEAD 为基线、
> 单独标注在途状态。

> **⚠️ 历史记录（v0.3 时代，非当前行为）**
>
> 本文是 2026-10 初、以 **v0.1.0** 为基线的缺陷清单与历轮加固记录，保留原文用于追溯，**不是
> 当前行为的文档**：
>
> - 文中的测试数量（如 91 / 232 / 238 / 264 等）都是**各轮历史快照**；当前测试数与门禁
>   结果见 [README](../../README.md)「质量与严格构建」与 CI。
> - **取消语义已变更**：0.4.2 的 **A-9** 把「取消的 fiber unload」从「cancel-safe drain
>   恢复 effect 后重试」改为**完成 teardown**（sync cleanup 内联执行、async cleanup 进
>   fiber 的 `deferred_async` 由下一次 dispose 收尾，fiber 落到 `Pending`/`Disposed`）。
>   本文 §10.8 的 **c16**（cancel-safe drain 恢复 effect + `ChildSweep` Drop 回填重试）是
>   **已被取代的历史机制**；当前语义见 [`spec/CORDIS-CORE.md`](../../spec/CORDIS-CORE.md)
>   §2.3（CORE-046）、[`docs/CORDIS-RUST.md`](../CORDIS-RUST.md) §2.3 与
>   [`docs/KERNEL-ISOLATION.md`](../KERNEL-ISOLATION.md) §3。
> - 文中与现状冲突的行号、API 名称与「在途/修复中」状态同样按历史快照阅读。

## 0. 结论（TL;DR）

1. **线 K 的 7 项方向正确，但不是"问题全集"，且 4 项规模明显低估**：
   A1/A2 ≈ 300–400 行（非 150–250），A4 ≈ 200–250（非 80），A5 ≈ 300–600（非 50–100），
   A7 ≈ 600–900（非 300–500）；A3 可行但应在途实现需合并 API（见 §3.3）；A6 的
   "20 行护栏"不成立（实测死锁，见 §3.6）。
2. **A1/A2/A4/A5 不是 4 个独立缺陷，而是同一个缺失机制的症状**：fiber 缺少
   **事务化的激活/卸载模型**（load epoch + inertia + committed/target + effect owner
   wrapper + parent-owned child fiber + 分类通知）。TS 参考实现已用这套机制修复
   （`vendor/README.md` "Local modifications" 第 6 条 + `fiber.ts:504-673`）。只做
   局部 patch（catch_unwind / 手动回滚）会留下 C1–C6 那一组成体系的漏洞。
3. **计划漏掉了比 7 项更严重的缺陷**：Failed provider 仍可被注入（C1）、子 fiber
   不归父 fiber 所有（C2）、重入 dispose 会把 fiber "复活"（C3）、restart 绕过
   inject gate（H7）、config 被重复解析（H8）。这些均已在冻结基线上编译探针复现。
4. **A5 的 "长驻进程 1 个 root 可忍" 是错的**：每次 boot 漏 6.6 KB（裸）/ 15.9–17.4 KB
   （带 1 个插件），**每个 plugin fiber 各有一个环**；`dispose()` 不能缓解（实测
   dispose 后仍 100% 泄漏）；在途 Weak-link 修复后仍 6.7 KB/boot（C5 未修）。
5. **Rust 特性引入的问题需要单独立项**（§5）：无 async Drop → Drop 里必须 block →
   实测在 tokio current_thread 中 `fiber.dispose()` 死锁（exit 124）；取消
   `dispose_async()` 会永久丢失清理且**顺序变正序**；unwind 期间 Drop 清理 panic =
   进程 abort（exit 134）；`Context/Fiber` 是 `Send+Sync` 但生命周期操作**没有任何
   串行化**（激活/卸载竞态可让 Disposed→Active 复活）；Arc 环泄漏；`'static`+
   `Send` 约束、TypeId/`&'static str` identity、无稳定 ABI（动态插件不可行）、
   `forbid(unsafe_code)` 限制、递归 Drop/激活/emit、每次 `get` 分配 String、全局
   `refresh_all` O(N²) 等。
6. **规范与测试的可追溯性目前不成立**：spec 声称测试断言 CORE-xx，但 `tests/` 中
   **没有任何 CORE- 字样**；§2.4 与 CORE-011 自相矛盾；prose 的 CORE 编号与表格
   错位；spec §3 的 API（`PluginEntry`/`FiberRef`/`impl Into`）与实现不一致；
   `logger.rs:19`、`reflect.rs:13` 引用的 `docs/CORDIS-RUST.md` 不存在；根 README 与
   crate README 的示例代码按当前签名**不能编译**（`plugin(|ctx| …)`、`provide(&KEY,…)`）。

## 1. 判据：论文与 DSH 文档要求的语义

论文把 Cordis 的两条性质形式化：**可逆效应**（每次 context 变换都携带运行时持有的
逆，fiber 持有累加器，LIFO 回滚）与**响应式余效应**（每次 context 变化按组件声明的
coeffect 规格分类，驱动激活/停用）。关键义务（docs-parity 抽取，含出处）：

| # | 义务 | 出处 | Rust v0.1.0 状态 |
|---|---|---|---|
| T1 | 每次 context 变更都经同一原语并携带逆 | 论文 §3.1.1 Def 2–3；§5.1.1 | 注册类 ✓；`ctx.set` 不注册 effect、不可逆（`context.rs:1137`） |
| T3 | 每 fiber 一个累加器；LIFO、恰好一次、async 清理被 await | §3.1.2 Thm 16 | ✓ 但被 panic/取消破坏（§5.R3/R5） |
| T4 | fiber 是**惯性**状态机：Loading/Unloading/Failed、committed 视图、可重算 target、reload 链 | §4.4、§5.1.3 | ✗ 无 inertia/committed/target（`fiber.rs:83-91`） |
| T5 | 恢复等价性按"可观测等价 ≃"判定 | §3.3.2 Def 33–34 | ✗ 无 ≃、无 load→unload 往返测试 |
| S1 | `inject` 是 coeffect 规格；只有满足时才激活 | §3.2.2 Def 21–22 | ✓ 但 `restart()` 绕过（H7） |
| S2 | 每次变化都要分类通知所有 inject 该 key 的活 fiber | §3.2.2 Def 22、§5.1.2 Alg 3 | ✗ 只有 gate 翻转/卸载通知；`set` 不通知；**provider 身份变化不重载依赖方**（H8） |
| S3 | provider 进入 UNLOADING 即停止提供，依赖方先停用，最后才撤 binding | §5.1.2、Thm 70 | ✗ 先删 store 再 `refresh_all`，依赖方清理时 service 已消失 |
| S6 | key = 值类型 + 操作集，交互须经 context 中介（迭代器） | §3.3.1 Def 28–30 | 设计性放弃（`Arc<T>` 直接给），只能靠约定 |
| S7 | 五种分发模式是契约（emit/waterfall/parallel/serial/bail） | PRIMER | 代码有 5 种，spec §2.5 只写 2 种 |
| S8 | 成对独立性/交换性；quiescence 只由最终配置决定 | §3.4、§4.3.2–3 | ✗ 单 fiber 顺序之外无模型；Rust 真并行使其失效 |

论文本身也明确：运行时**不校验**逆的 witness（§5.1.1），所以"unchecked disposer"不是
缺陷；真正的义务缺口是 **T1 的普遍性（set）**、**S2 的分类完整性**、**T4/T5 的惯性/等价
模型**、**S3 的撤销顺序**。

## 2. 五类根因（7 项只是其中一部分的症状）

- **G1 激活/重载不是事务**：`activate_fiber` 在 `Loading` 后直接调 `plugin.apply`
  （`fiber.rs:387-441`），无 epoch、无回滚、无 "after-checkpoint 状态重验"。→ A1/A2/C3/C5/H7/H8。
- **G2 effect 所有权边界不完整**：effect body 先跑、节点后挂（`context.rs:461-517`）；
  `UNLOADING` 拒绝发生在 body 之后；子 fiber 不是父 fiber 的 effect（`context.rs:576-633`）；
  跨 effect 的 drain 不吃 panic（`fiber.rs:483-501`）。→ A4/C2/C4/C5/C6/H11。
- **G3 通知/分类不是全局机制**：`refresh_dependents() = registry.refresh_all()`
  （`context.rs:1002`、`registry.rs:311-343`）按"依赖是否 ready"扫描所有 fiber，没有
  变化类型、没有 provider 身份、没有 effect 级依赖；失败 fiber 不重试；`set` 不触发。→ S2/S3/H8/C1。
- **G4 阻塞/同步异步边界**：内核自带 `block_on`（`exec.rs:57`），Drop、dispose、effect_future
  都会在任意线程上跑它。→ A6/R2/R5/R6。
- **G5 所有权环**：`ContextInner`/`FiberData`/`Registry` 全强引用，无 Drop 清扫。→ A5/R1。

## 3. 线 K 七项逐条评审

| 项 | 性质判断 | 规模判断 | 计划里的替代方案 | 评审结论 |
|---|---|---|---|---|
| A1/A2 apply panic 隔离 + 失败回滚 | 真缺陷；且 **Err 返回也不回滚**（不止 panic） | **低估 1.5–2×**（300–400 + 测试） | "host 包 catch_unwind + 手动 dispose_async" | 不成立：panic 会从 `Context::plugin` 直接穿出（`context.rs:630` `let _ =` 只吞 Err），host 拿不到 Fiber；从 `refresh_all` 穿出会污染别的插件 |
| A3 `ServiceKey<?Sized>` | 真需求，方案可行 | 120 行量级可信，但要**合并 API** | 两层 `Arc` 包 8 个键 | 可用但双跳；在途实现 (`ArcWrap` + `*_any`) 有混用即静默 None 的坑（§3.3） |
| A4 cleanup/listener panic 隔离 | 真缺陷，但范围要扩到 internal hooks/async lane/exporter/accessor/schema | **低估 2.5–3×**（200–250） | host 自己包 cleanup | 覆盖不彻底：内核内 5 类用户回调都在 lock-free 区，host 没法逐点包 |
| A5 Context Arc 环泄漏 | 真缺陷，但规模被严重低估 | **低估 3–6×**（300–600） | "无干净替代；1 root 可忍" | 事实错误：6.6 KB/裸 boot、15.9–17.4 KB/带插件 boot；每 fiber 一环；dispose 无效 |
| A6 异步上下文禁同步 dispose | 真缺陷，但不是护栏问题 | **低估 6–9×**（120–180 结构修复） | host 纪律（只用 dispose_async） | 不成立：Drop 兜底也会 block；实测 tokio 死锁；真正修法是"Drop 永不阻塞 + 取消安全 drain + executor seam" |
| A7 AsyncPlugin/异步激活 | 方向对；"host 驱动"替代方案不成立 | **低估 2×**（600–900 含 parity） | host 驱动 async install | host 驱动会丢 inject gating/refresh/restart/update/cancel/await-settled，且每 host 重写 100–200 行；TS 已证明内核持有激活 future 是对的形状 |

### 3.1 A1/A2（apply panic 隔离 + 失败回滚）
基线事实（frozen）：
- panic 路径：`fiber.rs:427` 无 catch → fiber 停在 `Loading`、`error=None`、
  `await_settled()` 返回 Ok；已注册的 service/listener/effect 全部留存。
- Err 路径：`fiber.rs:434-440` 只写 `error` + `Failed`，**不 drain effects、不 remove_owner、
  不 refresh** → "失败插件继续对外服务"。探针复现：service+listener 在 `Err` 后仍存活，
  且依赖方会对着 Failed provider 激活。
- 还会漏：校验失败/`internal/config` waterfall panic（`fiber.rs:404-423`）同样没隔离；
  apply 期间 `ctx.plugin()` 创建的子 fiber 不在回滚范围（`context.rs:576-633`）。
- 失败后 `Failed` fiber 在依赖变化时**永远不会重试**（`refresh_all` 只处理 Pending/Active）。

正确设计：`ActivationGuard { fiber, effect_base, epoch, done }`，RAII Drop 未 done →
按逆序 drain 本次注册的 effect（含子 fiber，要求 C2 先落地）、`remove_owner`、写 error、
`refresh_dependents`；`begin_operation` 覆盖激活（现在只覆盖 dispose/restart/update）。
验收：panic / Err / 校验失败 / 子 fiber 四种路径都回到 `Failed` 且 `get_effects()==[]`。

### 3.2 A4（cleanup/listener panic 隔离）
`catch_unwind` 只存在于：`Service::check`（`service.rs:39-43`）、`AsyncDisposer::dispose`
（`async_effects.rs:74-91`，逐个 catch + 延迟 resume）。**同步 `Disposer`、跨 effect 的
drain 循环、事件 sync lane、`parallel_async`/`serial_async`、waterfall、全部 internal
hooks、logger exporter、accessor、`ConfigSchema::validate`、effect body 均未隔离**。
最要命的是"跨 effect"：即使每个 `Disposer` 内部 catch 住了，`drain_effects_blocking`
（`fiber.rs:483-490`）一旦被 resume 的 panic 打断，剩余 effect 直接 drop（不跑），
`finish_unload` 不执行 → fiber 卡 `Unloading`、listener 不 remove、context/registry 泄漏。
建议：一次性 `catch/log/首 panic 延迟重抛` helper，边界放在"整个 activate_fiber"、
"每个 effect drain"、"每次 emit/hook 循环"、"exporter 循环"、"accessor/schema 调用"；
Drop 内只记录不 panic；poison 策略改为"恢复 `into_inner` 或永不在 lock 内跑用户代码"。

### 3.3 A3（`ServiceKey<?Sized>`）
- 可行性：`Arc<dyn Trait>` 无法 unsize 到 `Arc<dyn Any>`（MSRV 1.85 也拿不到 1.86 的
  trait upcasting），所以必须用 sized 包装；当前在途实现
  `ArcWrap<T: ?Sized>(Arc<T>)`（live `service.rs:17-38`）方向正确。
- 缺陷 1（API 双轨）：`provide` 存 `Arc<T>`、`provide_any` 存 `ArcWrap<T>`，两者不可互读；
  `set_any` 写到 `provide` 的位置后，typed `get` 会**静默返回 None**。应统一成一种存储
  表示（全部 `ArcWrap`）与一组 API（`ServiceKey<T: ?Sized>` + `provide/get` 直接接受
  `Arc<T>`），或把 `*_any` 标成 deprecated 兼容层。
- 缺陷 2（marker 方差）：`PhantomData<fn(&T)>` 是逆变；建议 `PhantomData<fn() -> Arc<T>>`
  （协变、且 `Arc<T>` 对 `?Sized` 仍是 Sized）。
- 缺陷 3（语义）：`ServiceKey<T: ?Sized + Send + Sync>` 要求 trait 本身 `Send+Sync`，
  即 `dyn Trait` 必须写成 `dyn Trait + Send + Sync`；如果将来要支持 `!Send` 本地
  服务（GUI/GPU host），这套 key/存储要一起改（见 R10）。
- 规模：~120 行可信（含测试 200+）。

### 3.4 A5（Arc 环）
见实测与环图：C1 root 自环、C2 dispose 只清 `ctx` 不清 `parent`、C3 每 plugin fiber 一环、
C4 `Registry.fibers` 强引用 + 只在下次 register 时 prune、C5 provide/accessor 的清理闭包
捕获 `Context`。实测：50 000 roots 全部泄漏（6.6 KB/boot）；带 1 插件 15.9–17.4 KB/boot；
`dispose()` 后仍 100% 泄漏；在途 Weak-link 快照仍 6.7 KB/boot（C5 未修）。TS 参考实现
在 dispose 时把 `ctx/context` 复位为 parent 并依赖 GC（`fiber.ts:322`），没有这个环 ——
环是 Rust 移植引入的。正确修法 ≈300–600 行（Weak `parent/ctx`、`Registry.fibers` 改 Weak
或 dispose 即 prune、provide/accessor 闭包持 `Weak<ContextInner>`、可选 `Drop for
ContextInner` 清扫）；API 行为变化：`Fiber::parent()/context()` 变 `Option`。

### 3.5 A6（同步 dispose 约束）
实测（tokio 1.53）：current_thread 中在 task 里 `fiber.dispose()` 且 cleanup 等 timer →
**deadlock（exit 124）**；`ctx.effect_future` 注册期就有同样问题；`timeout(dispose_async())`
会经 `AsyncDisposer::drop → dispose_blocking` 在 poll 内 block → 同样 124；unwind 期间
detached disposer 的清理 panic → **abort（exit 134）**。多线程 runtime 只是"卡一个 worker"。
"20 行 debug 断言"无法覆盖 host poll 入口（depth 0），且在 Drop 里 panic 本身就是 abort
风险。建议（120–180 行）：Drop 永不阻塞（丢弃并 warn / 入 orphan 队列）、drain guard 可
取消且保序、executor seam（在途 `BlockingExecutor` 是对的，但默认必须安全：默认不 block，
仅显式 sync 路径 block）、`Disposer::drop` 也做 panic 收敛。

### 3.6 A7（AsyncPlugin）
TS 的异步激活是内核持有的：`_reload()` 里 `await` apply、epoch 每次 await 后重验
（`fiber.ts:646-673`），`fiber.await()` 等待 inertia。Rust 目前 `apply` 同步
（`registry.rs:109`），inject gate 在 `plugin_with_key` 进入时只判一次（`context.rs:619`）。
若让 host 驱动：inject 满足性、Pending→Active 重试、restart/update、子 fiber 归属、
`await_settled` 语义都无法复用，且每个 host 重写一份。建议内核持有：
`AsyncPlugin::apply_async(&self) -> BoxFuture<'static, Result<()>>`（object-safe）+
`ActivationGuard` + 每 await 一次 epoch 校验 + 取消（host drop future）时回滚并把
`pending_ops` 纳入激活。规模：薄版 300–500，TS parity 600–900（在途已写 ~650 行但缺
epoch/取消/pending_ops）。

## 4. 线 K 漏掉的问题（与 TS 参考/论文对齐后）

严重度：P0 = 会产生对外错误行为/泄漏；P1 = 语义不一致、host 会踩；P2 = 文档/测试/诊断。

### P0
- **P0-1 Failed provider 仍被当作可用**：service 查找不检查 provider fiber 状态
  （`context.rs:385-399`），Failed/Loading 的 provider 注册的 service 仍在 store 里 ⇒
  依赖方对着失败插件激活（探针复现）。TS 侧 `reflect` 只暴露 ACTIVE provider 的 impl
  （`reflect.ts:237-243`）。
- **P0-2 子 fiber 不归父 fiber 所有**：`ctx.plugin()` 不把子 fiber 的 disposer 注册为父
  fiber 的 effect（`context.rs:576-633`）；父 dispose 后子 fiber 仍 Active（探针复现）。
  TS 在 `internal/plugin` 发布前就把 parent-owned disposer 交出去（`fiber.ts:265-297`）。
- **P0-3 激活/卸载无 epoch，重入可"复活"**：`transition(Active)` 无条件覆盖
  （`fiber.rs:427`）；dispose 发生在 apply 中 → 最终状态 `Active`、`ctx=None`、effect 空
  （探针复现 Disposed→Loading→Active）。TS 用 epoch 在每 checkpoint 前重验
  （`fiber.ts:625-639,646-658`）。
- **P0-4 跨 effect 清理 panic 中断 + 卡 Unloading**（见 §3.2；在途已修一半）。
- **P0-5 `ctx.effect` 所有权窗口**：body 先执行、effect 节点后挂；body panic 时
  `effect_stack.pop` 不执行（`context.rs:461-517`）⇒ 诊断树错挂 + 已注册清理无主；
  `UNLOADING` 拒绝发生在 body 之后 ⇒ 卸载期间仍能跑 body（探针复现）。TS 先挂 wrapper
  再执行、同步失败回滚（`fiber.ts:504-537`）。

### P1
- **P1-1 `restart()/update()` 绕过 inject gate**：`fiber.rs:244-271` 卸载后直接
  `activate_fiber`，不检查 `dependencies_ready` ⇒ 探针复现"缺依赖仍 apply"。TS 是
  `_setEpoch(INACTIVE)+_refresh()`（`fiber.ts:718-723`）。
- **P1-2 raw config 丢失 + 重复解析**：`FiberData.config` 存的是已 resolve 的值
  （`fiber.rs:421-426`），`refresh_all` 再把它当 raw 传回 `activate_fiber` ⇒
  `internal/config` 叠加（探针 R→RR、update 后 uRR）。TS 保留 `_config` raw、激活时才
  resolve（README item 15、`fiber.ts:641-644`）。
- **P1-3 provider 身份变化不触发依赖方重载**：分类只按"名字是否可用"（`registry.rs:345-347`），
  不含 provider fiber uid；isolate 阴影切换 provider 时依赖方仍用旧 `Arc`。TS epoch 含
  `impl.fiber.uid`（`fiber.ts:611-623`），论文案例也要求"provider 变了只有受影响依赖方
  重激活"。
- **P1-4 激活错误被吞/不记录**：`context.rs:630` `let _ = activate_fiber(...)`；`fiber.rs:434-440`
  只存 string 不 log；不同步 await 的 host 永远不知道插件失败（TS log + `_error`）。
- **P1-5 `inject` 只有名字列表**：TS 的 inject 是 `name → intercept config` 映射，且
  `internal/plugin` 观察者可在激活前补充依赖（`registry.ts:63-88`、`fiber.ts:265-297`）；
  Rust `inject: &'static [&'static str]` 两者都做不到（H10）。
- **P1-6 无 dispose 时 `internal/plugin` 广播/无 per-observer 收敛**：`internal.rs:375-379`
  只 create；一个 create hook panic 会穿过已插入 registry 的 fiber 到 host（H11）。
- **P1-7 监听者/waterfall 错误无法表达**：listener 只能返回 `Value`，并行分发的失败被
  吞（`events.rs:319-348`）；TS 用异常 + AggregateError（H12）。
- **P1-8 事件 scope filter 模型不同**：Rust 总是要求 isolate map 全等（`events.rs:152-159`），
  TS 只在 dispatch 带显式 scope/`Context.filter` 时过滤；探针：隔离 listener 收不到父级
  emit（0 vs TS 1）（H13）。
- **P1-9 Registry identity 退化**：`PluginKey::Named(Plugin::name())` 让同名不同插件共享
  runtime（`registry.rs:27-35,86-110`）；`plugin_typed` 用 Type 而 `plugin.key()` 用 Named，
  `get/has/delete` 与注册不一致（H14）。Rust 里闭包/函数项各有唯一类型，TypeId 语义也
  不是"逻辑身份"。
- **P1-10 `AsyncDisposer` 无 join**：第二次 `dispose()` 立即返回（`async_effects.rs:74-107`），
  多线程 host 里 unload 可能早于清理完成；TS 会 join in-flight cleanup（H15）。
- **P1-11 `ctx.set` 不注册 effect、不分类通知**（T1/S2）：值替换不可逆且不重算依赖方。
- **P1-12 provider 撤销顺序**（S3）：`insert_service` 的 disposer 先删 store 再
  `refresh_dependents()`（`context.rs:341-349`），依赖方清理时 provider 已不可见；且
  级联是**同步重入**的（drain → refresh_all → 依赖方 unload），深依赖链会栈增长、跨
  fiber 的 LIFO 顺序不成立。TS 用 inertia/epoch 异步化避免同步级联。

### P2
- **P2-1 spec 与测试不可追溯**：`tests/*.rs` 无任何 `CORE-`（已 grep 证实），但
  spec §4 声称"tests assert the same case IDs"；`91 tests` 数字正确。
- **P2-2 spec 内部矛盾**：§2.4 "cleanup panics are not masked" vs CORE-011
  "Cleanup panic does not skip remaining cleanups"；且 CORE-011 只有 async lane 满足。
- **P2-3 spec 编号错位**：prose §2.6 用 CORE-023/024 指 registry，表格 CORE-022 才是
  registry（logger/config/accessor 整段偏移）。
- **P2-4 spec §3 的 API 与实现不一致**：`plugin(impl Into<PluginEntry>) -> FiberRef`、
  `provide<T>(&self, key: &ServiceKey<T>, …, check: Option<AvailabilityCheck>)` 等都不存在；
  实际是 `Arc<dyn Plugin>`、按值 key、`Result<Fiber>`。`FiberRef` 在实现里是
  `pub type FiberRef = Arc<Mutex<FiberData>>`（暴露内部可变状态）。
- **P2-5 文档腐烂**：`logger.rs:19`/`reflect.rs:13` 引用不存在的 `docs/CORDIS-RUST.md`；
  根 README 的 `ctx.plugin(|ctx| …)` 与 crate README 的 `provide(&FS, …, None)` 按当前
  签名不能编译（无 `Fn` blanket impl、key 按值、返回 `Result`）。
- **P2-6 无 quiescence API**：只有 per-fiber `await_settled`（且不覆盖激活），loader/HMR
  需要"整树 idle"（论文 Thm 73/80；DSH 文档 `whenIdle` 级需求）。
- **P2-7 无 scoped layered registry / scope carrier**：DSH 的 tools/systemPrompt/llm 依赖
  "global+exact-scope 层、peek、合并顺序"；Rust 只有按服务名单点 isolate。
- **P2-8 分发模式文档缺 3/5**：`emit/waterfall` 之外的 `parallel/serial/bail` 在 spec §2.5
  未写，sync/async 两条 lane 与文档语义也不一致（sync lane 静默跳过 async listener）。
- **P2-9 无 `#[non_exhaustive]`**：`CordisError`/`FiberState` 新增变体即破坏下游 match
  （在途已加 `AsyncPluginNeedsAsync`，正是破坏点）。
- **P2-10 `Fiber::name()` 在持有 `FiberData` 锁时调用户 `plugin.name()`**
  （`fiber.rs:129-134`）；用户 panic → 锁中毒 → fiber 永久不可用。

## 5. Rust 特性直接引入的内核问题（R 系列）

> 这些是"JS 版不存在、Rust 版必须处理"的问题，不属于线 K 7 项，建议单独立项。

**R1 所有权环（Arc）** — 见 §3.4。GC→Arc 的移植代价：需要 Weak 边 + 显式 teardown，
且 `Drop` 不是 async 使清扫复杂化。实测 50k roots 全漏。

**R2 没有 async Drop（结构性）** — Rust 的 `Drop` 不能 await，所以"异步资源释放"
只能在 Drop 里 block（`async_effects.rs:97,110-118`）或泄漏。这是 A6/A7 的共同根因；
正确形态是把"释放"从 Drop 移到显式 `dispose_async`，Drop 只做 best-effort 不阻塞并
上报。论文的"异步 unloading 状态"（§5.1.3/§8.2 OSGi 对比）在 Rust 只能靠状态机显式建模。

**R3 unwind 边界** — 任何用户回调都可能是 unwind 源：apply/effect body/cleanup/listener/
hook/exporter/accessor/schema。当前只有 async cleanup 与 check 有 catch。后果：状态机卡死、
清理跳过、锁中毒、Drop 双 panic aborts（实测 134）。另外 `panic=abort`/WASM 让所有
`catch_unwind` 方案失效（`[profile]`/目标平台须文档化）；`AssertUnwindSafe` 换来的只是
"能编译"，panic 后中间态仍需回滚才能满足论文的恢复语义。

**R4 Send+Sync 真并发 vs 顺序演算** — `Context`/`Fiber`/`Registry` 全是 `Send+Sync`
（`Arc<Mutex<…>>` + `&self` API），但生命周期操作**没有任何串行化**
（`begin_operation` 只加计数器，`fiber.rs:535-540`）。实测：apply 进行中另一线程
`dispose()` → 复活/晚注册被拒；`refresh_all` 在锁外遍历快照，可与注册/卸载交错。
论文的交换性/观测等价（S8）建立在"顺序交错"上；Rust 必须二选一：**加 per-context/
per-fiber 生命周期锁**（并定义锁序）或**把句柄标成 `!Send`/单线程**（`PhantomData<*const ()>`
包装，给 GUI host 用）。当前两者都没有。

**R5 取消不安全** — host 可以随时 drop future（timeout/select/任务取消）：`dispose_async`
被 drop 后，已 take 的 effect 列表剩下的清理**永久丢失**，且未处理 effect 以**正序**被
`Vec` drop 掉（实测 e0→e1，契约是逆序）；`AsyncDisposer::drop` 在取消 poll 内 block 会
死锁。需要 cancel-safe drain guard（剩余列表回写 + 逆序处理）。

**R6 内核私有 executor** — `exec.rs` 的 `block_on`（自实现 Parker）只适合"无 reactor 的
future"：实测 tokio current_thread 中在 poll 内调用即挂起（timer 驱动线程被 park）。
嵌套 `block_on` 还会阻塞外层推进。要么只允许在"非 async 上下文"边界使用并在文档里
强约束，要么把执行交给 host（在途 `BlockingExecutor` seam 方向正确）。另外
`Settled::poll` 每次 poll 都 push waker、无去重、无 Drop 移除（实测 strong_count 2→7），
长期挂起的操作会积累 waker。

**R7 `'static` + `Send` 全约束** — `Cleanup`/`AsyncCleanup`/`BoxFuture<'static>`/
`inject: &'static [&'static str]`/事件名 `&'static str`/`ServiceKey` 的 `T: 'static'`：
embedder 不能在 effect 里借用栈/上下文（必须 `Arc` 克隆）、动态服务名/事件名要么
`Box::leak` 要么不支持、每个 future 一次 Box 分配。对 server host 可接受，对"按配置
动态命名"的 host 是硬限制。

**R8 类型擦除与 identity** — `ServiceValue = Arc<dyn Any + Send + Sync>`：
  - 服务键 = 名字字符串 + 类型双轨；同名不同类型 → 运行期 `DuplicateService`/静默 None，
    无编译期保证（`ServiceKey` 只是命名常量）。
  - `dyn Plugin`/`dyn Service` 无法 downcast 回具体类型（无 `Any` supertrait/`as_any`）⇒
    host 拿不到自己的插件实例/配置。
  - `PluginKey::Named(&'static str)` 合并同名不同插件；`TypeId` 只对 `'static` 具体类型
    有效，闭包按词法位置分型——dedupe 语义与"逻辑身份"脱节。
  - trait object 服务的 A3 方案必须绕道 sized wrapper（见 §3.3）。

**R9 无稳定 ABI ⇒ 动态插件不可行** — `Arc<dyn Plugin>` + `TypeId` + `&'static str` 全部
是 crate-internal ABI；Rust 动态库跨界传递 trait object 是 UB。所以"plugin kernel"在
Rust 里只能**静态链接**；若 host 需要第三方插件，必须进程隔离/WASM/`abi_stable` 之一。
这是与"可插拔内核"定位冲突的最大 Rust 约束，线 K 未涉及（也不该在本 crate 解决，但
spec/README 必须写明支持边界）。

**R10 无法表达 `!Send` 本地上下文** — 服务值被强制 `Send+Sync`（`service.rs:14`、
`ServiceKey<T: Send+Sync>`），GUI/GPU/音频等 host 的 thread-affine 资源无法作为服务；
需要 `LocalContext`（`!Send` 句柄 + 同线程存储）或 `Mutex` 包裹（许多资源不可跨线程）。
`forbid(unsafe_code)` 又堵死了手写 `unsafe impl Send` 的逃生口。

**R11 `#![forbid(unsafe_code)]` 的边界** — 优点（安全）之外：无法做 `Pin` projection/
自引用、`Arc::get_mut`/`Arc::try_unwrap` 优化、零拷贝借用型服务、intrusive 列表；
A3 只能走 wrapper 分配（每次 provide 多一次 `Arc`）。若未来要 !Send 本地上下文或
零拷贝读，必须开 `unsafe` 或换设计——现在应在 spec 里明确这条边界。

**R12 对象安全与 async trait** — `Plugin` 必须 object-safe（`dyn Plugin`），所以不能有
泛型方法/`async fn`/RPITIT；A7 只能返回 boxed future（每激活一次分配）。MSRV 1.85 也拿不到
trait upcasting（1.86），影响 A3 的"正统"写法。`Service` 同理：`Arc<dyn Trait>` 服务的
动态派发成本 + `Arc` 双跳。

**R13 递归与栈** — 递归点：effect 树 `Drop`/`snapshot`（`fiber.rs:28-56`）、
`apply→ctx.plugin→apply`（无深度限制）、`emit` 重入、waterfall `next()` 链、
`refresh_all→activate→provide→refresh_all` 同步级联。深度依赖链/嵌套插件在 8 MB 主线程
可行，但 tokio worker 默认 2 MB 栈会先溢出；需显式迭代化/深度上限。

**R14 语义版本演进** — 公开枚举无 `#[non_exhaustive]`；`CordisError`/`FiberState`/
`EventResult` 加变体对下游是破坏性变更，而 spec 又要求"behavior change = breaking"。
在 v0.2 之前应一次性加 `#[non_exhaustive]`，否则之后每次扩展都要 major。

**R15 性能与分配** — 每次 `get/require`：`name.to_string()` 分配 + `(u64,String)` 哈希 +
`ServiceEntry` 克隆 + `internal/get` waterfall 的 Arc/Vec 快照（`context.rs:362-399`）；
每次 `ctx.plugin()`/`provide()` 触发**全树** `refresh_all`（O(N)），boot 期累计 O(N²)；
waterfall 每次 `next()` 克隆 `Arc<[Value]>`；事件快照每 dispatch 克隆 Vec。Hot path
（DI/日志/事件）在 server 规模需要 `&'static str` 键 + 增量依赖索引。

**R16 `serde_json::Value` 配置 ⇒ 失去静态类型** — plugin/service 配置是 `Value`，
schema 校验在运行期（`ConfigSchema`）；Rust 本可用泛型 `Plugin<C: Deserialize>` 拿到编译期
配置类型，但 object-safe registry 把它挡在门外。至少应提供 typed plugin helper
（`PluginEntry::new::<C>()` 内部做 `serde_json::from_value`）。

**R17 多 root/全局状态** — `Registry`/`Events`/`LoggerService` 都挂在 root 下（无进程级
单例），多 root（多租户/测试）语义正确；但 `service_store` 在整棵树间按 `(scope, name)`
共享、scope id 由 registry 计数器与 FNV(label) 混用（`context.rs:1262`），极端情况下
label 哈希可能与计数器撞值。建议用独立命名空间或随机基址。

**R18 诊断/可观测性** — 无 `whenIdle`/无 `epoch` 暴露/无"为什么这个 fiber 是 Pending"
的诊断；`Fiber::name()` 持锁调用户代码；poison 后静默降级（`lock().ok()` 遍布全库）让
故障从"报错"变成"服务消失"。建议 `debug_assert!` + 结构化诊断事件（`internal/diag`）。

## 6. 建议：线 K v2（按依赖排序）

| # | 项目 | 覆盖 | 规模 | 依赖 |
|---|---|---|---|---|
| K1 | fiber 事务模型：epoch + inertia + committed/target + `ActivationGuard` + `pending_ops` 覆盖激活 | P0-1/2/3, C1–C6, H7/H9, S2/S3, T4 | 400–600 | — |
| K2 | effect 所有权与 quiescence：owner wrapper 先注册、UNLOADING 前置拒绝、RAII stack guard、per-effect 隔离、`whenIdle` | P0-4/5, A4, C5/C6, S3 | 250–350 | K1 |
| K3 | 并发串行化：生命周期锁/锁序 或 `!Send` handle；文档化 | R4, C3 复活, H15 | 150–250 | K1 |
| K4 | panic/poison 策略：统一 catch/log/延迟重抛、Drop 不 panic、poison 恢复、`panic=abort` 文档 | R3, A4, P2-9 | 200–300 | K2 |
| K5 | 所有权环：Weak `parent/ctx`、`Registry.fibers` Weak/prune、provide/accessor 闭包 Weak、可选 Drop 清扫 | A5/R1 | 300–600 | K1/K2 |
| K6 | 异步激活（A7）：`AsyncPlugin` + epoch 校验 + 取消回滚；先做 A6 的非阻塞 Drop | A7, R2, R5 | 300–500（parity 600–900） | K1/K4 |
| K7 | `?Sized` 服务统一 API（A3 修正）：单一存储表示 + 单一 API | A3, R8 | 120–200 | — |
| K8 | 语义/兼容：raw config 保留、provider 身份 epoch、inject 映射、错误通道、`#[non_exhaustive]` | P1-1..P1-10, P2-9 | 250–400 | K1 |
| K9 | embedder 能力：scoped layered store、scope carrier、typed config helper、诊断/`whenIdle` | P2-6/7, R16 | 300+ | K1/K2 |
| K10 | 规范与测试修复：CORE 编号/矛盾、spec §3 重写、README 示例可编译、CORE→测试映射、往返等价测试、`docs/CORDIS-RUST.md` | P2-1..P2-5, T5, S7 | 150–250 | 全部 |

顺序建议：K1 → K2 →（K4 与 K3 并行）→ K5/K6 → K8 → K7 → K9 → K10。A1/A2 合并进 K1，
A4 合并进 K2，A5 即 K5，A6 合并进 K4/K6，A3 即 K7，A7 即 K6 —— 原 7 项中没有一项应
独立于事务模型先做，否则会返工（例如单独给 `Disposer` 加 catch 无法解决跨 effect 中断）。

## 7. 在途工作去重（时间戳 2026-10-28 16:48–17:05 CST）

- 16:47 起工作区有 21 个文件改动；17:06 时 `git diff --stat` = **26 files, +1033/−231**，
  且 `context.rs` 在 17:06 仍在写、新增了 `tests/executor.rs`/`tests/hardening.rs` —— 即
  在途实现尚未收敛，本文的时间戳快照只用于去重参考。
- 已落地（与本文对照）：apply 的 `catch_unwind` + `fail_fiber_*` 回滚、sync cleanup/事件/
  waterfall 的 panic 收敛、per-effect drain 隔离、deferred async、`BlockingExecutor` seam、
  `provide_any/get_any`（A3，`ArcWrap`）、`AsyncPlugin`/`plugin_async`/`activate_fiber_async`
  （A7 主体，约 +650 行）、root 的 Weak link、`AsyncPluginNeedsAsync` 错误变体。
- 未落地：internal hooks/logger/reflect/config 的 panic 收敛；async lane/`join_all` 的隔离；
  effect body 的 owner window；C1/C2/C3/C5/C6；H7–H15；A5 的 C5 环；取消安全；spec/tests。
- ⚠️ **在途快照（17:00 复制）能编译，但 `cargo test` 卡死**：`async_apis_produce_send_futures`、
  `async_cleanup_panic_does_not_skip_remaining_cleanups`、`async_cleanup_runs_in_reverse_order_once`、
  `async_effect_on_inactive_fiber_is_rejected` 等 >60 s 未返回；`probe-own` 也观测到
  `ctx.plugin()` futex_wait 死锁（0 % CPU）。若这是修复中的中间态可忽略，否则**合并前必须
  先解决**（疑似锁序/重入或 activation guard 在锁内等待）。
- 结论：线 K 应 rebase 到该在途分支，删除已完成项，并**先修死锁**再继续叠加。

## 8. 附录：证据与复现

- 冻结基线：`/data/Cordis/target/scratch`（`git archive HEAD`），`cargo test --workspace`
  → 91 passed，exit 0。CARGO_HOME 可用 `/data/Cordis/target/scratch/cargo-home`
  （`~/.cargo` 只读、`/tmp` 每次调用清空是本机限制）。
- 所有权/泄漏探针：`/data/Cordis/target/scratch/probe-own`（`./probe_bin roots 50000` 等）。
- 异步/并发探针：`/data/Cordis/target/scratch/probe-async`（15 个 bin，`RESULTS.md`，
  `timeout 5` 作为 watch-dog：124=挂起、134=abort）。
- 语义 parity 探针：`/data/Cordis/target/scratch/parity-probe`、报告
  `/data/Cordis/target/scratch/parity-report/report.md`。
- 论文全文：docs-parity 已下载 PDF（不可用 HTML），义务表见
  `/data/Cordis/target/scratch/docs-parity/obligations.md`。
- 参考实现：TypeScript 版 Cordis（公开仓库 cordiverse/cordis 及其 vendored 变体）；
  其硬化记录见对应 vendor README 的 Local modifications 条目。

## 9. v0.2.0 复核（tag `v0.2.0` = `e0dab64`，2026-10-28 17:12–17:40 实测）

**基线确认**：独立复跑 `cargo test --workspace` = **103 passed / 0 failed**（10 lib +
86 integration + 7 doctests，与 README 一致），fmt clean；新增 `tests/hardening.rs`(10) +
`tests/executor.rs`(1) + CORE-028…039。快照 `/data/Cordis/target/scratch/v020`
（15 个 src sha256 == tag）。

**总判决**：原清单 ≈45 条 → **FIXED 7 / PARTIAL 9 / STILL 21 / v0.2 新引入或新暴露 10**。
v0.2 把"线 K"的主体（A1/A2、A4 的 sync 半边、A3、A7、restart gate）真正做掉了，但
**没有引入 epoch/inertia 事务模型**，所以 A5 的环、P0-3 复活、P0-5 owner window、
A4 的 hook/async 半边、以及全部 R 系列仍在；另外有 10 个 v0.2 新问题，其中 3 个是
语义/安全上的**倒退**。

### 9.1 FIXED（有实测证据）

| 原 ID | 结论 |
|---|---|
| A1/A2 | 回滚 + panic 隔离落地（CORE-028/029，`fiber.rs:453-489,539-601`）：Err 与 panic 都回滚 effect/service/listener、状态 Failed、panic 不外泄。tests: `failed_apply_rolls_back_effects_and_services`、`panicking_apply_is_isolated_and_rolled_back`。**边界残留见 9.4-6**。 |
| A6（部分） | `BlockingExecutor` seam：multi_thread(2) 装 `block_in_place(|| Handle::current().block_on(f))` 后 dispose 成功（exit 0）；current_thread 不可行；**默认仍死锁**。 |
| A7（核心） | `AsyncPlugin`/`plugin_async`/`refresh_async`：sync API 在 async fiber 返回 `AsyncPluginNeedsAsync`；`refresh_async` 遵守 inject gate；async apply Err 等待清理后回滚（v06d/f 实测）。 |
| P1-1 | `restart()` 重查 inject gate（CORE-039）；`update` 仍急切（9.2）。 |
| P2-2 | §2.4 与 CORE-030 矛盾消除（改为 drain-then-re-raise-first-panic）。 |
| C6（原 parity 表） | UNLOADING 前置拒绝；v0.1 已正确，此前探针结论已修正。 |
| 证据链 | 新增 12 个 CORE case 都有真实测试且通过；103 tests 数字准确。 |

### 9.2 PARTIAL

- **A4**：只覆盖 sync lane（`effect.rs`、`events.rs::call_sync`、`waterfall.rs` sync、
  fiber drain）。**internal hooks / logger exporters / reflect accessor / ConfigSchema /
  async event lane / async waterfall / effect body 仍未隔离**（探针：status-hook panic
  逃逸且第二个 hook 被跳过、exporter1 panic 跳过 exporter2、schema panic 后 fiber 停
  Loading、`parallel_async`/`serial_async` 逃逸、`waterfall_async` 逃逸）。
- **P0-4**：清理不再跳过、drain 级双 panic abort 已修（v0.1 SIGABRT → v0.2 exit 0）；
  但 `resume_unwind` 在 `finish_unload` **之前**（`fiber.rs:767/800` vs `:808-817`）⇒
  **`dispose` panic 后 fiber 卡 `Unloading`、runtime/registry 残留（reg=1）、ctx 图保留**；
  重试 `dispose` 可恢复。CORE-030 未覆盖状态收尾。
- **P0-1**：failed provider 的 service 现在随回滚移除（原症状消除）；但 `lookup` 仍只看
  `check()`、不看 provider 状态（`context.rs:452-470`）⇒ **Loading provider 的半初始化
  服务可被注入**（探针：依赖方 apply 1 次并 Active；TS 为 Pending）。
- **P1-4**：activation 错误可从 `await_settled` 拿到，但仍**不写日志**（records=0）。
- **H7/P1-1**：`restart` 已修；`update` 在 Pending fiber 上仍立即校验/报错（TS 延迟到激活）。
- **P2-1**：新 case 有 ID（hardening 6 + executor 1），**CORE-001…027 仍零 ID 引用**，
  README "tests assert the same case IDs" 仍不成立。
- **P2-4**：§3 已对齐 `plugin(Arc<dyn Plugin>)->Result<Fiber>`/`Fiber`/按值 key；但仍写
  不存在的 `EffectResult`/`AsyncEffectResult`、漏 ~20 个公开 fn、§2.2/§2.4 正文仍描述
  错误 API（`provide(name,…)`、`ctx.effect(|| {})`）。

### 9.3 STILL

**P0 级**
- **P0-3 无 epoch/inertia ⇒ 复活**：`transition` 无 Disposed 守卫、apply 返回后无条件
  `transition(Active)`（`fiber.rs:584`；async `:663`）。实测：apply 中 dispose → 最终
  `Active, ctx=None, effects=[]`（sync + async 均复现），后续注册被拒。**未列入
  Known limitations**。
- **P0-5 effect owner window**：body 先跑（`context.rs:556`）、节点后挂（:560）；body
  panic 跳过 `effect_stack.pop`（:558）⇒ 后续 effect 变成不可见子节点
  （`get_effects=[]`）；body 内 dispose 自己 → 先执行后被拒。
- **A5 剩余**：plugin fiber 仍 17.5 KB/boot 泄漏、**dispose 后仍 100% 泄漏**
  （`Registry.fibers` 强引用 + `parent` 永不清：`registry.rs:47/371`、`fiber.rs:738-740`）；
  `ctx.accessor` 的 disposer 仍强捕获 Context（`reflect.rs:153-160`）→ 7.2 KB/boot；
  `dispose plugin + dispose root + drop` 仍泄漏。
- **A6 默认路径**：tokio current_thread 里 `fiber.dispose()` 仍 **exit 124**；
  `ctx.effect_future`/`effect_stream` 注册期走裸 `block_on`（`context.rs:964/1002`），
  **绕过 BlockingExecutor**；current_thread 上"拒绝式"executor 因 Drop 重入 → **abort 134**。
- **R2/R3**：detached Disposer/AsyncDisposer 在 unwind 中 panic → abort 134；无 poison
  恢复（20 处静默 `.ok()`；service-store 中毒后 `get=false`、`provide=false`）。
- **R5 取消**：`dispose_async` 被 drop ⇒ 在飞 effect 剩余清理永久丢失、剩余 effect
  **正序**跑、状态卡 Unloading。
- **R8**：`provide`/`provide_any` 双轨混用静默 None（探针 `Some(7)` vs `None`）；无
  `as_any` downcast；`PluginKey::Named` 合并同名插件（size=1、delete 误删 2 个）、
  `plugin.key()` 与 `plugin_typed` 不一致。
- **R14**：仍无 `#[non_exhaustive]`（CordisError/FiberState/EventResult/PluginKind）。
- **R15**：`get` 仍 `name.to_string()` + `(u64,String)` 哈希 + ServiceEntry clone；
  `provide` 触发全树 `refresh_all`（boot O(N²)）。
- **R18**：无 whenIdle/quiescence；`Fiber::name()` 仍持 FiberData 锁调用户代码
  （探针：name() panic ⇒ state 谎报 Disposed、dispose Err）。

**P1 级（全部 STILL）**：P1-2 raw config（探针 R→RR→uRR；README 说明与实现相反）、
P1-3 无 provider 身份 epoch、P1-5 inject 只有名字列表、P1-6 无 dispose 广播/无
per-observer containment（status hook 在 Loading→Failed 时可使 `ctx.plugin` panic，
违反 CORE-029）、P1-7 无错误通道（且新策略变成吞 panic）、P1-8 scope filter 仍是
isolate map 全等（探针 0 vs TS 1）、P1-10 AsyncDisposer 无 join（第二次 dispose 0ms
vs 首次 400ms）、P1-11 `ctx.set` 仍不注册 effect/不通知、P1-12 撤销顺序仍先删 store
再通知。

**P2 级**：P2-3 编号错位（§2.7 正文未改，且与新增 CORE-028..039 撞号）、P2-5
`docs/CORDIS-RUST.md` 仍缺、README 示例仍 **E0308**、P2-6/P2-7/P2-8/P2-9 均 STILL。

**R 系列**：14/15 STILL（R4 仅文档化且范围写窄：只写 dispose/restart/update，未写
activation-vs-dispose；R12 属设计选择）。

### 9.4 v0.2 新引入 / 新暴露的问题（按严重度）

1. **fail-open 语义（安全相关）**：waterfall 中间件 panic 后**继续跑 `next.next()`**
   （`waterfall.rs:95-107`）⇒ 本应 veto/中断的策略中间件失败后操作**照常执行**；TS 是
   panic 中止。sync listener panic 也被吞（`events.rs::call_sync`）。panic 逃逸契约
   变成三种（cleanup 重抛 / listener 吞 / waterfall 继续），spec 只写了第一种。
2. **dispose panic 破坏状态收尾**（9.2-P0-4）：卡 Unloading + registry 残留 + ctx 图保留。
3. **`deferred_async` 泄漏 + 跨 lane 顺序破坏**：失败激活把 AsyncDisposer 存进
   `deferred_async`（`fiber.rs:135,471-475`）；Failed fiber 永不 dispose ⇒ 清理永不执行
   （探针 before=[] after dispose=["async-cleanup"]）；跨 sync/async 的逆序契约
   （CORE-007）在 rollback 路径不成立。
4. **provide-cleanup 静默失效（A/B 实测回归）**：Weak upgrade 失败即 return
   （`context.rs:355-368`）⇒ provider context 先 drop 时 service 残留、再 provide 报
   `DuplicateService`；v0.1 行为正确。
5. **`*_any` 绕过 accessor**：`get_any/set_any` 不查 `find_accessor`
   （`context.rs:407/428`），与 `get/get_named` 不一致。
6. **rollback 信封未隔离**：`transition(Loading)`、`internal/config` waterfall、
   `validate_config` 在 apply catch 之外（`fiber.rs:557-565`）⇒ 这些点 panic 会逃出
   `ctx.plugin`、fiber 卡 Loading、无回滚；`fail_fiber_*` 的 `transition(Failed)` 也可能
   被 status hook panic 打断（违反 CORE-029）；poison 时 rollback 静默 `Vec::new()`。
7. **async 激活不可取消**：`plugin_async` future 被 drop ⇒ 停在 Loading + 残留 effect、
   无回滚；`await_settled` 不覆盖激活（Loading 时 2.2µs 返回 Ok）；`refresh_all_async`
   串行 await，一个挂起拖住全体。
8. **BlockingExecutor 全局 OnceLock** 首次生效不可替换；executor 拒绝/panic 会泄漏
   cleanups 并可能 abort；`set_blocking_executor` 返回值常被忽略。
9. **Weak 根链静默 no-op**：root handle 全 drop 后 `dispose()/restart()` 返回 Ok 但
   `finish_unload` 跳过 remove_owner/unregister（`fiber.rs:722-746`）；`refresh_all` 对
   `ctx_ref()==None` 静默 continue（`registry.rs:434-440`）。
10. **Known limitations 描述错误/低估**：`update` 说明写反（校验在 `internal/update`
    **之前**，且 hook 替换后的 config 不经再校验就 restart）；"dispose everything on
    shutdown" 不足以释放（10k/10k 仍泄漏）；"root-only cycle fixed" 只对 provide 路径
    成立（accessor 例外）；复活竞态（activation-vs-dispose）未列入。

### 9.5 建议 v0.2.1（先做 6 件）

1. **fail-open 回退**：waterfall panic 必须终止链（至少不执行 `next`）；listener panic
   策略二选一并写入 spec（建议 dispatch 记录并继续、提供 `emit_checked`；waterfall 中止）。
2. **状态收尾原子化**：`finish_unload` 放进 `catch_unwind` 的 finally（先 finish 再重抛）；
   apply 返回后与 rollback 后都做 Disposed/epoch 守卫（顺带修 P0-3 与 async 取消）。
3. **P0-5**：effect wrapper 先注册 + `effect_stack` RAII guard（~40 行）。
4. **A4 收尾**：internal hooks/logger exporter/accessor/schema/async lane/async waterfall
   统一 `catch + log`；Drop 路径不 panic（`thread::panicking()` 时跳过阻塞/重抛）。
5. **A5 收尾**：plugin fiber 的 ctx/parent 弱化 + dispose 清 parent + `Registry.fibers`
   弱引用/ dispose 即 prune + accessor disposer Weak；provide-cleanup 按 `(scope,name)`
   删除而不依赖 upgrade。
6. **规范/文档**：修 §2.7 编号、§2.2/§2.4 正文、§3 `EffectResult`、README 示例、
   `docs/CORDIS-RUST.md`；把 P0-3/P0-5/deferred_async/BlockingExecutor 的真实边界写进
   Known limitations（或修掉）。

### 9.6 复核复现路径

- v0.2 快照 `/data/Cordis/target/scratch/v020`；`CARGO_HOME=…/cargo-home cargo test
  --workspace` → 103 passed。
- 所有权：`/data/Cordis/target/scratch/verify-own/probe020`
  （roots / provider / accessor / loop / weak-correctness / stale-service / retention /
  disposer / abort；`stale_v010` vs `stale_v020` 为 A/B 回归证据）。
- 异步：`/data/Cordis/target/scratch/probe-async` + `RESULTS-v020.md`（p*/v06* bins，
  `timeout 5`：124=挂起、134=abort）。
- parity：`/data/Cordis/target/scratch/verify-parity`（源码 + report）。
- panic：`/data/Cordis/target/scratch/verify-panic/probe-v020`（A/B 同源探针）。
- Rust 特性：`/data/Cordis/target/scratch/verify-rust/probe`。

## 10. v0.3.0 进展（分支 `fix/kernel-hardening-v0.3`，HEAD `71e99f1`）

**状态**：v0.2 清单上的 P0 + 大多数 P1 + 大部分 R 已关闭；141 tests 全绿（含 22 条
red-team 验收 + 5 条 v0.3 语义用例），fmt/clippy 干净。

### 10.1 本轮关闭（第二轮之后新增）
| 项 | 状态 | 证据 |
|---|---|---|
| P1-2 raw config 保留、每次激活只解析一次 | 关闭 | `FiberData.raw_config`；`semantics::raw_config_is_re_resolved_from_raw_on_every_activation`（R→R，不再 R→RR）|
| P1-3 provider 身份 epoch（冻结依赖方在旧 provider 上） | 关闭 | `ServiceEntry.version` + `dep_versions`；`semantics::provider_version_change_reactivates_dependents`；isolate 阴影/替换/provider 重启都会重激活 |
| P1-11 `ctx.set` 不可逆/不分类 | 关闭 | 逆 effect（`set_revertible`）+ version bump；`semantics::set_is_revertible_and_classified` |
| P1-12 provider 撤销顺序（S3） | 关闭 | `take_effects` 先 `withdraw_owner_services` 再 drain；`semantics::provider_withdrawal_precedes_binding_removal` |
| P1-10 AsyncDisposer 无 join | 关闭 | join state + cancel-safe finish guard；`semantics::concurrent_async_dispose_joins_the_inflight_cleanup` |
| P1-1 restart 复活 / rt12 | 关闭（上一轮） | `redteam::rt12_restart_after_dispose_fails_safely` |
| A5 剩余的 dispose 无效、accessor 环、stale service、Drop 双 panic | 关闭 | 实测 10k plugin boots + dispose+drop → **0 泄漏**；accessor roots 0；drop-during-unwind 从 abort 134 → exit 101；stale `DuplicateService` 回归消失 |
| 固定点 refresh | 关闭 | refresh_depth CAS + dirty 标记，64 轮上限；200 injectors + provider withdrawal 无死锁 |

### 10.2 仍未关闭（v0.4 目标）
- **R4 生命周期并发串行化**：`dispose/restart/update` 并发仍靠 host 纪律（复活已 fence，竞争仍在）。
- **R5 取消安全**：drop `dispose_async` 会丢在飞 cleanup（无 cancel-safe drain guard）。
- **child fiber 归属**：父 fiber 不拥有子 fiber（README 已声明，实现待做）。
- **undisposed fiber 图泄漏**：17.8 KB/boot（未 dispose 时；dispose 后 0）。
- **quiescence/watchdog/quota**：`when_idle`、heartbeat、deadline、checkpoint、per-fiber 资源计量（隔离 L2 的前置 API）。
- **隔离只有文档约定**：本 crate 是**纯 Rust 进程内内核**——不引入 ABI/FFI/dylib/动态插件加载/进程隔离/WASM 宿主，也不把 `registry.replace` 之外的动态加载能力纳入范围。`docs/KERNEL-ISOLATION.md` 的 L3/L4 只是写给 **embedder** 的建议（在 crate 之外实现），不是本 crate 的实现目标。
- **R7/R8/R10/R13/R15/R16/R17**：`'static`/Send 约束、identity/downcast、`!Send` local context、递归上限、perf（去 String 分配 / refresh 索引）、typed config、scope id 命名空间。
- **P2-3/P2-6/P2-7/P2-8 等文档项**：spec §2.7 prose 编号、CORE-011 冗余、event 模式文档、scoped layered registry。
- **`Registry::replace`**（见 §10.3）。

### 10.3 上游（github.com/cordiverse/cordis）2026-10-28 更新与借鉴

上游 HEAD `26d9003`，今天两个代码提交：

| 上游提交 | 内容 | 我们的处置 |
|---|---|---|
| `74b7905` 16:07 | **`registry.replace()`**：把现有 fiber 原地重绑到新插件（保留 fiber 身份、换 runtime/inject/Config、清 store、`_revision++` 触发 epoch reload）、新增 `internal/replace` 事件、旧插件句柄经 WeakMap 转发、`delete()` 降为 `_unregister`；HMR 第二阶段改为调用 replace；新增 16 个测试 | **v0.4 借**：我们有 dep-version epoch，可等价实现 `Fiber::replace(&self, Arc<dyn Plugin>)` + `Registry::replace(key, plugin)`；适配 `internal/replace` 钩子并移植那 16 个用例（约 150–250 LOC）|
| `bd427d0` 02:34 | **epoch 变化时关闭 async-generator effect**：`try/finally` + `await iter.return?.()`，否则迭代器被丢弃、`finally` 清理被静默跳过 | **部分等价**：我们的 `effect_stream` 是注册期主动 drain，没有长驻 iterator，此 bug 类不存在；但 `effect_future`/长驻异步 effect 的"epoch 变化即关闭"语义缺失，v0.4 与取消安全一起做（文档+测试先行）|
| `26d9003` 16:22 | HMR 文档 | 跳过 |

**重要事实**：DSH 与 picoaide 的 `vendor/cordis` 是 **fork 而不是快照**（2,696 行 vs
上游 1,998；`fiber.ts` 754 vs 554，含本地 inertia/事务化激活改动），因此上游修复需要
手工移植；`bd427d0` 的 bug 仍存在于 `vendor/cordis/src/fiber.ts:390`。我们的 Rust
v0.3 已经独立具备：settle 覆盖、CORE-014 隔离查找、provider 撤销顺序、
`set_revertible`、disposer join、parent/ctx 边断开、provider-identity epoch。仍缺：
`registry.replace`、长驻 async effect 的 epoch 关闭、parallel 错误聚合（上游
AggregateError 语义；我们现在是逐 listener 遏制+日志）。


### 10.4 第四轮补充（red-team round-2 攻击闭合）

red-team 第二轮针对 v0.3 新语义写了 33 条攻击用例：首轮 **27 通过 / 6 失败**，暴露
4 个根因；修复后 **33/33 全绿**（已入仓 `tests/redteam3_attacks.rs`），全套 **174 tests**。
| 根因 | 修复 |
|---|---|
| `update()`/`update_async()` 把 resolve 后的值当 raw 存回 → 再次激活叠加（`y|R|R`）| raw 只在**校验成功后**提交；`internal/update` hook 透传时不得存回 resolve 值，只有"替换成不同 config"才成为新 raw（CORE-040）|
| 失败的 update 已把被拒绝的 raw 写入 | 同上：raw 提交移到校验成功之后（at19）|
| set 的逆在 provider 卸载时恢复成 `withdrawn=false` → 依赖方在 `Unloading` provider 上重新激活（重复 cleanup）| 逆恢复时若 owner 处于 `Unloading/Disposed` 则保持 `withdrawn=true`；set 在卸载期直接拒绝写（at09）|
| `set_revertible` 公开 API 绕过 ownership 检查 | 检查移入 `set_revertible` 内部（`set`/`set_any`/公开 API 同一条路径，at12）|
| 嵌套链 A→B→C 卸载顺序错误（B 先于 C 清理，C 的 binding 已移除）| 撤回后先 `Registry::retire_unready()` 迭代到不动点（叶子先退），再 drain（at23）|

已知未闭合的 red-team 非失败风险（下一轮）：(1) at16 依赖方会短暂对 `Loading`
provider 激活（provider 状态未参与可用性判定，即原 P0-1 的残余）；(2) set 在
`Unloading` 期被拒绝前已发生的写侧副作用需要内部测试固化。

### 10.5 第五轮（R4 + R5）：生命周期串行化与取消安全

- **R4 生命周期串行化**：每个 fiber 增加 lifecycle slot（owner thread token + 重入深度）。
  `dispose`/`restart`/`update` 与 async 版本互相排斥：**其它线程**的并发调用返回
  `CordisError::LifecycleBusy`（非阻塞、可安全重试），**同线程重入**（cleanup 里 dispose
  自己、update 调 restart）仍然允许。激活期间的 dispose 仍走 fencing 回滚。新增
  CORE-045 + `tests/concurrency.rs` 三个用例；rt13 改成新契约（并发 dispose 被拒，restart
  正常完成，无僵尸）。
- **R5 取消安全**：`AsyncDisposer::dispose` 与 `Fiber::dispose_async` 增加 cancel-safe
  drain guard——future 在 await 中被打断时，**未开始的 cleanup 会被放回**（顺序保持，
  下次 dispose 继续跑），只有当时正在 await 的那一个 cleanup future 无法回收（Rust 无
  cancel-safe await），会记日志。新增 CORE-046 + 两个取消用例。
- 全套 **178 tests / 21 suites** 全绿，fmt/clippy clean。
- **范围更正（用户要求）**：本 crate 是**纯 Rust 静态链接内核**，不引入 ABI/FFI/dylib/
  动态加载/进程隔离/WASM 宿主；`KERNEL-ISOLATION.md` 的 L3/L4 只是 embedder 建议，已加
  范围声明；`Registry::replace` 属于纯 Rust 语义（同进程内替换插件实现），可以借；
  进程隔离/动态 ABI 方案不属于本 crate 范围。

### 10.6 第六轮（R4/R5 反证闭合 + 范围更正）

red-team round-3（28 条 R4/R5 攻击）首轮 22/28，暴露 3 个根因，修复后 **28/28 全绿**，
全套 **206 tests / 22 suites**：

| 根因 | 修复 |
|---|---|
| 取消 disposal 时 Drop 同步执行 cleanup（current_thread 死锁 / runtime 外 panic / 丢清理）| 取消改为**纯恢复**：`CancelSafeDrain` 只回填未开始的 cleanup 并标记 cancelled；`AsyncDisposer::Drop` 在 cancelled 时不再 block/执行；fiber 级 `CancelSafeUnload` 用 `last_mut` 把在飞 effect 留在列表中，取消时整体回填（顺序不变）|
| 重入 restart/update（从 cleanup 内）在外层 drain 后重新激活 → effect 泄漏 | `restart/update`（含 async）在 `Unloading` 状态返回 `InactiveEffect`；`Disposed` 返回 `Disposed`（与 README 一致）|
| 慢 apply 与 restart 竞争：过期激活失败回滚了新一代的 effect/service（restart 返回 Ok 但 fiber Failed）| 每次激活分配 generation；effect 打上注册时的 generation；过期尝试的成功/失败只丢弃**它自己**注册的 effect，不再 transition/rollback 新一代 |

**范围更正（用户明确要求）**：本 crate 是**纯 Rust、静态链接、进程内 Cordis 内核**。
不引入 ABI/FFI/cdylib/dylib/动态加载/进程隔离/WASM 宿主；进程隔离方案属于
embedder 层，不属于本 crate。`KERNEL-ISOLATION.md` 已加"L3/L4 只是 embedder 建议"的范围声明。
`Registry::replace` 属于同进程纯 Rust 语义，可在下一轮按上游语义实现。

### 10.7 第七轮：child-fiber 归属（CORE-047）

- `ctx.plugin` 现在把子 fiber 注册为**父 fiber 的 effect**（`Weak` 句柄，避免
  parent↔child 环），父卸载时按 effect 逆序级联 dispose 整棵子树；父激活失败的回滚
  同样会回收已创建的子 fiber。`tests/child_ownership.rs` 覆盖 4 种场景（父 dispose 级联、
  失败回滚、三层嵌套、root fiber 级联）。
- 全套 **210 tests / 22 suites** 全绿；v0.2 起"child fiber 不被父拥有"的 Known
  limitation 已从 README 删除，spec 增加 CORE-047。
- 下一轮：`Registry::replace`（上游纯 Rust 语义）、未 dispose fiber 回收兜底、
  quiescence/watchdog/quota API，并跑新一轮独立对抗验证。

### 10.8 第八轮：child ownership 对抗闭环（CORE-047 加固）

red-team round-4（22 条 child-ownership 攻击）首轮 18/22，暴露 4 个根因，修复后
**22/22 全绿**，全套 **232 tests**：

> **（0.4.2 更新）** 232 tests 是当轮历史快照。本表 c16 的「cancel-safe drain 恢复 effect +
> `ChildSweep` Drop 回填重试」机制已被 0.4.2 的 **A-9** 取代：取消的 fiber unload 现在
> **完成 teardown**（sync cleanup 内联、async cleanup 延迟到 `deferred_async`，fiber 落到
> `Pending`/`Disposed`），不再回退旧状态。以下内容保留为历史记录；当前语义见 spec §2.3
> （CORE-046）与 `docs/CORDIS-RUST.md` §2.3。

| 根因 | 修复 |
|---|---|
| c14 父 dispose 撞上子 fiber restart：ownership 清理吞掉 `LifecycleBusy`，父 Disposed 后子仍 Active | 子 fiber 增加 `dispose_requested` 标志：Busy 时置位，子的 restart/update 结束时 `finalize_requested_dispose` 在持有 lifecycle 的同一线程上完成 dispose |
| c16 取消父 `dispose_async` 中途：在飞 ownership effect 丢失，重试后子卡 `Unloading` | ownership 改为**可恢复的 effect**（父效果表由 cancel-safe drain 恢复）+ 父 fiber 保留 `Weak` 子列表 + 取消安全的 sweep guard（`ChildSweep` 在 Drop 回填未处理的子）；重试时重新驱动。**（历史机制：0.4.2 A-9 已改为「取消即完成 teardown」，见本节开头注）** |
| c10 取消 `plugin_async` 后子仍 Active（deferred 路径） | 失败/取消回滚路径 `fail_fiber_*`/`discard_*` 都执行 `sweep_children_*`，子被同步/异步回收 |
| c12 A→B→A 递归 loading → 栈溢出 SIGABRT | 增加 `MAX_PLUGIN_DEPTH = 256` 加载深度上限：超限返回 `InvalidPlugin`（深度 100/500/1000 正常，1500 之前会栈溢出——现在统一在 256 处被拦截，实测 c05/c12 通过）|

已知边界（文档已写）：取消 disposal 时**最多丢失当时正在 await 的那一个 cleanup future**
（Rust 无 cancel-safe await），其余 cleanup 在重试中按逆序执行一次；`c16` 断言按此边界修正。

> **（0.4.2 更新）** 该「恢复 effect 后重试」的取消边界已被 A-9 取代：取消 unload 会完成
> teardown（sync 内联 / async 延迟），fiber 不会停在 `Unloading` 或回退成假 `Active`；仅
> 在飞的那一个 async cleanup future 不可恢复。当前语义见 spec §2.3 CORE-046。

### 10.9 第九轮：`Registry::replace`（CORE-048，上游 74b7905 的纯 Rust 移植）

- `Registry::replace(old_key, new: Arc<dyn Plugin>)` / `replace_async`：旧 runtime 的
  所有 fiber **原地重绑**到新实现（保留 fiber id/身份与 raw config），runtime 迁到新
  plugin 的 key，并按正常激活管线重新加载（inject gate、config 解析、失败回滚）。
- `Fiber::replace_plugin` / `replace_plugin_async` 提供单 fiber 版本。
- `tests/replace.rs` 覆盖 6 个场景（换实现+依赖方重激活、未知 key、async 替换、
  已 dispose fiber 安全失败、raw config 不叠加、单 fiber 替换）。
- 全套 **238 tests / 23 suites** 全绿；纯 Rust、无 ABI/FFI（范围红线不变）。

### 10.10 第十轮：replace 的原子性（round-5 反证闭合）

red-team round-5（22 条 replace 攻击）首轮 19/22，2 个根因，修复后 **22/22**：

| 根因 | 修复 |
|---|---|
| t04/t22：`Registry::replace` 先 rebind 后 reload，reload 被 `LifecycleBusy` 拒绝时已部分迁移（fiber 改名、key 迁移但服务还是旧的）| 记录 `RebindUndo` 快照（旧 runtime、每个 fiber 的旧 PluginKind/runtime_key、target 是否新建）；任一 fiber reload 失败即回滚 runtime 表与全部 fiber 绑定，再返回 Err |
| t10：`Fiber::replace_plugin` 只换实现不迁移 runtime key（`get(named(new))==None`、无法 delete 新 key）| `Fiber::replace_plugin(_async)` 现在通过 `Registry::rebind_single_fiber` 完成旧 runtime detach + 新 runtime attach + `runtime_key` 更新；reload 失败时同样回滚实现与 key |

### 10.11 第十一轮：quiescence + 协作取消（CORE-049/050）

- **quiescence**：`Registry::is_idle()` / `Registry::when_idle()` / `Context::when_idle()`。
  `transition` 与生命周期操作结束时唤醒 idle waiters；`Load`/`Unloading` 或 `pending_ops>0`
  都视为不静默。纯 Rust、无 preemption——只是 supervisor/shutdown 需要的观测原语。
- **协作取消**：`Fiber::cancel()` / `is_cancelled()` + `Context::checkpoint()`；插件在长任务里
  定期 checkpoint，取消时得到 `CordisError::Cancelled` 并可走正常回滚路径。内核**不会**
  强杀插件（Rust 进程内无法安全打断线程）。
- `tests/quiescence.rs` 4 个用例；全套 **264 tests / 25 suites** 全绿。

### 10.12 第十二轮：quiescence 反证闭合（round-6）

red-team round-6（19 条 quiescence/cancel 攻击）首轮 16/19，3 个根因，修复后 **19/19**：

| 根因 | 修复 |
|---|---|
| f01 终態 dispose 漏唤醒：`end_operation` 用 `ctx_ref()` 通知 idle，而 Disposed fiber 的 ctx 已被清空 → waiter 永久 pending（真实线程下挂起）| `FiberData` 保存 `Option<Arc<Registry>>`；`end_operation`/`transition` 改为经 registry 通知，不依赖 ctx 链接 |
| f02 被丢弃的 `when_idle` future 泄漏 waker（100 个全被保留并唤醒）| `when_idle` 改为具名 future `IdleWait`（id + Drop 注销，与 `Settled` 同模式）|
| f03 root fiber 的生命周期对 quiescence 不可见（`registry.fibers` 不含 fiber 0）| `Registry` 以 Weak 保存 root fiber；`is_idle`/`when_idle` 把 root 的 state 与 `pending_ops` 计入 |

- 全套 **283 tests / 26 suites** 全绿；quiescence/cancel 语义现在覆盖 root fiber、漏唤醒与 waiter 泄漏边界。

### 10.13 第十三轮：R17 scope-id 命名空间修正

- `isolate_labeled` 之前用 FNV-1a(label) 生成 scope id，与 `isolate()` 的计数器 id 属于
  两个命名空间，理论上可能撞值导致两个本应隔离的 scope 共享服务。现在 label → id 的映射
  放在 `Registry::label_scopes`，id 来自同一个 `next_scope_id()` 计数器，**两个命名空间
  不再可能碰撞**；`isolate_labeled` 的语义（同 label 共享 scope）不变。
- 全套 **283 tests** 仍全绿。

### 10.14 第十四轮：round-7 终验（零新失败）

- red-team round-7（18 条最终验证：R17 scope 可见性矩阵、quiescence 三个 round-6 回归、
  取消隔离/checkpoint 回滚、50 次 replace 循环、1000 roots labeled scope 泄漏）**18/18
  通过 ×3 次**；全量 **301 tests / 30 suites 绿，exit 0**。**无新的内核失败**。
- 唯一观察项是测试自身的竞态：`t07_is_idle_when_idle_stress` 里无界 `while is_idle()`
  观测自旋在 CPU 负载下可能错过 1ms 激活窗口而永久自旋（2/200 负载探针）。已把自旋改为
  5s 上限 + 继续由 `when_idle`/join 判定，t07 单独跑 3/3 通过；内核没有任何 missed wakeup。
- 至此七轮对抗共 **183 条**攻击用例全部在仓内常绿。

### 10.15 第十五轮：文档终稿 + round-7 验收入仓

- docs-parity 完成 spec/README/docs 一致性：§1/§2.6 child-fiber ownership、§2.4 CORE-043、
  §3 v0.3 全量 API（when_idle/checkpoint/cancel/is_cancelled/raw_config/replace_*、
  Registry::is_idle/when_idle/replace、Cancelled/LifecycleBusy/non_exhaustive）、
  §4 CORE-011 标记为 async-lane 重复、§5.8 ABI 边界、§6 v0.3.0 + CORE-040…050；
  README 改为 301 tests、删除已修限制（replace/quiescence/cancellation）、保留诚实残余
  （未 dispose 泄漏 ~9 KB/boot 实测、sync dispose 需 executor、update 校验时机、
  ctx.effect owner window、取消最多丢在飞 cleanup、abort/UB/栈/OOM/hang 不可遏制、
  无 ABI/动态加载、无抢占/forced thread kill）。两份 README 示例重新编译通过。
- round-7 终验 18 条用例入仓（`tests/redteam8_attacks.rs`），连同 t07 自旋竞态修复；
  七轮累计 **183 条**对抗用例常绿。
- 最终：**301 tests / 30 suites 全绿**，fmt + clippy -D warnings 干净。

### 10.16 第十六轮：shutdown 路径 `dispose_all`（CORE-051）

- 未 dispose fiber 泄漏（~9 KB/boot 实测）现在有正式关闭路径：
  `Context::dispose_all()` / `Registry::dispose_all()` 逐个 dispose 所有 fiber 与 root
  fiber，收集首个错误但继续尝试；返回后 registry 为空且 `is_idle()` 为 true。
  README 的 shutdown 指引从"自己遍历"改为直接调用该 API。
- `tests/child_ownership.rs` 新增 `dispose_all_reclaims_the_whole_tree`；全套
  **302 tests / 30 suites** 全绿，fmt/clippy clean。
