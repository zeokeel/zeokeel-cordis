# Changelog

本文件格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，
版本号遵循 [Semantic Versioning](https://semver.org/lang/zh-CN/)。

## [Unreleased]

**v0.4.x 语义冻结**：不再新增语义能力；仅接受 bugfix、安全修复、性能改进（不改变语义）
与文档修正。

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

[Unreleased]: https://github.com/zeokeel/zeokeel-cordis/compare/v0.4.2...HEAD
[0.4.2]: https://github.com/zeokeel/zeokeel-cordis/releases/tag/v0.4.2
[0.4.1]: https://github.com/zeokeel/zeokeel-cordis/releases/tag/v0.4.1
[0.4.0]: https://github.com/zeokeel/zeokeel-cordis/releases/tag/v0.4.0
[0.3.0]: https://github.com/zeokeel/zeokeel-cordis/releases/tag/v0.3.0
[0.2.0]: https://github.com/zeokeel/zeokeel-cordis/releases/tag/v0.2.0
[0.1.0]: https://github.com/zeokeel/zeokeel-cordis/releases/tag/v0.1.0
