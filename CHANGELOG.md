# Changelog

本文件格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，
版本号遵循 [Semantic Versioning](https://semver.org/lang/zh-CN/)。

## [Unreleased]

**v0.4.x 语义冻结**：不再新增语义能力；仅接受 bugfix、安全修复、性能改进（不改变语义）
与文档修正。

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

[Unreleased]: https://github.com/zeokeel/zeokeel-cordis/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/zeokeel/zeokeel-cordis/releases/tag/v0.3.0
[0.2.0]: https://github.com/zeokeel/zeokeel-cordis/releases/tag/v0.2.0
[0.1.0]: https://github.com/zeokeel/zeokeel-cordis/releases/tag/v0.1.0
