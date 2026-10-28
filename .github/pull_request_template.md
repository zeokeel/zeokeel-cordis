## 变更摘要

<!-- 一段话说明为什么 / 改了什么 -->

关联 issue：#

## 变更类型

- [ ] fix（bugfix，不改变语义）
- [ ] docs（文档）
- [ ] test（测试）
- [ ] build / ci（构建与 CI）
- [ ] refactor（不改变语义）
- [ ] 语义变更（**功能冻结期需维护者批准**，并附 spec 条目）

## 验证

- [ ] `cargo test` 通过
- [ ] `cargo test --features supervision` 通过
- [ ] `cargo fmt --all -- --check` 通过
- [ ] `cargo clippy --all-targets -- -D warnings` 通过
- [ ] `cargo clippy --all-targets --features supervision -- -D warnings` 通过
- [ ] `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps` 通过

## 契约与范围

- [ ] 如涉及行为变化：已更新 `spec/CORDIS-CORE.md`（CORE-0xx）与 `tests/`
- [ ] 已更新受影响的 `docs/` 与 `CHANGELOG.md`
- [ ] 变更仍在内核职责范围内（见 README「内核 / 宿主边界」）
- [ ] 没有引入新的 `unsafe`（crate 级 `forbid`）

## 其他

<!-- 兼容性影响、迁移说明、性能数据等 -->
