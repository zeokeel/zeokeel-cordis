# 安全策略

## 支持版本

| 版本 | 支持 |
| --- | --- |
| `0.3.x` | ✅ 安全修复与 bugfix |
| `0.2.x` 及更早 | ❌ 已 EOL |

v0.3.x 是当前功能冻结线；安全修复会以补丁版本形式发布。

## 报告漏洞

**请不要在公开 issue 中报告安全漏洞。**

请使用 GitHub 的私有安全报告渠道：

- 仓库页 → **Security** → **Advisories** → **Report a vulnerability**
  （<https://github.com/zeokeel/Cordis/security/advisories/new>）

报告请包含：受影响的版本/commit、复现步骤或 PoC、影响评估、建议修复（如有）。
维护者会尽力在 **72 小时内**确认收到，并在修复可用后协调披露。

## 威胁模型与隔离边界

内核的隔离承诺是**进程内 best-effort**，不是沙箱。请以
[`docs/KERNEL-ISOLATION.md`](docs/KERNEL-ISOLATION.md) 为准，摘要：

**可以遏制（L1）**

- 插件 `apply` / `apply_async` 的 unwinding panic：激活失败并回滚，内核继续运行；
- 清理函数、事件监听器、内部 hook、配置 schema、访问器、日志导出器的 panic：记录并隔离；
- 生命周期并发调用：返回 `LifecycleBusy`，不产生数据竞争。

**进程内不可遏制（`panic=unwind` 之外）**

- `panic="abort"` 构建：本 crate 直接编译失败（编译期 guard）；
- 栈溢出、OOM/分配器 abort、依赖或宿主中的 `unsafe` UB、死锁/无限循环、
  线程被外部 kill：**内核无法处理**，必须由宿主用进程隔离（L3）或 WASM（L4）解决。

**内核不提供的**：沙箱、动态插件加载/稳定 ABI、抢占式超时、线程/fd/内存配额强制、
权限系统。需要这些能力时请在 embedder 层实现。

可选 feature `supervision` 提供**观测与协作**手段（能力自省、per-fiber 配额、
心跳与 watchdog 观测、协作取消），不改变上述边界。

## 依赖安全

- CI 使用 `cargo audit`（RustSec）检查已知漏洞，失败会阻塞合并；
- Dependabot（GitHub Actions + Cargo）每周检查更新；
- 引入新依赖时请评估维护状态与 `unsafe` 使用情况。
