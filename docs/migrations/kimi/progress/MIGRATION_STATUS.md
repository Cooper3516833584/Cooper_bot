# Kimi 迁移进度

本目录记录 `Cooper_bot → Kimi Code CLI` 迁移的实际证据。实施规格来源于用户提供的 `Cooper_bot_Kimi_Migration_Plan`；规格没有整包复制到仓库，避免把未实施的方案文档作为代码改动提交。

## 当前环境

| 项目 | 当前记录 |
| --- | --- |
| 固定 commit SHA | `0bf099f78c71f0ffe9a3fcafa3273275553ea719` |
| 平台 / Python | Windows / Python 3.13.5 |
| Kimi Code | 0.34.0，原生 `kimi.exe` |
| 默认模型 alias | `deepseek/deepseek-v4-flash` |
| thinking | 已启用；provider 声明支持 `max`，当前有效档位未能从脱敏检查确认 |
| public/admin 权限实测 | BLOCKED_EXTERNAL |
| 真实 Kimi JSONL / 搜索实测 | BLOCKED_EXTERNAL |
| 项目测试基线 | smoke PASS；unit FAIL（迁移前既有失败，见 `01_baseline.md`） |

## 阶段状态

| 阶段 | 名称 | 状态 | 证据 |
| --- | --- | --- |
| 01 | 固定基线与 CLI 合同 | BLOCKED_EXTERNAL | `01_baseline.md`、`01_callsite_inventory.md`、`01_cli_contract.md`、`01_handoff.md` |
| 02 | 保护 DeepSeek 非聊天 | PASS | `02_handoff.md` |
| 03 | 配置与 profiles | NOT_STARTED | — |
| 04 | CLI 运行器 | NOT_STARTED | — |
| 05 | 上下文与图片 | NOT_STARTED | — |
| 06 | 统一聊天路由 | NOT_STARTED | — |
| 07 | 日历联网 | NOT_STARTED | — |
| 08 | 清理旧实现 | NOT_STARTED | — |
| 09 | 汇总验收 | NOT_STARTED | — |
| 10 | 部署与回滚 | NOT_STARTED | — |

`BLOCKED_EXTERNAL` 不表示 Kimi 不可用；它表示阶段 01 所要求的隔离 profile、真实 JSONL 协议、搜索与运行时工具边界尚未在无业务密钥的临时环境完成验证。
