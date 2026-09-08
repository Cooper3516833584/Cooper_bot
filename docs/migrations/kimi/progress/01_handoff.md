# 阶段 01 交接

## 阶段

- 阶段编号与名称：01｜固定基线、盘点调用者、实测 CLI 合同
- 开始 / 结束 SHA：`0bf099f78c71f0ffe9a3fcafa3273275553ea719` / 未提交
- 工作区原有改动：`config/ai/private_chat_prompts.json`、`cooper_bot/core/config.py`
- 本次状态：`BLOCKED_EXTERNAL`

## 修改摘要

| 文件 | 变更 | 原因 |
| --- | --- | --- |
| `docs/migrations/kimi/progress/MIGRATION_STATUS.md` | 新增阶段状态 | 跟踪真实验收状态。 |
| `docs/migrations/kimi/progress/01_baseline.md` | 新增基线 | 记录 SHA、工作区与测试证据。 |
| `docs/migrations/kimi/progress/01_callsite_inventory.md` | 新增调用图 | 防止误迁 DeepSeek 内部任务或提前删除日历入口。 |
| `docs/migrations/kimi/progress/01_cli_contract.md` | 新增 CLI 合同 | 固定已观察到的 Kimi 二进制与未验证运行时边界。 |
| `docs/migrations/kimi/progress/01_handoff.md` | 新增交接 | 按阶段规格记录结论。 |

未改动高风险文件；未改动用户已有文件；无整文件重写或换行转换。

## 验证证据

| 命令 | 结果 / 退出码 |
| --- | --- |
| `python --version` | Python 3.13.5 / 0 |
| `python test_client.py unit` | 169 passed、2 failed、19 errors / 1（既有基线） |
| `python test_client.py smoke` | 4 passed / 0 |
| `kimi --version`、`kimi --help`、`kimi doctor`、`kimi provider list` | 0；已记录于 CLI 合同 |

L1 单元：已运行，基线失败如 `01_baseline.md` 所列。
L2 fake 进程：NOT_RUN。
L3 真实 Kimi：BLOCKED_EXTERNAL。
L4 目标机业务：NOT_RUN。

## 权限、历史与副作用

本阶段未改 public/admin profile、个人授权、历史、去重、自动重试或取消逻辑。未启动 `client.py`、未发 QQ 消息、未执行本机操作、未调用模型；因此没有新增外部副作用或凭据泄露。

## 未满足门槛与下一步

阶段 01 的 SHA、平台、版本、调用图及测试基线已记录；真实 JSONL fixture、搜索、工具拒绝和进程生命周期仍未验证，故状态不能为 PASS。

下一阶段为 `02｜保护 DeepSeek 非聊天`。但若严格按迁移规格的“门槛未通过即停止”，必须先建立隔离 Kimi profile，或明确接受将真实 CLI 验收推迟为 `BLOCKED_EXTERNAL` 后再执行仅离线的阶段 02。
