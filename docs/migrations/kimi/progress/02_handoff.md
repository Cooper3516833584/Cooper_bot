# 阶段 02 交接

## 阶段

- 阶段编号与名称：02｜保护 DeepSeek 非聊天
- 开始 SHA：`0af0c8e`
- 结束 SHA：未提交
- 工作区原有改动：`config/ai/private_chat_prompts.json`、`cooper_bot/core/config.py`
- 本次状态：`PASS`

## 修改摘要

| 文件 | 变更 | 原因 |
| --- | --- | --- |
| `cooper_bot/modules/ai/aisvc.py` | 新增 `deepseek_task_ready`、`deepseek_task_text()` 和同步实现；保留 `_chat_sync()` 的兼容转发 | 让内部 DeepSeek 无状态文本任务不依赖未来会改为 Kimi 的 QQ chat 接口。 |
| `cooper_bot/modules/calendar/daily_calendar.py` | 日历补充文案改用显式 DeepSeek 任务接口 | 防止阶段 06 切换 QQ chat 后误将日历文案迁到 Kimi。 |
| `test/unit/test_aisvc_deepseek_tasks.py` | 新增任务接口与兼容转发测试 | 固定原模型、温度、thinking、reasoning effort 与超时语义。 |
| `test/unit/test_daily_calendar.py` | 新增日历显式接口与不可用回退测试 | 断言日历不调用 QQ chat，DeepSeek 不可用时保留原模板。 |
| `docs/migrations/kimi/progress/MIGRATION_STATUS.md` | 标记阶段 02 通过 | 保留迁移验收记录。 |

未修改命令路由、Kimi 配置、Antigravity 实现、数据格式或 DeepSeek SDK/helper。高风险文件仅有 `aisvc.py` 的局部新增/转发。

## 需求与调用图

- B 类邮件、通知、资料分类/纠偏/摘要已直接使用原 DeepSeek helper，未作无意义重写。
- B 类日历补充文案原先经 `chat()` 间接调用，现显式使用 `deepseek_task_ready` / `deepseek_task_text()`。
- A 类 QQ chat 在本阶段仍经 `chat()` / `_chat_sync()` 的 DeepSeek 兼容转发，未切 Kimi。
- C 类日历联网事实查询仍使用 Antigravity 受限入口，留待阶段 07 迁移。
- 没有新增聊天 fallback，Kimi 调用次数为 0（本阶段尚无 Kimi runner）。

## 验证证据

| 命令 | 实际结果与退出码 |
| --- | --- |
| `python -m pytest test/unit/test_aisvc_deepseek_tasks.py test/unit/test_daily_calendar.py test/unit/test_email_notify.py --basetemp=runtime/.pytest_kimi_tasks` | 25 passed / 0 |
| `python test_client.py unit --basetemp=runtime/.pytest_kimi_unit` | 193 passed、2 failed / 1；两项均为阶段 01 已记录基线失败 |
| `python -m compileall -q ...` | 0 |
| `git diff --check` | 0 |

测试临时目录仅为 `runtime/.pytest_kimi_tasks` 与 `runtime/.pytest_kimi_unit`，已在验证后删除。未调用真实 API、Kimi 或 QQ 服务。

## 权限、历史与副作用

本阶段没有改动 public/admin 权限、管理员授权、聊天历史、消息去重、自动重试、取消或日志边界。DeepSeek 内部任务仍使用原凭据和 HTTP/SDK helper；没有把凭据传递给 Kimi。

## 风险与下一步

`chat_ready` 目前仍是 `deepseek_task_ready` 的兼容别名，`_chat_sync()` 也是临时兼容转发；二者在阶段 06 切换 QQ chat 时必须重新收口，不能提前删除。

下一阶段：03｜配置与 profiles。阶段 01 的真实 Kimi public/profile 验收仍为 `BLOCKED_EXTERNAL`，阶段 03 必须先建立独立 home、工作目录、Agent 与工具白名单，不能复用默认个人 Kimi 配置。
