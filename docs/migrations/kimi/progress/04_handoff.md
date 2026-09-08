# 阶段 04 交接

## 阶段

- 阶段编号与名称：04｜CLI 子进程适配器
- 开始 SHA：`b1a94dd`
- 结束 SHA：未提交
- 本次状态：`BLOCKED_EXTERNAL`

## 修改摘要

| 文件 | 变更 | 原因 |
| --- | --- | --- |
| `cooper_bot/modules/ai/kimi_cli.py` | 增加有界异步 runner、可信 argv、环境白名单、JSONL 解析、超时/取消/关闭与安全异常 | 后续聊天只经 `create_subprocess_exec()` 调用 Kimi，绝不经 shell 拼接 QQ 输入。 |
| `test/unit/test_kimi_cli.py` | 新增 fake executable 测试 | 覆盖 argv 注入、工具事件过滤、坏协议、空回复、超时与 Windows 参数预算。 |

## 验证证据

| 命令 | 实际结果与退出码 |
| --- | --- |
| `python -m pytest test/unit/test_kimi_cli.py test/unit/test_kimi_profiles.py --basetemp=runtime/.pytest_kimi_runner` | 9 passed / 0 |
| `python -m compileall -q cooper_bot/modules/ai/kimi_cli.py test/unit/test_kimi_cli.py` | 0 |
| `git diff --check` | 0 |

测试临时目录已删除，未启动真实 Kimi、未发 QQ 消息。

## 安全与限制

- argv 固定为可执行文件、固定 Agent、empty-skills、`stream-json` 与单个 prompt；不含 `--session`、`--continue`、`--auto` 或 `--yolo`。
- stdout/stderr 并发读取，stdout 限制 8 MiB，stderr 只保留有限尾部且不输出到 QQ/普通日志。
- 解析器只接受测试合同内的 assistant JSONL；未知真实协议不能原样转发。
- `aclose()` 仅终止本 runner 登记的进程。Windows 当前是 parent terminate/kill 降级，尚未完成 Job Object 或真实进程树验证；admin 因此不能标 ready。

## 未满足门槛与下一步

阶段 01 没有真实固定版本 fixture，故完整 event 语义、真实 WebSearch、stderr 边界、Windows 子进程树、取消与关闭仍是 `BLOCKED_EXTERNAL`。没有将 fake 协议当作生产验收。

下一阶段：05｜上下文与图片。可先接入既有历史和视觉渲染，且仍不切换 QQ 路由。
