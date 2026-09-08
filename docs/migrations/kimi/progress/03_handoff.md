# 阶段 03 交接

## 阶段

- 阶段编号与名称：03｜配置、工具权限 profiles 与能力自检
- 开始 SHA：`31cf642`
- 结束 SHA：未提交
- 工作区原有改动：`config/ai/private_chat_prompts.json`、`cooper_bot/core/config.py`
- 本次状态：`BLOCKED_EXTERNAL`

## 修改摘要

| 文件 | 变更 | 原因 |
| --- | --- | --- |
| `cooper_bot/core/config.py` | 增加 `AI_KIMI_*` 配置与安全数值解析 | 固定 Kimi CLI、version、profile、home、cwd、超时和开关，不影响旧聊天路由。 |
| `cooper_bot/modules/ai/kimi_cli.py` | 增加 profile/settings/readiness 静态校验与环境白名单 | 把 public/admin profile 固定为内部配置，避免子进程继承业务密钥。 |
| `config/ai/kimi/public.md` | 新增仅 WebSearch 的 public Agent | 默认不授予本机工具或 subagent。 |
| `config/ai/kimi/admin.md` | 新增精确 admin 工具列表 | 为后续个人授权路由准备，不以 `*` 放宽权限。 |
| `config/ai/kimi/kimi.env.example` | 新增无密钥配置示例 | 明确 provider/OAuth 凭据不入仓库。 |
| `test/unit/test_kimi_profiles.py` | 新增 profile 与环境白名单测试 | 检查项目内 public cwd、缺 subagent 禁用和业务密钥继承均被拒绝。 |

## 验证证据

| 命令 | 实际结果与退出码 |
| --- | --- |
| `python -m pytest test/unit/test_kimi_profiles.py test/unit/test_aisvc_deepseek_tasks.py --basetemp=runtime/.pytest_kimi_profiles` | 7 passed / 0 |
| `python -m compileall -q cooper_bot/core/config.py cooper_bot/modules/ai/kimi_cli.py` | 0 |
| `git diff --check` | 0 |

测试临时目录在验证后已删除。Kimi 本机静态检查显示 CLI 可定位，但默认 profile home、workdir 和 empty-skills 目录未建立，故 public/admin profile 均不 ready；这是失败关闭行为。

## 权限与外部依赖

- `build_kimi_env()` 仅传递明确允许的 Windows/locale/代理/证书变量，并固定 profile 专属 `KIMI_CODE_HOME`；不会继承 `TOKEN`、邮件密码或 legacy Kimi engine 覆盖变量。
- public Agent 仅声明 `WebSearch`，两份 Agent 均声明 `subagents: []`。
- 静态配置不能证明 Kimi 实际拒绝 Read/Bash/Write/MCP/hooks/plugins，也不能证明 WebSearch、模型鉴权或 max effort 已生效。

## 未满足门槛与下一步

public/admin home 需要部署者以最小必要 provider/认证配置建立，且要在项目树外建立 public workdir。之后必须在隔离 profile 做真实 JSONL、WebSearch 与工具拒绝探针。因此本阶段状态为 `BLOCKED_EXTERNAL`，不把静态 tests 当作运行时安全验证。

下一阶段：04｜CLI 子进程适配器。可实施离线 fake-process 测试，但真实 Windows 进程树和协议 fixture 仍需固定 CLI 验证。
