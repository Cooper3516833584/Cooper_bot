# 配置入口

实际运行配置统一位于 `config/private/`，整个目录不提交到 Git。
公开仓库只保存 `config/examples/` 下的示例。示例不会自动作为运行配置加载。

首次部署时，将所需的示例复制到 `config/private/` 的相同相对位置，去掉文件名末尾的 `.example`，再填写实际值。
例如 `examples/ai/group_chat_prompts.json.example` 对应 `private/ai/group_chat_prompts.json`。
Kimi 的 `public_home_security.example.toml` 是独立安全配置参考，应按已有 profile 部署说明使用。

| 文件或目录 | 用途 | 修改后生效方式 |
| --- | --- | --- |
| `settings.env` | 模型、记忆、视觉、文件收发、保留策略、日历测试目标及诊断设置 | 重启 |
| `secrets.env` | NapCat 账号、token、管理员、邮箱账号及凭据 | 重启；NapCat 账号设置另需按原方式重建容器 |
| `permissions.json` | `group_levels` 群权限下限映射，级别为整数 0 到 3 | 重启 |
| `api_key.txt` | 既有六行模型凭据格式，空行必须保留 | 按模型服务原有加载方式 |
| `ai/system_prompt.txt` | 默认系统角色提示词 | 重启 |
| `ai/group_chat_prompts.json` | 群聊角色覆盖 | 自动重读 |
| `ai/private_chat_prompts.json` | 私聊角色覆盖 | 自动重读 |
| `ai/group_notice_prompts.json` | 群通知分类和省流提示词 | 自动重读 |
| `ai/kimi/` | 公共和管理员 agent 提示词及部署参考 | 提示词修改后建议重启 |
| `calendar/daily_calendar_config.json` | 按群设置的每日提醒 | 自动重读 |
| `calendar/editor_duty_config.json` | 值班群、发布账号、排除名单、日期与策略 | `group_id`、`publisher_id` 修改后重启；其余设置自动重读 |
| `replies/` | 精确、关键词、连续文本回复 | 自动重读 |
| `wiki/1037wiki_categories.json` | Wiki 分类资源 | 按工具原有加载方式 |
| `url_and_token.txt`、`1037wiki_refresh_token.txt` | Wiki 认证，保留自动刷新写回格式 | 按工具原有加载方式 |

环境变量优先级：**已有系统环境变量 > `secrets.env` > `settings.env` > 代码中的通用默认值**。
保留 `secrets.env` 中旧功能键的兼容性，但维护新设置时请放进 `settings.env`，避免重复定义。
文件中的空值按原有解析规则回退到默认值。示例中的空群列表、零值目标不会启用定向提醒。

`CALENDAR_TEST_GROUP_ID` 指定 `/calendartest` 使用的群级配置；未设置时使用通用日历参数，不发送到目标群。
`MEMORY_PROBE_SCOPE` 和 `MEMORY_PROBE_ACTOR` 仅供手动诊断脚本使用，也可用命令行参数覆盖。
`LAUNCHER_PYTHON_PATH` 和 `LAUNCHER_PY_PATH` 可指定 Windows 启动器使用的解释器。
启动器默认查找当前用户桌面下的 `Cooper_bot`，其他位置可设置系统环境变量 `COOPER_BOT_ROOT`。

聊天记录、权限数据库、提交记录、日历发送状态、Kimi profile 认证与缓存仍按原有格式保存在运行目录，
它们不是可编辑功能配置。`runtime/`、`storage/` 均不提交到 Git；Kimi profile home 如配置在仓库外，也须自行妥善保管。
回复图片仍放在 `storage/auto_replies/images/`，不扩大 NapCat 的文件挂载和访问范围。

提交前检查 `git diff --cached` 和 `git ls-files config/private runtime storage`，确保真实配置和运行数据没有进入暂存区。
仅添加 `.gitignore` 不会取消已跟踪文件；现有部署迁移时应先保留本地配置，再取消旧配置的 Git 跟踪。
