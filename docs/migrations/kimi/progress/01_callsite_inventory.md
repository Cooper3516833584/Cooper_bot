# 阶段 01：AI 调用图盘点

分类遵循迁移规格：A 为 QQ 自由聊天（迁 Kimi）；B 为 DeepSeek 非聊天任务（保留）；C 为 Antigravity 非聊天（迁 Kimi，不能直接删除）；D 为共享 helper、配置或数据（默认保留）。

## A：QQ 自由聊天

| 位置 | 符号 / 用途 | 后续处理 |
| --- | --- | --- |
| `cooper_bot/commands/commands.py` | `_handle_ai_chat_trigger()` | 保留触发、视觉、回复、去重和会话流程；阶段 06 移除 `g/c/Gemini/Claude/Antigravity` 前缀分派，统一调用 Kimi `chat*` 接口。 |
| `cooper_bot/commands/commands.py` | `_split_ai_chat_backend()`、`_is_antigravity_busy_error()`、`_antigravity_busy_reply()` | 阶段 06/08 删除旧后端选择与 Antigravity 专属错误文案。 |
| `cooper_bot/modules/ai/aisvc.py` | `chat()`、`chat_with_context()` 及 `_chat_sync()`、`_chat_with_context_sync()` | 当前默认 QQ 聊天直接调用 DeepSeek；阶段 02 先保护内部调用，后续改为 Kimi。 |
| `cooper_bot/modules/ai/aisvc.py` | `gemini_chat*`、`restricted_gemini_chat*` | 当前 Antigravity 的完整/受限聊天入口；阶段 04–08 用 Kimi runner 替换后删除。 |
| `cooper_bot/modules/vision/vision_skill.py` 与 `aisvc.py` | 视觉 slots、内存历史、`collect_unresolved_vision_slots()` | D 类共享上下文；Kimi 仍复用机器人历史与视觉文本，不能改为 Kimi 会话续接。 |

## B：DeepSeek 非聊天任务，必须保留

| 位置 | 符号 / 用途 | 当前依赖 |
| --- | --- | --- |
| `cooper_bot/modules/ai/aisvc.py` | `classify_email()` / `_classify_email_sync()` | DeepSeek Chat Completions JSON。 |
| `cooper_bot/modules/ai/aisvc.py` | `classify_notice()`、`reason_notice()` 及 v2 实现 | DeepSeek reasoner。 |
| `cooper_bot/modules/ai/aisvc.py` | `_generate_summary()` 与资料整理流程 | DeepSeek 文本/结构化总结。 |
| `cooper_bot/modules/calendar/daily_calendar.py` | `_render_message()` | 当前通过通用 `aisvc.chat()` 生成已核验事实的补充文案；阶段 02 必须显式改接 DeepSeek 内部任务，避免 QQ chat 改为 Kimi 后误迁。 |
| `cooper_bot/modules/ai/aisvc.py` | embedding、semantic 索引与资料状态流程 | 共享 AI 配置/存储，非自由聊天；不属于本迁移的替换范围。 |

## C：Antigravity 非聊天

| 位置 | 符号 / 用途 | 后续处理 |
| --- | --- | --- |
| `cooper_bot/modules/calendar/daily_calendar.py` | `_call_web_model()` | 当前以 `gemini_chat_ready` 和 `restricted_gemini_calendar_chat()` 请求联网事实 JSON，且有 gemini/claude 主备模型；阶段 07 改为 Kimi public 无状态入口。 |
| `cooper_bot/modules/ai/aisvc.py` | `restricted_gemini_calendar_chat()` / `_restricted_gemini_calendar_chat_sync()` | 当前为日历提供 Antigravity 受限执行；迁移完成前不得删除。 |

日历事实查询被 `_validate_web_events()` 过滤为本地节气/传统节日或官方来源；该校验必须保留。日历文案生成与事实查询是两条独立 AI 链路。

## D：共享实现、配置和生命周期

- `cooper_bot/core/config.py`：旧 `AI_GEMINI_*` / `AI_CLAUDE_MODEL` 配置；阶段 03 添加 Kimi 配置，阶段 08 才移除旧项。
- `config/ai/gemini_cli_chat_only.toml`：Antigravity 的 `google_web_search` 白名单；不能照搬为 Kimi 配置。
- `cooper_bot/modules/ai/aisvc.py`：`_build_chat_payload()`、`_create_deepseek_client()`、`_create_reasoner_completion()`、`_post_json()` 是 DeepSeek 共享 helper，保留。
- `cooper_bot/core/router.py::build_ctx()`：`ADMIN_USERS` 直接给 3 级，但群聊还可能由 `GROUP_LEVEL` 提升。未来电脑授权必须核验个人管理员身份，不能仅看 `ctx.level`。
- `client.py`：每会话锁、全局 dispatch semaphore、断线时在途任务取消。Kimi runner 后续应接入关闭/取消流程，不能启动生产客户端作为测试。
- `test/unit/test_aisvc_gemini_chat.py`、`test/integration/test_commands_gemini_dispatch.py`、`test/integration/test_commands_dispatch.py`、`test/smoke/test_core_routes_smoke.py`：含 Antigravity fake 与断言；阶段 08 才删除/替换。

本盘点未发现其他受版本管理代码中的 Antigravity 运行调用者；后续阶段仍须对新增/动态调用复查。
