# aichat 统一消息流与 QQ 引用

本次修改以 `36c6a383d7966127969149d77a3d734c9a5c0df0` 为基线。默认聊天仍使用 Kimi CLI，原命令、权限、管理员 computer 隔离和长期记忆事务不变；不新增依赖、数据库或服务。

## 数据与安全边界

- `client.py` 在业务路由和任务创建前同步观察 OneBot `message` / `message_sent`。自身消息只观察，不进入命令、反广告或被动长期记忆。
- `OneBotAPI` 在发送确认为 `status=ok`、`retcode=0` 且有真实消息 ID 后补记。普通群发、私发及合并转发的统一封装均覆盖；HTTP 未确认不会因此重发。
- ACK、事件和重复投递按“会话 + 消息 ID”合并，首次观察序号固定，事件内容优先。同文不同 ID 保留。仅复用进程内的 `AIService._chat_sessions`，每会话 300 条、最后活动后 30 分钟失效，重启清空。
- public 不再业务处理后重复追加历史；当前输入单独传递，历史以其观察序号为边界，排除当前及后到消息。admin 仍按 `admin:<actor>:<session>` 独立提交，群中管理员结果只在实际收件人私聊被 ACK 记录。
- `reply()` 仅补注发起人。发送前的归属提示最多每会话 64 条、60 秒有效，只匹配同会话的完整消息体；歧义不会猜测。这些提示不能作为已送达证据。
- 引用先查同会话缓存，仅在实际触发 aichat 且确有需要时调用一次 `get_msg`。群号、私聊对象、作者无法确认即不可用；不借引用绕过成员退出。只将原文送入顶层 `quoted_message`，不追加为新历史、不当成当前用户原创。通过 `get_msg` 取得的引用原文不写入长期记忆。
- 引用图与当前图片使用不同槽 ID 和来源，复用现有视觉能力及数量限制。`VISION_CAPTURE_CONTEXT_IMAGES=false` 仍只限制被动图片采集。模型只收到可读占位或视觉描述，不收到媒体 URL、路径或 base64。

## 文件清单

| 文件 | 修改目的 |
| --- | --- |
| `client.py` | 接收前观察、自消息短路、每次连接绑定 ACK 回调 |
| `cooper_bot/core/onebot.py` | 可选同步发送确认回调，异常不影响发送返回值 |
| `cooper_bot/modules/ai/message_context.py` | 纯消息/CQ 解析、会话及作者判定、媒体占位与输入限制 |
| `cooper_bot/modules/ai/quote_context.py` | 缓存优先引用解析、来源/成员检查、引用视觉与大小限制 |
| `cooper_bot/modules/ai/aisvc.py` | 唯一历史 upsert、观察序号、归属及视觉补注、引用载荷 |
| `cooper_bot/commands/commands.py` | 删除分散追加及省流预写；public/admin 提交分流；触发后解析引用 |
| `test/conftest.py` | 将测试 MemoryStore 路径隔离到临时工作区 |
| `test/unit/test_onebot_message_context.py` | 解析、会话、上限/TTL、合并、顺序及归属元数据 |
| `test/unit/test_client_message_stream.py` | WS 观察先于路由、自消息保护、重连、ACK 与真实私聊目标 |
| `test/unit/test_quote_context.py` | 缓存/API、跨会话拒绝、作者/退出检查、时序与引用载荷 |
| `test/unit/test_aisvc_kimi_chat.py` | 更新为真实观察/发送确认链路，验证 public/admin 隔离 |
| `test/unit/test_ai_visual_context.py` | 引用与当前图片分离、共用一次查询、纯引用触发 |
| `test/unit/test_kimi_cli.py` | Windows 裁剪旧历史后保留完整顶层引用 |
| `test/unit/test_memory_prompting.py` | 未经验证的缓存引用不得直接写入捕获输入 |
| `test/integration/test_commands_dispatch.py` | 命令/省流实际 ACK 记录及发送未确认无虚假历史 |

## 验证记录

本地测试使用 Fake OneBot/Kimi，不发送真实 QQ 消息。测试命令附加 `-o addopts='' -p no:cacheprovider --basetemp runtime/<独立目录>`，避免默认报告输出和缓存影响工作区。

| 执行命令（pytest 均附加上述隔离参数） | 实际结果 |
| --- | --- |
| `python -B -m pytest -q test/unit/test_quote_context.py test/unit/test_onebot_message_context.py test/unit/test_ai_visual_context.py test/unit/test_kimi_cli.py --disable-warnings` | 阶段专项 103 passed |
| `python -B -m pytest -q test/unit/test_client_message_stream.py test/unit/test_aisvc_kimi_chat.py test/unit/test_quote_context.py test/integration/test_commands_dispatch.py test/integration/test_memory_chat_flow.py --disable-warnings` | 阶段专项 95 passed |
| `python -B -m pytest -o addopts='' -q -rs -p no:cacheprovider --basetemp runtime/test_message_stream_verified --disable-warnings` | 最终 **801 passed, 13 skipped**，0 failed，74.92 秒 |
| `python -B -m pytest -o addopts='' -q -rs -p no:cacheprovider --basetemp runtime/test_ssocr_wsl_verified test/unit/test_signin_ocr.py --disable-warnings`（允许 WSL 访问） | OCR 补验 **73 passed**，0 skipped，0 failed，30.24 秒；包含之前跳过的 13 项 |
| `python -B -m compileall -q client.py cooper_bot` | 成功，退出码 0 |
| `git diff --check` | 成功，退出码 0 |

上述全量测试中的 13 项跳过均因测试沙箱访问 WSL 被拒绝，属于原有 OCR 测试；没有跳过本次新增的消息流/引用测试。后续在沙箱外只读确认 WSL ssocr 2.23.1 和自动发现正常，ssocr 并未损坏。16 个修改/新增文件通过 UTF-8、换行及替代字符检查；修改内容通过已知私有账号扫描。已有文件未批量格式化或改换行。

## 生产检查与部署

2026-10-29 只读检查：`get_version_info` 返回 NapCat.Onebot **4.10.41** / OneBot v11；本地挂载配置有一条启用的 WS Server，端口为 3001（项目宿主机映射为 13001），`messagePostFormat=array`，`reportSelfMessage=false`。本次未修改生产配置或重启 bot。代码按操作者后续明确授权，通过普通新增提交推送 GitHub，不重写已有历史。

真实群/私聊 `message_sent`、`get_msg` 字段与端到端回复尚未现场验收；生产侧 `reportSelfMessage` 尚待操作者开启并验收，不能以单元测试代替。

1. 先部署本次代码并重启 bot，确认 `/ping` 与原有命令正常。这一步必须先于开启自身事件，以保证防回环代码生效。
2. 在 NapCat WebUI 找到 **WS_URI 实际连接的正向 WebSocket Server**，仅将这条连接的 `reportSelfMessage` 设为 `true`，保持 `messagePostFormat=array`。不要修改 HTTP Server 或其他 WS 连接，不改端口、token、绑定地址。
3. 保存并按 WebUI 要求重连/重启连接。确认实际接到带真实消息 ID 的 `message_sent`。只观察结构化事件；不要解析 `docker logs` 或 NapCat 内部数据库。
4. 在测试群和测试私聊完成下面验收，再安排正式使用。生产私有配置位于忽略目录 `runtime/napcat/config/`，不要提交真实配置或事件全文。

现场验收清单（当前均待执行）：

- 群、私聊各发一次 `/ping`，确认用户与 bot 各一条，ACK/自身事件无重复，bot 不再触发命令、AI、反广告或被动记忆。
- 直接调用发送封装的定时业务发一条可控通知，后续 aichat 能看到并引用，作者为 bot。
- 引用缓存内/外群友文字、bot 消息、图片/表情；验证作者准确，缓存外一次 `get_msg`，缺少群/私聊来源则明确不可用。
- 当前带图且引用别人图片时，区分两位作者；只有引用无正文时，私聊或群内 @bot 可正常触发。
- A/B 交错发言、C 在实际 bot 回复后提问，验证观察顺序；两个私聊和管理员结果不串会话。
- 测试环境模拟 HTTP 未确认后到达自身事件，确认只发一次、只补记一次；不必在生产刻意断网。
- 验证重连、300 条/30 分钟窗口、成员退出、清除/epoch 和现有 `/memory` 行为。

## 限制与回滚

NapCat 没有上报的离线消息无法恢复；撤回/过期、缺少合法会话元数据或不允许使用的引用会明确不可用。开启自身事件前，其他模块发送或 HTTP 未确认的消息仍可能缺失。未知私聊目标安全跳过，不能从 bot 身份猜测收件人。

回滚时停止 bot，先在当前 WS Server 关闭 `reportSelfMessage`，再回退本次局部代码，避免旧版本发生自回复。保留已有私有配置和历史提交，不需要数据库迁移。重启后检查 `/ping`、aichat、`/memory`、通知；内存短期历史自然清空。
