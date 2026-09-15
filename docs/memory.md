# 聊天记忆

聊天记忆默认关闭，按会话开启：总开关 `AI_MEMORY_ENABLED=true`（它本身不采集、不外发，但 `/memory on` 需要它才能工作），新作用域建出来时 `enabled=0`、`capture_mode=directed`，在 `/memory on` 之前不保存任何正文。要彻底关闭：把 `AI_MEMORY_ENABLED` 设为 `false` 后重启，关闭期间不创建也不打开数据库。数据库默认位于 `runtime/databases/ai/chat_memory.sqlite3`，它不在 `/find` 可访问的资料目录中。

`/memory on` 需要权限等级 2 或以上：私聊开启当前私聊会话；群里开启当前 public group scope（等价 `/memory group on all`，会记录该群普通文本消息），群会话已开启时普通成员也能用 `/memory on` 把自己加回来。`/memory off` 只让本人退出。群级开关同样可用 `/memory group on directed|all`、`/memory group off`（仅可信个人管理员）；`AI_MEMORY_GROUP_ALLOWLIST` 留空表示不限制可开启的群，填了群号则只有这些群能开启。

私聊用户可使用 `/memory on`、`status`、`remember <内容>`、`list [页码]`、`search <关键词>`、`forget <ID 或唯一前缀>`、`history [关键词] [--before seq]`、`clear` 和 `new`。`clear` 需要 60 秒内、绑定当前作用域与 epoch 的一次性确认码。`/memory remember` 不会自动打开会话：会话没开时提示先 `/memory on`。群内个人 `on/off` 只管理本人；`group clear` 仅限可信个人管理员。

`/memory off` 后立即停止采集**和使用**该成员在当前作用域的记忆：其正文不再进入快照、`/memory history`、事实召回，其事实也不再发往 embedding provider 或参与后台派生；同一作用域其他成员不受影响。已写入的原始事件不会因此立即物理删除（需要物理清除请用 `/memory clear`），但在这段时间内不会以任何形式被读取或外发。

管理员电脑执行域是独立的 explicit-only 作用域：只保存主动 `remember` 的内容，不保存自动聊天正文或工具输出。关闭总开关不会删除数据库；需要回滚时关闭开关并重启。使用 SQLite backup API 备份，勿复制正在运行的单个数据库文件，也不要把备份置于资料目录或提交进 Git。

本地删除会清理当前记忆库中的对应事实、摘要和作业；`new` 只开始新对话并保留长期事实。删除不能撤回已经进入发送边界或已经发给 QQ、模型供应商、日志系统或既有离线备份的数据。

事实默认不做向量化（`AI_MEMORY_EMBEDDING_ENABLED=false`）：开启后 provider 与模型取自 `config/private/api_key.txt` 第 3/4 行和 `AI_EMBED_MODEL`。发给 embedding provider 的只有当前作用域中仍处于启用状态的事实文本与查询文本，不含聊天原文；每条事实在真正调用 provider 前都会重新校验作用域开关、成员 opt-out 状态与 revision，`/memory off` 的成员其事实不会再外发，batch 中途退出也会立即停止后续发送。provider 未配置、调用失败或维度不一致时自动退化为纯词法，不影响回复。`/memory status` 显示向量是否就绪与已向量化条数，设置 `AI_MEMORY_EMBEDDING_ENABLED=true` 才会启用。

`AI_MEMORY_SUMMARY_ENABLED=false`（高流量群里不做高频摘要）。`AI_MEMORY_AUTO_EXTRACT_ENABLED=true`：按成员累计 `AI_MEMORY_AUTO_EXTRACT_MIN_EVENTS=200` 条新消息才批量判断一次长期事实，每次只看 `cursor` 之后最早的 200 条（一条不落，也不会一次把几千条塞给模型），每条 `own_text` 最多截断 200 字送模型；每天最多 `AI_MEMORY_AUTO_EXTRACT_DAILY_BUDGET=20` 次，超了就把作业推迟一小时再试、cursor 不推进。这两条链路会把记忆内容发往模型网关，`/memory off` 的成员内容不会进入。普通群消息的图片不会为了记忆额外调用视觉模型（`VISION_CAPTURE_CONTEXT_IMAGES=false`）：只有消息真正触发回复时才解析图片，并把已经得到的解析结果随该轮一起写入 `visual_text`。

重启恢复：已开启的会话保持开启（状态存在 SQLite），已保存的聊天不丢，成员抽取游标 `extracted_through_input_seq` 不清零；启动时会把上次崩溃遗留的 `running` 作业立即放回队列（不等旧 lease 过期），并扫一遍已开启 scope 中活跃成员，把"已经够 200 条但崩溃前没来得及排队"的抽取补上。某批因为普通异常把重试次数用尽变成 `failed` 后，只要系统再次发现这同一批（成员发了新消息，或 Bot 重启触发补排）就会把这条 job 重新激活（`attempts` 归零、清 lease 与错误码），因此不会永久堵住 cursor；`succeeded` 的批不会被重跑。
