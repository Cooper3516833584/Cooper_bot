# 聊天记忆

聊天记忆默认开启（`AI_MEMORY_ENABLED=true`）。要关闭：设置 `AI_MEMORY_ENABLED=false` 后重启，关闭期间不创建也不打开数据库。数据库默认位于 `runtime/databases/ai/chat_memory.sqlite3`，它不在 `/find` 可访问的资料目录中。

私聊用户可使用 `/memory on`、`status`、`remember <内容>`、`list [页码]`、`search <关键词>`、`forget <ID 或唯一前缀>`、`history [关键词] [--before seq]`、`clear` 和 `new`。`clear` 需要 60 秒内、绑定当前作用域与 epoch 的一次性确认码。群记忆还需要 `AI_MEMORY_GROUP_ALLOWLIST`，并由可信个人管理员运行 `/memory group on directed` 或 `all`；`all` 会被动保存普通群聊但不会因此调用模型。群内个人 `on/off` 只管理本人，群级策略及 `group clear` 仅由可信个人管理员管理。

`/memory off` 后立即停止采集**和使用**该成员在当前作用域的记忆：其正文不再进入快照、`/memory history`、事实召回，其事实也不再发往 embedding provider 或参与后台派生；同一作用域其他成员不受影响。已写入的原始事件不会因此立即物理删除（需要物理清除请用 `/memory clear`），但在这段时间内不会以任何形式被读取或外发。

管理员电脑执行域是独立的 explicit-only 作用域：只保存主动 `remember` 的内容，不保存自动聊天正文或工具输出。关闭总开关不会删除数据库；需要回滚时关闭开关并重启。使用 SQLite backup API 备份，勿复制正在运行的单个数据库文件，也不要把备份置于资料目录或提交进 Git。

本地删除会清理当前记忆库中的对应事实、摘要和作业；`new` 只开始新对话并保留长期事实。删除不能撤回已经进入发送边界或已经发给 QQ、模型供应商、日志系统或既有离线备份的数据。

事实默认会被向量化以提升召回：`AI_MEMORY_EMBEDDING_ENABLED=true`，provider 与模型取自 `config/private/api_key.txt` 第 3/4 行和 `AI_EMBED_MODEL`。发给 embedding provider 的只有当前作用域中仍处于启用状态的事实文本与查询文本，不含聊天原文；每条事实在真正调用 provider 前都会重新校验作用域开关、成员 opt-out 状态与 revision，`/memory off` 的成员其事实不会再外发，batch 中途退出也会立即停止后续发送。provider 未配置、调用失败或维度不一致时自动退化为纯词法，不影响回复。`/memory status` 显示向量是否就绪与已向量化条数，设置 `AI_MEMORY_EMBEDDING_ENABLED=false` 可完全关闭。
