# 聊天记忆

聊天记忆默认关闭。部署者设置 `AI_MEMORY_ENABLED=true` 后重启机器人；数据库默认位于 `runtime/databases/ai/chat_memory.sqlite3`，它不在 `/find` 可访问的资料目录中。

私聊用户可使用 `/memory on`、`status`、`remember <内容>`、`list [页码]`、`search <关键词>`、`forget <ID 或唯一前缀>`、`history [关键词] [--before seq]`、`clear` 和 `new`。`clear` 需要 60 秒内、绑定当前作用域与 epoch 的一次性确认码。群记忆还需要 `AI_MEMORY_GROUP_ALLOWLIST`，并由可信个人管理员运行 `/memory group on directed` 或 `all`；`all` 会被动保存普通群聊但不会因此调用模型。群内个人 `on/off` 只管理本人，群级策略及 `group clear` 仅由可信个人管理员管理。

管理员电脑执行域是独立的 explicit-only 作用域：只保存主动 `remember` 的内容，不保存自动聊天正文或工具输出。关闭总开关不会删除数据库；需要回滚时关闭开关并重启。使用 SQLite backup API 备份，勿复制正在运行的单个数据库文件，也不要把备份置于资料目录或提交进 Git。

本地删除会清理当前记忆库中的对应事实、摘要和作业；`new` 只开始新对话并保留长期事实。删除不能撤回已经进入发送边界或已经发给 QQ、模型供应商、日志系统或既有离线备份的数据。
