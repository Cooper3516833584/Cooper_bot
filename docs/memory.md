# 聊天记忆

聊天记忆默认关闭。部署者设置 `AI_MEMORY_ENABLED=true` 后重启机器人；数据库默认位于 `runtime/databases/ai/chat_memory.sqlite3`，它不在 `/find` 可访问的资料目录中。

私聊用户可使用 `/memory on`、`status`、`remember <内容>`、`list`、`search <关键词>`、`forget <ID>`、`clear` 和 `new`。`clear` 需要 60 秒内的确认码。群记忆还需要 `AI_MEMORY_GROUP_ALLOWLIST`，并由可信个人管理员运行 `/memory group on directed` 或 `all`。群成员可用 `/memory off` 停止自己后续采集和使用。

管理员电脑执行域是独立的 explicit-only 作用域：只保存主动 `remember` 的内容，不保存自动聊天正文或工具输出。关闭总开关不会删除数据库；需要回滚时关闭开关并重启。使用 SQLite backup API 备份，勿复制正在运行的单个数据库文件，也不要把备份置于资料目录或提交进 Git。

本地删除会清理当前记忆库中的对应事实、摘要和作业；不能撤回已经发给 QQ、模型供应商、日志系统或既有离线备份的数据。
