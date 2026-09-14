# 02 chat integration

AIService 构造惰性 MemoryService；启用且当前 scope 已开启时，聊天先持久登记、按 scope 串行、从 confirmed 历史取快照，并在真实 reply 返回后才提交 confirmed/unconfirmed。未启用时仍走原 Kimi 内存缓存路径。admin 路径只留无正文请求元数据和显式事实。

验证：`test/unit/test_aisvc_kimi_chat.py`、`test/unit/test_commands_kimi.py` 通过。client 原有最外层 `aisvc.aclose()` 已覆盖 MemoryService 关闭。
