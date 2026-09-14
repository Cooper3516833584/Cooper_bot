# 03 commands

新增 `/memory` 显式命令并在通用非 AI 记忆钩子之前处理，避免管理命令写回聊天缓存。支持 on/off/status/remember/list/search/forget/clear/new/history 及受 allowlist 约束的群管理入口；clear 为一次性、60 秒确认。

验证：`test/integration/test_commands_dispatch.py` 通过。命令未连接生产 QQ 或模型。
