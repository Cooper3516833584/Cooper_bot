# 07 acceptance

实际命令（全部退出码 0）：

- `python -m compileall -q cooper_bot client.py`
- `python -m pytest --collect-only -q test`
- `python -m pytest -q test/unit`
- `python -m pytest -q test/unit/test_memory_policy.py test/unit/test_memory_store.py test/unit/test_aisvc_kimi_chat.py test/unit/test_commands_kimi.py test/integration/test_commands_dispatch.py test/unit/test_client_lifecycle.py`

真实 QQ、Kimi/DeepSeek 和 embedding 未执行；没有凭据授权或生产消息操作。性能基准未执行。回滚演练为关闭 `AI_MEMORY_ENABLED` 后重启，独立数据库不删除；资料索引、权限库、Kimi profile 均不受影响。
