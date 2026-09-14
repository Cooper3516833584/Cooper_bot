# 01 storage

新增独立 `cooper_bot.modules.memory` SQLite Store、结构化身份/作用域策略和默认关闭的配置。数据库连接只在单线程 executor 内创建使用；表含 scope、members、events、facts、summaries、jobs、embeddings、forget markers 和 daily usage。删除会提高 epoch，显式事实按 scope + subject 强制过滤。

验证：`python -m pytest -q test/unit/test_memory_policy.py test/unit/test_memory_store.py`（通过）。回滚：关闭 `AI_MEMORY_ENABLED` 并保留独立数据库。
