# 01 storage

新增独立 `cooper_bot.modules.memory` SQLite Store、结构化身份/作用域策略和默认关闭的配置。数据库连接只在单线程 executor 内创建使用；表含 scope、members、events、facts、summaries、jobs、embeddings、forget markers 和 daily usage。删除会提高 epoch，显式事实按 scope + subject 强制过滤。

验证：`python -m pytest -q test/unit/test_memory_policy.py test/unit/test_memory_store.py`（通过）。回滚：关闭 `AI_MEMORY_ENABLED` 并保留独立数据库。

9563319 修复阶段 01：`append_input_once` 以数据库实际插入结果返回明确的 `inserted` 标志；重复 pending/completed 事件不再进入模型。turn 获取 scope 锁后会复查 epoch、conversation、scope/member policy，过期或已关闭的排队输入在模型调用前标为 failed。服务启动恢复旧进程遗留的 pending/generated，且不会自动重发。

验证：`python -m pytest -q test/unit/test_memory_turn_safety.py`（6 passed）；memory、AI、dispatch、client lifecycle 相关组合测试（61 passed）。

9563319 修复阶段 04：启动时按完整 input block 执行 raw TTL/每 scope 数量上限，只清 terminal block，不随机切断问答或删除 pending/generated 在途记录。默认 master switch 改回关闭；配置来源的 DB 路径必须位于私有 `runtime/databases` 根内，否则 fail closed 且不创建文件。显式传入 `db_path` 仅用于可信测试注入。
