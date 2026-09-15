# 01 storage

新增独立 `cooper_bot.modules.memory` SQLite Store、结构化身份/作用域策略和默认关闭的配置。数据库连接只在单线程 executor 内创建使用；表含 scope、members、events、facts、summaries、jobs、embeddings、forget markers 和 daily usage。删除会提高 epoch，显式事实按 scope + subject 强制过滤。

验证：`python -m pytest -q test/unit/test_memory_policy.py test/unit/test_memory_store.py`（通过）。回滚：关闭 `AI_MEMORY_ENABLED` 并保留独立数据库。

9563319 修复阶段 01：`append_input_once` 以数据库实际插入结果返回明确的 `inserted` 标志；重复 pending/completed 事件不再进入模型。turn 获取 scope 锁后会复查 epoch、conversation、scope/member policy，过期或已关闭的排队输入在模型调用前标为 failed。服务启动恢复旧进程遗留的 pending/generated，且不会自动重发。

验证：`python -m pytest -q test/unit/test_memory_turn_safety.py`（6 passed）；memory、AI、dispatch、client lifecycle 相关组合测试（61 passed）。

9563319 修复阶段 04：启动时按完整 input block 执行 raw TTL/每 scope 数量上限，只清 terminal block，不随机切断问答或删除 pending/generated 在途记录。修复阶段 04 曾把默认 master switch 改回关闭（缺陷矩阵 D15）；2026-09-14 按产品决定改为**默认开启**，口径见 `docs/memory.md` 与 `07_acceptance.md`。配置来源的 DB 路径必须位于私有 `runtime/databases` 根内，否则 fail closed 且不创建文件。显式传入 `db_path` 仅用于可信测试注入。

第二轮 R2-D01/R2-D02/R2-D05：`_Turn.abort()` 现在区分"尚未 generated"与"已经 generated"——生成前异常会把 pending 输入收尾为 `failed`，不再留下永久 pending 与永久重复事件；`__aenter__()` 用 `try/except BaseException` 包住准备阶段（锁的获取与释放用 `_lock_acquired` 记账，避免 double release），任何异常（含任务取消）都会释放 scope lock 并收尾输入。`_set_member_policy_and_invalidate` 不再执行 `DELETE FROM memory_facts WHERE scope_id=? AND source_kind!='explicit_memory'`：退出成员只停止"使用"其记忆，不再误删同 scope 其他成员的 auto facts，也不再让 extraction cursor 与已删事实脱节。该成员事实的可见性改由 `memory_members.enabled` 在查询层过滤（`list_facts`/`history`）。

验证：`python -m pytest -q test/unit/test_memory_r2_regressions.py`（25 passed，含 T-R2-01~T-R2-04、T-R2-08/09）；负向对照已确认旧实现下"输入保持 pending""锁保持 locked""B 的 auto facts 被删除"三种违规均可复现。
