# 06 hybrid optional

向量检索已实现，并随 `AI_MEMORY_EMBEDDING_ENABLED`（默认开启）生效。

- 向量化：worker 的 `embed_facts` 作业按 scope + fingerprint 回填，每批 `AI_MEMORY_EMBEDDING_BATCH_SIZE` 条并持续跑到没有缺失事实为止（**没有日预算上限**）；`put_embedding` 每个事实只保留当前 fingerprint 的一行，`prune_stale_embeddings` 清掉 superseded / revision 不符的行。
- 召回：`search_facts` 先做 scope/subject SQL 过滤，再把词法分与余弦分融合（`vectors.fusion_order`，权重 0.5/0.5，低于 `AI_MEMORY_EMBEDDING_MIN_SIMILARITY` 的余弦不计入）。空查询（`/memory list`）不融合，保持按更新时间列出可见事实。
- 降级：开关关闭、provider 未配置、调用失败、维度不一致时，行为与纯词法逐条等价，且不写任何向量、不阻塞回复。
- 隐私：发给 embedding provider 的只有该 scope 的事实文本与查询文本，不含聊天原文。
- 展示：`/memory status` 输出向量是否就绪、已向量化条数 / 可见事实条数。

验证：`test/unit/test_memory_vectors.py`（16 passed）。真实 embedding provider 已实测（2026-09-14）：`api.siliconflow.cn` 的 `BAAI/bge-m3`，`/embeddings` 可用，1024 维，同文本余弦 0.99996、跨文本 0.5949（探针 `tools/diagnostics/probe_memory_embedding.py`，只发两段固定测试文本）。

第二轮 R2-D04/R2-D09：向量路径补上 opt-out 与缓存失效闭环。

- 外发：每条事实调用 provider 前都会走 `fact_is_embedding_eligible`（scope 仍启用、成员未退出、fact 仍 active、revision 未变、embedding 仍启用）；成员 `/memory off` 后其事实不再外发，batch 中途退出也只停该成员的剩余条目。
- 回填调度：`requeue_job` 的 `state IN (...)` 包含 `running`，回填作业运行期间新写入的事实不会被丢弃；处理器按 `fact_id` 分页取批，取满一批就按本批最后一个 `fact_id` 继续下一批，取不满即结束。
- 队头阻塞：`facts_missing_embeddings` 在查询阶段就排除失效/过期 fact、subject 属于已 opt-out 成员的事实、以及已有当前 fingerprint + revision 的 fact；provider 调用前 `fact_is_embedding_eligible` 再逐条复查（覆盖查询之后突然 `/memory off` 的竞态）。
- 缓存：新增 `_clear_query_vector_cache(scope_id)`，在 member off / scope off / forget / clear / group clear / new（epoch 轮转）成功后清掉该 scope 的 query 向量；缓存项同时记录 fingerprint，换 embedding 模型后旧查询向量不再命中。

验证：`test/unit/test_memory_r2_regressions.py`（28 passed，含 T-R2-06/07、T-R2-17~T-R2-19）；负向对照确认旧实现下"off 后 fact 仍外发""clear 后缓存仍命中""回填作业 running 期间新增事实永久缺向量"均可复现。真实 provider 只在 2026-09-14 的历史探针中运行过，本轮未重跑。

第三轮 R3：回填取批改为在查询阶段就过滤，处理器按 `fact_id` 分页继续下一批，不再用"本批是否写出过向量"决定是否继续扫描——队头是已 opt-out 成员的事实时，后面的正常 fact 仍能拿到向量。验证：`test_memory_r2_regressions.py::test_r3_embedding_not_blocked_by_opt_out_fact`（负向对照下旧实现 provider 零调用、该 fact 永远缺向量）。
