# 06 hybrid optional

向量检索已实现，并随 `AI_MEMORY_EMBEDDING_ENABLED`（默认开启）生效。

- 向量化：worker 的 `embed_facts` 作业按 scope + fingerprint 回填，每批 `AI_MEMORY_EMBEDDING_BATCH_SIZE` 条并持续跑到没有缺失事实为止（**没有日预算上限**）；`put_embedding` 每个事实只保留当前 fingerprint 的一行，`prune_stale_embeddings` 清掉 superseded / revision 不符的行。
- 召回：`search_facts` 先做 scope/subject SQL 过滤，再把词法分与余弦分融合（`vectors.fusion_order`，权重 0.5/0.5，低于 `AI_MEMORY_EMBEDDING_MIN_SIMILARITY` 的余弦不计入）。空查询（`/memory list`）不融合，保持按更新时间列出可见事实。
- 降级：开关关闭、provider 未配置、调用失败、维度不一致时，行为与纯词法逐条等价，且不写任何向量、不阻塞回复。
- 隐私：发给 embedding provider 的只有该 scope 的事实文本与查询文本，不含聊天原文。
- 展示：`/memory status` 输出向量是否就绪、已向量化条数 / 可见事实条数。

验证：`test/unit/test_memory_vectors.py`（16 passed）。真实 provider 是否可用需运行 `tools/diagnostics/probe_memory_embedding.py`（会联网，只发两段固定测试文本）。
