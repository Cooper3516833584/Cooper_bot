# 06 hybrid optional

未开启向量增强：`AI_MEMORY_EMBEDDING_ENABLED=false` 是安全默认值。Store 已为版本化 embeddings 和持久 job 预留独立表；当前检索始终退化为本地词法，不会把记忆发送给 embedding provider。
