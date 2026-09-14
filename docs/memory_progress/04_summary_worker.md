# 04 summary worker

提供 SQLite 持久作业和单 worker 基础设施，以及只经 ModelGateway JSON 原语调用的 MemoryLLM/摘要 schema 校验器。默认 `AI_MEMORY_SUMMARY_ENABLED=false`；因此上线基础版不发摘要模型请求。

限制：尚未启用的摘要策略不会改变聊天行为；真实 provider 未运行。
