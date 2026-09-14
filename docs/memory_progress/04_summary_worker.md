# 04 summary worker

提供 SQLite 持久作业和单 worker 基础设施，以及只经 ModelGateway JSON 原语调用的 MemoryLLM/摘要 schema 校验器。默认 `AI_MEMORY_SUMMARY_ENABLED=false`；因此上线基础版不发摘要模型请求。

9563319 修复阶段 06：schema v3 原地增加 summary source range 与 job base_version；worker 随 MemoryService 幂等启停，claim 支持原子状态转换、lease 过期恢复、有限重试和持久 daily budget。stable cutoff 停在最早 pending 前，confirmed assistant 按 parent input 纳入；写回校验 epoch/cid/base_version/target CAS。下一轮只加载摘要水位后的 recent raw，并把摘要作为 data-only memory_context。

验证：`test/unit/test_memory_summary.py` 覆盖 v2→v3 迁移、阈值、cutoff、parent assistant、CAS、lease、预算、慢 LLM 与 clear/off 晚到结果（8 passed）。相关组合 45 passed；真实 provider 未运行。
