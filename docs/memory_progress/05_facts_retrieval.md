# 05 facts retrieval

显式事实使用当前 scope 的中文二元组/英文词法检索，按相关性和更新时间稳定排序，并作为不可信 `memory_context` 注入 Kimi prompt。事实查询先做 scope/subject SQL 过滤；不跨群、私聊或 admin/public 域。

自动事实提取默认关闭，显式记忆功能不依赖它。

2026-09-14 向量检索：显式与自动抽取的事实由 worker 的后台作业（`kind=embed_facts`）向量化，`fingerprint` 取 `memory-embed:<AI_EMBED_MODEL>`，按 fact revision 校验有效性；召回时 `retrieval.scored` 的词法分与余弦分按 0.5/0.5 融合，低于 `AI_MEMORY_EMBEDDING_MIN_SIMILARITY` 的余弦不计入。provider 未配置、调用失败或维度不一致时一律退化为纯词法。查询向量按 scope + query 缓存 60 秒，避免同一轮 turn 重复请求。

验证：`test/unit/test_memory_vectors.py`（16 passed）；10 个 memory 专项文件 68 passed；含 AI/dispatch/Kimi argv/lifecycle 的组合 151 passed。真实 embedding provider 已实测（2026-09-14，用户授权联网，只发两段固定测试文本）：`api.siliconflow.cn` 的 `BAAI/bge-m3` 返回 1024 维，同文本余弦 0.99996、跨文本 0.5949。

9563319 修复阶段 07：开启后按 public scope + actor 的稳定 terminal own_text 建持久 extraction job，使用独立 cursor；quoted/visual/assistant、metadata、truncated/unsafe 内容不进入抽取输入。程序固定 subject/scope，并在写入事务中复核 epoch/cid/member/cursor、真实 evidence 子串与 forget marker。合法空结果推进 cursor；同 key explicit active 优先；forget marker 记录真实证据水位，旧 job 失效而新 seq 的明确重述可重建。

验证：`test/unit/test_memory_extraction.py`（5 passed）；00–07 相关组合 125 passed。使用 fake ModelGateway，真实 provider 未运行。
