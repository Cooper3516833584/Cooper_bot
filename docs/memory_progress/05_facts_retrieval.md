# 05 facts retrieval

显式事实使用当前 scope 的中文二元组/英文词法检索，按相关性和更新时间稳定排序，并作为不可信 `memory_context` 注入 Kimi prompt。事实查询先做 scope/subject SQL 过滤；不跨群、私聊或 admin/public 域。

自动事实提取默认关闭，显式记忆功能不依赖它。

2026-09-14 向量检索：显式与自动抽取的事实由 worker 的后台作业（`kind=embed_facts`）向量化，`fingerprint` 取 `memory-embed:<AI_EMBED_MODEL>`，按 fact revision 校验有效性；召回时 `retrieval.scored` 的词法分与余弦分按 0.5/0.5 融合，低于 `AI_MEMORY_EMBEDDING_MIN_SIMILARITY` 的余弦不计入。provider 未配置、调用失败或维度不一致时一律退化为纯词法。查询向量按 scope + query 缓存 60 秒，避免同一轮 turn 重复请求。

验证：`test/unit/test_memory_vectors.py`（16 passed）；10 个 memory 专项文件 68 passed；含 AI/dispatch/Kimi argv/lifecycle 的组合 151 passed。真实 embedding provider 已实测（2026-09-14，用户授权联网，只发两段固定测试文本）：`api.siliconflow.cn` 的 `BAAI/bge-m3` 返回 1024 维，同文本余弦 0.99996、跨文本 0.5949。

9563319 修复阶段 07：开启后按 public scope + actor 的稳定 terminal own_text 建持久 extraction job，使用独立 cursor；quoted/visual/assistant、metadata、truncated/unsafe 内容不进入抽取输入。程序固定 subject/scope，并在写入事务中复核 epoch/cid/member/cursor、真实 evidence 子串与 forget marker。合法空结果推进 cursor；同 key explicit active 优先；forget marker 记录真实证据水位，旧 job 失效而新 seq 的明确重述可重建。

验证：`test/unit/test_memory_extraction.py`（5 passed）；00–07 相关组合 125 passed。使用 fake ModelGateway，真实 provider 未运行。

第二轮 R2-D03/R2-D04/R2-D06/R2-D07/R2-D08：opt-out 与事实优先级都按"同一 fact_key / 同一成员"收敛。

- 查询层：`list_facts` 与 `history` 都带 `memory_members.enabled=0` 的 NOT EXISTS 过滤，已 `/memory off` 的成员，其事实与正文对任何调用方都不可见（含本人），而其他成员不受影响。
- 外发层：`fact_is_embedding_eligible(scope_id, fact_id, revision)` 在每条事实真正调用 embedding provider 之前复核 scope 开关、成员 opt-out、fact active 与 revision；batch 中途 opt-out 只停该成员的剩余事实，其他成员继续。回填作业的 `requeue` 现在也覆盖 `running`，扫描期间新写入的事实不会因为"作业正在跑"而丢掉排队。
- 优先级：`_apply_extracted_facts` 去掉了"该 subject 存在任意 explicit 就跳过全部 auto 候选"的全局闸门，改为只按同一 `(scope_id, subject_id, fact_key)` 判断——active explicit 存在则忽略 auto，否则更新 auto revision；`_save_explicit_fact` 与 supersede 语句同样按 key 收敛，保证同一 key 最多一个 active revision。freeform explicit（`explicit.<hash>` key）不再阻断无关 auto key。
- cursor：抽取改为显式结果 `ExtractionResult(executed, facts, retryable)`。只有模型真正执行（含合法返回 `[]`）才推进 cursor；budget 耗尽或 provider 不可用时抛 `ExtractionDeferred`，cursor 不推进、作业由 worker 退避重试。

验证：`test/unit/test_memory_r2_regressions.py`（25 passed，含 T-R2-05~T-R2-16）；负向对照确认旧实现下"off 后历史泄露正文""off 后事实仍外发""budget=0 仍推进 cursor""任意 explicit 阻断全部 auto"均可复现。真实 provider 与真实 QQ 链路未运行。
