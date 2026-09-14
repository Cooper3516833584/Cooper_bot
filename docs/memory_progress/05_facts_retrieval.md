# 05 facts retrieval

显式事实使用当前 scope 的中文二元组/英文词法检索，按相关性和更新时间稳定排序，并作为不可信 `memory_context` 注入 Kimi prompt。事实查询先做 scope/subject SQL 过滤；不跨群、私聊或 admin/public 域。

自动事实提取默认关闭，显式记忆功能不依赖它。

9563319 修复阶段 07：开启后按 public scope + actor 的稳定 terminal own_text 建持久 extraction job，使用独立 cursor；quoted/visual/assistant、metadata、truncated/unsafe 内容不进入抽取输入。程序固定 subject/scope，并在写入事务中复核 epoch/cid/member/cursor、真实 evidence 子串与 forget marker。合法空结果推进 cursor；同 key explicit active 优先；forget marker 记录真实证据水位，旧 job 失效而新 seq 的明确重述可重建。

验证：`test/unit/test_memory_extraction.py`（5 passed）；00–07 相关组合 125 passed。使用 fake ModelGateway，真实 provider 未运行。
