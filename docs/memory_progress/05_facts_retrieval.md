# 05 facts retrieval

显式事实使用当前 scope 的中文二元组/英文词法检索，按相关性和更新时间稳定排序，并作为不可信 `memory_context` 注入 Kimi prompt。事实查询先做 scope/subject SQL 过滤；不跨群、私聊或 admin/public 域。

自动事实提取默认关闭，且真实模型未运行；显式记忆功能不依赖它。
