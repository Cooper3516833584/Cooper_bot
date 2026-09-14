# 07 acceptance

修复包 08 阶段（测试矩阵 / 最终验收 / 灰度）的验收报告，外加 2026-09-14 的向量检索实现与默认开关调整。所有数字来自本机实际执行，未执行项明确标注 NOT-RUN。

## 版本与环境

- 审查基线 SHA：`9563319a43bd2e4744b6064199530c6dc73c415a`；memory 修复与向量检索都是该 HEAD 之上的提交/working tree。
- Python / SQLite：3.13.5 / 3.49.1。
- 默认配置（`cooper_bot/core/config.py`）：
  - `AI_MEMORY_ENABLED=true`、`AI_MEMORY_EMBEDDING_ENABLED=true` —— **2026-09-14 按产品决定改为默认开启，覆盖修复包 D15「master switch 应默认关闭」的口径**；关闭时不创建也不打开数据库。
  - `AI_MEMORY_SUMMARY_ENABLED=false`、`AI_MEMORY_AUTO_EXTRACT_ENABLED=false` —— 这两条链路会把群聊原文送给模型网关，且从未接过真实 provider，保持关闭。
  - `AI_MEMORY_GROUP_ALLOWLIST` 默认空（群记忆必须显式配置）；`AI_MEMORY_MAX_EVENTS_PER_SCOPE=3000`、`AI_MEMORY_RECENT_EVENTS=40`、`AI_MEMORY_TOP_K=6`、`AI_MEMORY_CONTEXT_CHAR_BUDGET=10000`。
  - 向量：`AI_MEMORY_EMBEDDING_MIN_SIMILARITY=0.35`、`AI_MEMORY_EMBEDDING_BATCH_SIZE=16`、`AI_MEMORY_EMBEDDING_TIMEOUT_SECONDS=30`。**embedding 没有日预算上限**（2026-09-14 按产品决定移除 `AI_MEMORY_EMBEDDING_DAILY_BUDGET`）。
- 执行前仓库已有未跟踪 `runtime/`，未覆盖或清理。测试临时目录 `.pytest_tmp` 曾 ACL 异常（`Get-Acl`/`ls` 均被拒绝），已重命名为 `.pytest_tmp_broken_20260914` 让 pytest 重建；未删除任何用户数据。

## 本阶段新增

- `test/unit/test_memory_deletion.py`（4 例）：forget 清理派生行并写 forget marker 水位；forget 后显式重述仍可见；`clear_subject` 清空并让在途 turn 变为 stale；`/memory group clear` 轮换 conversation 并清空该 scope 的 events/facts/summaries/embeddings/jobs/markers。
- `test/unit/test_memory_vectors.py`（16 例，全 fake provider、零网络）：写入即向量化、无词法重叠也能靠向量召回、开关关闭/未配置/调用失败时逐条等价于纯词法、超过一个批次的回填会全部跑完（无日预算上限）、模型换代后重新回填并丢弃旧 fingerprint、superseded 事实的向量被清理、维度不一致被忽略、查询向量 60s 缓存、融合排序改变 prompt 中 facts 顺序、慢 provider 不阻塞记忆 DB。
- `tools/diagnostics/memory_perf_probe.py`：08 第 7 节的本地性能探针（虚构数据、临时库、结束即清理），并新增向量打分与融合排序的纯本地 CPU 开销测量。
- `tools/diagnostics/probe_memory_embedding.py`：真实 embedding provider 探针（只发两段固定测试文本，不含任何聊天/记忆内容）。**未运行**（需联网授权）。

## 实际测试命令 + 退出码

- `python -m compileall -q cooper_bot client.py`：退出码 0。
- 10 个 memory 专项文件（policy/store/turn_safety/commands/deletion/prompting/summary/extraction/vectors/chat_flow）：退出码 0，68 passed。
- 15 个 memory+AI+dispatch+Kimi argv+lifecycle 文件（上述 10 个 + `test_aisvc_kimi_chat.py`、`test_commands_kimi.py`、`test/integration/test_commands_dispatch.py`、`test_client_lifecycle.py`、`test_kimi_cli.py`）：退出码 0，151 passed。
- `python -m pytest test`：退出码 1，`1 failed, 420 passed`。唯一失败是 `test/unit/test_vision_skill.py::test_resolve_image_ready`（实际 `max_tokens=1600`、旧断言期望 800），与 memory 无关，`00_baseline.md` 已记录为修复前已有失败；本次未顺手改动。
- 默认值翻转后的副作用检查：全量测试跑完 `runtime/databases/ai/` 仍只有 `semantic_store.sqlite3`（2026-09-04），**没有**新建/写入 `chat_memory.sqlite3`。
- `python tools/diagnostics/memory_perf_probe.py`：退出码 0，结果见性能节。

各阶段文档（`01`–`05`、`04_summary_worker.md`）中的“组合测试 N passed”是各阶段当时的文件集合口径；本阶段以本节命令与数字为准。

## M001-M090

原始 M 矩阵不在本修复包内，只能按主题映射到现有测试，无法逐条核对测试 ID。

- M001-M016（存储、身份、开关语义、幂等）：PASS（`test_memory_policy.py`、`test_memory_store.py`、`test_memory_prompting.py` 的 master-off / 路径校验）。
- M017-M035（聊天顺序、stale/send gate、admin、prompt/argv）：PASS（`test_memory_turn_safety.py`、`test_aisvc_kimi_chat.py`、`test_kimi_cli.py`）。
- M036-M051（命令、群权限、opt-out、forget/clear、passive capture）：PASS（`test_memory_commands.py`、`test_memory_deletion.py`、`test_memory_chat_flow.py`）。
- M052-M063（summary schema/worker/cutoff/CAS/lease/budget）：PASS，但 summary 默认关闭；测试在 monkeypatch 打开开关后验证 schema v2→v3、cutoff、parent assistant、CAS、lease、日预算与晚到结果拒绝。
- M064-M078（auto extraction/evidence/cursor/conflict/forget marker）：PASS，同样默认关闭；测试覆盖程序固定 subject/scope、evidence 子串校验、legacy cursor、explicit 优先、forget marker 防复活。
- M079-M084（embedding/向量增强）：自动测试 PASS（`test_memory_vectors.py`，fake provider 覆盖写入、召回、降级、回填、失效、维度、缓存、融合排序）；**真实 provider NOT-RUN**，`probe_memory_embedding.py` 未执行。
- M085（在线 backup/恢复）：PASS（`test_memory_store.py::test_online_backup_restores_consistent_memory_database`）。
- M086（迁移/future schema）：PASS（`test_future_schema_fails_closed`、`test_schema_v2_migrates_summary_and_job_columns_without_rebuild`）。
- M087（本地性能前置）：顺序单查询 p95 < 100 ms 达标（含向量打分）；500 并发过载见性能节，单列为已知限制。
- M088（真实 QQ/Kimi/DeepSeek）：NOT-RUN，无凭据授权、未做生产消息操作。
- M089（真实灰度部署）：NOT-RUN，需要部署窗口与用户授权。
- M090（回滚）：本地配置/重启演练 PASS（`test_master_off_restart_preserves_database_for_rollback`）；生产回滚 NOT-RUN。

## 确定性并发与 prompt 级断言

- 并发测试使用 `asyncio.Event` / threading barrier，不用随机 sleep：重复 pending、排队 turn 遇到 new/clear/off、clear 与 send permit 两种先后顺序、worker 晚到摘要/事实、lease 重启恢复、慢 embedding provider 不阻塞 DB。
- prompt 级断言检查传给 fake runner 的真实 payload：跨群内容、opt-out 成员旧内容、forgotten 事实均不出现在 prompt；群历史保留真实 `[发言人QQ:...]`；current input 只出现一次；`fit_prompt_budget` 先裁 optional context 且不截断 current/system。
- 向量路径同样有 prompt 级断言：融合排序会改变 `memory_context.facts` 的顺序（`test_prompt_context_uses_fused_order`）。
- admin 权限链测试只记录 fake runner 的请求：管理员显式事实可用、volatile history 继续写回且 SQLite 中 admin 事件正文为空、`clear_admin_chat_history` 只清对应 actor/session；没有真实执行 shell 或桌面操作。
- 所有测试使用 `tmp_path` SQLite、fake Kimi runner、fake OneBot reply、fake ModelGateway / fake embedding provider 与虚构 QQ/群号，未调用真实 Kimi admin 工具。

## 真实 QQ / Kimi / DeepSeek / embedding provider

NOT-RUN。全部验证在本地 fake 组件上完成，未连接真实 QQ、Kimi CLI、DeepSeek 或电脑执行域；embedding 也只用了 fake provider。

## 性能（本机实测，命令：`python tools/diagnostics/memory_perf_probe.py`）

样本：20 个 scope × 3000 条终结事件（= `AI_MEMORY_MAX_EVENTS_PER_SCOPE` 默认上限，合计 60,000 events）、500 facts、500 次查询；`runtime/temp` 临时库，结束即清理。

- 库大小 25,116,672 bytes（含 WAL），seed 1.16 s。
- snapshot 读取（按 conversation 取最近 40 条）：p50 35.0 ms、p95 45.6 ms、p99 74.2 ms。
- facts 词法检索：p50 1.30 ms、p95 1.76 ms、p99 2.28 ms。
- 顺序组合查询（snapshot + facts + 词法排序）：p50 38.8 ms、p95 50.6 ms、p99 81.0 ms → 顺序目标 p95 < 100 ms 达标。
- 500 次查询同时进入单线程 DB executor：p50 19,894 ms、p95 20,082 ms、max 20,101 ms、wall 20,113 ms ≈ 500 × 单次 38 ms 的排队结果。
- 向量打分 + 融合排序（500 条候选 × 1024 维，纯本地 CPU、无网络）：p50 6.75 ms、p95 8.83 ms、max 10.85 ms。第一版实现逐条做 list↔ndarray 转换，实测 p50 314 ms，已改为 `from_blob` + 批量 `similarity_scores`（查询只归一化一次）后降到 6.75 ms。

与更早报告的差异（更正）：上一版记录的“顺序 p50 0.526 ms / 500 并发 p95 287.761 ms”没有留下可复现脚本，本阶段无法复现；实测顺序开销几乎全部来自 snapshot 读取（旧口径与仅测 facts 词法查询的量级一致），因此以上表为准。

## 已知限制

- **默认开启但真实环境未验证**：`AI_MEMORY_ENABLED` / `AI_MEMORY_EMBEDDING_ENABLED` 已默认开启（产品决定），而真实 QQ/Kimi/DeepSeek、真实 embedding provider、真实灰度都没有跑过。也就是说"默认开"与"未经真实环境验证"同时成立，扩大范围前需要先跑 `probe_memory_embedding.py` 与一次单私聊灰度。
- embedding provider 是否支持 `/embeddings`、维度多少、限流如何，均未实测；未配置时整条向量路径静默退化为词法。
- 换 `AI_EMBED_MODEL` 会让所有旧向量按 fingerprint 失效并重新回填；回填按 `AI_MEMORY_EMBEDDING_BATCH_SIZE` 分批但**不设日预算上限**，会一直调用 provider 直到没有缺失事实——大批量换模型时对 provider 的请求量没有闸门，只受批间顺序执行（单 worker，一次一条）限制。
- 单线程 DB executor 下 500 并发过载排队 p95 ≈ 20 s；正常顺序聊天路径 p95 ≈ 51 ms。向量打分额外约 7 ms/次（500 条候选）。
- `snapshot_rows` 在单 conversation 达到 retention 上限时 p95 ≈ 46 ms（本报告样本即最坏情况）。
- summary / auto_extract 默认关闭：代码路径已被测试覆盖，但默认配置下不会真正产生模型请求，也未在真实 provider 下端到端验证。
- `/memory list` 会展示 `group:<gid>` 主体的事实，而 `/memory forget` 只在 `user:<actor>` 主体内解析 ID 前缀，因此群约定目前只能用 `/memory group clear` 清除（未改行为，记为待决）。
- `/memory off`、`forget` 与成员策略变化会删除该 scope 全部 assistant 混合回复与摘要（隐私优先，代价是其他成员的历史摘要一起失效）。
- 全量测试仍有一个基线 vision 断言失败（1600 vs 800），未在 memory 修复中改动。
- M088-M090 的真实环境项未执行。

## 回滚演练

`AI_MEMORY_ENABLED=false` 后重启：独立 DB 保留、不删库、不降 schema；增强项可分别关闭 `AI_MEMORY_SUMMARY_ENABLED` / `AI_MEMORY_AUTO_EXTRACT_ENABLED` / `AI_MEMORY_EMBEDDING_ENABLED`（关闭 embedding 后 `search_facts` 与纯词法逐条等价）。自动测试已验证关闭后不创建/不修改 DB，重新启用后仍能读到原显式事实。

## 是否可上线

**不能判定为"已验证可上线"**。memory 的阻断安全项与向量检索都有自动测试证据，默认开关也按产品要求打开了；但真实 QQ/Kimi/DeepSeek、真实 embedding provider、真实灰度与生产回滚均未执行，高并发 DB 排队容量未验证，summary/auto_extract 仍默认关闭且未在真实 provider 下端到端跑过，全量测试也仍有一个基线 vision 断言失败。按 08 第 10 节，这不满足"可上线"的举证要求——默认开启属于产品决定，风险需以真实灰度结果来确认或回退。
