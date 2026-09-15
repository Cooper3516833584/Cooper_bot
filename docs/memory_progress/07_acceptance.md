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
- `tools/diagnostics/probe_memory_embedding.py`：真实 embedding provider 探针（只发两段固定测试文本，不含任何聊天/记忆内容）。**已于 2026-09-14 运行**（用户授权联网）：`api.siliconflow.cn` 的 `BAAI/bge-m3`，`/embeddings` 可用，1024 维，同文本余弦 0.99996、跨文本 0.5949。

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
- M079-M084（embedding/向量增强）：自动测试 PASS（`test_memory_vectors.py`，fake provider 覆盖写入、召回、降级、回填、失效、维度、缓存、融合排序）；**真实 provider 已执行**（2026-09-14）：`api.siliconflow.cn` 的 `BAAI/bge-m3` 返回 1024 维，同文本余弦 0.99996、跨文本 0.5949。M079-M084 可判 PASS（本机 + 真实 provider 各一次），但生产消息链路仍未验证。
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

QQ / Kimi / DeepSeek：NOT-RUN，未连接真实 QQ、Kimi CLI、DeepSeek 或电脑执行域。

embedding provider：**已实测**（2026-09-14，用户授权联网）。`tools/diagnostics/probe_memory_embedding.py` 只发两段固定测试文本、不含任何聊天或记忆内容，结果：`api.siliconflow.cn` 的 `BAAI/bge-m3`，`/embeddings` 可用，维度 1024，同文本两次调用余弦 0.99996，跨文本余弦 0.5949。限流、超时、错误码等其余 provider 行为仍未实测。

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

- **默认开启、真实消息链路未验证**：`AI_MEMORY_ENABLED` / `AI_MEMORY_EMBEDDING_ENABLED` 已默认开启（产品决定）；embedding provider 已实测可用，但真实 QQ/Kimi/DeepSeek 消息链路与真实灰度仍未跑过。扩大范围前至少要做一次单私聊灰度。
- embedding provider 的限流、超时与错误码行为未实测；未配置或调用失败时整条向量路径静默退化为词法（已由自动测试覆盖）。
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

**不能判定为"已验证可上线"**。memory 的阻断安全项与向量检索都有自动测试证据，embedding provider 也已实测可用，默认开关按产品要求打开；但真实 QQ/Kimi/DeepSeek 消息链路、真实灰度与生产回滚均未执行，高并发 DB 排队容量未验证，summary/auto_extract 仍默认关闭且未在真实 provider 下端到端跑过，全量测试也仍有一个基线 vision 断言失败。按 08 第 10 节，这不满足"可上线"的举证要求——默认开启属于产品决定，风险需以真实灰度结果来确认或回退。

## 第二轮 R2 修复（2026-09-15）

审查基线 `986cb4e52c6a8a08b794bcecc033a738dc8ffa18`（HEAD 未变，无 reset / rebase）。对应修复包 `Downloads/Cooper_bot_memory_repair_round2_986cb4e` 的 00–05 号文档；所有数字来自本机实际执行，未执行项标注 NOT RUN。

### 修改文件

- `cooper_bot/modules/memory/service.py`：`_Turn` 生命周期（`__aenter__` 以 `try/except BaseException` 包住准备阶段、抽出 `_enter`、新增 `_release_lock`、`abort()` 区分"未生成 / 已生成"、`__aexit__` 改为按记账释放锁）；新增 `_clear_query_vector_cache` 与 query 向量 fingerprint 校验；新增 `fact_is_embedding_eligible`；回填作业逐条 eligibility 复核 + 仅在"写出过向量且仍有缺口"时重排；抽取改为 `ExtractionResult` / `ExtractionDeferred`。
- `cooper_bot/modules/memory/store.py`：member off 不再删除整个 scope 的派生事实；`list_facts` / `history` 过滤 opt-out 成员；新增 `embedding_eligible`；`apply_extracted_facts` 去掉全局 explicit 闸门并改为按 key 收敛 active；`save_explicit_fact` 同 key 收敛；`requeue_job` 覆盖 `running`。
- `cooper_bot/modules/memory/models.py`：新增 `ExtractionResult`、`ExtractionDeferred`。
- 测试：新增 `test/unit/test_memory_r2_regressions.py`（25 例）；`test/unit/test_memory_extraction.py` 中 1 例按"同一 fact_key 才判优先级"的新语义改写。
- 文档：`docs/memory.md`、`docs/memory_progress/` 的 `00_baseline.md`、`01_storage.md`、`02_chat_integration.md`、`05_facts_retrieval.md`、`06_hybrid_optional.md`、本文件。
- **未改动** `cooper_bot/core/config.py`（R2-D10 见下）；未改动 `commands.py` / `aisvc.py` 消息主路径。

### R2-D01 ~ R2-D10

| ID | 级别 | 状态 | 修复点 | 回归测试 |
|---|---|---|---|---|
| R2-D01 | P0 | PASS（单测） | `abort()` 在未 generated 时把 pending 输入收尾为 `failed`，不再永久占用同一 `source_event_id` | T-R2-01（service 层 + commands 触发路径） |
| R2-D02 | P0 | PASS（单测） | `__aenter__` 异常（含取消）释放 scope lock，`_lock_acquired` 记账防 double release | T-R2-02（snapshot_rows / search_facts 两个失败点） |
| R2-D03 | P0 | PASS（单测） | `_history` 关联 `memory_members`，只返回 enabled 成员 | T-R2-05 |
| R2-D04 | P0 | PASS（单测） | 每条 fact 真正调用 provider 前 `fact_is_embedding_eligible` 复核 | T-R2-06、T-R2-07 |
| R2-D05 | P1 | PASS（单测） | 删除 `DELETE FROM memory_facts WHERE source_kind!='explicit_memory'`，off 只停用不物理删除 | T-R2-08 |
| R2-D06 | P1 | PASS（单测） | `ExtractionResult.executed=False` 时抛 `ExtractionDeferred`：cursor 不推进、作业退避重试 | T-R2-10、T-R2-11、T-R2-12 |
| R2-D07 | P1 | PASS（单测） | 去掉"该 subject 存在任意 explicit 就跳过全部 auto 候选"的全局闸门 | T-R2-13 |
| R2-D08 | P1 | PASS（单测） | 同一 `(scope_id, subject_id, fact_key)` 最多一个 active revision：explicit 优先、active auto 被 supersede | T-R2-14、T-R2-15 |
| R2-D09 | P1 | PASS（单测） | `_clear_query_vector_cache` 接入 member off / scope off / forget / clear / group clear / new，缓存项带 fingerprint | T-R2-17、T-R2-18、T-R2-19 |
| R2-D10 | 配置 | REVIEWED（维护者决定保留默认开启） | 未改 `config.py`；`docs/memory.md` 补齐 embedding 外发语义与 opt-out 语义 | T-R2-20 |

未被列进矩阵但同属本轮修复的一个确定性漏洞：回填作业 `running` 期间新写入的事实无法再排队（`requeue_job` 只匹配终结状态）→ 该批事实永久缺向量。已修为 `state IN (...)` 含 `running`，并有确定性回归测试 `test_r2_embed_backfill_picks_up_facts_written_while_job_runs`。

### T-R2-01 ~ T-R2-20

| ID | 场景 | 结果 | 测试 |
|---|---|---|---|
| T-R2-01 | 生成前异常不留 pending | PASS | `test_r2_t01_pre_generate_exception_does_not_leave_pending_input`、`test_r2_t01_kimi_path_failure_leaves_no_pending_input` |
| T-R2-02 | `__aenter__` 异常不泄漏 lock | PASS | `test_r2_t02_enter_failure_releases_scope_lock[snapshot_rows]` / `[search_facts]` |
| T-R2-03 | failed input 不阻塞 stable cutoff | PASS | `test_r2_t03_failed_input_does_not_block_stable_cutoff` |
| T-R2-04 | generated 后异常状态机一致 | PASS | `test_r2_t04_exception_after_generate_finishes_turn_as_failed` |
| T-R2-05 | opt-out 后 history 不泄露旧正文 | PASS | `test_r2_t05_opt_out_member_is_hidden_from_history` |
| T-R2-06 | opt-out 后 embedding backfill 不外发 | PASS | `test_r2_t06_opt_out_member_facts_are_not_sent_to_embedding` |
| T-R2-07 | embedding batch 中途 off 停止后续外发 | PASS | `test_r2_t07_mid_batch_opt_out_stops_remaining_sends` |
| T-R2-08 | B facts 不受 A off 影响 | PASS | `test_r2_t08_opt_out_keeps_other_members_auto_facts` |
| T-R2-09 | A off 不删除 B auto facts | PASS | 同上（断言 `auto_extracted` 计数仍为 2） |
| T-R2-10 | budget=0 cursor 不推进 | PASS | `test_r2_t10_budget_exhausted_does_not_advance_cursor` |
| T-R2-11 | provider timeout cursor 不推进 | PASS | `test_r2_t11_provider_failure_does_not_advance_cursor` |
| T-R2-12 | 模型真实返回空 facts 时 cursor 推进 | PASS | `test_r2_t12_genuine_empty_result_advances_cursor` |
| T-R2-13 | explicit answer_style 不阻止 auto location | PASS | `test_r2_t13_freeform_explicit_does_not_block_unrelated_auto_key` |
| T-R2-14 | explicit 同 key 优先于 auto | PASS | `test_r2_t14_explicit_same_key_wins_over_auto` |
| T-R2-15 | auto 后 explicit 能 supersede | PASS | `test_r2_t15_explicit_supersedes_previous_auto` |
| T-R2-16 | freeform explicit 不阻断其他 key | PASS | 同 T-R2-13（并断言 explicit key 为 `explicit.` 前缀） |
| T-R2-17 | clear 清 query-vector cache | PASS | `test_r2_t17_privacy_actions_invalidate_query_vector_cache[clear]` |
| T-R2-18 | forget 清 query-vector cache | PASS | 同上 `[forget]`（另有 `[new]` / `[member_off]` / `[scope_off]`） |
| T-R2-19 | epoch rotate 后旧 cache 不命中 | PASS | `test_r2_t19_epoch_rotate_does_not_reuse_stale_query_vector` |
| T-R2-20 | 默认配置与文档一致 | PASS | `test_r2_t20_memory_switches_default_and_docs_agree`、`test_r2_t20_env_can_disable_both_switches_in_isolated_process` |

### 对抗性并发（05 号文档 A/B/C）

- A. enter 异常：T-R2-02 覆盖——turn1 在 `snapshot_rows` / `search_facts` 抛异常后，turn2 在 `asyncio.timeout(5)` 内成功 acquire，且锁最终为未占用。
- B. embedding 中途 off：T-R2-07 覆盖——batch `a1,a2,b1`；a1 已发出（无法撤回）、`A /memory off` 后 a2 停止外发、b1 正常发送。
- C. budget exhaustion：T-R2-10 覆盖——`daily budget=0` 时 cursor 保持 0（`< target`）且模型完全未被调用；恢复预算后同一批事件仍能抽取到目标 seq。

### 实际测试命令 + 结果

- `python -m compileall -q cooper_bot client.py`：退出码 0。
- `python -m pytest -q test/unit/test_memory_r2_regressions.py`：25 passed；单独连跑 10 次全绿。
- 11 个 memory 专项文件（policy / store / turn_safety / commands / deletion / prompting / summary / extraction / vectors / chat_flow + 本轮新增）：93 passed，退出码 0。
- `python -m pytest test`（单进程独占，避免与其它 pytest 共用 `--basetemp`）：`1 failed, 445 passed in 50.43s`；唯一失败是基线已有的 `test/unit/test_vision_skill.py::test_resolve_image_ready`（实际 1600、旧断言 800），与 memory 无关，本轮未改动。
- 负向对照（把旧实现临时装回运行期验证测试有效，脚本跑完即删、未留在仓库）：R2-D01/D02/D03/D04/D05/D06/D07/D09 八项违规全部复现；另确认"回填作业 running 期间新写入事实永久缺向量"在旧的 `requeue_job` 下确定复现。
- 说明：本轮曾出现一次 `test_memory_vectors.py::test_embedding_has_no_daily_cap_and_drains_the_backlog` 偶发失败，当时存在我自己并发运行多个 pytest 进程共用 `--basetemp=.pytest_tmp` 的情况，因此无法把该次失败单独归因；单进程口径下该用例 3/3 通过。上一条中那个真实漏洞与该表现一致，已独立用确定性测试锁定并修复。

### 未执行项（NOT RUN）

- 真实 QQ / Kimi CLI / DeepSeek 消息链路：NOT RUN（全程使用 fake runner 与 fake gateway）。
- 真实 embedding provider 调用：NOT RUN（本轮全部 fake provider；上次真实调用是 2026-09-14 的历史探针）。
- 生产灰度部署与生产回滚演练：NOT RUN。
- 真实群多成员并发 opt-out、provider 限流 / 超时错误码 / 配额行为：NOT RUN。

### 残余风险

- `requeue_job` 现在会把 `running` 的回填作业重新置为 `queued`，同一作业可能被 worker 再领一次；处理器幂等（只处理仍缺向量的 fact，写入为 REPLACE），且只在"写出过向量且仍有缺口"时重排，未观察到活锁。这是本轮唯一改动作业状态机的点，灰度时应观察 provider 请求量。
- 重排改为从头重扫缺失事实后，大 scope（已向量化条目很多、缺失很少）的单次扫描会退化为 O(已向量化条数)；功能正确、无活锁，但换 embedding 模型的整库回填会比之前更频繁地重扫。
- `/memory off`、`forget`、成员策略变化仍删除该 scope 全部 assistant 混合回复与摘要（沿用上一轮隐私优先口径），代价是其他成员的历史摘要一起失效；本轮只去掉了"连带删除其他成员 auto facts"这一步。
- budget 持续耗尽时抽取作业重试 3 次后标记 failed，同一批事件要等新消息触发新作业才会继续（cursor 未推进，不丢事实，但延迟取决于是否有后续对话）。
- `ExtractionResult.retryable` 目前恒为 True（非执行态都算可重试），保留为后续区分"可重试 / 不可重试"的接口位。
- 全量测试仍有 1 个基线 vision 断言失败。

### 是否建议进入真实 QQ 小流量灰度

建议按 05 号文档进入**第一阶段**小流量灰度：`AI_MEMORY_ENABLED=true`、`AI_MEMORY_SUMMARY_ENABLED=false`、`AI_MEMORY_AUTO_EXTRACT_ENABLED=false`、`AI_MEMORY_EMBEDDING_ENABLED=false`，先验证近期持久历史、explicit remember、off / clear / new 与 admin 电脑操作；随后依次放开 summary、auto facts，最后才考虑 embedding。

但必须明确：本轮结论全部来自 fake provider 与单进程自动测试，真实 QQ / Kimi / DeepSeek / embedding provider 与生产回滚均为 NOT RUN，因此上述建议是"可以开始小流量验证"，**不是"已验证可上线"**。
