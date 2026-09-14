# 07 acceptance

修复包 08 阶段（测试矩阵 / 最终验收 / 灰度）的验收报告。所有数字来自本机实际执行，未执行项明确标注 NOT-RUN。

## 版本与环境

- 审查基线 SHA：`9563319a43bd2e4744b6064199530c6dc73c415a`。实现位于该 HEAD 之上的未提交 working tree；未创建 commit。
- Python / SQLite：3.13.5 / 3.49.1。
- 默认配置（`cooper_bot/core/config.py`）：`AI_MEMORY_ENABLED=false`、`AI_MEMORY_SUMMARY_ENABLED=false`、`AI_MEMORY_AUTO_EXTRACT_ENABLED=false`、`AI_MEMORY_EMBEDDING_ENABLED=false`；`AI_MEMORY_GROUP_ALLOWLIST` 默认空（群记忆必须显式配置）；`AI_MEMORY_MAX_EVENTS_PER_SCOPE=3000`、`AI_MEMORY_RECENT_EVENTS=40`、`AI_MEMORY_TOP_K=6`、`AI_MEMORY_CONTEXT_CHAR_BUDGET=10000`。
- 执行前仓库已有未跟踪 `runtime/`，未覆盖或清理。测试临时目录 `.pytest_tmp` 存在 ACL 异常（`Get-Acl`/`ls` 均被拒绝），已将其重命名为 `.pytest_tmp_broken_20260914` 让 pytest 重建；未删除任何用户数据。

## 本阶段新增

- `test/unit/test_memory_deletion.py`（新增 4 例）：forget 清理派生行并写 forget marker 水位；forget 后显式重述仍可见；`clear_subject` 清空并让在途 turn 变为 stale；`/memory group clear` 轮换 conversation 并清空该 scope 的 events/facts/summaries/embeddings/jobs/markers。
- `tools/diagnostics/memory_perf_probe.py`（新增）：08 第 7 节的本地性能探针，虚构数据、临时库、结束即清理，不触网、不碰真实记忆库。

## 实际测试命令 + 退出码

- `python -m compileall -q cooper_bot client.py`：退出码 0。
- 9 个 memory 专项文件（policy/store/turn_safety/commands/deletion/prompting/summary/extraction/chat_flow）：退出码 0，52 passed。
- 14 个 memory+AI+dispatch+Kimi argv+lifecycle 文件（上述 9 个 + `test_aisvc_kimi_chat.py`、`test_commands_kimi.py`、`test/integration/test_commands_dispatch.py`、`test_client_lifecycle.py`、`test_kimi_cli.py`）：退出码 0，135 passed。
- `python -m pytest test`：退出码 1，`1 failed, 404 passed`。唯一失败是 `test/unit/test_vision_skill.py::test_resolve_image_ready`（实际 `max_tokens=1600`、旧断言期望 800），与 memory 无关，`00_baseline.md` 已记录为修复前已有失败；本次未顺手改动。
- `python tools/diagnostics/memory_perf_probe.py`：退出码 0，结果见性能节。

各阶段文档（`01`–`05`、`04_summary_worker.md`）中的“组合测试 N passed”是各阶段当时的文件集合口径；本阶段以本节命令与数字为准。

## M001-M090

原始 M 矩阵不在本修复包内，只能按主题映射到现有测试，无法逐条核对测试 ID。

- M001-M016（存储、身份、默认关闭、幂等）：PASS（`test_memory_policy.py`、`test_memory_store.py`、`test_memory_prompting.py` 的 master-off / 路径校验）。
- M017-M035（聊天顺序、stale/send gate、admin、prompt/argv）：PASS（`test_memory_turn_safety.py`、`test_aisvc_kimi_chat.py`、`test_kimi_cli.py`）。
- M036-M051（命令、群权限、opt-out、forget/clear、passive capture）：PASS（`test_memory_commands.py`、`test_memory_deletion.py`、`test_memory_chat_flow.py`）。
- M052-M063（summary schema/worker/cutoff/CAS/lease/budget）：PASS，但 summary 默认关闭；测试在 monkeypatch 打开开关后验证 schema v2→v3、cutoff、parent assistant、CAS、lease、日预算与晚到结果拒绝。
- M064-M078（auto extraction/evidence/cursor/conflict/forget marker）：PASS，同样默认关闭；测试覆盖程序固定 subject/scope、evidence 子串校验、legacy cursor、explicit 优先、forget marker 防复活。
- M079-M084（embedding/向量增强）：NOT-RUN；embedding 未实现且默认关闭，当前检索为 scope-first 词法。
- M085（在线 backup/恢复）：PASS（`test_memory_store.py::test_online_backup_restores_consistent_memory_database`）。
- M086（迁移/future schema）：PASS（`test_future_schema_fails_closed`、`test_schema_v2_migrates_summary_and_job_columns_without_rebuild`）。
- M087（本地性能前置）：顺序单查询 p95 < 100ms 达标；500 并发过载见性能节，单列为已知限制。
- M088（真实 QQ/Kimi/DeepSeek）：NOT-RUN，无凭据授权、未做生产消息操作。
- M089（真实灰度部署）：NOT-RUN，需要部署窗口与用户授权。
- M090（回滚）：本地配置/重启演练 PASS（`test_master_off_restart_preserves_database_for_rollback`）；生产回滚 NOT-RUN。

## 确定性并发与 prompt 级断言

- 并发测试使用 `asyncio.Event` / threading barrier，不用随机 sleep：重复 pending、排队 turn 遇到 new/clear/off、clear 与 send permit 两种先后顺序、worker 晚到摘要/事实、lease 重启恢复。
- prompt 级断言检查传给 fake runner 的真实 payload：跨群内容、opt-out 成员旧内容、forgotten 事实均不出现在 prompt；群历史保留真实 `[发言人QQ:...]`；current input 只出现一次；`fit_prompt_budget` 先裁 optional context 且不截断 current/system。
- admin 权限链测试只记录 fake runner 的请求：管理员显式事实可用、volatile history 继续写回且 SQLite 中 admin 事件正文为空、`clear_admin_chat_history` 只清对应 actor/session；没有真实执行 shell 或桌面操作。
- 所有测试使用 `tmp_path` SQLite、fake Kimi runner、fake OneBot reply、fake ModelGateway 与虚构 QQ/群号，未调用真实 Kimi admin 工具。

## 真实 QQ / Kimi / DeepSeek

NOT-RUN。全部验证在本地 fake 组件上完成，未连接真实 QQ、Kimi CLI、DeepSeek 或电脑执行域。

## 性能（本机实测，命令：`python tools/diagnostics/memory_perf_probe.py`）

样本：20 个 scope × 3000 条终结事件（= `AI_MEMORY_MAX_EVENTS_PER_SCOPE` 默认上限，合计 60,000 events）、500 facts、500 次查询；`runtime/temp` 临时库，结束即清理。

- 库大小 25,116,672 bytes（含 WAL），seed 1.159 s。
- snapshot 读取（按 conversation 取最近 40 条）：p50 34.243 ms、p95 42.693 ms、p99 50.466 ms。
- facts 词法检索：p50 0.747 ms、p95 1.129 ms、p99 1.571 ms。
- 顺序组合查询（snapshot + facts + 词法排序）：p50 36.266 ms、p95 47.011 ms、p99 53.128 ms → 顺序目标 p95 < 100 ms 达标。
- 500 次查询同时进入单线程 DB executor：p50 18,618.303 ms、p95 18,810.319 ms、p99 18,825.132 ms、max 18,828.421 ms、wall 18,839.592 ms ≈ 500 × 单次 37 ms 的排队结果。

与上一版报告的差异（重要更正）：上一版记录的“顺序 p50 0.526 ms / 500 并发 p95 287.761 ms”没有留下任何可复现脚本，本阶段无法复现。实测顺序开销几乎全部来自 snapshot 读取（旧口径与仅测 facts 词法查询的量级一致），因此以上表为准。snapshot 较慢的原因是它按 conversation 读取整段历史、再在 Python 侧截取最近 `AI_MEMORY_RECENT_EVENTS` 条；开启 summary 后从水位之后读取会更快。

## 已知限制

- 单线程 DB executor 下 500 并发过载排队 p95 ≈ 18.8 s；正常顺序聊天路径 p95 ≈ 47 ms。扩大群范围前需要观察或改批量/缓存。
- `snapshot_rows` 在单 conversation 达到 retention 上限时 p95 ≈ 43 ms（本报告样本即最坏情况）。
- summary / auto_extract 默认关闭：代码路径已被测试覆盖，但默认配置下不会真正产生模型请求，也未在真实 provider 下端到端验证。
- `/memory list` 会展示 `group:<gid>` 主体的事实，而 `/memory forget` 只在 `user:<actor>` 主体内解析 ID 前缀，因此群约定目前只能用 `/memory group clear` 清除（未改行为，记为待决）。
- `/memory off`、`forget` 与成员策略变化会删除该 scope 全部 assistant 混合回复与摘要（隐私优先，代价是其他成员的历史摘要一起失效）。
- embedding / 向量召回未实现且默认关闭。
- 全量测试仍有一个基线 vision 断言失败（1600 vs 800），未在 memory 修复中改动。
- M088-M090 的真实环境项未执行。

## 回滚演练

`AI_MEMORY_ENABLED=false` 后重启：独立 DB 保留、不删库、不降 schema；增强项可分别关闭 `AI_MEMORY_SUMMARY_ENABLED` / `AI_MEMORY_AUTO_EXTRACT_ENABLED` / `AI_MEMORY_EMBEDDING_ENABLED`。自动测试已验证关闭后不创建/不修改 DB，重新启用后仍能读到原显式事实。

## 是否可上线

NO（不可全面上线）。memory 的阻断安全项已有自动测试证据，可以先以全部开关关闭的代码形态部署，并按 08 第 8 节顺序灰度；但真实 QQ/Kimi/DeepSeek、真实灰度与生产回滚均未执行，高并发 DB 排队容量未验证，summary/auto_extract 默认关闭且未在真实 provider 下端到端跑过，全量测试也仍有一个基线 vision 断言失败。
