# 00 baseline

- 实际起始提交：`9563319a43bd2e4744b6064199530c6dc73c415a`。
- Python / SQLite：3.13.5 / 3.49.1。
- 测试入口：`python -m pytest`，测试树包含 `test/unit`、`test/integration`、`test/smoke`。
- 已核实：方案所列 commands、AIService、ModelGateway、Kimi CLI、router 和 client 生命周期符号均存在。router 的 `Ctx` 没有 `self_id`，因此实现从可信 OneBot `evt.self_id` 读取并仅允许固定 `AI_MEMORY_BOT_ID` 兜底。
- 源码漂移：client 已在最外层调用 `aisvc.aclose()`；无需改 WebSocket 重连循环。现有 public/admin Kimi profile、30 分钟内存历史、视觉 slot 和群转私聊路径保持不变。
- 执行前已发现用户原有未跟踪 `runtime/`，未覆盖或清理。
- 基线命令与退出码：
  - `python -m compileall -q cooper_bot client.py`：0。
  - `python -m pytest -q test/unit/test_memory_policy.py test/unit/test_memory_store.py`：0（3 passed）。
  - `python -m pytest -q test/unit/test_aisvc_kimi_chat.py test/unit/test_commands_kimi.py test/integration/test_commands_dispatch.py test/unit/test_client_lifecycle.py`：0（52 passed）。
  - `python -m pytest -q test`：1；唯一失败为 `test/unit/test_vision_skill.py::test_resolve_image_ready`，实际 `max_tokens=1600`、旧测试期望 800，与 memory 无直接关系，记为修复前已有失败。

## 第二轮 R2 基线（986cb4e）

- 实际 HEAD：`986cb4e52c6a8a08b794bcecc033a738dc8ffa18`，与修复包基线一致；未做 reset，未 rebase。
- 基线命令与退出码：
  - `python -m compileall -q cooper_bot client.py`：0。
  - `python -m pytest -q test`（单进程独占运行）：`1 failed, 420 passed`，失败即上文 vision `max_tokens` 断言。该口径由本轮最终结果（445 passed）减去本轮新增的 25 例回归测试反推，并与 `07_acceptance.md` 在 9563319 上记录的 420 passed 一致。
  - 与另一个 pytest 进程并发共用 `--basetemp=.pytest_tmp` 时，`test/unit/test_memory_vectors.py::test_embedding_has_no_daily_cap_and_drains_the_backlog` 出现过一次偶发失败（单独跑 3/3 通过）；该异常未能在单进程口径下复现，已按"回填作业 running 期间重新入队会被丢弃"的真实漏洞另行修复并补确定性回归测试。
- R2-D01 ~ R2-D10 逐项复核：在本 HEAD 上全部仍然存在（D10 属配置口径，不是代码缺陷）。
- 已知与 memory 无关的基线问题：lunar-python 环境依赖、vision `max_tokens` 测试与实现不一致（1600 vs 800）；两项都未混入本轮 memory 修复。
