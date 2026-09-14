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
