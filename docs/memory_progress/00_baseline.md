# 00 baseline

- 实际起始提交：`4ce892abe3c8bc0b362c0c662dac5fbbb018611b`
- Python / SQLite：3.13.5 / 3.49.1。
- 测试入口：`python -m pytest`，测试树包含 `test/unit`、`test/integration`、`test/smoke`。
- 已核实：方案所列 commands、AIService、ModelGateway、Kimi CLI、router 和 client 生命周期符号均存在。router 的 `Ctx` 没有 `self_id`，因此实现从可信 OneBot `evt.self_id` 读取并仅允许固定 `AI_MEMORY_BOT_ID` 兜底。
- 源码漂移：client 已在最外层调用 `aisvc.aclose()`；无需改 WebSocket 重连循环。现有 public/admin Kimi profile、30 分钟内存历史、视觉 slot 和群转私聊路径保持不变。
- 基线收集命令成功：`python -m pytest --collect-only -q test`；执行前已发现用户原有未跟踪 `runtime/`，未读取或修改。
