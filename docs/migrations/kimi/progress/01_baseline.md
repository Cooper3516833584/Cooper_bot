# 阶段 01：基线

采集时间：2026-09-08（Asia/Shanghai）

## 代码与工作区

- 固定 SHA：`0bf099f78c71f0ffe9a3fcafa3273275553ea719`
- 平台：Windows；Python `3.13.5`。
- 运行前已有、且未由本阶段修改的工作区变更：
  - `config/ai/private_chat_prompts.json`（3 行新增）
  - `cooper_bot/core/config.py`（2 行变更）
- `git` 提示无法访问用户全局 ignore 文件；本阶段未改变 Git 配置。

## 测试基线

测试入口与 fixture 已检查：`test_client.py` 按 unit/integration/smoke 选择目录；`test/conftest.py` 使用临时目录并阻断外部 AI、QQ 与 OCR 服务。未启动 `client.py`。

| 命令 | 退出码 | 结果 |
| --- | --- | --- |
| `python test_client.py unit` | 1 | 169 passed、2 failed、19 errors |
| `python test_client.py smoke` | 0 | 4 passed |

unit 的既有失败/环境问题：

- `test_default_system_prompt_includes_level_one_command_guidance`：期望文案与当前 `AI_SYSTEM_PROMPT` 不一致。
- `test_resolve_image_ready`：期望 `max_tokens == 800`，实际为 1600。
- `.pytest_tmp` 无法删除（Windows `PermissionError`），导致 19 个用例在 `tmp_path` 创建期失败。

这些失败发生在任何迁移代码变更之前；本阶段没有修复、删除或掩盖它们。

## 阶段范围

本阶段只建立基线、调用图和 CLI 合同，未切换 QQ 聊天、未删除 Antigravity、未调用模型、未发送 QQ 消息、未启动生产服务。
