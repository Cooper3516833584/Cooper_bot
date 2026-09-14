# 03 commands

新增 `/memory` 显式命令并在通用非 AI 记忆钩子之前处理，避免管理命令写回聊天缓存。支持 on/off/status/remember/list/search/forget/clear/new/history 及受 allowlist 约束的群管理入口；clear 为一次性、60 秒确认。

验证：`test/integration/test_commands_dispatch.py` 通过。命令未连接生产 QQ 或模型。

9563319 修复阶段 02：个人 `/memory on` 在公共群中只解除本人 opt-out，不能启用群 scope；公共群策略只允许可信个人管理员通过 `group on/off` 修改。group remember 不会暗中开启群记忆，公共群 `new` 也仅允许可信个人管理员。所有 group profile（含 admin）统一受部署 allowlist 限制。

成员策略实际变化会在同一 SQLite 事务内递增 epoch、移除旧混合 assistant/summary 与非显式派生事实、取消旧作业；snapshot 排除已 opt-out 成员的旧 raw。相关 memory、AI、dispatch、lifecycle 组合测试：67 passed。

9563319 修复阶段 05：status 输出真实 master/scope/member/mode/profile 与功能开关；list 支持分页，forget/replace 接受当前 scope+subject 内唯一 ID 前缀，history 支持关键词和稳定 before 游标并显示时间、actor、role/source。clear token 绑定 scope/action/target/epoch 且一次性消费；新增管理员 group clear。`all` 模式在普通群消息路由末端幂等采集 passive_chat，不调用 Kimi；directed 与 `/memory` 命令不采集。

基础版 00–05 相关组合测试：112 passed。完整 `python -m pytest -q test` 仍只有基线已有的 `test_vision_skill.py::test_resolve_image_ready`（1600 vs 800）失败，无新增 memory 失败。
