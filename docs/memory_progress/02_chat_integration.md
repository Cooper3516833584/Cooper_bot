# 02 chat integration

AIService 构造惰性 MemoryService；启用且当前 scope 已开启时，聊天先持久登记、按 scope 串行、从 confirmed 历史取快照，并在真实 reply 返回后才提交 confirmed/unconfirmed。未启用时仍走原 Kimi 内存缓存路径。admin 路径只留无正文请求元数据和显式事实。

验证：`test/unit/test_aisvc_kimi_chat.py`、`test/unit/test_commands_kimi.py` 通过。client 原有最外层 `aisvc.aclose()` 已覆盖 MemoryService 关闭。

9563319 修复阶段 03：每个 scope 增加短 send gate，turn 在 gate 内复核 epoch/cid/member/policy 并取得发送开始许可；member/group policy、forget、clear、new 等失效操作也在同一 gate 内提交。clear 先提交会阻止旧回答发送；permit 先取得则只允许该次已进入发送边界的回复继续。

admin memory turn 改为继续使用隔离的 `admin:<actor>:<session>` 临时历史，并写回新 turn；SQLite 仍为 metadata-only，只从当前 admin scope 注入显式 facts。admin off/clear/new 会清理对应临时键。相关组合测试：72 passed。

9563319 修复阶段 04：当前轮保存安全的引用文本和 ready 视觉描述，拒绝 URL/base64/本地路径；群持久 user 历史恢复时保留 actor QQ，并以普通 user 内容而非 system 指令注入。最终 payload 接入确定性预算裁剪，current/system 永不截断，Kimi runner 继续执行同源 Windows argv 最终硬限制。相关组合测试：106 passed。
