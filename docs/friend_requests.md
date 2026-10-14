# 私聊好友申请补全

2026-10-14 13:12 的好友提示进入 AI 聊天，随后 `/get` 多次返回
`rich media transfer failed`；人工添加好友后，13:42 发送成功。
NapCat 4.10.41 的原生申请记录显示 `reqType=9`（允许任何人添加）、
`isDecide=true`、`isUnread=false`。这种记录被 NapCat 的常规
`request.friend` 上报过滤，因此原有自动同意代码没有执行。

`client.py` 收到私聊中的精确文本“请求添加你为好友”时，额外调度独立的
后台好友补全任务；原消息仍按原流程进入命令/AI 分发，不等待补全，也不
新增固定回复或会话日志。沿用 `AUTO_APPROVE_FRIEND_REQUEST` 和
`AUTO_APPROVE_FRIEND_REMARK`，不改变文件访问权限、路径检查或配置默认值。
群消息、群临时会话、其他文本保留原有分发。

NapCat 标准接口不能按 QQ 号查找申请。`deploy/napcat/cooper_friend.mjs`
提供 `cooper_complete_friend_request`，通过原生好友服务查找该用户最新的
真实申请，处理待确认、已同意未添加、允许任何人添加和添加失败的记录。
不处理发出的申请、被拒绝/忽略的申请，已在好友列表中时直接返回。
使用与 NapCat 标准同意接口相同的 `approvalFriendRequest` 补全关系，
最多等待 3 秒确认本地好友列表；未确认时只记录后台警告。
HTTP 超时不会再通过 WS 重复提交这项操作。

Docker Compose 已包含插件文件的只读挂载。首次部署需运行
`docker compose up -d napcat` 并重启 Python bot；NapCat 会从插件目录加载它。
现有容器也可复制该文件到 `/app/napcat/plugins/cooper_friend.mjs`，
通过已鉴权的 WebUI 插件管理启用，再重启 Python bot。
此插件适配安装版 NapCat 4.10.41；升级 NapCat 后需核对插件初始化参数和
原生好友服务。插件缺失/失败时只记录后台警告，原有 aichat 回复继续运行。

验证命令：

```powershell
python -m pytest test/unit/test_friend_requests.py test/unit/test_client_lifecycle.py test/smoke/test_core_routes_smoke.py test/integration/test_commands_dispatch.py --basetemp=runtime/temp/pytest_friend_fix
node test/unit/test_napcat_friend.mjs
```

自动测试覆盖 `reqType=9` 已处理/已读记录、已是好友、目标隔离、拒绝记录、
原生操作未生效、关闭自动通过、接口失败、后台补全不阻塞且不回复，以及
HTTP 超时不重复提交。
运行环境可验证插件加载及已是好友的返回；新的单向好友实际回加和文件发送
仍需新申请现场验收，QQ 风控或对方验证设置可能使关系不能立即建立。
