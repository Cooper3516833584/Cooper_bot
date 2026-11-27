# 私聊好友申请补全

2026-10-14 13:12 的好友提示进入 AI 聊天，随后 `/get` 多次返回
`rich media transfer failed`；人工添加好友后，13:42 发送成功。
NapCat 4.10.41 的原生申请记录显示 `reqType=9`（允许任何人添加）、
`isDecide=true`、`isUnread=false`。这种记录被 NapCat 的常规
`request.friend` 上报过滤，因此原有自动同意代码没有执行。

QQ 设置为“允许任何人”时曾只上报 `notice.friend_add`。设置为“需要验证信息”后，
新申请会进入带 `flag` 的标准审批流程。
[NapCat 社区记录](https://github.com/NapNeko/NapCatQQ/issues/1601)

2026-11-27 19:56 的原生记录同样标为 `reqType=9`、已处理且已读，当时对方仍未出现在
机器人的好友列表。QQ Linux 3.2.21 上确认了 `reqToAddFriends` 接口及目标验证设置的回调。

`client.py` 将 `request.friend`、`notice.friend_add` 和精确私聊提示“请求添加你为好友”
交给后台补全任务，带 `flag` 时核对申请时间。插件同时监听 NapCat 常规上报过滤掉的
新入站申请，通过 `notice.cooper_friend_request` 通知 Python。多个事件只触发同一用户的
一次处理中任务，私聊命令和 AI 分发照常。功能沿用 `AUTO_APPROVE_FRIEND_REQUEST` 和
`AUTO_APPROVE_FRIEND_REMARK`。插件接口缺失时，带 `flag` 的标准好友申请仍可自动审批。

NapCat 标准接口不能按 QQ 号查找申请。`deploy/napcat/cooper_friend.mjs`
提供 `cooper_complete_friend_request`，通过原生好友服务查找该用户最新的
真实申请，处理待确认、已同意未添加、允许任何人添加和添加失败的记录。
已在好友列表中时直接返回。先通过 `approvalFriendRequest` 审批，等待最多 3 秒确认。
常规审批后仍未建立关系时，插件核对 10 分钟以内的真实入站申请，读取对方的好友验证设置，
使用 `reqToAddFriends` 回加一次。允许直接添加或发送普通验证消息时可自动回加，安全验证题
等设置由 QQ 客户端处理。发出的申请、待对方确认的回加、已拒绝或忽略的申请均跳过。
QQ 收到回加申请只代表申请已提交，好友状态仍以好友列表确认。HTTP 超时保留为未确认状态。

Docker Compose 已包含插件文件的只读挂载。首次部署需运行
`docker compose up -d napcat` 并重启 Python bot；NapCat 会从插件目录加载它。
现有容器也可复制该文件到 `/app/napcat/plugins/cooper_friend.mjs`，
通过已鉴权的 WebUI 插件管理重载，再重启 Python bot。
升级 NapCat 或 QQ 后需核对插件初始化参数和原生好友服务。

验证命令：

```powershell
python -m pytest test/unit/test_friend_requests.py test/unit/test_client_lifecycle.py test/smoke/test_core_routes_smoke.py test/integration/test_commands_dispatch.py --basetemp=runtime/temp/pytest_friend_fix
node test/unit/test_napcat_friend.mjs
```

自动测试覆盖已读的入站申请、好友目标校验、拒绝和历史记录、主动回加去重、验证方式、
插件回退、关闭自动通过和私聊分发。实际回加和文件发送使用新申请验收，QQ 验证要求
可能让申请保持待对方确认。
