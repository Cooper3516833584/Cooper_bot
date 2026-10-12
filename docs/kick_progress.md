# Kick 实施进度

## 00｜需求与基线

- 基线 HEAD：`d1f9c1fb63cfdd4ba50893f97e126957dbbf4156`，分支 `main`。
- 工作区已有用户修改及未跟踪文件（命令、AI、日历、记忆、OCR等）；实施中保留，不覆盖。
- 已核实个人 Bot 权限来自 `ADMIN_USERS`/`PermService`，`ctx.level` 可能受 `GROUP_LEVEL` 提级，不能用于此功能授权。
- 目标新增 moderation 存储、规则、复核、服务模块及精确命令入口；仅局部接入 OneBot、commands、client 和配置。
- 尚未执行生产网络、真人踢人或线上模型调用。

## 检查点｜原有工作区修改

- 用户随后要求先把原有未提交修改推到 GitHub；已将原先跟踪文件修改提交并推送到 `codex/checkpoint-uncommitted-changes`，提交 `ff5bcb9`（`chore: checkpoint pending local changes`）。
- 私有运行数据、OCR临时材料及含真实账号示例的诊断脚本没有纳入该检查点。
- 本次反广告实现是在该分支继续完成；新功能尚未推送。

## 01｜短期存储与群作用域

- 新增 SQLite `groups`、`messages`、`cases` 表，开启 WAL、`busy_timeout` 和 `synchronous=FULL`。
- 消息按 Bot/群/message_id 去重，保留负消息 ID；只在群开关和 revision 匹配时提交。
- 实现群开关恢复、运行中案卷中断、采集缺口标记、45分钟消息清理及案卷保留策略。
- 验证开关关闭不落消息、重复事件去重、短期案卷证据保留和存储故障时的运行态关闭。

## 02｜筛选票与成员核验

- 实现固定六票、20秒闭区间；B票要求至少两条不同消息且去空白文本合计100字符。
- 只有本地票至少2项才进入队列；群成员查询严格校验角色、群号、账号和 `data.level`，角色必须为member才可能进入模型复核。
- 不使用 `ctx.level`、个人权限或QQ等级替代群等级。

## 03｜OneBot与命令

- 增加成员查询、踢出、撤回和私聊合并转发封装；写请求优先HTTP且不在超时后经WS补发。
- WS echo 改为UUID，避免同一毫秒同action请求串回包。
- 新增 `/kick on`、`/kick off`、`/kick status`，个人权限2/3级校验，最多一个可信私聊证据接收人配置 `KICK_AUDIT_QQ`。
- `dispatch` 最前方识别命令，客户端另设不经过普通会话锁的短路径；off先同步撤销本地generation。

## 04｜采集与异步生命周期

- 新增固定64项候选队列和2个worker；消息存储在工作线程运行，不在WS接收循环调用审核API。
- 每个已开启群用短锁保证事件落盘/投票顺序；控制命令不等待该锁。
- 断线关闭服务、取消并等待worker、标记未完成案卷，不重放旧副作用。存储故障导致off未保存时，在当前进程重连后仍保持关闭。

## 05｜全量图片与模型复核

- 复核只使用目标账号当前群的本地冻结快照；逐个创建真实媒体段slot，按4张一批调用现有VisionSkill，不受普通聊天的单消息图片数量上限影响。
- 任一图片结果不完整、输入超过64000字符、模型结果JSON/证据不匹配或超时，均转为uncertain并静默结束。
- 文本复核走现有DeepSeek gateway；不走Kimi聊天、工具或记忆链路，不访问广告链接或解码二维码。

## 06｜证据与处置

- 确认block后，先持久化当前30分钟目标消息快照，再将全部内容按20节点及256KiB请求上限分批转发。
- 超长文本按原message_id拆分连续part节点；无法无损容纳的非文本段停止处置。
- 顺序固定为证据转发、群提示、再次核验Bot/目标角色、移群、仅撤回已校验证据消息、私聊结果。
- 各写动作先记attempted并最多发送一次；unknown不会盲重试或伪报成功。

## 07｜测试与验收

- 新增 `test/unit/test_moderation.py`，覆盖六票/窗口边界、负ID与去重、并发落盘、off竞态、21张图和第21张失败、证据分批、WS echo及Fake处置成功/uncertain静默链。
- `pytest test/unit/test_moderation.py test/unit/test_client_lifecycle.py test/integration/test_commands_dispatch.py -q`：通过。
- `pytest -q`：全仓仅有一个失败 `test/unit/test_vision_skill.py::test_resolve_image_ready`，断言期望 `max_tokens=800`，当前未修改的视觉实现传入1600；其余全仓用例通过。
- 随包 `tools/verify_package.py`：文档/资产完整性、UTF-8、JSON、Python语法、41条合成投票案例和上游指纹均通过。

## 08｜文档与现场边界

- 新增 `docs/kick.md`，说明命令、筛选与静默边界、接收人、数据保留、模型数据流和申诉/关闭方式。
- 未连接真实QQ、NapCat、生产LLM/VLM；合并转发节点格式、私聊可达性、实际群管理员/撤回权限仍需测试群现场验证。
- 未推送本次功能实现；不声称识别准确率或无误踢保证。
