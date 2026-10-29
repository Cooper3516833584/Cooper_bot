# AI 提示词管理

运行提示词统一位于 `config/private/ai/prompts/`。安全示例镜像保存在 `config/examples/ai/prompts/`。
首次部署需显式复制所需文件并去掉末尾 `.example`；运行时不会自动读取 examples、旧目录或迁移备份。
目录移动不改变已有用户号、群号、JSON 字段、角色覆盖顺序、模型或调用参数。

## 聊天与通知

| 文件（相对于 prompts/） | 用途和影响范围 | 生效方式 |
| --- | --- | --- |
| `chat/persona.txt` | 全局默认角色，也用于日历补充文案的 DeepSeek 任务链 | 重启 |
| `chat/group_overrides.json` | 按群覆盖 Kimi 普通聊天角色，保留原有 default/groups 结构 | 自动重读 |
| `chat/private_overrides.json` | 按用户覆盖 Kimi 私聊角色，保留原有 default/users 结构 | 自动重读 |
| `kimi/public.md` | Kimi 公共 profile 的 agent，参与普通聊天和日历联网查询 | 重启 |
| `kimi/admin.md` | Kimi 管理员 profile 的 agent；不能通过普通角色配置取得管理员权限 | 重启 |
| `notice/prompts.json` | 按 default/group、url/file 等种类覆盖分类和省流阶段 | 自动重读 |
| `notice/defaults.json` | 缺少专用通知模板时使用的通用分类与省流模板 | 重启 |

通知变量为 `{{source}}`、`{{content}}`、`{{silent_token}}`。覆盖模板可只使用其中部分，不能使用其他变量。
现有聊天 JSON 是完整角色正文，不作运行变量替换。通知 JSON 的既有字段和优先级保持不变。
现有三个 JSON 的损坏回退行为保持原有规则；不会将旧缓存中的私有内容恢复到文件。

## 业务模板目录

下表 ID 对应 `prompt_store.py` 中的固定映射。文件均在启动期首次建立的进程快照中读取，修改后需要重启。
表中列出的变量必须保留，其他 `{{变量}}` 会使模板校验失败。JSON 示例本身的花括号不用转义。

| ID / 文件 | 用途 | 变量 |
| --- | --- | --- |
| `web.judge` / `web/judge.txt` | DeepSeek 任务的联网需求判定 | 无；必须保留 `[WEB_SEARCH]` 协议标记 |
| `web.compose` / `web/compose.txt` | 搜索结果整合的系统说明 | 无 |
| `web.compose_input` / `web/compose_input.txt` | 整合阶段的用户问题与搜索素材 | question、material |
| `email.classify` / `email/classify.txt` | 邮件分类和摘要，邮件字段仍以独立 JSON 数据发送 | 无 |
| `materials.classify_tbd` / `materials/classify_tbd.txt` | TBD 文件选择已有学科目录 | subjects、relative_path、filename、summary、keywords、snippet |
| `materials.review_subject` / `materials/review_subject.txt` | 检查明显错放的资料 | 上述变量及 current_subject |
| `materials.summary_title` / `materials/summary_title.txt` | 无正文资料摘要 | subject、filename、file_type |
| `materials.summary_content` / `materials/summary_content.txt` | 含正文片段的资料摘要 | subject、filename、file_type、snippet |
| `calendar.events` / `calendar/events.txt` | 重要日联网核验和选题 | date、snapshot |
| `calendar.holidays` / `calendar/holidays.txt` | 年度法定节假日及调休安排核验 | year |
| `calendar.render_note` / `calendar/render_note.txt` | 在确定性日历文案后增加一句补充 | date、snapshot、events |
| `memory.extract` / `memory/extract.txt` | 从自身发言提取稳定事实 | 无；事件作为独立 JSON 数据发送 |
| `memory.summary` / `memory/summary.txt` | 记忆摘要 | 无；旧摘要与事件作为独立 JSON 数据发送 |
| `vision.describe` / `vision/describe.txt` | 多模态图片描述 | 无；图片仍作为多模态数据发送 |
| `moderation.review` / `moderation/review.txt` | 群内广告和欺骗性引流审核 | 无；待审核消息及图片描述作为独立 JSON 数据发送 |

`notice.defaults` 的变量与通知覆盖一致；它是默认 JSON 配置，不是一次整文件变量替换。
模板替换只执行一次，用户内容中出现 `{{...}}` 不会再次展开。不支持 Python 表达式、文件 include 或执行代码。

## 安全与兼容边界

提示词中的输出 JSON 字段和固定标记必须与程序协议匹配。读取器检查必要字段说明；最终输出仍由原有代码验证。
审核证据、记忆引用与作用域、文件移动目录和置信度、日历官方来源、视觉文件访问范围均不由模板决定。

以下属于强制约束，继续留在代码，不提供关闭开关：

- `aisvc.py` 的自动业务能力边界、视觉来源说明和 Kimi 会话身份规则。
- Kimi 请求 JSON 的角色、历史、引用及可信管理员上下文构造。
- `memory/service.py` 的显式记忆优先规则、`memory/prompting.py` 的 data-only 上下文和预算剪裁。
- `moderation/review.py`、`memory/llm.py` 等模块的输出和证据校验。
- Kimi 权限/能力探针和 provider 诊断探针。它们不是日常业务提示词，修改业务模板不会改变要测试的权限边界。

模板缺失、空白、非 UTF-8、超长、变量错误或必要协议说明缺失时，不会向模型发送空模板。
对应任务按安全路径处理：审核返回 uncertain，记忆任务不写入结果并沿用重试，邮件沿用异常通知，
资料摘要使用原有兜底，资料分类不执行未经确认的移动，日历保留本地事实及基础文案。
通知无可用模板时不发起模型请求，沿用静默结果；联网合成模板不可用时沿用普通任务回退。

修改资料摘要模板不会自动重建已有资料索引，修改记忆模板不会重写已有事实或摘要。
视觉描述缓存键包含实际提示词内容，避免不同提示词的描述互相命中。

以下命令只读检查 16 项业务模板，不调用模型、不运行权限探针，也不显示模板正文：

```powershell
python -B -m cooper_bot.modules.ai.prompt_store
```

私有配置迁移备份只用于人工恢复，不是第二个运行配置源。新目录已准备好后再切换代码，验证后保留备份并移除旧位置重复文件。
