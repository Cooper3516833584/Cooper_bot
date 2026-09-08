# 阶段 01：Kimi CLI 合同

## 已验证的本机事实

| 项目 | 结果 |
| --- | --- |
| 可执行文件 | `C:\Users\Cooper\.kimi-code\bin\kimi.exe`（原生 `.exe`，非 `.cmd` 包装器） |
| 版本 | `0.34.0` |
| SHA-256 | `36BD5659FB5D310EDC06AC6196DAC6E7F7528BFDF04D41F3BED9B29CAAF4D206` |
| 配置诊断 | `kimi doctor` 通过 `config.toml` 与 `tui.toml` 校验。 |
| provider | 脱敏 `kimi provider list` 显示一个 `deepseek` OpenAI-compatible provider（4 个模型）。 |
| 默认 alias | `deepseek/deepseek-v4-flash`。 |
| thinking | 全局 thinking 已启用；provider capability 声明含 thinking/tool_use，支持的 effort 列表包含 `max`。 |
| 环境覆盖 | `KIMI_CODE_HOME`、`KIMI_CODE_LEGACY_FLAG`、`KIMI_SHELL_PATH` 均未设置。 |

`support_efforts` 包含 `max` 只证明 provider 能力，不证明当前每一次调用实际采用 max；脱敏配置中未找到当前 effort 的明确值。因此“DeepSeek V4 Flash / max”仍需以隔离 profile 的真实请求验证。

## 观察到的命令行接口

`kimi --help` 显示：

- `-p` / `--prompt` 非交互执行；
- `--output-format text|stream-json`；
- `-m` / `--model`；
- `--agent-file`、`--skills-dir`、`--add-dir`；
- `--yolo`、`--auto`、`--plan`；
- `provider`、`doctor`、`acp`、`web` 等子命令。

阶段 04 的候选 argv 是 `kimi --agent-file <fixed-profile> --skills-dir <empty-dir> --output-format stream-json --prompt <prompt>`。不得使用 `shell=True`，不得使用 `--session` 或 `--continue`，且不把 `--prompt` 与 `--auto` / `--yolo` 混用，直到实测证明语义。

## BLOCKED_EXTERNAL：尚未验证的运行时合同

未创建含必要认证但不继承业务密钥的独立 public Kimi home，也未创建固定 public Agent。因此本阶段没有执行真实模型 prompt，以下均为 `BLOCKED_EXTERNAL`：

- 实际 engine、默认 alias 与当前 `max` effort 的端到端生效结果；
- `stream-json` 的完整事件顺序、终态、最终正文抽取、空正文与非零退出协议；
- 真实 WebSearch 是否由当前 host/provider 提供；
- public 对 Bash/Read/Write/Agent/MCP 的运行时拒绝，及 hooks/plugins/skills 不自动加载；
- 超时、取消、后台任务及 Windows 子进程树回收；
- 中文、引号、换行、反斜杠、emoji、超长输入的真实 JSONL fixture。

没有用默认个人 home 运行探针，因为那会继承个人配置，不能作为 public 安全边界证据；也没有把机器人 DeepSeek 凭据复制给 Kimi。后续必须先在阶段 03 建立隔离 profile，再用无敏感 prompt 采集脱敏 fixture。
