# 飞书只读 MCP 部署与回滚

此功能默认关闭。启用后，能使用 public Kimi 聊天的 QQ 用户都可能请求应用获准访问的资料；启用前应确认对应群聊和私聊适合看到这些内容。应用访问范围还需在飞书后台配置，目标文档由操作者另行授权。

## 固定权限范围

只使用官方 `@larksuiteoapi/lark-mcp@0.5.1`、`tenant_access_token` 和以下四个只读 API：

- `bitable.v1.appTable.list`
- `bitable.v1.appTableField.list`
- `bitable.v1.appTableRecord.search`
- `docx.v1.document.rawContent`

本功能不配置任何文档、知识库或多维表格 ID，也不做写入、同步或全文索引。MCP 进程运行在 Python bot 启动的 public Kimi 子进程中；密钥不会注入全局环境、admin profile 或 NapCat。

## 宿主机配置

默认模式保持 `AI_FEISHU_ENABLED=false`，public 工具仅为 `WebSearch`，不需要安装飞书 MCP 或创建飞书凭证。默认部署请使用 `public_home_security.example.toml` 与 `public.md.example`。

仅在确实需要开放飞书只读查询时，才使用下方独立启用步骤：将配置和提示词样例替换为文件名含 `_feishu` 的版本，并设置 `AI_FEISHU_ENABLED=true`。启用前应完成以下本地 MCP 与凭证配置。

1. 安装 Node.js 20+，并在运行 bot 的宿主机固定安装官方 MCP 包。Windows 示例：

   ```powershell
   node --version
   npm --version
   npm install --prefix "C:\CooperBotRuntime\feishu-mcp" --save-exact "@larksuiteoapi/lark-mcp@0.5.1"
   ```

2. 仅在本机创建 `config/private/feishu/credentials.json`，内容格式如下；替换占位值，并限制文件 ACL 仅允许运行 bot 的账号读取。不要将真实值放进 `secrets.env`、环境变量、命令行、仓库或日志。

   ```json
   {
     "app_id": "cli_REPLACE_WITH_YOUR_APP_ID",
     "app_secret": "REPLACE_WITH_YOUR_APP_SECRET"
   }
   ```

   POSIX 系统设置权限为 `0600`。Windows 上可用 `icacls` 为该文件关闭继承并仅授予 bot 服务账号读取权限；在修改 ACL 前确认服务实际使用的账号。

3. 在实际 `AI_KIMI_PUBLIC_HOME` 下创建 `mcp.json`。下例中的两条路径需换成该机器上实际的绝对路径。配置不应含 `env`、凭证、preset 或额外工具：

   ```json
   {
     "mcpServers": {
       "feishu": {
         "command": "C:\\Program Files\\nodejs\\node.exe",
         "args": [
           "C:\\CooperBotRuntime\\feishu-mcp\\node_modules\\@larksuiteoapi\\lark-mcp\\dist\\cli.js",
           "mcp", "--token-mode", "tenant_access_token", "--tool-name-case", "snake",
           "--language", "zh", "--tools",
           "bitable.v1.appTable.list,bitable.v1.appTableField.list,bitable.v1.appTableRecord.search,docx.v1.document.rawContent"
         ],
         "enabledTools": [
           "bitable_v1_appTable_list",
           "bitable_v1_appTableField_list",
           "bitable_v1_appTableRecord_search",
           "docx_v1_document_rawContent"
         ]
       }
     }
   }
   ```

4. 合并 `config/examples/ai/kimi/public_home_security_feishu.example.toml` 的安全设置和 `config/examples/ai/prompts/kimi/public_feishu.md.example` 的工具列表到实际 public profile。保留现有 provider、model、认证设置。admin profile 不加飞书权限。
5. 将 `AI_FEISHU_ENABLED=true` 写入 `config/private/settings.env`，重启 bot，并运行现有 Kimi 能力诊断。实现会校验精确工具列表、本地凭证格式和 MCP 参数；配置错误会使 public profile 未就绪。运行期观测到不在白名单的工具会拒绝结果并锁定 public profile，直到可信诊断重新验证。

当前没有目标文档时，只能验证本地配置和工具边界；真实文档读取、无授权时拒绝访问及撤权后失败等现场验收待操作者授权文档后进行。MCP API 错误不会被当作成功读取，QQ 侧不应输出内部凭证或原始错误对象。

## 停用与回滚

停用时先停止 bot，再从 public home 移除 `feishu` MCP server，并将 public `config.toml` 和 agent 工具列表恢复为仅 `WebSearch`（可复制默认 `public_home_security.example.toml` 与 `public.md.example`，保留 provider/model/auth）；随后设置 `AI_FEISHU_ENABLED=false`，重启并运行 Kimi 能力诊断。只关闭开关但保留 MCP 或额外工具会触发静态安全拒绝。凭证可在本地安全删除；若凭证曾泄露，应先在飞书后台轮换 Secret，再更新本地文件。

## Git 泄密检查

从仓库根目录执行并人工检查所有暂存文件：

```powershell
git check-ignore -v config/private/feishu/credentials.json
git check-ignore -v runtime/kimi/public/mcp.json
git ls-files config/private runtime storage
git status --short
git diff --cached --name-only
git diff --cached --check
```

第二条路径若位于仓库外，不适用，应确认它不在任何 Git worktree 内。显式暂存所需文件，不要盲目添加整个工作树。
