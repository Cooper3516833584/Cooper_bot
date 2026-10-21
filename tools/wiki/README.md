# 1037wiki 手动同步工具

独立于 bot 运行时的资料库同步接口，供 AI agent 或人工手动调用。默认只做 **dry-run**，加 `--execute` 才会上传/下载。

## 认证

token 文件：

- `config/private/url_and_token.txt`：第一行站点地址，第二行 access token（`eyJ...`）
- `config/private/1037wiki_refresh_token.txt`：refresh token；工具在 401 时会自动调用 `POST /auth/refresh` 换新 token 并回写这两个文件

如果 refresh token 也过期：

1. 在 Edge 打开并登录 `https://1037.wiki/`
2. 从 Edge Local Storage 读取 `_https://1037.wiki` 下的 `auth-token-1037wiki` 和 `rf-token-1037wiki`
3. 分别写回上述两个文件

本仓库不记录、不打印 token。`config/private/` 已被 `.gitignore` 忽略。

## 常用命令

```bash
# 验证 token
python tools/wiki/1037wiki_sync.py token-check --json

# 全量远端清单 + 本地差集（只读；--refresh 强制重抓）
python tools/wiki/1037wiki_sync.py inventory --refresh --json

# 上传预演：默认只看索引内的文档
python tools/wiki/1037wiki_sync.py upload --scope indexed --json

# 真正上传前 5 个页面
python tools/wiki/1037wiki_sync.py upload --scope indexed --execute --limit 5

# 下载预演
python tools/wiki/1037wiki_sync.py download --json

# 真正下载：最多 50 个文件、最多 2GB
python tools/wiki/1037wiki_sync.py download --execute --max-files 50 --max-mb 2048

# 上传 + 下载一体化预演
python tools/wiki/1037wiki_sync.py sync --json

# 同步后重建 bot 的 /find 索引
python tools/wiki/1037wiki_sync.py refresh-index --json
```

## 去重规则

- **文件级**：远端文件 `etag`（MD5）与本地文件 MD5 对比；相同则跳过上传/下载。
- **页面级**：上传前检查远端页面标题；同名页面默认跳过，可用 `--allow-existing-title` 覆盖。
- **部分重复**：一个页面里只有部分文件已在远端时，默认跳过整页；`--allow-partial` 只补缺失文件。
- **自己的页面**：下载默认跳过当前账号上传的页面（`--include-own` 可包含）。
- **大小限制**：单文件 200MB，超过则跳过并报告。
- **版权风险**：标题/描述/标签/文件名/摘要含教材、课本、Z-Library、出版社等标记的页面默认跳过；`--include-copyright-risk` 可显式覆盖。请自行确认上传授权。

## 本地范围

`--scope` 控制参与上传的本地文件：

- `indexed`（默认）：`semantic_store.sqlite3` 已索引的文档，最安全。
- `documents`：资料目录下所有文档/压缩包（PDF、Office、TXT、MD、ZIP、RAR、7Z）。
- `all`：所有非隐藏文件（谨慎使用，可能包含图片/工程资源）。

## 下载落点

- 课程页面：`<学科>/<页面标题>/<文件名>`
- 生活类页面：`生活资料/<分类>/<页面标题>/<文件名>`
- 其他页面：`1037wiki_其他/<父分类>/<分类>/<页面标题>/<文件名>`

已存在的同名文件不会覆盖；内容相同则跳过，内容不同则加 ` (1037wiki)` 后缀。

## 状态与缓存

全部位于 `runtime/state/wiki/`：

- `1037wiki_remote_inventory.json`：远端页面/文件清单缓存
- `1037wiki_local_md5_cache.json`：本地 MD5 缓存（按路径+大小+mtime）
- `1037wiki_sync_state.json`：上传/下载记录
- `1037wiki_sync_plan.json`：最近一次 dry-run/执行计划
- `1037wiki_sync_report.json`：最近一次 inventory 报告

旧的 `1037wiki_upload_plan.json`、`1037wiki_upload_state.json` 仍保留，新工具不会覆盖它们。

## 设计边界

- 不接入 bot 运行时，不修改 `client.py` / `aisvc.py`。
- 默认 dry-run，任何写操作都需要显式 `--execute`。
- 上传走平台 `POST /file/upload/url`（服务端按 MD5 复用）→ S3 PUT → `POST /file/upload/finish`。
- 下载走 `GET /file/{id}/download` 获取签名地址，带 `Referer: https://1037.wiki/`，下载后校验大小和 MD5。
- 请求内置浏览器请求头、gzip 解压、限速和指数退避，避免触发平台反爬。
- 平台对 API 有严格的配额（响应头 `x-ratelimit-limit: 5`，实测约 2 次突发后按 `Retry-After` 等 20–30 秒）。全量下载 800+ 文件可能需要数小时，建议放后台运行；中断后直接重跑同一条命令即可按 MD5 续传。
- WAF 挑战由 `waf_cookie.js` 在 Node 的最小 vm 沙箱中求解；没有 Node 时会退化为重试并最终报错。Node 只用于计算 Cookie，不接触仓库文件或网络。

