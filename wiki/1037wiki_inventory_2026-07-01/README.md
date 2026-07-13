# 1037wiki 资料清单快照（2026-07-01）

本目录汇总两类记录：

1. **本次从 1037wiki 批量下载的资料记录**：见 `download_files.csv` 和 `download_pages.csv`。
2. **本机已经上传到 1037wiki 的资料记录**：见 `uploaded_files.csv`、`uploaded_pages.csv` 和 `upload_plan_pages.csv`。

## 本批下载口径

- 批次 ID：`1037wiki_download_2026-07-01_assumed_archived`
- 处理口径：用户确认文件已经归档，本快照按之前的 `manifest.json` / `download_state.json` 记录把本批资料整体视作已下载完成。
- 不再按当前磁盘位置判断文件是否存在。
- `download_files.csv` 中的 `original_download_state`、`original_error`、`recorded_path` 仅用于追溯原下载器记录，不影响本批次的 `batch_status`。
- `download_pending_retry.csv` 保留稳定表头，但当前为空，因为本批次按已完成处理。

## 下载统计

- 排除上传者：`cooper_bot`
- 已排除页面：100 页，约 2.946 GiB
- 本批下载页面：490 页
- 本批文件条目：4029 条
- 本批唯一文件 ID：3872 个
- 本批按记录视作已完成：3872 个唯一文件
- 原始下载状态统计：{'done': 3744, 'failed': 126, 'done_and_failed': 2}
- 原始 `download_state`：done=3746，failed=128
- 本批记录体量：约 24.412 GiB

## 已上传到 1037wiki

- 上传计划页面：138 页
- 已上传唯一标题：100 个
- 已上传文件记录：828 条
- 上传错误：0 条
- 上传计划状态：{'skipped_by_default_policy': 37, 'uploaded': 101}

## 文件说明

- `registry_summary.json`：机器可读总览，记录批次 ID、统计数字和源文件路径。
- `download_files.csv`：本批 1037wiki 下载文件清单，每行一个唯一 `file_id`。
- `download_pages.csv`：本批 1037wiki 下载页面清单，每行一个页面。
- `download_pending_retry.csv`：后续重试清单；按当前口径为空，仅保留表头。
- `uploaded_files.csv`：本机已上传文件清单，按本地 `source_rel` 和远端 `upload_file_id` 区分。
- `uploaded_pages.csv`：本机已上传页面清单，按页面标题和远端页面 ID 区分。
- `upload_plan_pages.csv`：上传计划清单，包含 `uploaded`、`skipped_by_default_policy`、`pending_non_risk` 等状态。
- `download_upload_title_overlap.csv`：下载页面标题与上传标题的重名/近似同名记录。

## 以后如何区分新增资料

- 继续下载 1037wiki 新资料时，建议新建 `wiki/1037wiki_inventory_YYYY-MM-DD` 快照目录，使用新的 `batch_id`。
- 判断 1037wiki 是否已有下载记录，优先对比 `download_files.csv` 的 `file_id`。
- 判断本机资料是否已经上传，优先对比 `uploaded_files.csv` 的 `source_rel`，其次看 `uploaded_pages.csv` 的 `title` 和 `upload_page_id`。
- 本批次已经排除 `Cooper_bot` 上传者；以后继续下载时也建议保持同一排除规则，避免下载自己发布的资料。
