# config.py
from __future__ import annotations

import os
import re
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

# ========== 读取敏感配置（secrets.env） ==========
# 目标：把 TOKEN / ACCOUNT / ADMIN_USERS 等敏感信息从代码里剥离出去
# - docker-compose.yml 通过 env_file: ./secrets.env 传给 NapCat 容器
# - Python 侧这里也会读同一个 secrets.env（若环境变量已存在则不覆盖）
SECRETS_ENV_PATH = BASE_DIR / "secrets.env"

def _load_env_file(path: Path):
    try:
        txt = path.read_text(encoding="utf-8")
    except Exception:
        return
    for raw in txt.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        if not k:
            continue
        # 环境里已设置则不覆盖（方便你用系统环境变量/CI）
        if k not in os.environ:
            os.environ[k] = v

_load_env_file(SECRETS_ENV_PATH)

def _get_env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    if v is None:
        return default
    v = str(v).strip()
    return v if v else default

def _get_env_path(name: str, default: Path) -> Path:
    raw = _get_env(name, "")
    if not raw:
        return default
    p = Path(raw).expanduser()
    if p.is_absolute():
        return p
    return (BASE_DIR / p).resolve()

def _parse_int_set(s: str) -> set[int]:
    out: set[int] = set()
    for part in re.split(r"[\s,，]+", (s or "").strip()):
        if not part:
            continue
        try:
            out.add(int(part))
        except Exception:
            pass
    return out


# 资料库根目录（宿主机）
DATA_DIR = BASE_DIR / "data"
LOG_DIR = BASE_DIR / "logs"

# NapCat / OneBot v11
TOKEN = _get_env("TOKEN", "CHANGE_ME_TOKEN")
WS_URI = f"ws://127.0.0.1:3001/?access_token={TOKEN}"

# 你已经配好的 HTTP Server（可选；目前阶段不依赖它）
HTTP_PORT = 3010
HTTP_BASE = f"http://127.0.0.1:{HTTP_PORT}"

# 如果 NapCat 的 HTTP Server 也配置了 access_token，这里会自动带上（同时放在 query + Authorization）
HTTP_TOKEN = _get_env("HTTP_TOKEN", TOKEN)

# 会话多久没说话就切一份日志
IDLE_SPLIT_SECONDS = 1800

# 权限等级：0游客 1临时 2好友 3管理员
# 管理员 QQ 号（/whoami 里显示的 user_id）
ADMIN_USERS = _parse_int_set(_get_env("ADMIN_USERS", "")) or {3516833584}

# 群权限：可选的群级别“下限”（通常不需要；你也可以留空）
GROUP_LEVEL = {
    # 1076684416: 2,
}

# 未见过的 QQ 默认 0（游客，不能访问资料库）
DEFAULT_LEVEL = 0

# ===== 资料库根（宿主机目录）=====
# (root_name, path, min_level)
# 需求：1级(临时)可访问 public + group；2级(好友)再加 friend；管理员全部。
DOC_ROOTS = [
    ("public", DATA_DIR / "public", 1),
    ("friend", DATA_DIR / "friend", 2),
    ("admin",  DATA_DIR / "admin",  3),
]

# 群/个人专属目录
GROUP_DOCS_DIR = DATA_DIR / "groups"   # data/groups/<group_id>/...
USER_DOCS_DIR  = DATA_DIR / "users"    # data/users/<user_id>/...

# 权限库（群里发过言的人会写进这里 -> level>=1）
PERM_DB_PATH = USER_DOCS_DIR / "_perm_levels.json"


# ===== Handin（作业提交）=====
# 任务数据库
HANDIN_DB_PATH = DATA_DIR / "_handin_tasks.json"
# 私聊提交临时收件箱
HANDIN_INBOX_DIR = USER_DOCS_DIR / "_handin_inbox"
# 旧版群内任务目录名（仅用于兼容迁移）：data/groups/<group_id>/<HANDIN_TASKS_DIRNAME>/<task>/files/
HANDIN_TASKS_DIRNAME = "handin"

# 新版提交文件根目录（不再放在 data/groups 下，避免群成员通过 /find 看到他人提交）
# data/handin/<group_id>/<task>/files/
HANDIN_ROOT_DIR = DATA_DIR / "handin"
# 班级名册（放在 data/friend/ 下）
ROSTER_XLSX_PATH = DATA_DIR / "friend" / "班级名册.xlsx"
# 时区（用于解析提醒/截止时间）
TIMEZONE = "Asia/Shanghai"

# NapCat 本地缓存 temp 映射（用于私聊文件提交：不走网络下载，直接拷贝缓存文件）
NAPCAT_TEMP_CONTAINER_DIR = "/app/.config/QQ/NapCat/temp"
NAPCAT_TEMP_HOST_DIR = _get_env_path(
    "NAPCAT_TEMP_HOST_DIR",
    BASE_DIR / "napcat_qq" / "NapCat" / "temp",
)


# ===== NapCat 容器内的资料库挂载点 =====
# docker-compose 里把 ./data 挂载到这个路径后，upload_* action 才能读到文件
DATA_DIR_CONTAINER = "/bot_data"

# ===== NapCat 专用上传目录（更稳定的发送文件方式）=====
# 宿主机目录（与 docker-compose 的 ./upload_* 挂载对应）
UPLOAD_GROUP_HOST_DIR = BASE_DIR / "upload_group_file"
UPLOAD_PRIVATE_HOST_DIR = BASE_DIR / "upload_private_file"
# 容器内目录（docker-compose 挂载到 /data/upload_*）
UPLOAD_GROUP_CONTAINER_DIR = "/data/upload_group_file"
UPLOAD_PRIVATE_CONTAINER_DIR = "/data/upload_private_file"

# 是否在发送时把文件名转为 ASCII（可规避部分 NapCat/QQNT 对中文文件名的兼容问题）
# 建议默认 False：优先保留原文件名；若发送失败会在代码里自动回退到 ASCII 名重试。
SEND_FILENAME_ASCII_SAFE = False

# 发送文件遇到 "rich media transfer failed" 时，自动重试的等待时间（秒）
# Docker Desktop (Windows) 的 bind mount 有时存在同步延迟，大文件更容易触发。
SEND_RETRY_DELAYS = [0.8, 1.8]

# 若原文件发送失败（尤其是 doc/docx/pdf 等），可自动打包为 zip 再发一次作为兜底。
AUTO_ZIP_FALLBACK = True

# 大文件提示阈值（MB）：发送/接收超过该大小的文件时提示“请耐心等待”
LARGE_FILE_WARN_MB = 50

# 展示/搜索限制
LS_LIMIT = 50
FIND_LIMIT = 50
FIND_MAX_SCAN = 100000   # 最多扫描多少个文件/目录项，避免卡死

# ===== 新增：/get 多文件默认打包 =====
# /get 选择文件数 > 该阈值时，默认打包成一个 zip 发送
GET_ZIP_THRESHOLD = 4

# ===== 新增：Handin 归档保留策略 =====
# 在任务创建者最后一次 /handinget 后，保留 N 天再清理归档（/handinstatus /handincheck 仅展示仍可 /handinget 的任务）
HANDIN_KEEP_DAYS_AFTER_LAST_GET = 30

# 手动/定时清理 inbox：收件箱内临时文件保留 N 天（避免长期运行堆积）
HANDIN_INBOX_KEEP_DAYS = 30
