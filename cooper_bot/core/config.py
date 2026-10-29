# config.py
from __future__ import annotations

import os
import re
import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BASE_DIR = PROJECT_ROOT  # 兼容旧代码；迁移后仍表示仓库根目录

CONFIG_DIR = PROJECT_ROOT / "config"
STORAGE_DIR = PROJECT_ROOT / "storage"
RUNTIME_DIR = PROJECT_ROOT / "runtime"
TOOLS_DIR = PROJECT_ROOT / "tools"

DOCUMENTS_DIR = STORAGE_DIR / "documents"
SUBMISSIONS_DIR = STORAGE_DIR / "submissions"
DATABASES_DIR = RUNTIME_DIR / "databases"
STATE_DIR = RUNTIME_DIR / "state"
WORKSPACES_DIR = RUNTIME_DIR / "workspaces"
STAGING_DIR = RUNTIME_DIR / "staging"
TEMP_DIR = RUNTIME_DIR / "temp"
LOG_DIR = RUNTIME_DIR / "logs"
NAPCAT_DIR = RUNTIME_DIR / "napcat"

PRIVATE_CONFIG_DIR = CONFIG_DIR / "private"
AI_CONFIG_DIR = PRIVATE_CONFIG_DIR / "ai"
AI_PROMPTS_DIR = AI_CONFIG_DIR / "prompts"
REPLIES_DIR = PRIVATE_CONFIG_DIR / "replies"
AUTO_REPLY_IMAGES_DIR = STORAGE_DIR / "auto_replies" / "images"
WIKI_CONFIG_DIR = PRIVATE_CONFIG_DIR / "wiki"

# ========== 读取敏感配置（secrets.env） ==========
# 目标：把 TOKEN / ACCOUNT / ADMIN_USERS 等敏感信息从代码里剥离出去
# - docker-compose.yml 通过 env_file: ./config/private/secrets.env 传给 NapCat 容器
# - Python 侧这里也会读同一个 secrets.env（若环境变量已存在则不覆盖）
SECRETS_ENV_PATH = PRIVATE_CONFIG_DIR / "secrets.env"

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
SETTINGS_ENV_PATH = PRIVATE_CONFIG_DIR / "settings.env"
_load_env_file(SETTINGS_ENV_PATH)

def _get_env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    if v is None:
        return default
    v = str(v).strip()
    return v if v else default

def _get_env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    s = str(raw).strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    return bool(default)

def _get_env_path(name: str, default: Path) -> Path:
    raw = _get_env(name, "")
    if not raw:
        return default
    p = Path(raw).expanduser()
    if p.is_absolute():
        return p
    return (BASE_DIR / p).resolve()

def _get_env_float(name: str, default: float, minimum: float, maximum: float) -> float:
    try:
        value = float(_get_env(name, str(default)) or default)
    except (TypeError, ValueError):
        return float(default)
    return max(float(minimum), min(value, float(maximum)))

def _get_env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(_get_env(name, str(default)) or default)
    except (TypeError, ValueError):
        return int(default)
    return max(int(minimum), min(value, int(maximum)))

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
DATA_DIR = DOCUMENTS_DIR  # 兼容旧变量名，仅表示可访问文档根目录
FIND_INDEX_PATH = DATABASES_DIR / "file_search" / "find_index.json"
CLIENT_LOCK_PATH = STATE_DIR / "client" / "client.lock"

# NapCat / OneBot v11
TOKEN = _get_env("TOKEN", "CHANGE_ME_TOKEN")
WS_URI = _get_env("WS_URI", f"ws://127.0.0.1:13001/?access_token={TOKEN}")

# 你已经配好的 HTTP Server（可选；目前阶段不依赖它）
HTTP_PORT = _get_env_int("HTTP_PORT", 13010, 1, 65535)
HTTP_BASE = _get_env("HTTP_BASE", f"http://127.0.0.1:{HTTP_PORT}")

# 如果 NapCat 的 HTTP Server 也配置了 access_token，这里会自动带上（同时放在 query + Authorization）
HTTP_TOKEN = _get_env("HTTP_TOKEN", TOKEN)

# 自动通过好友申请（OneBot request_type=friend）
AUTO_APPROVE_FRIEND_REQUEST = _get_env_bool("AUTO_APPROVE_FRIEND_REQUEST", True)
# 通过后自动设置好友备注（可选；为空则不设置）
AUTO_APPROVE_FRIEND_REMARK = _get_env("AUTO_APPROVE_FRIEND_REMARK", "")

# 会话多久没说话就切一份日志
IDLE_SPLIT_SECONDS = _get_env_int("IDLE_SPLIT_SECONDS", 1800, 1, 86400)

# 权限等级：0游客 1临时 2好友 3管理员
# 管理员 QQ 号（/whoami 里显示的 user_id）
ADMIN_USERS = _parse_int_set(_get_env("ADMIN_USERS", ""))
# 可选的已确认案件证据接收人；未设置时使用现有管理员QQ号中的最小值。
_KICK_AUDIT_QQ_RAW = _get_env("KICK_AUDIT_QQ", "")
KICK_AUDIT_QQ = (
    0 if not _KICK_AUDIT_QQ_RAW else
    int(_KICK_AUDIT_QQ_RAW)
    if len(_KICK_AUDIT_QQ_RAW) <= 20
    and _KICK_AUDIT_QQ_RAW.isascii()
    and _KICK_AUDIT_QQ_RAW.isdigit()
    and int(_KICK_AUDIT_QQ_RAW) > 0
    else -1
)
ENABLE_OCR = _get_env_bool("ENABLE_OCR", False)

# 群权限：可选的群级别“下限”（通常不需要；你也可以留空）
PERMISSIONS_CONFIG_PATH = PRIVATE_CONFIG_DIR / "permissions.json"

def _load_group_levels(path: Path) -> dict[int, int]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8")).get("group_levels", {})
    except FileNotFoundError:
        return {}
    if not isinstance(raw, dict):
        raise ValueError("group_levels must be an object")
    levels = {}
    for key, level in raw.items():
        if not str(key).isascii() or not str(key).isdigit() or len(str(key)) > 20 or int(key) <= 0:
            raise ValueError("invalid group_levels key")
        if type(level) is not int or level not in (0, 1, 2, 3):
            raise ValueError("invalid group permission level")
        levels[int(key)] = level
    return levels

GROUP_LEVEL = _load_group_levels(PERMISSIONS_CONFIG_PATH)

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
GROUP_DOCS_DIR = DATA_DIR / "groups"   # storage/documents/groups/<group_id>/...
USER_DOCS_DIR  = DATA_DIR / "users"    # storage/documents/users/<user_id>/...

# 权限库（群里发过言的人会写进这里 -> level>=1）
PERM_DB_PATH = DATABASES_DIR / "permissions" / "levels.json"


# ===== Handin（作业提交）=====
# 任务数据库
HANDIN_DB_PATH = DATABASES_DIR / "handin" / "tasks.json"
# 私聊提交临时收件箱
HANDIN_INBOX_DIR = STAGING_DIR / "handin"
# 旧版群内任务目录名（仅用于兼容迁移）：data/groups/<group_id>/<HANDIN_TASKS_DIRNAME>/<task>/files/
HANDIN_TASKS_DIRNAME = "handin"

# 新版提交文件根目录（不再放在 data/groups 下，避免群成员通过 /find 看到他人提交）
# data/handin/<group_id>/<task>/files/
HANDIN_ROOT_DIR = SUBMISSIONS_DIR / "handin"
# 班级名册（放在 storage/documents/friend/ 下）
ROSTER_XLSX_PATH = DATA_DIR / "friend" / "班级名册.xlsx"
# 时区（用于解析提醒/截止时间）
TIMEZONE = _get_env("TIMEZONE", "Asia/Shanghai")

# ===== 每日重要日提醒 =====
# 配置与运行状态分离：配置可由管理员编辑，联网事实、年度放假安排和发送记录都写入 runtime/。
DAILY_CALENDAR_CONFIG_PATH = PRIVATE_CONFIG_DIR / "calendar" / "daily_calendar_config.json"
EDITOR_DUTY_CONFIG_PATH = PRIVATE_CONFIG_DIR / "calendar" / "editor_duty_config.json"
CALENDAR_TEST_GROUP_ID = _get_env_int("CALENDAR_TEST_GROUP_ID", 0, 0, 9_999_999_999)
MEMORY_PROBE_SCOPE = _get_env("MEMORY_PROBE_SCOPE", "")
MEMORY_PROBE_ACTOR = _get_env_int("MEMORY_PROBE_ACTOR", 0, 0, 9_999_999_999)
DAILY_CALENDAR_DATA_DIR = STATE_DIR / "calendar"

# ===== 管理员邮箱新邮件提醒 =====
# 邮箱账号和 IMAP 凭据统一放在 config/private/secrets.env。
EMAIL_NOTIFY_ENABLED = _get_env_bool("MAIL_NOTIFY_ENABLED", False)
EMAIL_NOTIFY_POLL_SECONDS = max(15, int(_get_env("MAIL_NOTIFY_POLL_SECONDS", "60") or "60"))
EMAIL_NOTIFY_MAX_BODY_CHARS = max(2000, int(_get_env("MAIL_NOTIFY_MAX_BODY_CHARS", "16000") or "16000"))
EMAIL_NOTIFY_FETCH_BYTES = max(65536, int(_get_env("MAIL_NOTIFY_FETCH_BYTES", "524288") or "524288"))
EMAIL_NOTIFY_STATE_PATH = STATE_DIR / "email_notify" / "state.json"

# NapCat 本地缓存 temp 映射（用于私聊文件提交：不走网络下载，直接拷贝缓存文件）
NAPCAT_TEMP_CONTAINER_DIR = "/app/.config/QQ/NapCat/temp"
NAPCAT_TEMP_HOST_DIR = _get_env_path(
    "NAPCAT_TEMP_HOST_DIR",
    NAPCAT_DIR / "qq" / "NapCat" / "temp",
)


# ===== NapCat 容器内的资料库挂载点 =====
# docker-compose 里把 ./storage/documents 挂载到这个路径后，upload_* action 才能读到文件
DATA_DIR_CONTAINER = "/bot_data"

# ===== NapCat 专用上传目录（更稳定的发送文件方式）=====
# 宿主机目录（与 docker-compose 的 ./upload_* 挂载对应）
UPLOAD_GROUP_HOST_DIR = STAGING_DIR / "group"
UPLOAD_PRIVATE_HOST_DIR = STAGING_DIR / "private"
# 容器内目录（docker-compose 挂载到 /data/upload_*）
UPLOAD_GROUP_CONTAINER_DIR = "/data/upload_group_file"
UPLOAD_PRIVATE_CONTAINER_DIR = "/data/upload_private_file"

# 是否在发送时把文件名转为 ASCII（可规避部分 NapCat/QQNT 对中文文件名的兼容问题）
# 建议默认 False：优先保留原文件名；若发送失败会在代码里自动回退到 ASCII 名重试。
SEND_FILENAME_ASCII_SAFE = _get_env_bool("SEND_FILENAME_ASCII_SAFE", False)

# 发送文件遇到 "rich media transfer failed" 时，自动重试的等待时间（秒）
# Docker Desktop (Windows) 的 bind mount 有时存在同步延迟，大文件更容易触发。
SEND_RETRY_DELAYS = [0.8, 1.8]

# 若原文件发送失败（尤其是 doc/docx/pdf 等），可自动打包为 zip 再发一次作为兜底。
AUTO_ZIP_FALLBACK = _get_env_bool("AUTO_ZIP_FALLBACK", True)

# 大文件提示阈值（MB）：发送/接收超过该大小的文件时提示“请耐心等待”
LARGE_FILE_WARN_MB = _get_env_int("LARGE_FILE_WARN_MB", 50, 1, 100000)

# 展示/搜索限制
LS_LIMIT = _get_env_int("LS_LIMIT", 100, 1, 10000)
FIND_DIR_LIMIT = _get_env_int("FIND_DIR_LIMIT", 100, 1, 10000)
FIND_FILE_LIMIT = _get_env_int("FIND_FILE_LIMIT", 100, 1, 10000)
FIND_LIMIT = max(FIND_DIR_LIMIT, FIND_FILE_LIMIT)  # 兼容旧代码
FIND_MAX_SCAN = 100000   # 最多扫描多少个文件/目录项，避免卡死

# ===== 新增：/get 多文件默认打包 =====
# /get 选择文件数 > 该阈值时，默认打包成一个 zip 发送
GET_ZIP_THRESHOLD = _get_env_int("GET_ZIP_THRESHOLD", 4, 1, 10000)

# ===== 新增：Handin 归档保留策略 =====
# 在任务创建者最后一次 /handinget 后，保留 N 天再清理归档（/handinstatus /handincheck 仅展示仍可 /handinget 的任务）
HANDIN_KEEP_DAYS_AFTER_LAST_GET = _get_env_int("HANDIN_KEEP_DAYS_AFTER_LAST_GET", 30, 1, 3650)

# 手动/定时清理 inbox：收件箱内临时文件保留 N 天（避免长期运行堆积）
HANDIN_INBOX_KEEP_DAYS = _get_env_int("HANDIN_INBOX_KEEP_DAYS", 30, 1, 3650)


# ===== AI（DeepSeek + Embedding）=====
AI_API_KEY_PATH = PRIVATE_CONFIG_DIR / "api_key.txt"
AI_MATERIAL_DIR = DATA_DIR / "public" / "textbook_and_material"
AI_SEMANTIC_STORE_PATH = DATABASES_DIR / "ai" / "semantic_store.sqlite3"
# Chat memory is deliberately separate from the searchable material index.
# Keep these values close to the other AI storage settings so deployments have
# one conventional environment-variable parser and no second secrets file.
# 部署默认：总开关保持开启，因为 /memory on 需要它才能工作（它本身不采集也不外发任何数据）。
# 真正的"默认关闭"由作用域默认值实现——私聊/群聊都要显式 /memory on（群里由可信管理员 /memory group on 开群）。
AI_MEMORY_ENABLED = _get_env_bool("AI_MEMORY_ENABLED", True)
AI_MEMORY_DB_PATH = _get_env_path("AI_MEMORY_DB_PATH", DATABASES_DIR / "ai" / "chat_memory.sqlite3")
AI_MEMORY_BOT_ID = _get_env_int("AI_MEMORY_BOT_ID", 0, 0, 9_999_999_999)
# 群记忆默认全开：留空表示所有群都启用；显式列出群号时只在这些群启用（收窄用）。
AI_MEMORY_GROUP_ALLOWLIST = _parse_int_set(_get_env("AI_MEMORY_GROUP_ALLOWLIST", ""))
AI_MEMORY_RAW_RETENTION_DAYS = _get_env_int("AI_MEMORY_RAW_RETENTION_DAYS", 30, 1, 3650)
AI_MEMORY_MAX_EVENTS_PER_SCOPE = _get_env_int("AI_MEMORY_MAX_EVENTS_PER_SCOPE", 20000, 100, 100000)
AI_MEMORY_MAX_FACT_CHARS = _get_env_int("AI_MEMORY_MAX_FACT_CHARS", 500, 1, 4000)
AI_MEMORY_MAX_STORED_EVENT_CHARS = _get_env_int("AI_MEMORY_MAX_STORED_EVENT_CHARS", 6000, 100, 20000)
AI_MEMORY_RECENT_EVENTS = _get_env_int("AI_MEMORY_RECENT_EVENTS", 20, 1, 300)
AI_MEMORY_CONTEXT_CHAR_BUDGET = _get_env_int("AI_MEMORY_CONTEXT_CHAR_BUDGET", 7000, 500, 30000)
AI_MEMORY_TOP_K = _get_env_int("AI_MEMORY_TOP_K", 3, 1, 20)
AI_MEMORY_SCOPE_MAX_WAITERS = _get_env_int("AI_MEMORY_SCOPE_MAX_WAITERS", 4, 0, 32)
AI_MEMORY_GLOBAL_MAX_WAITERS = _get_env_int("AI_MEMORY_GLOBAL_MAX_WAITERS", 32, 1, 256)
AI_MEMORY_DB_BUSY_TIMEOUT_MS = _get_env_int("AI_MEMORY_DB_BUSY_TIMEOUT_MS", 2000, 1, 30000)
AI_MEMORY_SUMMARY_ENABLED = _get_env_bool("AI_MEMORY_SUMMARY_ENABLED", False)
AI_MEMORY_SUMMARY_MIN_EVENTS = _get_env_int("AI_MEMORY_SUMMARY_MIN_EVENTS", 20, 2, 1000)
AI_MEMORY_SUMMARY_MAX_CHARS = _get_env_int("AI_MEMORY_SUMMARY_MAX_CHARS", 4000, 200, 12000)
AI_MEMORY_SUMMARY_DAILY_BUDGET = _get_env_int("AI_MEMORY_SUMMARY_DAILY_BUDGET", 50, 0, 10000)
# 高流量群：按成员累计 200 条才做一次长期事实判断，一天最多 20 次。
AI_MEMORY_AUTO_EXTRACT_ENABLED = _get_env_bool("AI_MEMORY_AUTO_EXTRACT_ENABLED", True)
AI_MEMORY_AUTO_EXTRACT_MIN_EVENTS = _get_env_int("AI_MEMORY_AUTO_EXTRACT_MIN_EVENTS", 200, 1, 1000)
AI_MEMORY_AUTO_EXTRACT_DAILY_BUDGET = _get_env_int("AI_MEMORY_AUTO_EXTRACT_DAILY_BUDGET", 20, 0, 10000)
# 记忆向量检索（默认关闭；开启后 provider 未配置时自动退化为纯词法）。
AI_MEMORY_EMBEDDING_ENABLED = _get_env_bool("AI_MEMORY_EMBEDDING_ENABLED", False)
AI_MEMORY_EMBEDDING_MIN_SIMILARITY = _get_env_float("AI_MEMORY_EMBEDDING_MIN_SIMILARITY", 0.35, 0.0, 1.0)
AI_MEMORY_EMBEDDING_BATCH_SIZE = _get_env_int("AI_MEMORY_EMBEDDING_BATCH_SIZE", 16, 1, 100)
AI_MEMORY_EMBEDDING_TIMEOUT_SECONDS = _get_env_float("AI_MEMORY_EMBEDDING_TIMEOUT_SECONDS", 30.0, 1.0, 120.0)
AI_LEGACY_DIR = DATABASES_DIR / "ai" / "legacy"
AI_INDEX_PATH = AI_LEGACY_DIR / "all_files_index.json"
AI_METADATA_PATH = AI_LEGACY_DIR / "file_metadata.json"
AI_VECTORS_PATH = AI_LEGACY_DIR / "file_vectors.npy"
AI_STATE_DIR = STATE_DIR / "ai"
AI_MATERIAL_SCAN_MARKS_PATH = AI_STATE_DIR / "material_scan_marks.json"
AI_MATERIAL_STATE_CACHE_PATH = AI_STATE_DIR / "material_state_cache.json"

AI_GROUP_CHAT_PROMPTS_PATH = AI_PROMPTS_DIR / "chat" / "group_overrides.json"
AI_GROUP_NOTICE_PROMPTS_PATH = AI_PROMPTS_DIR / "notice" / "prompts.json"
AI_PRIVATE_CHAT_PROMPTS_PATH = AI_PROMPTS_DIR / "chat" / "private_overrides.json"
ANSWER_FILE_PATH = REPLIES_DIR / "answer.txt"
KEYWORD_ANSWER_FILE_PATH = REPLIES_DIR / "keyword_answer.txt"
CONSECUTIVE_REPLY_CONFIG_PATH = REPLIES_DIR / "consecutive_reply.json"
# docker-compose 将 AUTO_REPLY_IMAGES_DIR 挂载到这个路径，供 CQ 图片消息读取。
AUTO_REPLY_IMAGES_CONTAINER_DIR = "/bot_auto_replies"
WIKI_CATEGORIES_PATH = WIKI_CONFIG_DIR / "1037wiki_categories.json"
WIKI_STATE_DIR = STATE_DIR / "wiki"

# 文本任务使用 deepseek-flash；联网检索单独使用 DeepSeek Search API（无需模型名）。
AI_CHAT_MODEL = _get_env("AI_CHAT_MODEL", "deepseek-flash")
AI_WEB_SEARCH_ENABLED = _get_env_bool("AI_WEB_SEARCH_ENABLED", True)
AI_EMBED_MODEL = _get_env("AI_EMBED_MODEL", "BAAI/bge-m3")
AI_BOT_NICK = _get_env("AI_BOT_NICK", "Cooper_bot")

# ===== Kimi Code CLI（QQ 自由聊天迁移配置）=====
# Kimi 的 provider/认证独立保存在各 profile home，绝不复用 api_key.txt。
_KIMI_PUBLIC_WORKDIR_DEFAULT = Path(os.getenv("LOCALAPPDATA") or Path.home()) / "Cooper_bot" / "kimi_public"
AI_KIMI_ENABLED = _get_env_bool("AI_KIMI_ENABLED", True)
AI_KIMI_CLI_PATH = _get_env("AI_KIMI_CLI_PATH", "kimi")
AI_KIMI_MODEL = _get_env("AI_KIMI_MODEL", "")
AI_KIMI_PUBLIC_HOME = _get_env_path("AI_KIMI_PUBLIC_HOME", RUNTIME_DIR / "kimi" / "public")
AI_KIMI_ADMIN_HOME = _get_env_path("AI_KIMI_ADMIN_HOME", RUNTIME_DIR / "kimi" / "admin")
AI_KIMI_PUBLIC_WORKDIR = _get_env_path("AI_KIMI_PUBLIC_WORKDIR", _KIMI_PUBLIC_WORKDIR_DEFAULT)
AI_KIMI_ADMIN_WORKDIR = _get_env_path("AI_KIMI_ADMIN_WORKDIR", WORKSPACES_DIR / "kimi_admin")
AI_KIMI_PUBLIC_AGENT_PATH = _get_env_path("AI_KIMI_PUBLIC_AGENT_PATH", AI_PROMPTS_DIR / "kimi" / "public.md")
AI_KIMI_ADMIN_AGENT_PATH = _get_env_path("AI_KIMI_ADMIN_AGENT_PATH", AI_PROMPTS_DIR / "kimi" / "admin.md")
AI_KIMI_TIMEOUT_SECONDS = _get_env_float("AI_KIMI_TIMEOUT_SECONDS", 120.0, 10.0, 600.0)
AI_KIMI_ADMIN_TIMEOUT_SECONDS = _get_env_float("AI_KIMI_ADMIN_TIMEOUT_SECONDS", 480.0, 10.0, 600.0)
AI_KIMI_MAX_CONCURRENCY = _get_env_int("AI_KIMI_MAX_CONCURRENCY", 3, 1, 16)
AI_KIMI_ADMIN_ENABLED = _get_env_bool("AI_KIMI_ADMIN_ENABLED", False)
AI_KIMI_ALLOW_GROUP_COMPUTER = _get_env_bool("AI_KIMI_ALLOW_GROUP_COMPUTER", False)
# Kimi 搜索桥：把 CLI 的 WebSearch 请求转成 DeepSeek 联网搜索（只绑 127.0.0.1）。
AI_KIMI_SEARCH_BRIDGE_ENABLED = _get_env_bool("AI_KIMI_SEARCH_BRIDGE_ENABLED", True)
AI_KIMI_SEARCH_BRIDGE_PORT = _get_env_int("AI_KIMI_SEARCH_BRIDGE_PORT", 18783, 1, 65535)
AI_KIMI_SEARCH_BRIDGE_TIMEOUT_SECONDS = _get_env_float("AI_KIMI_SEARCH_BRIDGE_TIMEOUT_SECONDS", 90.0, 5.0, 300.0)

AI_SEARCH_LIMIT = int(_get_env("AI_SEARCH_LIMIT", "10") or "10")
AI_SEARCH_MIN_SIMILARITY = float(_get_env("AI_SEARCH_MIN_SIMILARITY", "0.35") or "0.35")
AI_FALLBACK_ERROR_REPLY = (
    "哎呀，我的脑子好像卡壳了（API报错/网络波动），请稍后重试，或者@Cooper 检查一下我的后台服务器吧！🔌"
)

AI_SYSTEM_PROMPT_PATH = AI_PROMPTS_DIR / "chat" / "persona.txt"
try:
    AI_SYSTEM_PROMPT = AI_SYSTEM_PROMPT_PATH.read_text(encoding="utf-8")
except FileNotFoundError:
    AI_SYSTEM_PROMPT = "You are a QQ assistant. Refer to /help for supported commands."


# ===== 视觉描述 Skill（VISION_*）=====
# 视觉 API 配置优先从环境变量 / secrets.env 读取；
# 未配置时兜底读取 api_key.txt 第 5、6 行（视觉 base url + key）。
def _read_vision_config_from_api_key_txt() -> "tuple[str, str]":
    # api_key.txt 是位置型配置：空行表示该行留空，必须保留行位，
    # 否则未配置 Embedding 时第 5、6 行的 Vision 配置会左移到第 3、4 行。
    try:
        lines = [
            x.strip()
            for x in AI_API_KEY_PATH.read_text(encoding="utf-8").splitlines()
        ]
        if len(lines) < 6:
            lines += [""] * (6 - len(lines))
        return lines[4].rstrip("/"), lines[5]
    except Exception:
        pass
    return "", ""


_VISION_BASE_URL_FROM_FILE, _VISION_API_KEY_FROM_FILE = _read_vision_config_from_api_key_txt()

VISION_ENABLED = _get_env_bool("VISION_ENABLED", bool(_VISION_BASE_URL_FROM_FILE and _VISION_API_KEY_FROM_FILE))
VISION_API_KEY = _get_env("VISION_API_KEY", _VISION_API_KEY_FROM_FILE)
VISION_BASE_URL = _get_env("VISION_BASE_URL", _VISION_BASE_URL_FROM_FILE)
VISION_MODEL = _get_env("VISION_MODEL", "qwen3.5-flash")
VISION_TIMEOUT_SECONDS = float(_get_env("VISION_TIMEOUT_SECONDS", "20") or "20")
VISION_MAX_IMAGES_PER_MESSAGE = int(_get_env("VISION_MAX_IMAGES_PER_MESSAGE", "20") or "20")
VISION_MAX_IMAGE_BYTES = int(_get_env("VISION_MAX_IMAGE_BYTES", str(8 * 1024 * 1024)) or str(8 * 1024 * 1024))
VISION_MAX_EDGE = int(_get_env("VISION_MAX_EDGE", "1024") or "1024")
# 描述上限：与 VisionSkill.description_max_chars 生效值一致，保持原有的宽松长度。
VISION_DESCRIPTION_MAX_CHARS = int(_get_env("VISION_DESCRIPTION_MAX_CHARS", "2400") or "2400")
VISION_MAX_CONCURRENCY = int(_get_env("VISION_MAX_CONCURRENCY", "4") or "4")
VISION_CACHE_MAX_ENTRIES = int(_get_env("VISION_CACHE_MAX_ENTRIES", "512") or "512")
VISION_CACHE_TTL_SECONDS = float(_get_env("VISION_CACHE_TTL_SECONDS", "21600") or "21600")
VISION_NEGATIVE_CACHE_TTL_SECONDS = float(_get_env("VISION_NEGATIVE_CACHE_TTL_SECONDS", "60") or "60")
VISION_CAPTURE_CONTEXT_IMAGES = _get_env_bool("VISION_CAPTURE_CONTEXT_IMAGES", False)
