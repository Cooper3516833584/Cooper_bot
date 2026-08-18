# config.py
from __future__ import annotations

import os
import re
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
AI_CONFIG_DIR = CONFIG_DIR / "ai"
REPLIES_DIR = CONFIG_DIR / "replies"
WIKI_CONFIG_DIR = CONFIG_DIR / "wiki"

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
WS_URI = f"ws://127.0.0.1:13001/?access_token={TOKEN}"

# 你已经配好的 HTTP Server（可选；目前阶段不依赖它）
HTTP_PORT = 13010
HTTP_BASE = f"http://127.0.0.1:{HTTP_PORT}"

# 如果 NapCat 的 HTTP Server 也配置了 access_token，这里会自动带上（同时放在 query + Authorization）
HTTP_TOKEN = _get_env("HTTP_TOKEN", TOKEN)

# 自动通过好友申请（OneBot request_type=friend）
AUTO_APPROVE_FRIEND_REQUEST = _get_env_bool("AUTO_APPROVE_FRIEND_REQUEST", True)
# 通过后自动设置好友备注（可选；为空则不设置）
AUTO_APPROVE_FRIEND_REMARK = _get_env("AUTO_APPROVE_FRIEND_REMARK", "")

# 会话多久没说话就切一份日志
IDLE_SPLIT_SECONDS = 1800

# 权限等级：0游客 1临时 2好友 3管理员
# 管理员 QQ 号（/whoami 里显示的 user_id）
ADMIN_USERS = _parse_int_set(_get_env("ADMIN_USERS", ""))
ENABLE_OCR = _get_env_bool("ENABLE_OCR", False)

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
TIMEZONE = "Asia/Shanghai"

# ===== 每日重要日提醒 =====
# 配置与运行状态分离：配置可由管理员编辑，联网事实、年度放假安排和发送记录都写入 runtime/。
DAILY_CALENDAR_CONFIG_PATH = CONFIG_DIR / "calendar" / "daily_calendar_config.json"
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
SEND_FILENAME_ASCII_SAFE = False

# 发送文件遇到 "rich media transfer failed" 时，自动重试的等待时间（秒）
# Docker Desktop (Windows) 的 bind mount 有时存在同步延迟，大文件更容易触发。
SEND_RETRY_DELAYS = [0.8, 1.8]

# 若原文件发送失败（尤其是 doc/docx/pdf 等），可自动打包为 zip 再发一次作为兜底。
AUTO_ZIP_FALLBACK = True

# 大文件提示阈值（MB）：发送/接收超过该大小的文件时提示“请耐心等待”
LARGE_FILE_WARN_MB = 50

# 展示/搜索限制
LS_LIMIT = 100
FIND_DIR_LIMIT = 100
FIND_FILE_LIMIT = 100
FIND_LIMIT = max(FIND_DIR_LIMIT, FIND_FILE_LIMIT)  # 兼容旧代码
FIND_MAX_SCAN = 100000   # 最多扫描多少个文件/目录项，避免卡死

# ===== 新增：/get 多文件默认打包 =====
# /get 选择文件数 > 该阈值时，默认打包成一个 zip 发送
GET_ZIP_THRESHOLD = 4

# ===== 新增：Handin 归档保留策略 =====
# 在任务创建者最后一次 /handinget 后，保留 N 天再清理归档（/handinstatus /handincheck 仅展示仍可 /handinget 的任务）
HANDIN_KEEP_DAYS_AFTER_LAST_GET = 30

# 手动/定时清理 inbox：收件箱内临时文件保留 N 天（避免长期运行堆积）
HANDIN_INBOX_KEEP_DAYS = 30


# ===== AI（DeepSeek + Embedding）=====
AI_API_KEY_PATH = PRIVATE_CONFIG_DIR / "api_key.txt"
AI_MATERIAL_DIR = DATA_DIR / "public" / "textbook_and_material"
AI_SEMANTIC_STORE_PATH = DATABASES_DIR / "ai" / "semantic_store.sqlite3"
AI_LEGACY_DIR = DATABASES_DIR / "ai" / "legacy"
AI_INDEX_PATH = AI_LEGACY_DIR / "all_files_index.json"
AI_METADATA_PATH = AI_LEGACY_DIR / "file_metadata.json"
AI_VECTORS_PATH = AI_LEGACY_DIR / "file_vectors.npy"
AI_STATE_DIR = STATE_DIR / "ai"
AI_MATERIAL_SCAN_MARKS_PATH = AI_STATE_DIR / "material_scan_marks.json"
AI_MATERIAL_STATE_CACHE_PATH = AI_STATE_DIR / "material_state_cache.json"

AI_GROUP_CHAT_PROMPTS_PATH = AI_CONFIG_DIR / "group_chat_prompts.json"
AI_GROUP_NOTICE_PROMPTS_PATH = AI_CONFIG_DIR / "group_notice_prompts.json"
AI_PRIVATE_CHAT_PROMPTS_PATH = AI_CONFIG_DIR / "private_chat_prompts.json"
ANSWER_FILE_PATH = REPLIES_DIR / "answer.txt"
KEYWORD_ANSWER_FILE_PATH = REPLIES_DIR / "keyword_answer.txt"
WIKI_CATEGORIES_PATH = WIKI_CONFIG_DIR / "1037wiki_categories.json"
WIKI_STATE_DIR = STATE_DIR / "wiki"

AI_CHAT_MODEL = _get_env("AI_CHAT_MODEL", "deepseek-v4-pro")
AI_WEB_SEARCH_ENABLED = _get_env("AI_WEB_SEARCH_ENABLED", "1") == "1"
AI_WEB_SEARCH_MODEL = _get_env("AI_WEB_SEARCH_MODEL", "")
AI_EMBED_MODEL = _get_env("AI_EMBED_MODEL", "BAAI/bge-m3")
AI_BOT_NICK = _get_env("AI_BOT_NICK", "Cooper_bot")
AI_GEMINI_CLI_PATH = _get_env("AI_GEMINI_CLI_PATH", "agy")
AI_GEMINI_MODEL = _get_env("AI_GEMINI_MODEL", "Gemini 3.1 Pro (High)")
AI_CLAUDE_MODEL = _get_env("AI_CLAUDE_MODEL", "Claude Opus 4.6 (Thinking)")
AI_GEMINI_TIMEOUT_SECONDS = float(_get_env("AI_GEMINI_TIMEOUT_SECONDS", "480") or "480")
AI_GEMINI_WORKDIR = _get_env_path("AI_GEMINI_WORKDIR", WORKSPACES_DIR / "ai_cli" / "general")
AI_GEMINI_RESTRICTED_WORKDIR = _get_env_path("AI_GEMINI_RESTRICTED_WORKDIR", WORKSPACES_DIR / "ai_cli" / "restricted")
AI_GEMINI_POLICY_PATH = _get_env_path("AI_GEMINI_POLICY_PATH", AI_CONFIG_DIR / "gemini_cli_chat_only.toml")
AI_SEARCH_LIMIT = int(_get_env("AI_SEARCH_LIMIT", "10") or "10")
AI_SEARCH_MIN_SIMILARITY = float(_get_env("AI_SEARCH_MIN_SIMILARITY", "0.35") or "0.35")
AI_FALLBACK_ERROR_REPLY = (
    "哎呀，我的脑子好像卡壳了（API报错/网络波动），请稍后重试，或者@Cooper 检查一下我的后台服务器吧！🔌"
)

AI_SYSTEM_PROMPT = """# 核心角色与身份设定

你是 Cooper_bot，一个由校内同学 Cooper（qq号：3516833584）开发的 QQ 聊天机器人。

你是一个基于 AI 的智能助手，你的目标是服务于校内同学，解决学习资料获取、群内协作和文件提交中的痛点，帮助同学们更高效地获取学习资料和信息。

只要后台服务正常运行，你就会一直在线陪伴大家。



# 目标受众与对话基调

1. 你的受众是校内同学。

2. 语气要求：热情、真诚、接地气、带有工科生的干练。像一个同级好友，不要端着架子，也不要像刻板的客服。

3. 坦诚你的 AI 身份：如果遇到不懂的情感问题或纯人类的线下体验，大方承认自己是 Cooper 写的 AI 机器人，不要伪装成人类去共情，但可以提供符合逻辑的建议或安慰。

# 机器人业务指令提示（1 级用户）

你需要知道机器人对 1 级用户开放的常用指令：

- /help 或 /h：查看命令速览。
- /ping：检查机器人是否在线。
- /whoami：查看当前用户、群聊和权限信息。
- /count：开始临时收集名单；进入模式后发送 end 结束并清空。
- /countlist：查看已提交名单和未交名单。
- /countremove 序号：从已提交名单中移除指定人名。
- /ls [root/子目录]：查看资料库目录。
- /find 搜索内容 [可选: root/子目录]：搜索资料，支持关键词或直接描述需求。
- /get 序号：获取 /find 结果中的文件或文件夹；可以填写多个序号，例如 /get 1 2 3 4。
- /find 返回结果后，可以直接回复序号进入下级目录。

如果用户的消息看起来是在尝试使用机器人业务指令，但存在漏写 /、指令拼写错误、参数缺失或格式错误，你应该指出可能的问题，并给出最可能的正确写法。不要声称已经替用户执行指令，也不要编造不存在的指令；如果无法判断用户想做什么，提醒用户发送 /help 查看命令速览。

# 兜底与边界条件

- 遇到敏感、涉政、引战或违反 QQ 社区规则的话题，巧妙且生硬地转移话题，或幽默地表示“这个话题超纲啦，我只是个专注学习的机器人”。

- 遇到你确实无法解决的技术故障，请回复：“哎呀，我的脑子好像卡壳了（API报错/网络波动），请稍后重试，或者@Cooper 检查一下我的后台服务器吧！🔌”
"""


# ===== 视觉描述 Skill（VISION_*）=====
# 视觉 API 配置优先从环境变量 / secrets.env 读取；
# 未配置时兜底读取 api_key.txt 第 5、6 行（视觉 base url + key）。
def _read_vision_config_from_api_key_txt() -> "tuple[str, str]":
    try:
        lines = [
            x.strip()
            for x in AI_API_KEY_PATH.read_text(encoding="utf-8").splitlines()
            if x.strip()
        ]
        if len(lines) >= 6:
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
VISION_DESCRIPTION_MAX_CHARS = int(_get_env("VISION_DESCRIPTION_MAX_CHARS", "240") or "240")
VISION_MAX_CONCURRENCY = int(_get_env("VISION_MAX_CONCURRENCY", "4") or "4")
VISION_CACHE_MAX_ENTRIES = int(_get_env("VISION_CACHE_MAX_ENTRIES", "512") or "512")
VISION_CACHE_TTL_SECONDS = float(_get_env("VISION_CACHE_TTL_SECONDS", "21600") or "21600")
VISION_NEGATIVE_CACHE_TTL_SECONDS = float(_get_env("VISION_NEGATIVE_CACHE_TTL_SECONDS", "60") or "60")
VISION_CAPTURE_CONTEXT_IMAGES = _get_env_bool("VISION_CAPTURE_CONTEXT_IMAGES", True)
