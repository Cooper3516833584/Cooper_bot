from __future__ import annotations

import asyncio
import html
import json
import re
import threading
import urllib.error
import urllib.request
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np

from config import (
    AI_API_KEY_PATH,
    AI_BOT_NICK,
    AI_CHAT_MODEL,
    AI_EMBED_MODEL,
    AI_FALLBACK_ERROR_REPLY,
    AI_INDEX_PATH,
    AI_MATERIAL_DIR,
    AI_METADATA_PATH,
    AI_SEARCH_LIMIT,
    AI_SEARCH_MIN_SIMILARITY,
    AI_SYSTEM_PROMPT,
    AI_VECTORS_PATH,
)

try:
    import PyPDF2  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    PyPDF2 = None

try:
    import fitz  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    fitz = None

try:
    from docx import Document  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    Document = None

try:
    import aiohttp  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    aiohttp = None

try:
    from bs4 import BeautifulSoup  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    BeautifulSoup = None

try:
    from openai import OpenAI  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    OpenAI = None

try:
    from rapidocr_onnxruntime import RapidOCR  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    RapidOCR = None


class AIService:
    _SKIP_FILENAMES = {
        "all_files_index.json",
        "file_metadata.json",
        "file_vectors.npy",
        "build_index.py",
        "build_vectors.py",
    }
    _ALLOWED_SUFFIXES = {".pdf", ".docx", ".ppt", ".pptx"}
    _NOTICE_SILENT_TOKEN = "[静默]"

    def __init__(self, log):
        self.log = log
        self.api_key_path = Path(AI_API_KEY_PATH)
        self.material_dir = Path(AI_MATERIAL_DIR)
        self.index_path = Path(AI_INDEX_PATH)
        self.metadata_path = Path(AI_METADATA_PATH)
        self.vectors_path = Path(AI_VECTORS_PATH)
        self.notice_prompt_config_path = Path(__file__).resolve().parent / "group_notice_prompts.json"

        self.bot_nick = str(AI_BOT_NICK or "Cooepr_bot")
        self.chat_model = str(AI_CHAT_MODEL or "deepseek-chat")
        self.embed_model = str(AI_EMBED_MODEL or "BAAI/bge-m3")
        self.search_limit = max(1, int(AI_SEARCH_LIMIT))
        self.search_min_similarity = float(AI_SEARCH_MIN_SIMILARITY)
        self.system_prompt = str(AI_SYSTEM_PROMPT or "").strip()
        self.fallback_error_reply = str(AI_FALLBACK_ERROR_REPLY or "").strip() or (
            "哎呀，我的脑子好像卡壳了（API报错/网络波动），请稍后重试，或者@Cooper 检查一下我的后台服务器吧！🔌"
        )

        self.deepseek_base_url = ""
        self.deepseek_api_key = ""
        self.embedding_base_url = ""
        self.embedding_api_key = ""

        self._lock = threading.RLock()
        self._semantic_meta: List[dict] = []
        self._semantic_norm_vectors: np.ndarray = np.empty((0, 0), dtype=np.float64)
        self._rapid_ocr = None
        self._notice_prompt_cache_mtime: Optional[float] = None
        self._notice_prompt_cache: Dict[str, object] = {"default": {}, "groups": {}}

    @property
    def chat_ready(self) -> bool:
        return bool(self.deepseek_base_url and self.deepseek_api_key and self.system_prompt)

    @property
    def semantic_ready(self) -> bool:
        return bool(
            self.embedding_base_url
            and self.embedding_api_key
            and self._semantic_norm_vectors.ndim == 2
            and self._semantic_norm_vectors.shape[0] > 0
            and self._semantic_norm_vectors.shape[1] > 0
            and len(self._semantic_meta) == int(self._semantic_norm_vectors.shape[0])
        )

    @property
    def notice_ready(self) -> bool:
        return bool(self.deepseek_api_key and OpenAI is not None)

    async def bootstrap_sync(self) -> None:
        await asyncio.to_thread(self._bootstrap_sync_sync)

    async def semantic_find_paths(self, demand: str, limit: Optional[int] = None) -> List[Path]:
        return await asyncio.to_thread(self._semantic_find_paths_sync, demand, limit)

    async def chat(self, user_input: str) -> str:
        return await asyncio.to_thread(self._chat_sync, user_input)

    async def extract_notice_file_head(self, path: Path, max_chars: int = 4000, max_pages: int = 6) -> str:
        return await asyncio.to_thread(
            self._extract_notice_file_head_sync,
            Path(path),
            int(max_chars),
            int(max_pages),
        )

    async def extract_notice_url_head(self, url: str, max_chars: int = 4000) -> str:
        return await self._extract_notice_url_head_async(str(url or "").strip(), int(max_chars))

    async def classify_notice(
        self,
        source: str,
        snippet: str,
        group_id: Optional[int] = None,
        kind: str = "notice",
    ) -> bool:
        return await asyncio.to_thread(
            self._classify_notice_sync_v2,
            str(source or ""),
            str(snippet or ""),
            group_id,
            str(kind or "notice"),
        )

    async def reason_notice(
        self,
        source: str,
        snippet: str,
        group_id: Optional[int] = None,
        kind: str = "notice",
    ) -> str:
        return await asyncio.to_thread(
            self._reason_notice_sync_v2,
            str(source or ""),
            str(snippet or ""),
            group_id,
            str(kind or "notice"),
        )

    @classmethod
    def is_notice_silent(cls, text: str) -> bool:
        return str(text or "").strip() == cls._NOTICE_SILENT_TOKEN

    @staticmethod
    def sanitize_reasoner_output(text: str) -> str:
        s = str(text or "")
        s = re.sub(r"<think\b[^>]*>[\s\S]*?</think>", "", s, flags=re.IGNORECASE)
        s = s.strip()
        if s.startswith("```"):
            s = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", s)
            s = re.sub(r"\s*```$", "", s)
        # QQ 里不需要 Markdown 星号样式，统一去掉。
        s = s.replace("*", "")
        # 兜底：去掉“原文未提及/未知/暂无/待通知”这类占位行。
        bad_tokens = ("原文未提及", "未知", "暂无", "待通知")
        kept = []
        for ln in s.splitlines():
            line = ln.strip()
            if not line:
                continue
            if any(tok in line for tok in bad_tokens):
                continue
            kept.append(line)
        s = "\n".join(kept).strip()
        return s.strip()

    @staticmethod
    def _normalize_notice_prompt_lines(value: object) -> List[str]:
        if isinstance(value, str):
            s = value.strip()
            return [s] if s else []
        if isinstance(value, list):
            out: List[str] = []
            for item in value:
                s = str(item or "").strip()
                if s:
                    out.append(s)
            return out
        return []

    def _builtin_notice_prompt_config(self) -> Dict[str, object]:
        return {
            "default": {
                "classify_prompt_lines": [
                    "【角色设定】",
                    "你是 QQ 群里的通知过滤助手。",
                    "",
                    "【任务】",
                    "请判断下面内容是否属于“需要群成员采取动作、流程、报名、缴费、开会、填写、提交、关注截止时间”的通知。",
                    "如果只是学习资料、课程介绍、科普文章、普通新闻、经验分享、宣传内容、无明确行动要求的参考材料，请判定为静默。",
                    "如果内容和本群成员关系不大，或无法明确看出需要本群成员行动，也请判定为静默。",
                    "",
                    "【输出要求】",
                    "如果应该静默，只输出：{{silent_token}}",
                    "如果应该回复，只输出：[通知]",
                    "不要输出任何其他字符。",
                    "",
                    "【来源】",
                    "{{source}}",
                    "",
                    "【内容片段】",
                    "{{content}}",
                ],
                "reason_prompt_lines": [
                    "【角色设定】",
                    "你是 QQ 群里的 AI 助手，请基于通知全文生成一份给本群成员看的简洁省流说明。",
                    "",
                    "【要求】",
                    "只提取原文明确出现的信息，不要补充、猜测、外推或编造。",
                    "报名方式、费用、地点、对象、截止时间等字段，只有原文明确写到才允许写入。",
                    "如果某项信息原文没写，就直接省略，不要写“未知”“暂未提及”“待通知”之类占位话术。",
                    "",
                    "【输出格式】",
                    "📢 【省流通知】[核心标题]",
                    "🎯 核心事由：[一句话概括]",
                    "🙋 涉及人员：[按原文填写]",
                    "✅ 需要做什么：",
                    "1. ...",
                    "2. ...",
                    "3. ...",
                    "⏰ 截止时间：[若无则写“无明确截止时间”]",
                    "",
                    "【风格】",
                    "控制在 6 到 10 行。",
                    "可以用 emoji。",
                    "不要 Markdown 星号，不要代码块，不要额外添加模板外字段。",
                    "",
                    "【输入来源】",
                    "{{source}}",
                    "",
                    "【通知全文/片段】",
                    "{{content}}",
                ],
            },
            "groups": {},
        }

    def _load_notice_prompt_config(self) -> Dict[str, object]:
        path = self.notice_prompt_config_path
        fallback = self._builtin_notice_prompt_config()
        try:
            mtime = path.stat().st_mtime
        except Exception:
            return fallback

        with self._lock:
            if self._notice_prompt_cache_mtime == float(mtime):
                return self._notice_prompt_cache

            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                if not isinstance(data, dict):
                    raise ValueError("prompt config root must be an object")
                default_cfg = data.get("default") or {}
                groups_cfg = data.get("groups") or {}
                if not isinstance(default_cfg, dict):
                    default_cfg = {}
                if not isinstance(groups_cfg, dict):
                    groups_cfg = {}
                normalized = {"default": default_cfg, "groups": groups_cfg}
                self._notice_prompt_cache = normalized
                self._notice_prompt_cache_mtime = float(mtime)
                self.log.info(f"群通知解析：已加载群提示词配置 {path.name}")
                return normalized
            except Exception as e:
                self.log.warning(f"群通知解析：读取群提示词配置失败 {path.name}: {e}")
                self._notice_prompt_cache = fallback
                self._notice_prompt_cache_mtime = float(mtime)
                return fallback

    def _select_notice_prompt_lines(
        self,
        group_id: Optional[int],
        kind: str,
        stage: str,
    ) -> List[str]:
        cfg = self._load_notice_prompt_config()
        default_cfg = cfg.get("default") if isinstance(cfg, dict) else {}
        groups_cfg = cfg.get("groups") if isinstance(cfg, dict) else {}
        if not isinstance(default_cfg, dict):
            default_cfg = {}
        if not isinstance(groups_cfg, dict):
            groups_cfg = {}

        group_cfg = groups_cfg.get(str(group_id), {}) if group_id is not None else {}
        if not isinstance(group_cfg, dict):
            group_cfg = {}

        kind_name = str(kind or "").strip().lower()
        kind_field = f"{kind_name}_{stage}_prompt_lines" if kind_name else ""
        generic_field = f"{stage}_prompt_lines"

        candidates = []
        if kind_field:
            candidates.append(group_cfg.get(kind_field))
        candidates.append(group_cfg.get(generic_field))
        if kind_field:
            candidates.append(default_cfg.get(kind_field))
        candidates.append(default_cfg.get(generic_field))

        for item in candidates:
            lines = self._normalize_notice_prompt_lines(item)
            if lines:
                return lines

        builtin = self._builtin_notice_prompt_config().get("default", {})
        if isinstance(builtin, dict):
            for item in (builtin.get(kind_field), builtin.get(generic_field)):
                lines = self._normalize_notice_prompt_lines(item)
                if lines:
                    return lines
        return []

    def _render_notice_prompt(
        self,
        lines: List[str],
        source: str,
        content: str,
    ) -> str:
        template = "\n".join(lines).strip()
        return (
            template
            .replace("{{source}}", str(source or "未知来源"))
            .replace("{{content}}", str(content or ""))
            .replace("{{silent_token}}", self._NOTICE_SILENT_TOKEN)
        )

    def _bootstrap_sync_sync(self) -> None:
        self.material_dir.mkdir(parents=True, exist_ok=True)
        self._load_api_config()

        index_list = self._load_json_list(self.index_path)
        metadata_list = self._load_json_list(self.metadata_path)
        vector_matrix = self._load_vectors(self.vectors_path)
        metadata_list, vector_matrix = self._align_metadata_vectors(metadata_list, vector_matrix)

        actual_rels = self._scan_material_files()
        old_index_count = len(index_list)

        cleaned_index: List[dict] = []
        seen_index_rels = set()
        for item in index_list:
            rel = self._normalize_rel(item.get("file_path"))
            if (not rel) or (rel in seen_index_rels) or (rel not in actual_rels):
                continue
            abs_path = self.material_dir / rel
            cleaned_index.append(self._normalize_index_item(item, rel, abs_path))
            seen_index_rels.add(rel)

        new_rels = sorted(actual_rels - seen_index_rels)
        if new_rels:
            self.log.info(f"AI 索引：发现 {len(new_rels)} 个新文件，开始生成摘要与向量")
        for idx, rel in enumerate(new_rels, 1):
            try:
                entry = self._build_index_entry(rel)
                cleaned_index.append(entry)
                self.log.info(f"AI 索引：新增[{idx}/{len(new_rels)}] {rel}")
            except Exception as e:
                self.log.warning(f"AI 索引：新增失败 {rel}: {e}")

        self._save_json(self.index_path, cleaned_index)

        existing_vec_by_rel = self._metadata_vector_map(metadata_list, vector_matrix, valid_rels=actual_rels)
        rebuilt_metadata: List[dict] = []
        rebuilt_vectors: List[np.ndarray] = []
        vec_dim: Optional[int] = None

        for item in cleaned_index:
            rel = self._normalize_rel(item.get("file_path"))
            if not rel:
                continue

            vec = existing_vec_by_rel.get(rel)
            if vec is None:
                vec = self._build_vector_for_index_item(item)
                if vec is None:
                    self.log.warning(f"AI 向量：跳过 {rel}（向量生成失败）")
                    continue

            vec = np.asarray(vec, dtype=np.float64).reshape(-1)
            if vec_dim is None:
                vec_dim = int(vec.size)
            if vec.size != vec_dim:
                self.log.warning(f"AI 向量：跳过 {rel}（维度不一致 {vec.size} != {vec_dim}）")
                continue

            rebuilt_metadata.append(
                {
                    "file_path": self._to_store_rel(rel),
                    "filename": str(item.get("filename") or Path(rel).name),
                    "subject": str(item.get("subject") or self._subject_from_rel(rel)),
                }
            )
            rebuilt_vectors.append(vec)

        matrix = (
            np.vstack(rebuilt_vectors).astype(np.float64, copy=False)
            if rebuilt_vectors
            else np.empty((0, 0), dtype=np.float64)
        )
        self._save_json(self.metadata_path, rebuilt_metadata)
        np.save(self.vectors_path, matrix)

        removed = max(0, old_index_count - len(cleaned_index) + len(new_rels))
        self.log.info(
            f"AI 索引：同步完成，现有索引 {len(cleaned_index)} 条，向量 {matrix.shape[0]} 条，新增 {len(new_rels)}，清理 {removed}"
        )

        self._reload_semantic_cache()

    def _load_api_config(self) -> None:
        lines: List[str] = []
        try:
            lines = [x.strip() for x in self.api_key_path.read_text(encoding="utf-8").splitlines() if x.strip()]
        except Exception as e:
            self.log.warning(f"AI 配置：读取 api_key.txt 失败: {e}")
            return

        if len(lines) < 4:
            self.log.warning("AI 配置：api_key.txt 至少需要 4 行（deepseek base/key + embedding base/key）")
            return

        self.deepseek_base_url = lines[0].rstrip("/")
        self.deepseek_api_key = lines[1]
        self.embedding_base_url = lines[2].rstrip("/")
        self.embedding_api_key = lines[3]
        self.log.info("AI 配置：已加载 DeepSeek 与 Embedding API")

    def _reload_semantic_cache(self) -> None:
        metadata = self._load_json_list(self.metadata_path)
        vectors = self._load_vectors(self.vectors_path)
        metadata, vectors = self._align_metadata_vectors(metadata, vectors)

        if vectors.size <= 0 or vectors.ndim != 2 or not metadata:
            with self._lock:
                self._semantic_meta = []
                self._semantic_norm_vectors = np.empty((0, 0), dtype=np.float64)
            self.log.warning("AI 检索：向量库为空，/find 引号语义检索不可用")
            return

        norm = np.linalg.norm(vectors, axis=1, keepdims=True)
        norm[norm == 0.0] = 1.0
        norm_vectors = vectors / norm

        with self._lock:
            self._semantic_meta = metadata
            self._semantic_norm_vectors = norm_vectors.astype(np.float64, copy=False)

        self.log.info(
            f"AI 检索：载入 {len(metadata)} 条向量，维度 {int(self._semantic_norm_vectors.shape[1])}"
        )

    def _semantic_find_paths_sync(self, demand: str, limit: Optional[int] = None) -> List[Path]:
        q = str(demand or "").strip()
        if (not q) or (not self.semantic_ready):
            return []

        q_vec = self._embed_text(q)
        if q_vec is None:
            return []
        q_arr = np.asarray(q_vec, dtype=np.float64).reshape(-1)
        if q_arr.size <= 0:
            return []

        q_norm = np.linalg.norm(q_arr)
        if q_norm == 0.0:
            return []
        q_arr = q_arr / q_norm

        with self._lock:
            meta = list(self._semantic_meta)
            norm_vectors = self._semantic_norm_vectors.copy()

        if norm_vectors.ndim != 2 or norm_vectors.shape[0] != len(meta):
            return []
        if norm_vectors.shape[1] != q_arr.size:
            self.log.warning(
                f"AI 检索：查询向量维度不匹配 ({q_arr.size} != {norm_vectors.shape[1]})"
            )
            return []

        sims = np.dot(norm_vectors, q_arr)
        if sims.ndim != 1:
            return []

        top_k = max(1, min(int(limit or self.search_limit), int(norm_vectors.shape[0])))
        min_sim = float(self.search_min_similarity)

        order = np.argsort(-sims)
        out: List[Path] = []
        seen = set()
        for idx in order:
            if len(out) >= top_k:
                break
            score = float(sims[idx])
            if score < min_sim:
                continue
            rel = self._normalize_rel((meta[idx] or {}).get("file_path"))
            if not rel or rel in seen:
                continue
            seen.add(rel)
            p = self.material_dir / rel
            if p.exists() and p.is_file():
                out.append(p.resolve())
        return out

    def _chat_sync(self, user_input: str) -> str:
        if not self.chat_ready:
            raise RuntimeError("chat not ready")

        content = str(user_input or "").strip()
        if not content:
            return "想聊点啥？发我一句话就行。"

        payload = {
            "model": self.chat_model,
            "messages": [
                {"role": "system", "content": self.system_prompt},
                {"role": "user", "content": content},
            ],
            "temperature": 0.4,
        }
        url = self._join_url(self.deepseek_base_url, "chat/completions")
        data = self._post_json(url, payload, self.deepseek_api_key, timeout=90.0)
        text = self._extract_chat_text(data)
        if not text:
            raise RuntimeError("empty chat response")
        return text

    def _extract_notice_file_head_sync(self, path: Path, max_chars: int = 4000, max_pages: int = 6) -> str:
        p = Path(path)
        if (not p.exists()) or (not p.is_file()):
            return ""
        suffix = p.suffix.lower()
        if suffix == ".pdf":
            text = self._read_pdf_head_fitz(p, max_pages=max_pages, max_chars=max_chars)
            if self._has_enough_text(text):
                return text
            text2 = self._read_pdf_head(p, max_pages=max_pages, max_chars=max_chars)
            if self._has_enough_text(text2):
                return text2
            # 扫描件兜底：尝试 OCR
            text3 = self._read_pdf_head_ocr(p, max_pages=max_pages, max_chars=max_chars)
            if self._has_enough_text(text3):
                self.log.info(f"群通知解析：已使用 OCR 提取扫描 PDF 文本 {p.name}")
                return text3
            return text3
        if suffix == ".docx":
            return self._read_docx_head(p, max_chars=max_chars)
        return ""

    @staticmethod
    def _has_enough_text(text: str, min_chars: int = 20) -> bool:
        compact = re.sub(r"\s+", "", str(text or ""))
        return len(compact) >= int(min_chars)

    async def _extract_notice_url_head_async(self, url: str, max_chars: int = 4000) -> str:
        if (not url) or (not (url.startswith("http://") or url.startswith("https://"))):
            return ""
        if aiohttp is None or BeautifulSoup is None:
            self.log.warning("群通知解析：缺少 aiohttp 或 beautifulsoup4，回退到 urllib 简易解析")
            return await asyncio.to_thread(self._extract_notice_url_head_urllib_sync, url, int(max_chars))

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        timeout = aiohttp.ClientTimeout(total=25.0)

        html = ""
        content_type = ""
        try:
            async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
                async with session.get(url, allow_redirects=True) as resp:
                    if int(resp.status) >= 400:
                        self.log.warning(f"群通知解析：链接抓取返回状态码={int(resp.status)} url={url[:160]}")
                        return ""
                    content_type = str(resp.headers.get("Content-Type") or "").lower()
                    html = await resp.text(errors="ignore")
        except Exception as e:
            self.log.warning(f"群通知解析：网页抓取失败 {url[:120]}: {e}")
            return await asyncio.to_thread(self._extract_notice_url_head_urllib_sync, url, int(max_chars))

        if not html:
            self.log.info(f"群通知解析：链接响应正文为空 url={url[:160]}")
            return ""

        if "text/plain" in content_type and "<html" not in html[:500].lower():
            plain = re.sub(r"\s+", " ", html).strip()
            return plain[:max_chars]

        try:
            soup = BeautifulSoup(html, "html.parser")
            for bad in soup(["script", "style", "noscript", "svg", "canvas", "iframe"]):
                bad.decompose()
            body = soup.body if soup.body is not None else soup
            text = body.get_text(separator="\n", strip=True)
            text = re.sub(r"[ \t]+", " ", text)
            text = re.sub(r"\n{2,}", "\n", text).strip()
            if not text:
                self.log.info(f"群通知解析：网页正文提取为空 url={url[:160]}")
            return text[:max_chars]
        except Exception as e:
            self.log.warning(f"群通知解析：网页正文提取失败 {url[:120]}: {e}")
            return self._extract_notice_url_head_urllib_sync(url, int(max_chars))

    def _extract_notice_url_head_urllib_sync(self, url: str, max_chars: int = 4000) -> str:
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=25.0) as resp:
                content_type = str(resp.headers.get("Content-Type") or "").lower()
                raw = resp.read(1024 * 1024)  # 1MB upper bound
        except Exception as e:
            self.log.warning(f"群通知解析：urllib 抓取失败 {url[:120]}: {e}")
            return ""

        txt = raw.decode("utf-8", errors="ignore")
        if not txt:
            return ""

        if "text/plain" in content_type and "<html" not in txt[:500].lower():
            plain = re.sub(r"\s+", " ", txt).strip()
            return plain[:max_chars]

        # Simple HTML cleanup fallback when bs4/aiohttp are unavailable.
        cleaned = re.sub(r"(?is)<(script|style|noscript|svg|canvas|iframe)\b.*?>.*?</\1>", " ", txt)
        cleaned = re.sub(r"(?is)<br\s*/?>", "\n", cleaned)
        cleaned = re.sub(r"(?is)</(p|div|li|h[1-6]|tr|section|article)>", "\n", cleaned)
        cleaned = re.sub(r"(?is)<[^>]+>", " ", cleaned)
        cleaned = html.unescape(cleaned)
        cleaned = re.sub(r"[ \t]+", " ", cleaned)
        cleaned = re.sub(r"\n{2,}", "\n", cleaned).strip()
        return cleaned[:max_chars]

    def _classify_notice_sync(self, source: str, snippet: str) -> bool:
        if not self.deepseek_api_key:
            return False
        if OpenAI is None:
            return False

        content = str(snippet or "").strip()
        if not content:
            return False
        if len(content) > 6000:
            content = content[:6000]

        base_url = (self.deepseek_base_url or "https://api.deepseek.com/v1").rstrip("/")
        client = OpenAI(api_key=self.deepseek_api_key, base_url=base_url)
        prompt = (
            "你是电气2410班群消息过滤器。\n"
            "请判断下面内容是否属于“需要同学执行动作/流程/截止日期”的通知。\n"
            "如果是纯学习资料/课件/教材/日历/介绍，请只输出：[静默]\n"
            "如果是需要执行动作的通知，请只输出：[通知]\n"
            "禁止输出任何其他字符。\n\n"
            f"来源：{source or '未知来源'}\n\n"
            f"内容片段：\n{content}"
        )
        try:
            resp = client.chat.completions.create(
                model="deepseek-reasoner",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
            )
            raw = ""
            try:
                raw = str(resp.choices[0].message.content or "").strip()
            except Exception:
                raw = ""
            out = self.sanitize_reasoner_output(raw)
            if out == "[通知]":
                self.log.info(f"群通知解析：分类结果=通知 source={source[:120]}")
                return True
            if out == self._NOTICE_SILENT_TOKEN:
                self.log.info(f"群通知解析：分类结果=静默 source={source[:120]}")
            elif out:
                self.log.info(f"群通知解析：分类结果=非标准输出但放行 source={source[:120]} output={out[:80]}")
            else:
                self.log.info(f"群通知解析：分类结果=空输出 source={source[:120]}")
            return (out != self._NOTICE_SILENT_TOKEN) and bool(out)
        except Exception as e:
            self.log.warning(f"群通知解析：分类调用失败 source={source[:120]} err={e}")
            return False

    def _reason_notice_sync(self, source: str, snippet: str) -> str:
        if not self.deepseek_api_key:
            raise RuntimeError("deepseek api key not ready")
        if OpenAI is None:
            raise RuntimeError("openai sdk is not installed")

        content = str(snippet or "").strip()
        if not content:
            return self._NOTICE_SILENT_TOKEN
        # 通知类尽量使用完整文本；极端长文档才截断，避免超大请求。
        if len(content) > 120000:
            content = content[:120000]

        base_url = (self.deepseek_base_url or "https://api.deepseek.com/v1").rstrip("/")
        client = OpenAI(api_key=self.deepseek_api_key, base_url=base_url)

        prompt = (
            "【角色设定】\n"
            "你是电气2410班的 AI 助手 Cooper_bot。\n\n"
            "【任务】\n"
            "根据给定通知全文，生成一份简洁清晰的省流说明。\n"
            "只提取原文中明确出现的信息，不要补充、猜测或外推。\n"
            "报名方式、费用、地点等信息仅在原文明确出现时才写入；未出现就直接省略。\n\n"
            "【输出格式（纯文本，不要*）】\n"
            "📢 【省流通知】[核心标题]\n"
            "🎯 核心事由：[一句话概括]\n"
            "🙋‍♂️ 涉及人员：[全体同学/团员/班委/指定人群]\n"
            "✅ 需要做什么：\n"
            "1. ...\n"
            "2. ...\n"
            "3. ...\n"
            "⏰ 截止时间：[若无则写“无明确截止时间”]\n\n"
            "【风格要求】\n"
            "- 保持简洁，控制在 6~10 行。\n"
            "- 可用 emoji。\n"
            "- 不要 Markdown，不要星号(*)，不要代码块。\n"
            "- 禁止输出“原文未提及/未知/暂无/待通知”等占位语。\n"
            "- 除上述模板外，不要额外追加字段。\n\n"
            "【输入来源】\n"
            f"{source or '未知来源'}\n\n"
            "【通知全文/片段】\n"
            f"{content}"
        )

        resp = client.chat.completions.create(
            model="deepseek-reasoner",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
        )

        raw = ""
        try:
            raw = str(resp.choices[0].message.content or "").strip()
        except Exception:
            raw = ""
        if (not raw) and hasattr(resp, "model_dump"):
            raw = self._extract_chat_text(resp.model_dump())
        out = self.sanitize_reasoner_output(raw)
        return out or self._NOTICE_SILENT_TOKEN

    def _classify_notice_sync_v2(
        self,
        source: str,
        snippet: str,
        group_id: Optional[int] = None,
        kind: str = "notice",
    ) -> bool:
        if not self.deepseek_api_key:
            return False
        if OpenAI is None:
            return False

        content = str(snippet or "").strip()
        if not content:
            return False
        if len(content) > 6000:
            content = content[:6000]

        lines = self._select_notice_prompt_lines(group_id, kind, "classify")
        prompt = self._render_notice_prompt(lines, source, content)

        base_url = (self.deepseek_base_url or "https://api.deepseek.com/v1").rstrip("/")
        client = OpenAI(api_key=self.deepseek_api_key, base_url=base_url)
        try:
            resp = client.chat.completions.create(
                model="deepseek-reasoner",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
            )
            raw = ""
            try:
                raw = str(resp.choices[0].message.content or "").strip()
            except Exception:
                raw = ""
            out = self.sanitize_reasoner_output(raw)
            if out == "[通知]":
                self.log.info(f"群通知解析：分类结果=通知 source={source[:120]}")
                return True
            if out == self._NOTICE_SILENT_TOKEN:
                self.log.info(f"群通知解析：分类结果=静默 source={source[:120]}")
            elif out:
                self.log.info(f"群通知解析：分类结果=非标准输出但放行 source={source[:120]} output={out[:80]}")
            else:
                self.log.info(f"群通知解析：分类结果=空输出 source={source[:120]}")
            return (out != self._NOTICE_SILENT_TOKEN) and bool(out)
        except Exception as e:
            self.log.warning(f"群通知解析：分类调用失败 source={source[:120]} err={e}")
            return False

    def _reason_notice_sync_v2(
        self,
        source: str,
        snippet: str,
        group_id: Optional[int] = None,
        kind: str = "notice",
    ) -> str:
        if not self.deepseek_api_key:
            raise RuntimeError("deepseek api key not ready")
        if OpenAI is None:
            raise RuntimeError("openai sdk is not installed")

        content = str(snippet or "").strip()
        if not content:
            return self._NOTICE_SILENT_TOKEN
        if len(content) > 120000:
            content = content[:120000]

        lines = self._select_notice_prompt_lines(group_id, kind, "reason")
        prompt = self._render_notice_prompt(lines, source, content)

        base_url = (self.deepseek_base_url or "https://api.deepseek.com/v1").rstrip("/")
        client = OpenAI(api_key=self.deepseek_api_key, base_url=base_url)
        resp = client.chat.completions.create(
            model="deepseek-reasoner",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
        )

        raw = ""
        try:
            raw = str(resp.choices[0].message.content or "").strip()
        except Exception:
            raw = ""
        if (not raw) and hasattr(resp, "model_dump"):
            raw = self._extract_chat_text(resp.model_dump())
        out = self.sanitize_reasoner_output(raw)
        return out or self._NOTICE_SILENT_TOKEN

    def _build_index_entry(self, rel: str) -> dict:
        rel = self._normalize_rel(rel)
        abs_path = self.material_dir / rel
        subject = self._subject_from_rel(rel)
        filename = abs_path.name
        ext = abs_path.suffix.lower().lstrip(".")
        if f".{ext}" not in self._ALLOWED_SUFFIXES:
            raise ValueError(f"unsupported file type: {abs_path.suffix}")

        content = ""
        if ext == "pdf":
            content = self._read_pdf_head(abs_path)
        elif ext == "docx":
            content = self._read_docx_head(abs_path)
        elif ext in ("ppt", "pptx"):
            # 按需求：PPT/PPTX 不解析正文，仅使用学科目录和文件名
            content = ""

        summary_data = self._generate_summary(
            subject=subject,
            filename=filename,
            file_type=ext,
            text_content=content,
            title_only=(ext in ("ppt", "pptx")),
        )
        return {
            "file_path": self._to_store_rel(rel),
            "subject": subject,
            "filename": filename,
            "file_type": ext,
            "keywords": summary_data.get("keywords") or [subject],
            "summary": summary_data.get("summary") or f"{subject}资料：{filename}",
        }

    def _build_vector_for_index_item(self, item: dict) -> Optional[np.ndarray]:
        combined_text = self._make_embedding_text(item)
        vec = self._embed_text(combined_text)
        if vec is None:
            return None
        arr = np.asarray(vec, dtype=np.float64).reshape(-1)
        return arr if arr.size > 0 else None

    def _generate_summary(
        self,
        subject: str,
        filename: str,
        file_type: str,
        text_content: str,
        title_only: bool = False,
    ) -> dict:
        if not self.deepseek_base_url or not self.deepseek_api_key:
            return self._fallback_summary(subject, filename, file_type)

        if title_only:
            text_content = ""

        snippet = (text_content or "").strip()
        if len(snippet) > 2000:
            snippet = snippet[:2000]

        if title_only:
            prompt = (
                "你是高校资料整理助手。请仅根据“学科目录名”和“文件名”生成标签与摘要。\n"
                "不要假设正文内容，不要编造具体知识点细节。\n"
                f"学科目录：{subject}\n"
                f"文件名：{filename}\n"
                f"文件类型：{file_type}\n\n"
                "输出严格 JSON：\n"
                '{"keywords":["词1","词2"],"summary":"一句精简说明"}'
            )
        else:
            if not snippet:
                snippet = "（正文不可提取，仅根据文件名和学科目录推断）"
            prompt = (
                "你是高校资料整理助手。请根据学科、文件名和文本片段生成标签与摘要。\n"
                "标签关注：课程名、知识点、资料类型、年份。\n"
                "摘要控制在 100~150 字，简洁、可检索。\n"
                f"学科目录：{subject}\n"
                f"文件名：{filename}\n"
                f"文件类型：{file_type}\n"
                f"正文片段：{snippet}\n\n"
                "输出严格 JSON：\n"
                '{"keywords":["词1","词2"],"summary":"一句精简说明"}'
            )

        payload = {
            "model": self.chat_model,
            "messages": [{"role": "user", "content": prompt}],
            "response_format": {"type": "json_object"},
            "temperature": 0.1,
        }
        url = self._join_url(self.deepseek_base_url, "chat/completions")
        try:
            data = self._post_json(url, payload, self.deepseek_api_key, timeout=120.0)
            text = self._extract_chat_text(data)
            parsed = self._parse_summary_json(text)
            if parsed:
                return parsed
        except Exception as e:
            self.log.warning(f"AI 摘要：{filename} 生成失败: {e}")
        return self._fallback_summary(subject, filename, file_type)

    def _embed_text(self, text: str) -> Optional[List[float]]:
        if not self.embedding_base_url or not self.embedding_api_key:
            return None
        payload = {"model": self.embed_model, "input": str(text or "")}
        url = self._join_url(self.embedding_base_url, "embeddings")
        try:
            data = self._post_json(url, payload, self.embedding_api_key, timeout=90.0)
            arr = (((data or {}).get("data") or [{}])[0] or {}).get("embedding")
            if isinstance(arr, list) and arr:
                return [float(x) for x in arr]
        except Exception as e:
            self.log.warning(f"AI 向量：embedding 请求失败: {e}")
        return None

    def _make_embedding_text(self, item: dict) -> str:
        subject = str(item.get("subject") or "")
        filename = str(item.get("filename") or "")
        keywords = item.get("keywords") or []
        if not isinstance(keywords, list):
            keywords = [str(keywords)]
        kw_text = ", ".join(str(x).strip() for x in keywords if str(x).strip())
        summary = str(item.get("summary") or "")
        return f"学科：{subject}\n文件名：{filename}\n标签：{kw_text}\n核心内容：{summary}"

    def _scan_material_files(self) -> set[str]:
        rels: set[str] = set()
        if not self.material_dir.exists():
            return rels
        for p in self.material_dir.rglob("*"):
            if not p.is_file():
                continue
            if p.suffix.lower() not in self._ALLOWED_SUFFIXES:
                continue
            if self._skip_file(p):
                continue
            try:
                rel = p.relative_to(self.material_dir).as_posix()
            except Exception:
                continue
            rels.add(rel)
        return rels

    def _skip_file(self, p: Path) -> bool:
        if p.name in self._SKIP_FILENAMES:
            return True
        if p.name.startswith("~$"):
            return True
        try:
            rel = p.relative_to(self.material_dir)
            if any(part.startswith(".") for part in rel.parts):
                return True
        except Exception:
            return True
        return False

    def _metadata_vector_map(
        self,
        metadata_list: List[dict],
        vectors: np.ndarray,
        valid_rels: set[str],
    ) -> Dict[str, np.ndarray]:
        out: Dict[str, np.ndarray] = {}
        if vectors.ndim != 2:
            return out
        rows = int(vectors.shape[0])
        for i, item in enumerate(metadata_list[:rows]):
            rel = self._normalize_rel((item or {}).get("file_path"))
            if (not rel) or (rel not in valid_rels) or (rel in out):
                continue
            out[rel] = vectors[i]
        return out

    def _normalize_index_item(self, item: dict, rel: str, abs_path: Path) -> dict:
        rel = self._normalize_rel(rel)
        subject = str(item.get("subject") or self._subject_from_rel(rel))
        filename = str(item.get("filename") or abs_path.name)
        file_type = str(abs_path.suffix.lower().lstrip("."))
        keywords = item.get("keywords") or []
        if not isinstance(keywords, list):
            keywords = [str(keywords)]
        keywords = [str(x).strip() for x in keywords if str(x).strip()]
        summary = str(item.get("summary") or "").strip()
        return {
            "file_path": self._to_store_rel(rel),
            "subject": subject,
            "filename": filename,
            "file_type": file_type,
            "keywords": keywords or [subject],
            "summary": summary or f"{subject}资料：{filename}",
        }

    @staticmethod
    def _align_metadata_vectors(metadata: List[dict], vectors: np.ndarray) -> Tuple[List[dict], np.ndarray]:
        if vectors.ndim == 1 and vectors.size > 0:
            vectors = vectors.reshape(1, -1)
        if vectors.ndim != 2:
            vectors = np.empty((0, 0), dtype=np.float64)
        vectors = vectors.astype(np.float64, copy=False)
        n = min(len(metadata), int(vectors.shape[0]))
        if n <= 0:
            return [], np.empty((0, 0), dtype=np.float64)
        return metadata[:n], vectors[:n]

    @staticmethod
    def _load_json_list(path: Path) -> List[dict]:
        if not path.exists():
            return []
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, list):
                return [x for x in data if isinstance(x, dict)]
        except Exception:
            pass
        return []

    @staticmethod
    def _save_json(path: Path, data: List[dict]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, ensure_ascii=False, indent=4), encoding="utf-8")

    @staticmethod
    def _load_vectors(path: Path) -> np.ndarray:
        if not path.exists():
            return np.empty((0, 0), dtype=np.float64)
        try:
            arr = np.load(path, allow_pickle=False)
            if isinstance(arr, np.ndarray):
                return arr
        except Exception:
            pass
        return np.empty((0, 0), dtype=np.float64)

    def _read_pdf_head(self, path: Path, max_pages: int = 5, max_chars: int = 2000) -> str:
        if PyPDF2 is None:
            return ""
        text_parts: List[str] = []
        try:
            with path.open("rb") as f:
                reader = PyPDF2.PdfReader(f)
                pages = len(reader.pages) if int(max_pages) <= 0 else min(len(reader.pages), int(max_pages))
                for i in range(pages):
                    t = reader.pages[i].extract_text() or ""
                    if t:
                        text_parts.append(t)
        except Exception as e:
            self.log.warning(f"AI 索引：读取 PDF 失败 {path.name}: {e}")
            return ""
        return "\n".join(text_parts)[:max_chars]

    def _read_pdf_head_fitz(self, path: Path, max_pages: int = 6, max_chars: int = 4000) -> str:
        if fitz is None:
            return ""
        chunks: List[str] = []
        try:
            doc = fitz.open(str(path))
            pages = int(doc.page_count) if int(max_pages) <= 0 else min(int(doc.page_count), int(max_pages))
            for i in range(pages):
                t = doc.load_page(i).get_text("text") or ""
                if t:
                    chunks.append(t)
            doc.close()
        except Exception as e:
            self.log.warning(f"群通知解析：读取 PDF 失败 {path.name}: {e}")
            return ""
        return "\n".join(chunks)[:max_chars]

    def _get_rapid_ocr(self):
        if RapidOCR is None:
            return None
        if self._rapid_ocr is False:
            return None
        if self._rapid_ocr is None:
            try:
                self._rapid_ocr = RapidOCR()
            except Exception as e:
                self.log.warning(f"群通知解析：OCR 引擎初始化失败（rapidocr_onnxruntime）: {e}")
                self._rapid_ocr = False
                return None
        return self._rapid_ocr

    def _read_pdf_head_ocr(self, path: Path, max_pages: int = 6, max_chars: int = 4000) -> str:
        if fitz is None:
            return ""
        ocr = self._get_rapid_ocr()
        if ocr is None:
            return ""

        chunks: List[str] = []
        try:
            doc = fitz.open(str(path))
            pages = int(doc.page_count) if int(max_pages) <= 0 else min(int(doc.page_count), int(max_pages))
            for i in range(pages):
                page = doc.load_page(i)
                # 2x 放大能明显提升扫描件 OCR 成功率
                pix = page.get_pixmap(matrix=fitz.Matrix(2, 2), alpha=False)
                arr = np.frombuffer(pix.samples, dtype=np.uint8)
                if arr.size <= 0:
                    continue
                arr = arr.reshape(pix.h, pix.w, pix.n)
                if pix.n >= 4:
                    arr = arr[:, :, :3]

                result, _elapse = ocr(arr)
                if not result:
                    continue
                for item in result:
                    if not isinstance(item, (list, tuple)) or len(item) < 2:
                        continue
                    txt = str(item[1] or "").strip()
                    if txt:
                        chunks.append(txt)
                if len("\n".join(chunks)) >= int(max_chars):
                    break
            doc.close()
        except Exception as e:
            self.log.warning(f"群通知解析：OCR 提取 PDF 失败 {path.name}: {e}")
            return ""
        return "\n".join(chunks)[:max_chars]

    def _read_docx_head(self, path: Path, max_chars: int = 2000) -> str:
        if Document is None:
            return ""
        chunks: List[str] = []
        cur = 0
        try:
            doc = Document(str(path))
            for para in doc.paragraphs:
                t = (para.text or "").strip()
                if not t:
                    continue
                chunks.append(t)
                cur += len(t)
                if cur >= max_chars:
                    break
        except Exception as e:
            self.log.warning(f"AI 索引：读取 DOCX 失败 {path.name}: {e}")
            return ""
        return "\n".join(chunks)[:max_chars]

    @staticmethod
    def _normalize_rel(raw: object) -> str:
        s = str(raw or "").strip().replace("\\", "/")
        if s.startswith("./"):
            s = s[2:]
        if s.startswith(".\\"):
            s = s[2:]
        return s.lstrip("/")

    @staticmethod
    def _to_store_rel(rel: str) -> str:
        rel = rel.replace("/", "\\").lstrip("\\")
        return f".\\{rel}"

    @staticmethod
    def _subject_from_rel(rel: str) -> str:
        parts = [x for x in rel.split("/") if x]
        return parts[0] if parts else "unknown"

    @staticmethod
    def _join_url(base: str, endpoint: str) -> str:
        b = str(base or "").rstrip("/")
        e = str(endpoint or "").lstrip("/")
        return f"{b}/{e}"

    @staticmethod
    def _extract_chat_text(resp: dict) -> str:
        try:
            return str((((resp or {}).get("choices") or [{}])[0] or {}).get("message", {}).get("content") or "").strip()
        except Exception:
            return ""

    @staticmethod
    def _parse_summary_json(text: str) -> Optional[dict]:
        raw = str(text or "").strip()
        if not raw:
            return None
        obj = None
        try:
            obj = json.loads(raw)
        except Exception:
            m = re.search(r"\{[\s\S]*\}", raw)
            if not m:
                return None
            try:
                obj = json.loads(m.group(0))
            except Exception:
                return None
        if not isinstance(obj, dict):
            return None
        keywords = obj.get("keywords") or []
        if not isinstance(keywords, list):
            keywords = [str(keywords)]
        keywords = [str(x).strip() for x in keywords if str(x).strip()]
        summary = str(obj.get("summary") or "").strip()
        if not summary:
            return None
        return {"keywords": keywords[:12], "summary": summary}

    @staticmethod
    def _fallback_summary(subject: str, filename: str, file_type: str) -> dict:
        stem = Path(filename).stem
        kws = [subject]
        if stem:
            kws.append(stem)
        if file_type:
            kws.append(file_type.lower())
        # 去重保序
        out = []
        seen = set()
        for x in kws:
            k = str(x).strip()
            if (not k) or (k in seen):
                continue
            seen.add(k)
            out.append(k)
        return {"keywords": out[:10], "summary": f"{subject}资料：{filename}"}

    @staticmethod
    def _post_json(url: str, payload: dict, api_key: str, timeout: float = 60.0) -> dict:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(url=url, data=body, method="POST")
        req.add_header("Content-Type", "application/json")
        req.add_header("Accept", "application/json")
        if api_key:
            req.add_header("Authorization", f"Bearer {api_key}")

        try:
            with urllib.request.urlopen(req, timeout=float(timeout)) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as e:
            detail = ""
            try:
                detail = e.read().decode("utf-8", errors="replace")
            except Exception:
                detail = str(e)
            raise RuntimeError(f"http {e.code}: {detail[:300]}")
        except Exception as e:
            raise RuntimeError(str(e))

        txt = raw.decode("utf-8", errors="replace").strip()
        if not txt:
            raise RuntimeError("empty response")
        try:
            obj = json.loads(txt)
        except Exception as e:
            raise RuntimeError(f"json decode failed: {e}")
        if not isinstance(obj, dict):
            raise RuntimeError("invalid response type")
        return obj
