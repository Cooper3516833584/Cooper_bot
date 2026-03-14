from __future__ import annotations

import asyncio
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
    from docx import Document  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    Document = None


class AIService:
    _SKIP_FILENAMES = {
        "all_files_index.json",
        "file_metadata.json",
        "file_vectors.npy",
        "build_index.py",
        "build_vectors.py",
    }
    _ALLOWED_SUFFIXES = {".pdf", ".docx", ".ppt", ".pptx"}

    def __init__(self, log):
        self.log = log
        self.api_key_path = Path(AI_API_KEY_PATH)
        self.material_dir = Path(AI_MATERIAL_DIR)
        self.index_path = Path(AI_INDEX_PATH)
        self.metadata_path = Path(AI_METADATA_PATH)
        self.vectors_path = Path(AI_VECTORS_PATH)

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

    async def bootstrap_sync(self) -> None:
        await asyncio.to_thread(self._bootstrap_sync_sync)

    async def semantic_find_paths(self, demand: str, limit: Optional[int] = None) -> List[Path]:
        return await asyncio.to_thread(self._semantic_find_paths_sync, demand, limit)

    async def chat(self, user_input: str) -> str:
        return await asyncio.to_thread(self._chat_sync, user_input)

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
                pages = min(len(reader.pages), int(max_pages))
                for i in range(pages):
                    t = reader.pages[i].extract_text() or ""
                    if t:
                        text_parts.append(t)
        except Exception as e:
            self.log.warning(f"AI 索引：读取 PDF 失败 {path.name}: {e}")
            return ""
        return "\n".join(text_parts)[:max_chars]

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
