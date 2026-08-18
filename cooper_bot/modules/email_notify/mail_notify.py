from __future__ import annotations

import asyncio
import imaplib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from email.header import decode_header, make_header
from email.parser import BytesParser
from email.policy import default
from email.utils import parseaddr, parsedate_to_datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Optional

from cooper_bot.core.config import (
    ADMIN_USERS,
    EMAIL_NOTIFY_ENABLED,
    EMAIL_NOTIFY_FETCH_BYTES,
    EMAIL_NOTIFY_MAX_BODY_CHARS,
    EMAIL_NOTIFY_POLL_SECONDS,
    EMAIL_NOTIFY_STATE_PATH,
)


@dataclass(frozen=True)
class ImapAccount:
    account_id: str
    label: str
    host: str
    port: int
    username: str
    password: str
    folder: str = "INBOX"


@dataclass(frozen=True)
class ParsedMail:
    uid: int
    sender: str
    subject: str
    received_at: str
    body: str
    has_attachment: bool
    attachment_names: tuple[str, ...] = ()


@dataclass(frozen=True)
class FetchBatch:
    uidvalidity: str
    latest_uid: int
    messages: tuple[ParsedMail, ...]
    reset_cursor: bool = False


class _HTMLTextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self._skip_depth = 0

    def handle_starttag(self, tag: str, attrs) -> None:
        _ = attrs
        name = str(tag or "").lower()
        if name in {"script", "style", "head"}:
            self._skip_depth += 1
        elif name in {"br", "p", "div", "li", "tr", "h1", "h2", "h3", "h4"}:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        name = str(tag or "").lower()
        if name in {"script", "style", "head"} and self._skip_depth:
            self._skip_depth -= 1
        elif name in {"p", "div", "li", "tr"}:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        if not self._skip_depth:
            self.parts.append(str(data or ""))

    def text(self) -> str:
        return "".join(self.parts)


class EmailNotifyService:
    _AD_CATEGORIES = {"advertisement", "advertising", "promotion", "promotional", "spam", "junk"}
    _MAX_MESSAGES_PER_POLL = 20

    def __init__(
        self,
        log,
        aisvc,
        *,
        enabled: bool = EMAIL_NOTIFY_ENABLED,
        poll_seconds: int = EMAIL_NOTIFY_POLL_SECONDS,
        max_body_chars: int = EMAIL_NOTIFY_MAX_BODY_CHARS,
        fetch_bytes: int = EMAIL_NOTIFY_FETCH_BYTES,
        state_path: Path = EMAIL_NOTIFY_STATE_PATH,
        accounts: Optional[list[ImapAccount]] = None,
    ) -> None:
        self.log = log
        self.aisvc = aisvc
        self.enabled = bool(enabled)
        self.poll_seconds = max(15, int(poll_seconds))
        self.max_body_chars = max(2000, int(max_body_chars))
        self.fetch_bytes = max(65536, int(fetch_bytes))
        self.state_path = Path(state_path)
        self.accounts = list(accounts) if accounts is not None else self._load_accounts_from_env()
        self._state = self._load_state()

    def _info(self, text: str) -> None:
        try:
            self.log.info(text)
        except Exception:
            pass

    def _warning(self, text: str) -> None:
        try:
            self.log.warning(text)
        except Exception:
            pass

    @staticmethod
    def _env(name: str, default_value: str = "") -> str:
        value = os.getenv(name)
        if value is None:
            return default_value
        value = str(value).strip()
        return value if value else default_value

    @classmethod
    def _load_accounts_from_env(cls) -> list[ImapAccount]:
        raw_names = cls._env("MAIL_IMAP_ACCOUNTS", "")
        names = [x for x in re.split(r"[\s,，]+", raw_names) if x]
        accounts: list[ImapAccount] = []
        seen: set[str] = set()
        for raw_name in names:
            account_id = re.sub(r"[^a-zA-Z0-9_-]+", "_", raw_name).strip("_").lower()
            env_id = re.sub(r"[^A-Z0-9]+", "_", raw_name.upper()).strip("_")
            if not account_id or not env_id or account_id in seen:
                continue
            prefix = f"MAIL_IMAP_{env_id}_"
            host = cls._env(prefix + "HOST")
            username = cls._env(prefix + "USER")
            password = cls._env(prefix + "PASSWORD")
            if not host or not username or not password:
                continue
            try:
                port = int(cls._env(prefix + "PORT", "993"))
            except Exception:
                port = 993
            accounts.append(
                ImapAccount(
                    account_id=account_id,
                    label=cls._env(prefix + "LABEL", raw_name),
                    host=host,
                    port=port,
                    username=username,
                    password=password,
                    folder=cls._env(prefix + "FOLDER", "INBOX"),
                )
            )
            seen.add(account_id)
        return accounts

    @staticmethod
    def _load_json(path: Path, default_value: Any) -> Any:
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return default_value

    def _load_state(self) -> dict[str, Any]:
        value = self._load_json(self.state_path, {})
        if not isinstance(value, dict):
            value = {}
        if not isinstance(value.get("accounts"), dict):
            value["accounts"] = {}
        return value

    def _save_state(self) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.state_path.with_suffix(self.state_path.suffix + ".tmp")
        tmp.write_text(json.dumps(self._state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        tmp.replace(self.state_path)

    @staticmethod
    def _decode_header(value: object) -> str:
        raw = str(value or "").strip()
        if not raw:
            return ""
        try:
            return str(make_header(decode_header(raw))).strip()
        except Exception:
            return raw

    @staticmethod
    def _normalize_text(value: object, max_chars: int) -> str:
        text = str(value or "").replace("\x00", "")
        text = re.sub(r"[ \t\f\v]+", " ", text)
        text = re.sub(r"\n[ \t]+", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text).strip()
        if len(text) > max_chars:
            return text[:max_chars].rstrip() + "\n（正文较长，已截取前部内容）"
        return text

    @staticmethod
    def _html_to_text(value: str) -> str:
        parser = _HTMLTextExtractor()
        try:
            parser.feed(str(value or ""))
            parser.close()
            return parser.text()
        except Exception:
            return re.sub(r"<[^>]+>", " ", str(value or ""))

    def _extract_body(self, message) -> str:
        plain_parts: list[str] = []
        html_parts: list[str] = []
        parts = message.walk() if message.is_multipart() else (message,)
        for part in parts:
            if part.is_multipart() or str(part.get_content_disposition() or "").lower() == "attachment":
                continue
            content_type = str(part.get_content_type() or "").lower()
            if content_type not in {"text/plain", "text/html"}:
                continue
            try:
                content = part.get_content()
            except Exception:
                payload = part.get_payload(decode=True) or b""
                charset = part.get_content_charset() or "utf-8"
                try:
                    content = payload.decode(charset, errors="replace")
                except Exception:
                    content = payload.decode("utf-8", errors="replace")
            if not isinstance(content, str):
                continue
            if content_type == "text/plain":
                plain_parts.append(content)
            else:
                html_parts.append(self._html_to_text(content))
        selected = "\n\n".join(plain_parts).strip() or "\n\n".join(html_parts).strip()
        return self._normalize_text(selected, self.max_body_chars)

    @staticmethod
    def _response_bytes(data: object, *, include_literals: bool = True) -> bytes:
        chunks: list[bytes] = []
        if not isinstance(data, (list, tuple)):
            return b""
        for item in data:
            if isinstance(item, tuple):
                if item and isinstance(item[0], bytes):
                    chunks.append(item[0])
                if include_literals and len(item) > 1 and isinstance(item[1], bytes):
                    chunks.append(item[1])
            elif isinstance(item, bytes):
                chunks.append(item)
        return b"\n".join(chunks)

    @staticmethod
    def _literal_bytes(data: object) -> bytes:
        if not isinstance(data, (list, tuple)):
            return b""
        return b"".join(
            item[1]
            for item in data
            if isinstance(item, tuple) and len(item) > 1 and isinstance(item[1], bytes)
        )

    def _parse_message(self, uid: int, raw: bytes, structure: bytes = b"") -> ParsedMail:
        message = BytesParser(policy=default).parsebytes(raw)
        from_header = self._decode_header(message.get("From"))
        display_name, address = parseaddr(from_header)
        sender = display_name.strip()
        if address:
            sender = f"{sender} <{address}>" if sender else address
        sender = sender or from_header or "未提供"
        subject = self._decode_header(message.get("Subject")) or "（无主题）"

        received_at = self._decode_header(message.get("Date"))
        try:
            parsed_date = parsedate_to_datetime(received_at)
            if parsed_date is not None:
                received_at = parsed_date.astimezone().strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass
        received_at = received_at or "未提供"

        attachment_names: list[str] = []
        for part in message.walk():
            filename = self._decode_header(part.get_filename())
            if filename:
                attachment_names.append(filename)
        upper_structure = structure.upper()
        has_attachment = bool(attachment_names) or any(
            marker in upper_structure for marker in (b'"ATTACHMENT"', b'"FILENAME"')
        )
        return ParsedMail(
            uid=int(uid),
            sender=self._normalize_text(sender, 300),
            subject=self._normalize_text(subject, 500),
            received_at=self._normalize_text(received_at, 120),
            body=self._extract_body(message),
            has_attachment=has_attachment,
            attachment_names=tuple(dict.fromkeys(attachment_names)),
        )

    @staticmethod
    def _extract_uidvalidity(client: imaplib.IMAP4_SSL) -> str:
        try:
            _name, values = client.response("UIDVALIDITY")
            if values:
                raw = values[-1]
                return raw.decode("ascii", errors="ignore") if isinstance(raw, bytes) else str(raw)
        except Exception:
            pass
        return ""

    @staticmethod
    def _parse_uids(data: object) -> list[int]:
        if not isinstance(data, (list, tuple)):
            return []
        values: list[int] = []
        for item in data:
            if not isinstance(item, bytes):
                continue
            for raw in item.split():
                try:
                    values.append(int(raw))
                except Exception:
                    continue
        return sorted(set(values))

    def _fetch_one_sync(self, client: imaplib.IMAP4_SSL, uid: int) -> ParsedMail:
        status, header_data = client.uid("fetch", str(uid), "(BODY.PEEK[HEADER] BODYSTRUCTURE)")
        if status != "OK":
            raise RuntimeError(f"header fetch failed: uid={uid} status={status}")
        status, body_data = client.uid(
            "fetch",
            str(uid),
            f"(BODY.PEEK[TEXT]<0.{self.fetch_bytes}>)",
        )
        if status != "OK":
            raise RuntimeError(f"body fetch failed: uid={uid} status={status}")
        header = self._literal_bytes(header_data)
        body = self._literal_bytes(body_data)
        if not header:
            raise RuntimeError(f"empty header: uid={uid}")
        structure = self._response_bytes(header_data, include_literals=False)
        return self._parse_message(uid, header + b"\r\n" + body, structure)

    @staticmethod
    def _send_netease_client_id(client: imaplib.IMAP4_SSL, account: ImapAccount) -> None:
        host = account.host.strip().lower()
        if host not in {"imap.163.com", "imap.126.com", "imap.yeah.net"}:
            return
        imaplib.Commands.setdefault("ID", ("AUTH", "SELECTED"))
        status, _data = client._simple_command(
            "ID",
            '("name" "Cooper_bot" "version" "1.0" "vendor" "Cooper_bot")',
        )
        if status != "OK":
            raise RuntimeError(f"netease client ID failed: status={status}")

    def _fetch_batch_sync(self, account: ImapAccount, current_state: dict[str, Any]) -> FetchBatch:
        client: Optional[imaplib.IMAP4_SSL] = None
        try:
            client = imaplib.IMAP4_SSL(account.host, account.port, timeout=30)
            status, _data = client.login(account.username, account.password)
            if status != "OK":
                raise RuntimeError(f"login failed: status={status}")
            self._send_netease_client_id(client, account)
            status, _data = client.select(account.folder, readonly=True)
            if status != "OK":
                raise RuntimeError(f"select failed: folder={account.folder} status={status}")

            uidvalidity = self._extract_uidvalidity(client)
            status, data = client.uid("search", None, "ALL")
            if status != "OK":
                raise RuntimeError(f"uid search failed: status={status}")
            all_uids = self._parse_uids(data)
            latest_uid = all_uids[-1] if all_uids else 0
            previous_validity = str(current_state.get("uidvalidity") or "")
            if not current_state or (previous_validity and uidvalidity and previous_validity != uidvalidity):
                return FetchBatch(uidvalidity=uidvalidity, latest_uid=latest_uid, messages=(), reset_cursor=True)

            try:
                last_uid = int(current_state.get("last_uid") or 0)
            except Exception:
                last_uid = 0
            new_uids = [uid for uid in all_uids if uid > last_uid][: self._MAX_MESSAGES_PER_POLL]
            messages = tuple(self._fetch_one_sync(client, uid) for uid in new_uids)
            return FetchBatch(uidvalidity=uidvalidity, latest_uid=latest_uid, messages=messages)
        finally:
            if client is not None:
                try:
                    client.logout()
                except Exception:
                    pass

    @staticmethod
    def _cq_escape(value: object) -> str:
        return str(value or "").replace("&", "&amp;").replace("[", "&#91;").replace("]", "&#93;")

    def _fallback_summary(self, message: ParsedMail) -> str:
        if message.body:
            return self._normalize_text(message.body, 1800)
        return "邮件正文未能提取，请打开邮箱查看具体内容。"

    async def _classify(self, message: ParsedMail) -> dict[str, Any]:
        classify = getattr(self.aisvc, "classify_email", None)
        if not callable(classify):
            return {"notify": True, "category": "unknown", "summary": self._fallback_summary(message)}
        try:
            result = await classify(
                sender=message.sender,
                subject=message.subject,
                body=message.body,
                has_attachment=message.has_attachment,
                attachment_names=list(message.attachment_names),
            )
        except Exception as e:
            self._warning(f"email notify model failed: uid={message.uid} err={e}")
            return {"notify": True, "category": "unknown", "summary": self._fallback_summary(message)}
        if isinstance(result, dict):
            return result
        return {"notify": True, "category": "unknown", "summary": self._fallback_summary(message)}

    def _format_notice(self, account: ImapAccount, message: ParsedMail, summary: str) -> str:
        if message.has_attachment:
            names = "、".join(message.attachment_names[:5])
            attachment_text = f"有（{names}）" if names else "有，请打开邮箱查看"
        else:
            attachment_text = "无"
        text = "\n".join(
            [
                "新邮件提醒",
                f"邮箱：{account.label}",
                f"发件人：{message.sender}",
                f"主题：{message.subject}",
                f"时间：{message.received_at}",
                f"附件：{attachment_text}",
                "",
                "邮件摘要：",
                summary or self._fallback_summary(message),
            ]
        )
        return self._cq_escape(self._normalize_text(text, 3800))

    @staticmethod
    def _send_response_ok(response: object) -> bool:
        if response is None:
            return True
        if not isinstance(response, dict):
            return False
        try:
            return response.get("status") == "ok" and int(response.get("retcode", 0) or 0) == 0
        except Exception:
            return False

    async def _process_message(self, api, account: ImapAccount, message: ParsedMail) -> bool:
        decision = await self._classify(message)
        category = str(decision.get("category") or "unknown").strip().lower()
        notify = category not in self._AD_CATEGORIES
        if not notify:
            self._info(f"email notify suppressed: account={account.account_id} uid={message.uid} category={category}")
            return True

        summary = self._normalize_text(decision.get("summary"), 2800) or self._fallback_summary(message)
        text = self._format_notice(account, message, summary)
        recipients: list[int] = []
        for uid in ADMIN_USERS:
            try:
                value = int(uid)
            except Exception:
                continue
            if value > 0:
                recipients.append(value)
        recipients = sorted(set(recipients))
        if not recipients:
            self._warning("email notify skipped: ADMIN_USERS is empty")
            return False

        all_ok = True
        for user_id in recipients:
            try:
                response = await api.send_private_msg(user_id, text)
                if not self._send_response_ok(response):
                    all_ok = False
                    self._warning(
                        f"email notify QQ send failed: account={account.account_id} uid={message.uid} admin={user_id}"
                    )
            except Exception as e:
                all_ok = False
                self._warning(
                    f"email notify QQ send error: account={account.account_id} uid={message.uid} admin={user_id} err={e}"
                )
        return all_ok

    def _account_state(self, account_id: str) -> dict[str, Any]:
        accounts = self._state.setdefault("accounts", {})
        value = accounts.get(account_id)
        return dict(value) if isinstance(value, dict) else {}

    def _set_cursor(self, account: ImapAccount, uidvalidity: str, last_uid: int) -> None:
        accounts = self._state.setdefault("accounts", {})
        accounts[account.account_id] = {
            "uidvalidity": str(uidvalidity or ""),
            "last_uid": max(0, int(last_uid)),
            "updated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        }
        self._save_state()

    async def _poll_account_once(self, api, account: ImapAccount) -> None:
        current_state = self._account_state(account.account_id)
        batch = await asyncio.to_thread(self._fetch_batch_sync, account, current_state)
        if batch.reset_cursor:
            self._set_cursor(account, batch.uidvalidity, batch.latest_uid)
            self._info(f"email notify cursor initialized: account={account.account_id} last_uid={batch.latest_uid}")
            return
        for message in batch.messages:
            if not await self._process_message(api, account, message):
                return
            self._set_cursor(account, batch.uidvalidity, message.uid)

    async def scheduler_loop(self, api) -> None:
        if not self.enabled:
            return
        if not self.accounts:
            self._warning("email notify enabled but no complete MAIL_IMAP_* account is configured")
            return
        self._info(f"email notify started: accounts={len(self.accounts)} poll={self.poll_seconds}s")
        while True:
            for account in self.accounts:
                try:
                    await self._poll_account_once(api, account)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self._warning(f"email notify poll failed: account={account.account_id} err={e}")
            await asyncio.sleep(self.poll_seconds)
