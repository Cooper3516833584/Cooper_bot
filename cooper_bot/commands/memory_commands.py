from __future__ import annotations

import secrets
import time
from dataclasses import replace

_CONFIRMATIONS: dict[str, dict] = {}


def _short_fact(row: dict) -> str:
    text = str(row["text"] or "")
    return f"{row['fact_id'][:8]}  {text[:160]}"


async def handle_memory_command(service, identity, text: str, *, clear_admin_history=None) -> tuple[str, bool]:
    """Return reply text and whether a group admin result must be private."""
    def _clear_admin_volatile() -> None:
        if identity.profile == "admin" and callable(clear_admin_history):
            try:
                clear_admin_history()
            except Exception:
                pass

    async def _new_confirmation(target_identity, action_name: str, target: str) -> str:
        scope_id, epoch = await service.confirmation_state(target_identity)
        token = secrets.token_urlsafe(12)
        _CONFIRMATIONS[token] = {"actor": identity.actor_user_id, "scope_id": scope_id, "action": action_name, "target": target, "epoch": epoch, "expires_at": time.time() + 60}
        return token

    async def _consume_confirmation(token: str, target_identity, action_name: str, target: str) -> bool:
        data = _CONFIRMATIONS.pop(token, None)
        if not data or float(data.get("expires_at") or 0) < time.time():
            return False
        scope_id, epoch = await service.confirmation_state(target_identity)
        return data == {"actor": identity.actor_user_id, "scope_id": scope_id, "action": action_name, "target": target, "epoch": epoch, "expires_at": data["expires_at"]}

    raw = str(text or "").strip().lstrip("/／").strip()
    parts = raw.split(maxsplit=2)
    action = parts[1].lower() if len(parts) > 1 else "help"
    argument = parts[2] if len(parts) > 2 else ""
    private = identity.profile == "admin" and identity.scene == "group"
    if action in {"help", ""}:
        return ("记忆：on/off/status、remember/list/search/forget、history/new/clear。群内 on 只管理本人；可信管理员用 group on [directed|all]/off/remember/clear。off 不物理删除，new 保留长期事实，clear 不能撤回已在途请求。", private)
    try:
        if action == "status":
            status = await service.status(identity)
            if not status["master_enabled"]:
                return ("聊天记忆总开关：关闭；当前不会创建或使用持久记忆。", private)
            if not status["scope_available"]:
                return ("聊天记忆总开关：开启；当前作用域不在部署允许范围内。", private)
            profile_text = "admin explicit-only" if status["profile"] == "admin" else "public"
            return (f"总开关：开启；scope：{'开启' if status['scope_enabled'] else '关闭'}；模式：{status['capture_mode']}；本人：{'启用' if status['member_enabled'] else '已退出'}；profile：{profile_text}；可见事实：{status['visible_facts']}；向量：{'就绪' if status['embedding_ready'] else '未就绪'}（{status['embedded_facts']}/{status['total_facts']}）；summary/auto/embedding：{int(status['summary_enabled'])}/{int(status['auto_extract_enabled'])}/{int(status['embedding_enabled'])}。", private)
        if not service.enabled:
            return ("聊天记忆总开关当前关闭；请由部署者设置 AI_MEMORY_ENABLED=true 后重启。", private)
        if action == "on":
            if identity.scene == "group" and identity.profile == "public":
                await service.set_member_enabled(identity, True, require_scope_enabled=True)
            else:
                await service.set_enabled(identity, True)
                await service.set_member_enabled(identity, True)
            return ("已开启当前作用域的记忆。", private)
        if action == "off":
            await service.set_member_enabled(identity, False)
            _clear_admin_volatile()
            return ("已停止采集和使用你在当前作用域的记忆；已有数据未立即物理删除。", private)
        if action == "remember":
            if not argument:
                return ("用法：/memory remember <要记住的内容>", private)
            replace_id = None
            if argument.startswith("--replace "):
                rest = argument[len("--replace "):].split(maxsplit=1)
                if len(rest) != 2: return ("用法：/memory remember --replace <ID> <内容>", private)
                replace_id, argument = rest
            row = await service.remember_explicit(identity, argument, replace_id=replace_id)
            return (f"已记住（ID：{row['fact_id']}）。", private)
        if action in {"list", "search"}:
            page = 1
            if action == "list" and argument:
                if not argument.isdigit() or int(argument) < 1: return ("用法：/memory list [页码]", private)
                page = int(argument)
            rows = await (service.search_facts(identity, argument) if action == "search" else service.list_facts(identity))
            page_rows = rows[(page-1)*20:page*20]
            return ("没有可见记忆。" if not page_rows else f"当前记忆（第 {page} 页）：\n" + "\n".join(_short_fact(x) for x in page_rows), private)
        if action == "forget":
            if not argument: return ("用法：/memory forget <ID>", private)
            ok = await service.forget_fact(identity, argument)
            return ("已遗忘该记忆及其派生内容。" if ok else "该记录不存在或不可管理。", private)
        if action == "clear":
            if argument.startswith("--confirm "):
                token=argument.split(maxsplit=1)[1]
                if not await _consume_confirmation(token,identity,"clear",f"user:{identity.actor_user_id}"): return ("确认码无效、已过期或不属于当前作用域。", private)
                await service.clear_subject(identity); _clear_admin_volatile(); return ("已清除你在当前作用域的记忆。", private)
            token=await _new_confirmation(identity,"clear",f"user:{identity.actor_user_id}")
            return (f"将清除你在当前作用域的记忆。60 秒内发送 /memory clear --confirm {token} 确认。", private)
        if action == "new":
            if identity.scene == "group" and identity.profile == "public" and not identity.personal_admin:
                return ("仅可信个人管理员可开始新的群记忆对话。", private)
            await service.rotate_conversation(identity); _clear_admin_volatile(); return ("已开始新对话；长期显式记忆仍会保留。", private)
        if action == "history":
            before = None
            terms = argument.split()
            if "--before" in terms:
                index=terms.index("--before")
                if index+1>=len(terms) or not terms[index+1].isdigit(): return ("用法：/memory history [关键词] [--before seq]",private)
                before=int(terms[index+1]);del terms[index:index+2]
            rows = await service.history(identity, query=" ".join(terms), before=before)
            if identity.profile == "admin": return ("管理员记忆模式不保存自动聊天历史。", private)
            lines=[f"{row['seq']} {time.strftime('%Y-%m-%d %H:%M',time.localtime(float(row['created_at'])))} QQ:{row['actor_user_id']} {row['role']}/{row['source_kind']}: {str(row['own_text'])[:160]}" for row in rows if row.get('own_text')]
            return ("没有可见历史。" if not lines else "当前作用域历史：\n"+"\n".join(lines), private)
        if action == "group":
            sub = argument.split(maxsplit=1)[0].lower() if argument else ""
            if identity.scene != "group" or not identity.personal_admin:
                return ("仅可信个人管理员可管理当前群记忆。", private)
            # Group policy is always public-group policy.  An administrator's
            # private execution profile must not make a second hidden group DB.
            group_identity = replace(identity, profile="public")
            if sub == "on":
                mode=(argument.split(maxsplit=1)[1].lower() if len(argument.split(maxsplit=1)) > 1 else "directed")
                await service.set_group_enabled(group_identity, True, mode); return (f"已开启当前群记忆（{mode}）。", False)
            if sub == "off":
                await service.set_group_enabled(group_identity, False); return ("已停止当前群的记忆采集和召回。", False)
            if sub == "remember":
                value=argument[len("remember"):].strip(); row=await service.remember_explicit(group_identity,value,subject_id=f"group:{group_identity.group_id}"); return (f"已保存群约定（ID：{row['fact_id']}）。",False)
            if sub == "clear":
                clear_argument=argument[len("clear"):].strip()
                target=f"group:{group_identity.group_id}"
                if clear_argument.startswith("--confirm "):
                    token=clear_argument.split(maxsplit=1)[1]
                    if not await _consume_confirmation(token,group_identity,"group_clear",target): return ("确认码无效、已过期或不属于当前群。",False)
                    await service.clear_group(group_identity); return ("已清除当前公共群作用域的记忆数据。",False)
                token=await _new_confirmation(group_identity,"group_clear",target)
                return (f"将清除当前公共群作用域的全部记忆数据。60 秒内发送 /memory group clear --confirm {token} 确认。",False)
            return ("用法：/memory group on [directed|all]、off、remember <内容>、clear", False)
    except ValueError as exc:
        return (f"记忆操作未完成：{exc}", private)
    except Exception:
        return ("记忆服务暂时不可用，当前聊天不会受影响。", private)
    return ("未知 memory 子命令；发送 /memory help 查看用法。", private)
