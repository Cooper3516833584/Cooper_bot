from __future__ import annotations

import secrets
import time
from dataclasses import replace

_CONFIRMATIONS: dict[str, tuple[int, str, str, float]] = {}


def _short_fact(row: dict) -> str:
    return f"{row['fact_id'][:8]}  {row['text']}"


async def handle_memory_command(service, identity, text: str) -> tuple[str, bool]:
    """Return reply text and whether a group admin result must be private."""
    raw = str(text or "").strip().lstrip("/／").strip()
    parts = raw.split(maxsplit=2)
    action = parts[1].lower() if len(parts) > 1 else "help"
    argument = parts[2] if len(parts) > 2 else ""
    private = identity.profile == "admin" and identity.scene == "group"
    if action in {"help", ""}:
        return ("记忆：on/off、status、remember <内容>、list、search <关键词>、forget <ID>、clear、new；群管理员可用 group on [directed|all]。", private)
    if not service.enabled:
        return ("聊天记忆总开关当前关闭；请由部署者设置 AI_MEMORY_ENABLED=true 后重启。", private)
    try:
        if action == "status":
            rows = await service.list_facts(identity)
            return (f"记忆已启用；当前可见显式记忆 {len(rows)} 条。管理员模式只保存主动 remember 内容。", private)
        if action == "on":
            await service.set_enabled(identity, True)
            await service.set_member_enabled(identity, True)
            return ("已开启当前作用域的记忆。", private)
        if action == "off":
            await service.set_member_enabled(identity, False)
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
            rows = await (service.search_facts(identity, argument) if action == "search" else service.list_facts(identity))
            return ("没有可见记忆。" if not rows else "当前记忆：\n" + "\n".join(_short_fact(x) for x in rows[:20]), private)
        if action == "forget":
            if not argument: return ("用法：/memory forget <ID>", private)
            ok = await service.forget_fact(identity, argument)
            return ("已遗忘该记忆及其派生内容。" if ok else "该记录不存在或不可管理。", private)
        if action == "clear":
            if argument.startswith("--confirm "):
                token=argument.split(maxsplit=1)[1]; data=_CONFIRMATIONS.pop(token,None)
                if not data or data[0] != identity.actor_user_id or data[1] != identity.scene or data[2] != identity.profile or data[3] < time.time(): return ("确认码无效、已过期或不属于当前作用域。", private)
                await service.clear_subject(identity); return ("已清除你在当前作用域的记忆。", private)
            token=secrets.token_urlsafe(12); _CONFIRMATIONS[token]=(identity.actor_user_id,identity.scene,identity.profile,time.time()+60)
            return (f"将清除你在当前作用域的记忆。60 秒内发送 /memory clear --confirm {token} 确认。", private)
        if action == "new":
            await service.rotate_conversation(identity); return ("已开始新对话；长期显式记忆仍会保留。", private)
        if action == "history":
            before = None
            terms = argument.split()
            if len(terms) == 2 and terms[0] == "--before" and terms[1].isdigit(): before=int(terms[1])
            rows = await service.history(identity, before=before)
            if identity.profile == "admin": return ("管理员记忆模式不保存自动聊天历史。", private)
            lines=[f"{row['seq']} {row['role']}: {row['own_text']}" for row in rows if row.get('own_text')]
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
                await service.set_enabled(group_identity, True, mode); return (f"已开启当前群记忆（{mode}）。", False)
            if sub == "off":
                await service.set_enabled(group_identity, False); return ("已停止当前群的记忆采集和召回。", False)
            if sub == "remember":
                value=argument[len("remember"):].strip(); row=await service.remember_explicit(group_identity,value,subject_id=f"group:{group_identity.group_id}"); return (f"已保存群约定（ID：{row['fact_id']}）。",False)
            return ("用法：/memory group on [directed|all]、off、remember <内容>", False)
    except ValueError as exc:
        return (f"记忆操作未完成：{exc}", private)
    except Exception:
        return ("记忆服务暂时不可用，当前聊天不会受影响。", private)
    return ("未知 memory 子命令；发送 /memory help 查看用法。", private)
