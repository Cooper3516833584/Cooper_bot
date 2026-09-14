from cooper_bot.modules.memory.models import CapturedInput, MemoryIdentity
from cooper_bot.modules.memory.policy import MemoryPolicy, may_capture, scope_for


def test_scope_uses_trusted_ids_and_admin_is_separate():
    public=MemoryIdentity(1,2,"group",3,"public")
    admin=MemoryIdentity(1,2,"group",3,"admin",True)
    assert scope_for(public) != scope_for(admin)
    assert may_capture(admin,CapturedInput("x","text",source_kind="direct_chat"),MemoryPolicy(scope_for(admin),True,"directed",True),True) is False
