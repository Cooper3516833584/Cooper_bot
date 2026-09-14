"""Private, application-owned chat memory.

The module intentionally has no dependency on OneBot or Kimi.  Callers pass
trusted identities and the service owns the SQLite boundary.
"""

from .models import CapturedInput, MemoryIdentity
from .service import MemoryService

__all__ = ("CapturedInput", "MemoryIdentity", "MemoryService")
