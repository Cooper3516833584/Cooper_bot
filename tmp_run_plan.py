import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, r"c:\Users\Cooper\Desktop\Cooper_bot")

from aisvc import AIService

class MockLog:
    def info(self, msg): print("[INFO]", msg)
    def warning(self, msg): print("[WARN]", msg)
class MockLogSvc:
    def __init__(self):
        self.log = MockLog()

async def main():
    logsvc = MockLogSvc()
    # AIService takes 1 argument `log`
    ai = AIService(logsvc.log)
    ai._load_api_config()
    ai.chat_ready = True
    
    tools = ["cancel_handin_task", "create_handin_task", "find_files", "generate_ai_reply", "list_directory", "list_handin_tasks", "send_group_message", "send_message", "send_private_message"]
    
    plan1 = await ai._parse_admin_plan_sync("在bot测试群发一句“成功”", 123456, tools, 5)
    print("Plan 1:", plan1)
    
    plan2 = await ai._parse_admin_plan_sync("你是谁", 3516833584, tools, 5)
    print("Plan 2:", plan2)

if __name__ == "__main__":
    asyncio.run(main())
