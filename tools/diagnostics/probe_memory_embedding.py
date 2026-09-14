# tools/diagnostics/probe_memory_embedding.py
"""诊断脚本：核对记忆向量检索用的 embedding provider 是否真的可用。

只发送两段固定测试文本，**不发送任何聊天记录、用户内容或记忆数据**；输出不打印 api key。
用自己的 `config/private/api_key.txt` 第 3/4 行（embedding base url / key）与
`AI_EMBED_MODEL` 组装的 provider 调用 `POST {base}/embeddings`。

报告：是否就绪、请求是否成功、向量维度、同一文本两次调用的余弦自相似度、
以及两段不同文本之间的余弦（用于确认不是恒定向量）。

用法：
    python tools/diagnostics/probe_memory_embedding.py
"""
from __future__ import annotations

import sys
from pathlib import Path
from urllib.parse import urlparse

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from cooper_bot.core import config  # noqa: E402
from cooper_bot.modules.ai.model_gateway import EMBEDDING_PROVIDER, ModelGateway  # noqa: E402
from cooper_bot.modules.memory import vectors  # noqa: E402

SAMPLE_A = "测试文本甲：周三之前提交数学作业。"
SAMPLE_B = "测试文本乙：今天的天气适合跑步。"


def _redacted_host(url: str) -> str:
    return urlparse(str(url or "")).netloc or "<unset>"


def main() -> int:
    gateway = ModelGateway(log=None)
    provider = gateway.provider(EMBEDDING_PROVIDER)
    print("=== probe_memory_embedding ===")
    print(f"base url host: {_redacted_host(provider.base_url)} (path hidden)")
    print(f"api key: {'<set>' if provider.api_key else '<unset>'}")
    print(f"model: {provider.model} (from AI_EMBED_MODEL={config.AI_EMBED_MODEL})")
    if not provider.ready:
        print("ready: false -> embedding base url/key 未配置，记忆检索会退化为纯词法。")
        return 2

    first = gateway.embed(SAMPLE_A)
    if not first:
        print("ready: true, but the embeddings request failed or returned no vector.")
        return 1
    second = gateway.embed(SAMPLE_A)
    other = gateway.embed(SAMPLE_B)
    print(f"ready: true; dimension: {len(first)}")
    print(f"same-text cosine: {vectors.cosine(first, second) if second else 'n/a'}")
    print(f"cross-text cosine: {vectors.cosine(first, other) if other else 'n/a'}")
    print("note: 这里只发了两段固定测试文本，未发送任何聊天内容或记忆数据。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
