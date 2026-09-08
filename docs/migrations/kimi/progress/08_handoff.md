# Phase 08 handoff

Production routing and configuration no longer select Gemini, Claude, or Antigravity. The old policy file and dedicated backend tests were removed, and help now documents Kimi chat and the removed prefix behavior.

DeepSeek remains for its non-chat task APIs, including calendar rendering, email, notice, material indexing, and embeddings. Legacy private implementation helpers that are no longer reachable are retained temporarily only where their removal would require a broad rewrite of `aisvc.py`; they have no public route or configuration source. Their removal is tracked as a remaining cleanup item rather than a deployment prerequisite.
