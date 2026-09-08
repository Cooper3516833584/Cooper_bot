# Phase 10 release report

Status: READY_FOR_DEPLOYMENT, not DEPLOYED.

Before enabling public chat, create the isolated homes and work directories from the Kimi configuration, merge `config/ai/kimi/public_home_security.example.toml` into the public home, and run a trusted capability probe. Record `kimi --version` only as diagnostics; it is not a readiness gate. Keep `AI_KIMI_ADMIN_ENABLED=false` initially.

After public QQ validation, an authorized deployer may enable the administrator profile, run `probe_kimi_capabilities()` from a trusted terminal, and only then consider private administrator computer requests. Group-originated computer requests additionally require `AI_KIMI_ALLOW_GROUP_COMPUTER=true` and always receive their reply privately.

Rollback: set `AI_KIMI_ADMIN_ENABLED=false` to disable computer access; set `AI_KIMI_ENABLED=false` to disable Kimi chat and retain DeepSeek non-chat tasks. Do not restore or auto-enable the removed legacy backend.
