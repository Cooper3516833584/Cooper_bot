# Phase 10 release report

Status: READY_FOR_DEPLOYMENT, not DEPLOYED.

Before enabling public chat, create the isolated homes and work directories from the Kimi configuration and verify the fixed Kimi version, `deepseek/deepseek-v4-flash` provider selection, and max thinking setting without exposing credentials. Keep `AI_KIMI_ADMIN_ENABLED=false` initially.

After public QQ validation, an authorized deployer may enable the administrator profile, run `probe_computer_capability()` from a trusted terminal, and only then consider private administrator computer requests. Group-originated computer requests additionally require `AI_KIMI_ALLOW_GROUP_COMPUTER=true` and always receive their reply privately.

Rollback: set `AI_KIMI_ADMIN_ENABLED=false` to disable computer access; set `AI_KIMI_ENABLED=false` to disable Kimi chat and retain DeepSeek non-chat tasks. Do not restore or auto-enable the removed legacy backend.
