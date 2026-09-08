# Kimi production hardening: no version pin

Status: implementation and mocked verification complete; real Kimi and QQ acceptance is `BLOCKED_EXTERNAL`.

## Readiness contract

`kimi --version` is diagnostic data only. A version change invalidates the capability cache, then the public capability must be probed again. It never succeeds or fails readiness by string comparison.

Public chat is fail-closed unless all of the following are true:

- static validation accepts the isolated public home, external workdir, `public.md`, empty skills directory, no MCP server, and public `config.toml`;
- the public global policy is exactly `[tools].enabled = ["WebSearch"]`;
- a matching cached probe, or a trusted fresh probe, confirms the stream protocol and blocks Bash, Read, and Write;
- no public request has observed a non-WebSearch tool call.

The sample policy is [`config/ai/kimi/public_home_security.example.toml`](../../../config/ai/kimi/public_home_security.example.toml). Merge it into the real public Kimi home without replacing provider, model, or authentication configuration.

The capability cache is `runtime/state/ai/kimi_capabilities.json`. It is an optimization only. Static validation always runs at startup, and cache reuse requires unchanged executable path, diagnostic version, public/admin agent files, public `config.toml`, and public MCP state.

## Trusted deployment probe

From a trusted local administrator session, instantiate `AIService` and await `probe_kimi_capabilities()`. It issues explicit public probes for Bash, Read, Write, and WebSearch; it writes only a harmless public-workdir probe file and removes it afterwards. With admin enabled, it also requires an observed Bash invocation and `KIMI_COMPUTER_PROBE` marker.

Do not run this from QQ input. If a probe or runtime audit fails, leave public chat blocked and repair the current Kimi CLI/configuration contract; do not weaken the tool allowlist, inherit bot secrets, set `KIMI_CODE_EXPERIMENTAL_FLAG`, or fall back to an unrestricted profile.

## Real-machine acceptance record

Not run in this workspace:

- `kimi --version` diagnostic capture: `NOT_RUN`.
- public QQ Bash/Read/prompt-injection attacks: `BLOCKED_EXTERNAL`.
- public WebSearch observation: `BLOCKED_EXTERNAL`.
- private administrator Bash marker: `BLOCKED_EXTERNAL`.
- non-administrator and group-level escalation checks: `BLOCKED_EXTERNAL`.

Mocked coverage verifies legacy and current stream-json tool structures, public forbidden-tool rejection, cache invalidation on version/config fingerprint change, and admin Bash isolation. These are not substitutes for the external acceptance steps above.
