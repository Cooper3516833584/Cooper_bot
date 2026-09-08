# Phase 06 handoff

Implemented Kimi-only QQ chat routing. The former `g`/`c` selectors are now ordinary user text. Public chat uses the public Kimi profile; computer routing requires an individually-admin user, the explicit admin flags, an isolated admin profile, and a deployment capability probe. Group-originated computer replies are private.

No automatic retry is performed for Kimi requests. This avoids replaying a possible computer side effect.

Validation: focused route, profile, runner, context, DeepSeek-task, calendar, and smoke tests passed locally.
