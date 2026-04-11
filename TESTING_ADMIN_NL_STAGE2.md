# Stage2 Admin NL Minimal Smoke Matrix

## Preconditions
- Admin account is in `ADMIN_USERS`
- `ENABLE_ADMIN_NL_CONTROL=1`
- Optional:
  - `ENABLE_ADMIN_NL_MULTI_STEP=1` to test model planner composition
  - `ENABLE_ADMIN_TARGET_ALIASES=1` and `data/admin_targets.json` filled to test aliases

## Manual Smoke Cases
1. Admin private text: `在群123456发：今晚交作业`
Expected: message sent, admin gets success summary.

2. Slash route isolation: `/find 高数`
Expected: goes to original slash pipeline, not admin NL control.

3. C-prefix isolation: `C你好`
Expected: goes to original private AI trigger, not admin NL control.

4. Alias path: `在高数群发：今晚交作业`
Expected: works when alias enabled and configured; fails with clear message when alias disabled.

5. Multi-step planner:
Input like `先帮我找高数期末复习，再发给我`
Expected:
- when `ENABLE_ADMIN_NL_MULTI_STEP=1`: planner may generate a safe plan and execute/confirm
- when `ENABLE_ADMIN_NL_MULTI_STEP=0`: no planner execution, falls back to original path

6. High-risk confirm:
Input like `给群123456连续发两条提醒：A、B`
Expected: requires confirm first, no execution before `确认`.

## Useful Test Commands
- `python -m pytest -q test/unit/test_admin_nl_flags_logging.py`
- `python -m pytest -q test/unit/test_admin_targets.py`
- `python -m pytest -q test/unit/test_admin_nl.py test/unit/test_admin_exec.py`
- `python -m pytest -q test/integration/test_commands_dispatch.py`
