"""Unified local entrypoint for project tests."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent
TEST_ROOT = PROJECT_ROOT / "test"
NAMED_TARGETS = {
    "all": TEST_ROOT,
    "unit": TEST_ROOT / "unit",
    "integration": TEST_ROOT / "integration",
    "smoke": TEST_ROOT / "smoke",
}


def parse_args(argv: list[str]) -> tuple[argparse.Namespace, list[str]]:
    parser = argparse.ArgumentParser(
        description="Run project tests by category or folder.",
    )
    parser.add_argument(
        "target",
        nargs="?",
        default="all",
        help="all | unit | integration | smoke | <path-under-test/>",
    )
    parser.add_argument("-k", "--keyword", help="Only run tests matching expression.")
    parser.add_argument("-m", "--marker", help="Only run tests with matching marker.")
    parser.add_argument("--maxfail", type=int, help="Stop after N test failures.")
    parser.add_argument("--lf", action="store_true", help="Run last failed tests first.")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output.")
    return parser.parse_known_args(argv)


def resolve_target(target: str) -> Path:
    if target in NAMED_TARGETS:
        resolved = NAMED_TARGETS[target]
    else:
        resolved = (TEST_ROOT / target).resolve()
        test_root_resolved = TEST_ROOT.resolve()
        try:
            resolved.relative_to(test_root_resolved)
        except ValueError as exc:
            raise ValueError("Target must be inside test/ directory.") from exc

    if not resolved.exists():
        raise ValueError(f"Test target does not exist: {resolved}")
    return resolved


def build_pytest_args(
    args: argparse.Namespace,
    extra: list[str],
    target_path: Path,
) -> list[str]:
    pytest_args: list[str] = [str(target_path)]

    if args.keyword:
        pytest_args.extend(["-k", args.keyword])
    if args.marker:
        pytest_args.extend(["-m", args.marker])
    if args.maxfail is not None:
        pytest_args.append(f"--maxfail={args.maxfail}")
    if args.lf:
        pytest_args.append("--lf")
    if args.verbose:
        pytest_args.append("-v")

    pytest_args.extend(extra)
    return pytest_args


def _target_label(raw_target: str, resolved: Path) -> str:
    if raw_target in ("", "all"):
        return "all tests"
    if raw_target in NAMED_TARGETS:
        return f"{raw_target} tests"
    try:
        rel = resolved.relative_to(PROJECT_ROOT).as_posix()
    except Exception:
        rel = str(resolved)
    return rel


def main(argv: list[str] | None = None) -> int:
    args, extra = parse_args(argv or sys.argv[1:])
    try:
        target_path = resolve_target(args.target)
        pytest_args = build_pytest_args(args, extra, target_path)
    except ValueError as exc:
        print(f"[test_client] {exc}", file=sys.stderr)
        return 2

    label = _target_label(args.target, target_path)
    print(f"[test_client] Running scope: {label}")
    print(f"[test_client] Target path: {target_path}")
    if extra:
        print(f"[test_client] Extra pytest args: {' '.join(extra)}")

    rc = int(pytest.main(pytest_args))
    print(f"[test_client] Pytest return code: {rc}")
    print(f"[test_client] Result: {'ALL PASSED' if rc == 0 else 'FAILED'}")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
