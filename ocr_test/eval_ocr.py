"""
eval_ocr.py — 将 ground_truth.json 与 OCR 实际输出对比，逐图报告差异。
运行方式：
    py ocr_test/eval_ocr.py
"""
import sys
from pathlib import Path as _P

# 结果写到文件，避免终端编码问题
_OUT_FILE = _P(__file__).parent / "eval_result.txt"
_f = open(_OUT_FILE, "w", encoding="utf-8")

class _Tee:
    def __init__(self, *streams): self.streams = streams
    def write(self, d):
        for s in self.streams:
            try: s.write(d)
            except Exception: pass
    def flush(self):
        for s in self.streams:
            try: s.flush()
            except Exception: pass
    def reconfigure(self, **_): pass

sys.stdout = _Tee(sys.stdout, _f)
sys.stderr = _Tee(sys.stderr, _f)
import json
import re
from pathlib import Path

# 把项目根目录加入 path，以便 import blackboard_ocr
ROOT = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(ROOT))

import cv2
from blackboard_ocr import recognize_homework_from_path, build_ocr

# ── 配置 ──────────────────────────────────
GT_FILE   = Path(__file__).parent / "ground_truth.json"
IMG_DIR   = Path(__file__).parent
SKIP_IMGS = set()
# ─────────────────────────────────────────


def parse_gt_line(line: str):
    """解析单行真值 'P210  4.4.5' -> ('P210', '4.4.5')"""
    line = line.strip()
    if not line:
        return None
    parts = re.split(r"\s+", line, maxsplit=1)
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    return None


def parse_gt(raw: str) -> list[tuple[str, str]]:
    result = []
    for line in raw.splitlines():
        item = parse_gt_line(line)
        if item:
            result.append(item)
    return result


def ocr_results_to_set(assignments: list[dict]) -> set[tuple[str, str]]:
    s = set()
    for a in assignments:
        page = (a.get("page") or "").strip()
        q    = (a.get("question") or "").strip()
        if page and q:
            s.add((page, q))
    return s


def fmt_item(page, q):
    return f"{page}  {q}"


def main():
    gt_raw: dict = json.loads(GT_FILE.read_text(encoding="utf-8"))

    engine = build_ocr()

    total_gt   = 0
    total_hit  = 0
    total_fp   = 0
    total_fn   = 0
    img_errors = []

    imgs = sorted(
        [p for p in IMG_DIR.iterdir()
         if p.is_file() and p.suffix.lower() in {".jpg", ".jpeg", ".png"}
         and p.stem.isdigit()],
        key=lambda p: int(p.stem),
    )

    for img_path in imgs:
        name = img_path.name
        if name in SKIP_IMGS:
            print(f"[SKIP] {name}")
            continue
        if name not in gt_raw:
            print(f"[NO GT] {name}")
            continue

        gt_list  = parse_gt(gt_raw[name])
        gt_set   = set(gt_list)

        result   = recognize_homework_from_path(str(img_path), engine=engine)
        ocr_set  = ocr_results_to_set(result.get("assignments", []))

        hits = gt_set & ocr_set
        fps  = ocr_set - gt_set
        fns  = gt_set  - ocr_set

        total_gt  += len(gt_set)
        total_hit += len(hits)
        total_fp  += len(fps)
        total_fn  += len(fns)

        ok = len(fns) == 0 and len(fps) == 0
        mark = "OK" if ok else "NG"
        print(f"\n[{mark}] {name}  (gt={len(gt_set)} ocr={len(ocr_set)} hit={len(hits)} fp={len(fps)} fn={len(fns)})")
        if not ok:
            if fns:
                for p, q in sorted(fns):
                    print(f"   FN(漏) {fmt_item(p,q)}")
            if fps:
                for p, q in sorted(fps):
                    print(f"   FP(多) {fmt_item(p,q)}")
            img_errors.append(name)

    prec = total_hit / max(total_hit + total_fp, 1) * 100
    rec  = total_hit / max(total_gt, 1) * 100
    f1   = 2 * prec * rec / max(prec + rec, 1e-9)

    print("\n" + "="*50)
    print(f"总GT条目: {total_gt}  命中: {total_hit}  漏检: {total_fn}  误报: {total_fp}")
    print(f"Precision: {prec:.1f}%  Recall: {rec:.1f}%  F1: {f1:.1f}%")
    print(f"存在误差的图片({len(img_errors)}): {img_errors}")


if __name__ == "__main__":
    main()
