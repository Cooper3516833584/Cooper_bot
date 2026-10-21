"""Render the production candidate/mask pipeline, optionally using WSL ssocr.

python -m tools.diagnostics.render_signin_ocr test/ocr --wsl-ssocr
"""
from __future__ import annotations

import argparse
from dataclasses import asdict
import json
from pathlib import Path
import time

import cv2
import numpy as np

from cooper_bot.modules.vision import signin_ocr as ocr


def render(image: np.ndarray, panels, readings, result, name: str) -> np.ndarray:
    width = 1100
    preview = cv2.resize(image, (width, round(image.shape[0] * width / image.shape[1])))
    for index, panel in enumerate(panels):
        points = np.round(panel.quad * width / image.shape[1]).astype(np.int32)
        cv2.polylines(preview, [points], True, (0, 255, 0), 2)
        cv2.putText(preview, f"#{index + 1}", tuple(points.min(axis=0)), 0, 0.7, (0, 255, 0), 2)
    rows = [preview]

    def row(label, content=None):
        canvas = np.full((185 if content is not None else 50, width, 3), 28, np.uint8)
        cv2.putText(canvas, label, (12, 28), 0, 0.6, (240, 240, 240), 1, cv2.LINE_AA)
        if content is not None:
            if content.ndim == 2:
                content = cv2.cvtColor(content, cv2.COLOR_GRAY2BGR)
            scale = min((width - 24) / content.shape[1], 140 / content.shape[0])
            tile = cv2.resize(content, None, fx=scale, fy=scale, interpolation=cv2.INTER_NEAREST)
            canvas[40:40 + tile.shape[0], 12:12 + tile.shape[1]] = tile
        rows.append(canvas)

    row(f"{name} | {result.time_text or 'NO CONSENSUS'} | {result.reason}")
    for index, panel in enumerate(panels):
        row(f"#{index + 1} rectified panel | geometry {panel.score:.2f}", panel.crop)
        levels = (0.4, 0.6, 0.8)
        if any(r.candidate_id == index and r.reason in {"ssocr_red_0.7", "ssocr_red_0.9"} for r in readings):
            levels += (0.7, 0.9)
        for variant, mask in ocr._ssocr_mask_variants(panel.crop, levels=levels):
            texts = [r.time_text for r in readings if r.candidate_id == index and r.reason == variant]
            row(f"#{index + 1} {variant} | {' / '.join(texts) or 'rejected'}", mask)
    return np.vstack(rows)


def contact_tile(panels, result, name: str) -> np.ndarray:
    tile = np.full((230, 550, 3), 28, np.uint8)
    cv2.putText(tile, name, (10, 22), 0, 0.48, (240, 240, 240), 1)
    cv2.putText(tile, result.time_text or "NO CONSENSUS", (10, 46), 0, 0.6, (0, 220, 100), 1)
    if panels:
        panel = panels[result.candidate_id]
        masks = ocr._ssocr_mask_variants(panel.crop, levels=(0.4, 0.6, 0.8, 0.7, 0.9))
        chosen = next((mask for variant, mask in masks if result.reason.startswith(variant)), None)
        for content, y in ((panel.crop, 55), (chosen, 140)):
            if content is None:
                continue
            if content.ndim == 2:
                content = cv2.cvtColor(content, cv2.COLOR_GRAY2BGR)
            scale = min(530 / content.shape[1], 80 / content.shape[0])
            content = cv2.resize(content, None, fx=scale, fy=scale, interpolation=cv2.INTER_NEAREST)
            tile[y:y + content.shape[0], 10:10 + content.shape[1]] = content
    return tile


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--pattern", default="*.jpg", help="Input glob, default: *.jpg")
    parser.add_argument("--output-dir", type=Path, help="Write previews/report here instead of beside the inputs")
    parser.add_argument("--wsl-ssocr", action="store_true", help="Explicit diagnostic backend; does not change bot configuration")
    parser.add_argument("--labels", type=Path, help="Optional JSON mapping image filename to manually verified HH:MM:SS")
    args = parser.parse_args()
    executable = ("wsl.exe", "-e", "ssocr") if args.wsl_ssocr else ocr._find_ssocr_executable()
    if not executable:
        parser.error("ssocr is not on PATH; install it or explicitly use --wsl-ssocr")
    labels = json.loads(args.labels.read_text(encoding="utf-8")) if args.labels else {}
    records = []
    tiles = []
    files = [p for p in sorted(args.directory.glob(args.pattern))
             if p.is_file() and not p.name.endswith(".signin_debug.png") and p.name != "signin_ocr_contact.png"]
    if not files:
        parser.error("No input images matched")
    output_dir = args.output_dir or args.directory
    output_dir.mkdir(parents=True, exist_ok=True)
    for path in files:
        started = time.perf_counter()
        image = cv2.imread(str(path))
        if image is None:
            records.append({"file": path.name, "error": "image_read_failed"})
            continue
        panels = ocr._display_candidates(image)
        readings = ocr._recognize_ssocr_candidates(panels, executable)
        best = ocr._select_consensus_candidate(readings)
        result = best or ocr._ParsedCandidate("", 0.0, 0.0, "no_ssocr_consensus")
        tiles.append(contact_tile(panels, result, path.name))
        output = output_dir / (path.stem + ".signin_debug.png")
        if not cv2.imwrite(str(output), render(image, panels, readings, result, path.name)):
            raise OSError(f"Could not render {output}")
        record = {
            "file": path.name, "time": result.time_text or None, "reason": result.reason,
            "precision": "minute" if len(result.time_text) == 5 else "second" if result.time_text else None,
            "panels": len(panels), "readings": [asdict(item) for item in readings],
            "seconds": round(time.perf_counter() - started, 3),
        }
        if path.name in labels:
            expected = labels[path.name]
            record.update(expected=expected, correct=bool(result.time_text) and
                          result.time_text == expected[:len(result.time_text)])
        records.append(record)
        print(f"{path.name}\t{result.time_text or '<none>'}\t{record.get('correct', 'unlabelled')}", flush=True)
    report = {"backend": executable, "images": records}
    (output_dir / "signin_ocr_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    if tiles:
        if len(tiles) % 2:
            tiles.append(np.zeros_like(tiles[0]))
        sheet = np.vstack([np.hstack(tiles[index:index + 2]) for index in range(0, len(tiles), 2)])
        if not cv2.imwrite(str(output_dir / "signin_ocr_contact.png"), sheet):
            raise OSError("Could not render contact sheet")
    labelled = [record for record in records if "correct" in record]
    print(f"Recognized: {sum(bool(r.get('time')) for r in records)}/{len(records)}; "
          f"labelled correct: {sum(r['correct'] for r in labelled)}/{len(labelled)}")
    return 0 if all(r.get("time") and r.get("correct", True) for r in records) else 1


if __name__ == "__main__":
    raise SystemExit(main())
