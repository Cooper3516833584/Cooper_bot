import csv
import json
import re
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
WIKI_DIR = Path(__file__).resolve().parent
ROOT = REPO / "data" / "public" / "textbook_and_material"
DB = REPO / "ai_semantic_store.sqlite3"

OUT_JSON = WIKI_DIR / "1037wiki_upload_plan.json"
OUT_CSV = WIKI_DIR / "1037wiki_upload_plan.csv"
OUT_MD = WIKI_DIR / "1037wiki_upload_plan.md"

PARENT_CATEGORY = "\u8bfe\u7a0b"
COURSE_MATERIAL = "\u8bfe\u7a0b\u8d44\u6599"
REVIEW_MATERIAL = "\u590d\u4e60\u8d44\u6599"
EXPERIMENT_MATERIAL = "\u5b9e\u9a8c\u8d44\u6599"
PAST_EXAMS = "\u5f80\u5e74\u9898"
SLIDES = "\u8bfe\u4ef6"
TEXTBOOK = "\u6559\u6750"

CATEGORY_RULES = [
    (TEXTBOOK, ["\u6559\u6750", "\u8bfe\u672c", "\u7535\u5b50\u6280\u672f\u57fa\u7840", "Z-Library", "\u4e2d\u6587\u7248", "\u7b2c\u516d\u7248", "\u7b2c\u4e03\u7248"]),
    (EXPERIMENT_MATERIAL, ["\u5b9e\u9a8c", "\u62a5\u544a", "\u4eff\u771f", "simulation", "lesson"]),
    (PAST_EXAMS, ["\u771f\u9898", "\u8bd5\u5377", "\u56de\u5fc6\u7248", "A\u5377", "B\u5377", "\u8003\u8bd5", "\u671f\u672b", "\u671f\u4e2d"]),
    (REVIEW_MATERIAL, ["\u590d\u4e60", "\u91cd\u70b9", "\u63d0\u7eb2", "\u4e32\u8bb2", "\u77e5\u8bc6\u70b9", "\u9898\u5e93", "\u5f00\u5377", "\u7b54\u6848", "\u4e60\u9898", "\u4f5c\u4e1a"]),
    (SLIDES, ["\u8bfe\u4ef6", "ppt", "pptx", "\u8bb2\u4e49", "slides"]),
]


def compact_text(value: object, limit: int) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip())
    return text[:limit]


def clean_tag(value: object) -> str:
    return compact_text(value, 30)


def stem_name(filename: object) -> str:
    return Path(str(filename or "")).stem.strip()


def normalize_subject(subject: object, rel: str) -> str:
    text = str(subject or "").strip()
    if not text:
        parts = Path(rel).parts
        text = parts[0] if parts else COURSE_MATERIAL
    if Path(text).suffix:
        return stem_name(text)
    return text


def guess_category(text: str, ext: str = "") -> str:
    lowered = str(text or "").lower()
    for category, words in CATEGORY_RULES:
        if any(word.lower() in lowered for word in words):
            return category
    if ext.lower() in {".ppt", ".pptx"}:
        return SLIDES
    return COURSE_MATERIAL


def group_key(rel: str, obj: dict) -> tuple[str, str]:
    parts = Path(rel).parts
    subject = normalize_subject(obj.get("subject"), rel)
    filename = obj.get("filename") or (parts[-1] if parts else rel)
    ext = Path(str(filename)).suffix
    combined = " ".join(
        [
            rel,
            str(filename),
            " ".join(map(str, obj.get("keywords") or [])),
            str(obj.get("summary") or ""),
        ]
    )
    if len(parts) <= 2:
        return subject, guess_category(combined, ext)
    second = parts[1]
    if len(parts) >= 4 and re.match(r"(?i)^chapter[._ -]?\d+", parts[2]):
        return subject, f"{second} {parts[2]}"
    return subject, second


def safe_title(subject: str, category: str, files: list[dict]) -> str:
    subject = compact_text(subject, 40)
    category = compact_text(category, 40)
    if len(files) == 1:
        base = compact_text(stem_name(files[0]["filename"]), 60)
        if base:
            return base
    if category == COURSE_MATERIAL or category == subject:
        return compact_text(f"{subject}{COURSE_MATERIAL}", 60)
    return compact_text(f"{subject}{category}", 60)


def build_description(subject: str, category: str, files: list[dict]) -> str:
    count = len(files)
    size_mb = sum(f["size"] for f in files) / 1024 / 1024
    top_keywords = [
        keyword for keyword, _count in Counter(k for f in files for k in f["keywords"]).most_common(8)
    ]
    sample_summaries: list[str] = []
    seen: set[str] = set()
    for item in files:
        summary = str(item.get("summary") or "").strip()
        if summary and summary not in seen:
            sample_summaries.append(summary)
            seen.add(summary)
        if len(sample_summaries) >= 2:
            break

    desc = f"\u6536\u5f55{subject}\u76f8\u5173{category}\uff0c\u5171{count}\u4e2a\u6587\u4ef6\uff0c\u7ea6{size_mb:.1f}MB\u3002"
    if top_keywords:
        desc += "\u5173\u952e\u8bcd\uff1a" + "\u3001".join(top_keywords[:8]) + "\u3002"
    if sample_summaries:
        desc += "\u5185\u5bb9\u6982\u89c8\uff1a" + " ".join(sample_summaries)[:180]
    return desc[:500]


def load_rows() -> list[dict]:
    rows: list[dict] = []
    conn = sqlite3.connect(DB)
    try:
        for rel, payload in conn.execute("SELECT rel, payload FROM index_items ORDER BY rel"):
            obj = json.loads(payload)
            path = ROOT / rel
            if not path.exists() or not path.is_file():
                continue
            filename = obj.get("filename") or path.name
            rows.append(
                {
                    "rel": rel,
                    "abs_path": str(path),
                    "subject": normalize_subject(obj.get("subject"), rel),
                    "filename": filename,
                    "keywords": [clean_tag(k) for k in (obj.get("keywords") or []) if clean_tag(k)],
                    "summary": str(obj.get("summary") or "").strip(),
                    "size": path.stat().st_size,
                    "ext": path.suffix.lower(),
                }
            )
    finally:
        conn.close()
    return rows


def build_plan(rows: list[dict]) -> list[dict]:
    groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for row in rows:
        groups[group_key(row["rel"], row)].append(row)

    plan: list[dict] = []
    for (subject, category), files in sorted(groups.items(), key=lambda item: (item[0][0], item[0][1])):
        files = sorted(files, key=lambda item: item["rel"])
        top_keywords = [
            keyword for keyword, _count in Counter(k for f in files for k in f["keywords"]).most_common(8)
        ]
        tags: list[str] = []
        for tag in [subject, category] + top_keywords:
            tag = clean_tag(tag)
            if tag and tag not in tags:
                tags.append(tag)
        size_mb = round(sum(f["size"] for f in files) / 1024 / 1024, 2)
        plan.append(
            {
                "title": safe_title(subject, category, files),
                "parent_category": PARENT_CATEGORY,
                "subject": subject,
                "material_category": category,
                "tags": tags[:10],
                "description": build_description(subject, category, files),
                "file_count": len(files),
                "size_mb": size_mb,
                "upload_mode_hint": "single_page_multi_file"
                if len(files) <= 50 and size_mb <= 500
                else "consider_zip_or_split",
                "files": [
                    {
                        "rel": f["rel"],
                        "path": f["abs_path"],
                        "filename": f["filename"],
                        "size_mb": round(f["size"] / 1024 / 1024, 2),
                        "summary": f["summary"],
                        "keywords": f["keywords"],
                    }
                    for f in files
                ],
            }
        )
    return plan


def write_outputs(rows: list[dict], plan: list[dict]) -> None:
    OUT_JSON.write_text(
        json.dumps(
            {
                "source_root": str(ROOT),
                "indexed_files": len(rows),
                "page_count": len(plan),
                "pages": plan,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
        newline="\n",
    )

    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "title",
                "parent_category",
                "subject",
                "material_category",
                "tags",
                "description",
                "file_count",
                "size_mb",
                "upload_mode_hint",
            ],
        )
        writer.writeheader()
        for page in plan:
            writer.writerow(
                {key: (";".join(page[key]) if key == "tags" else page[key]) for key in writer.fieldnames}
            )

    risk_pages = [page for page in plan if page["file_count"] > 50 or page["size_mb"] > 500]
    lines = [
        "# 1037wiki \u4e0a\u4f20\u6e05\u5355\u8349\u6848",
        "",
        f"- \u6e90\u76ee\u5f55\uff1a`{ROOT}`",
        f"- \u5df2\u7eb3\u5165\u7d22\u5f15\u6587\u4ef6\uff1a{len(rows)}",
        f"- \u9884\u8ba1\u9875\u9762\u6570\uff1a{len(plan)}",
        f"- \u603b\u5927\u5c0f\uff1a{sum(page['size_mb'] for page in plan):.2f} MB",
        "",
        "## \u5927\u9875\u9762/\u9700\u62c6\u5206\u6216\u538b\u7f29",
    ]
    if risk_pages:
        for page in sorted(risk_pages, key=lambda x: (x["file_count"], x["size_mb"]), reverse=True)[:40]:
            lines.append(
                f"- {page['title']}\uff1a{page['file_count']} \u4e2a\u6587\u4ef6\uff0c"
                f"{page['size_mb']} MB\uff0c\u5efa\u8bae `{page['upload_mode_hint']}`"
            )
    else:
        lines.append("- \u65e0")

    lines.extend(["", "## \u9875\u9762\u9884\u89c8"])
    for page in plan[:120]:
        lines.append(
            f"- {page['title']} | {page['file_count']} \u6587\u4ef6 | {page['size_mb']} MB | "
            f"\u6807\u7b7e\uff1a{', '.join(page['tags'][:6])}"
        )
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")


def main() -> None:
    rows = load_rows()
    plan = build_plan(rows)
    write_outputs(rows, plan)
    risk_count = len([page for page in plan if page["file_count"] > 50 or page["size_mb"] > 500])
    print(
        json.dumps(
            {
                "indexed_files": len(rows),
                "page_count": len(plan),
                "risk_pages": risk_count,
                "json": str(OUT_JSON),
                "csv": str(OUT_CSV),
                "md": str(OUT_MD),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
