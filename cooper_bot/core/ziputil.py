from __future__ import annotations

from pathlib import Path
import zipfile


# These formats are already compressed in most cases.
ZIP_STORED_SUFFIXES = {
    ".zip", ".rar", ".7z", ".gz", ".bz2", ".xz", ".zst", ".lz4",
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".heic", ".heif", ".avif",
    ".mp3", ".aac", ".m4a", ".flac", ".ogg", ".opus", ".wav", ".wma",
    ".mp4", ".mkv", ".avi", ".mov", ".wmv", ".flv", ".webm", ".m4v",
    ".pdf", ".docx", ".xlsx", ".pptx", ".odt", ".ods", ".odp",
    ".apk", ".ipa", ".jar", ".war", ".ear", ".whl",
    ".exe", ".msi", ".dmg", ".iso",
    ".woff", ".woff2", ".ttf", ".otf",
}

FAST_ZIP_COMPRESSLEVEL = 1


def _compress_type_for_name(name: str) -> int:
    suffix = Path(name).suffix.lower()
    if suffix in ZIP_STORED_SUFFIXES:
        return zipfile.ZIP_STORED
    return zipfile.ZIP_DEFLATED


def open_fast_zip(out_zip: Path) -> zipfile.ZipFile:
    """Open a zip file with faster defaults for bot-side packaging."""
    try:
        return zipfile.ZipFile(
            out_zip,
            "w",
            compression=zipfile.ZIP_DEFLATED,
            compresslevel=FAST_ZIP_COMPRESSLEVEL,
        )
    except TypeError:
        # Fallback for older Python versions without compresslevel in constructor.
        return zipfile.ZipFile(out_zip, "w", compression=zipfile.ZIP_DEFLATED)


def write_path(zf: zipfile.ZipFile, src: Path, arcname: str) -> None:
    zf.write(src, arcname=arcname, compress_type=_compress_type_for_name(arcname))
