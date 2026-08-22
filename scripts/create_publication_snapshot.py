"""Create and privacy-scan a source-only snapshot for a fresh public repository."""

import argparse
import re
import shutil
from pathlib import Path

EXCLUDED_PARTS = {
    ".agents",
    ".git",
    ".idea",
    ".venv",
    ".vscode",
    "__pycache__",
    "build",
    "dist",
    "rates",
    "sharesight_importer.egg-info",
}
EXCLUDED_SUFFIXES = {".code-workspace", ".pem", ".key", ".pyc"}
FORBIDDEN_NAMES = {"prices.csv", "dates.txt", ".env"}
PUBLIC_ROOT_FILES = {
    ".gitignore",
    ".python-version",
    "AGENTS.md",
    "CHANGELOG.md",
    "CONTRIBUTING.md",
    "LICENSE",
    "MANIFEST.in",
    "README.md",
    "SECURITY.md",
    "pyproject.toml",
    "uv.lock",
}
PUBLIC_DIRECTORIES = {".github", "docs", "examples", "scripts", "sharesight_importer"}
SECRET_PATTERNS = {
    "private key": re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    "GitHub token": re.compile(r"\bgh[pousr]_[A-Za-z0-9_]{30,}\b"),
    "OAuth bearer token": re.compile(r"\bBearer\s+[A-Za-z0-9._~-]{24,}\b"),
}


def create_snapshot(source, output):
    source = source.resolve()
    output = output.resolve()
    if output.exists() and any(output.iterdir()):
        raise ValueError(f"Output directory is not empty: {output}")
    output.mkdir(parents=True, exist_ok=True)
    copied = []
    for path in source.rglob("*"):
        relative = path.relative_to(source)
        if any(part in EXCLUDED_PARTS for part in relative.parts):
            continue
        if path.is_dir() or path.name in FORBIDDEN_NAMES or path.suffix in EXCLUDED_SUFFIXES:
            continue
        if len(relative.parts) == 1:
            if path.name not in PUBLIC_ROOT_FILES and path.suffix != ".py":
                continue
        elif relative.parts[0] not in PUBLIC_DIRECTORIES:
            continue
        destination = output / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, destination)
        copied.append(destination)
    problems = scan_snapshot(output)
    if problems:
        raise ValueError("Publication privacy scan failed: " + "; ".join(problems))
    return copied


def scan_snapshot(root):
    problems = []
    for path in root.rglob("*"):
        if path.is_dir():
            continue
        relative = path.relative_to(root)
        if path.name in FORBIDDEN_NAMES or "rates" in relative.parts:
            problems.append(f"forbidden private path {relative}")
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        for label, pattern in SECRET_PATTERNS.items():
            if pattern.search(text):
                problems.append(f"possible {label} in {relative}")
    return problems


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    source = Path(__file__).resolve().parents[1]
    copied = create_snapshot(source, args.output)
    print(f"Created privacy-scanned publication snapshot with {len(copied)} files: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
