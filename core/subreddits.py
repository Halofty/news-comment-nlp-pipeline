from __future__ import annotations

from pathlib import Path


def load_subreddit_allowlist(path: Path) -> set[str]:
    names = {
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }
    if not names:
        raise ValueError(f"subreddit allowlist is empty: {path}")
    folded = [name.casefold() for name in names]
    if len(folded) != len(set(folded)):
        raise ValueError(f"subreddit allowlist contains case-insensitive duplicates: {path}")
    return names
