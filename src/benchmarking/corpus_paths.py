"""
Deterministic corpus subsets for scaling benchmarks.

Uses lexicographically sorted .txt files under data/corpus/ so repeated runs
compare the same documents at small / medium / large sizes.
"""

import glob
import os
from typing import Dict, List, Tuple


def project_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def list_sorted_corpus_files(corpus_dir: str) -> List[str]:
    pattern = os.path.join(corpus_dir, "*.txt")
    return sorted(glob.glob(pattern))


def subsets_by_book_count(
    corpus_dir: str,
    sizes: Tuple[int, ...] = (5, 12, 20),
) -> Dict[str, List[str]]:
    """
    Map labels like 'small' / 'medium' / 'large' to lists of absolute paths.

    Uses min(len(all), requested) when the corpus has fewer books than requested.
    """
    all_paths = list_sorted_corpus_files(corpus_dir)
    labels = ("small", "medium", "large")
    out: Dict[str, List[str]] = {}
    for label, n in zip(labels, sizes):
        out[label] = all_paths[: min(n, len(all_paths))]
    return out


def describe_subset(paths: List[str]) -> str:
    return f"{len(paths)} files"

