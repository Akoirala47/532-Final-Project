"""
Benchmarking utilities.

Provides timing helpers and CSV result writing for the benchmark harness.
"""

import csv
import os
import time
from typing import Any, Callable, Dict, List, Optional, Tuple


def time_function(func: Callable, *args, **kwargs) -> Tuple[Any, float]:
    """
    Time a function call.

    Returns:
        Tuple of (function_result, elapsed_seconds).
    """
    start = time.perf_counter()
    result = func(*args, **kwargs)
    elapsed = time.perf_counter() - start
    return result, elapsed


def save_results(
    records: List[Dict[str, Any]],
    filepath: str,
    fieldnames: Optional[List[str]] = None,
) -> None:
    """
    Save benchmark records to a CSV file.

    Default columns match the original harness; extra keys (e.g. dataset_size,
    shuffle_partitions) are supported when fieldnames is passed or inferred.
    """
    if not records:
        raise ValueError("save_results: records list is empty")

    os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)

    if fieldnames is None:
        keys: List[str] = []
        for row in records:
            for k in row:
                if k not in keys:
                    keys.append(k)
        preferred = [
            "dataset_size",
            "shuffle_partitions",
            "analysis",
            "api_type",
            "elapsed_seconds",
            "corpus_path",
            "result_summary",
        ]
        fieldnames = [c for c in preferred if any(c in r for r in records)]
        for k in keys:
            if k not in fieldnames:
                fieldnames.append(k)

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)

    print(f"\nResults saved to: {filepath}")
