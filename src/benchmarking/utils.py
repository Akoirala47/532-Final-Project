"""
Benchmarking utilities.

Provides timing helpers and CSV result writing for the benchmark harness.
"""

import csv
import os
import time
from typing import Any, Callable, Dict, List, Tuple


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


def save_results(records: List[Dict], filepath: str) -> None:
    """
    Save benchmark records to a CSV file.

    Each record is a dict with keys like:
        analysis, api_type, elapsed_seconds, corpus_path, result_summary
    """
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    fieldnames = ["analysis", "api_type", "elapsed_seconds", "corpus_path", "result_summary"]

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    print(f"\nResults saved to: {filepath}")
