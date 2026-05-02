"""Shared Spark readers for text corpora (line mode vs whole-file / document mode)."""

from typing import Sequence, Union

from pyspark.sql import DataFrame, SparkSession

PathLike = Union[str, Sequence[str]]


def read_lines(spark: SparkSession, input_paths: PathLike) -> DataFrame:
    """One row per input line (matches WordCount / WordFrequency scripts)."""
    if isinstance(input_paths, (list, tuple)):
        if not input_paths:
            raise ValueError("read_lines: empty path list")
        # Pass a single list as `paths`; never *unpack — the 2nd positional arg is `wholetext`.
        return spark.read.text(list(input_paths))
    return spark.read.text(input_paths)


def read_wholetext_documents(spark: SparkSession, input_paths: PathLike) -> DataFrame:
    """One row per file; use for document-level analyses such as TF-IDF."""
    if isinstance(input_paths, (list, tuple)):
        if not input_paths:
            raise ValueError("read_wholetext_documents: empty path list")
        return spark.read.text(list(input_paths), wholetext=True)
    return spark.read.text(input_paths, wholetext=True)
