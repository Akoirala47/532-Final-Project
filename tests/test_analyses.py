"""Correctness checks: DataFrame vs Spark SQL on tiny fixtures."""

import os

import pytest
from pyspark.sql import SparkSession

from src.dataframe_api.tfidf import run_tfidf
from src.dataframe_api.word_count import run_word_count
from src.dataframe_api.word_frequency import run_word_frequency
from src.dataframe_api.word_pairs import run_word_pairs
from src.spark_sql.tfidf_sql import run_tfidf_sql
from src.spark_sql.word_count_sql import run_word_count_sql
from src.spark_sql.word_frequency_sql import run_word_frequency_sql
from src.spark_sql.word_pairs_sql import run_word_pairs_sql

FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixtures")
FIXTURE_PATHS = sorted(
    os.path.join(FIXTURE_DIR, f)
    for f in os.listdir(FIXTURE_DIR)
    if f.endswith(".txt")
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("pytest-spark")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("WARN")
    yield session
    session.stop()


def test_word_count_matches_sql(spark: SparkSession):
    df_n = run_word_count(spark, FIXTURE_PATHS)
    sql_n = run_word_count_sql(spark, FIXTURE_PATHS)
    assert df_n == sql_n
    assert df_n > 0


def test_word_frequency_top_word_matches(spark: SparkSession):
    df_r = run_word_frequency(spark, FIXTURE_PATHS).collect()[0]
    sql_r = run_word_frequency_sql(spark, FIXTURE_PATHS).collect()[0]
    assert df_r["words"] == sql_r["word"]
    assert df_r["count"] == sql_r["count"]


def test_word_pairs_top_pair_matches(spark: SparkSession):
    df_r = run_word_pairs(spark, FIXTURE_PATHS).collect()[0]
    sql_r = run_word_pairs_sql(spark, FIXTURE_PATHS).collect()[0]
    assert df_r["word1"] == sql_r["word1"]
    assert df_r["word2"] == sql_r["word2"]
    assert df_r["count"] == sql_r["count"]


def test_tfidf_dataframe_matches_sql(spark: SparkSession):
    df_rows = sorted(
        (r["word"], round(float(r["max_tfidf"]), 6))
        for r in run_tfidf(spark, FIXTURE_PATHS).collect()
    )
    sql_rows = sorted(
        (r["word"], round(float(r["max_tfidf"]), 6))
        for r in run_tfidf_sql(spark, FIXTURE_PATHS).collect()
    )
    assert df_rows == sql_rows


def test_read_lines_accepts_glob_string(spark: SparkSession):
    """Regression: single glob string used by CLI scripts."""
    one_file = os.path.join(FIXTURE_DIR, "doc_alpha.txt")
    from src.readers import read_lines

    df = read_lines(spark, one_file)
    assert df.count() >= 1
