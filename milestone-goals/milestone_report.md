# Milestone Goals Report

**Project:** PySpark Performance Benchmarking Study  
**Team:** Aayush Koirala, Benson Zheng · **Date:** April 14, 2026

---

## Milestone 1: Data Acquisition

**Goal:** Build a 10–50 book Gutenberg corpus, cleaned and unified.

**Done:** `data/download_books.py` pulls 20 public-domain titles from `gutenberg.org/cache/epub/{id}/`, strips START/END boilerplate via regex, writes to `data/corpus/`. Genres/eras vary; **~2,047,705 words** total (titles include *Pride and Prejudice*, *Moby Dick*, *Jane Eyre*, *The Iliad*, *Dracula*, etc.—full ID list in script or repo docs if needed).

---

## Milestone 2: Validation

**Goal:** Run WordCount, WordFrequency, WordPairs on the corpus; confirm correctness.

**Done:** Homework scripts refactored to `spark.read.text("data/corpus/*.txt")`, CLI path args, and shared functions (`run_word_count`, `run_word_frequency`, `run_word_pairs`) for the benchmark harness.

**Checks:** WordCount **2,047,705** (matches ingest). WordFrequency top token **the** (100,764). WordPairs top pair **(the, the)** — 127,540. Outcomes align with expectations for English fiction.

---

## Milestone 3: Spark SQL Refactoring

**Goal:** Reimplement all three analyses in Spark SQL; match DataFrame API results.

**Done:** `src/spark_sql/` scripts use temp views + `spark.sql()`. Word pairs use `LATERAL VIEW posexplode()` to mirror the DataFrame path. **Outputs match** the DataFrame versions.

---

## Milestone 4: Benchmarking Infrastructure

**Goal:** Timing harness and persisted results.

**Done:** `src/benchmarking/utils.py` — `time_function` (`perf_counter`), `save_results` → CSV (`analysis`, `api_type`, `elapsed_seconds`, `corpus_path`, `result_summary`). `benchmark_runner.py` — one `SparkSession`, runs 3 DataFrame + 3 SQL jobs, prints summary, writes `results/benchmark_results.csv`. Intended for scaling (corpus size / partitions) in the final phase.

**Sample (representative run):** WordCount ~2.45s (DataFrame) vs ~0.27s (SQL); WordFrequency / WordPairs ~0.04–0.09s; all result summaries match (e.g. 2,047,705 words; top word **the**; top pair **(the, the)** 127,540).
