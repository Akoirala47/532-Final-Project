# PySpark Performance Benchmarking Study

**Team:** Aayush Koirala, Benson Zheng  
**Course:** COMPSCI 532 — Systems for Data Science (SP26)

---

## Overview

This project benchmarks PySpark text workloads over a multi-book Gutenberg corpus: **WordCount**, **WordFrequency**, **WordPairs**, and a new **TF-IDF** analysis (document = one `.txt` file). Each analysis has a **DataFrame API** and a **Spark SQL** implementation. Scripts compare runtimes across **three corpus sizes**, tune **`spark.sql.shuffle.partitions`**, and save CSV + matplotlib charts under `results/`.

For a full narrative (objectives, what was achieved, and how to read every CSV and figure), see **`report.md`**.

---

## Prerequisites

- **Python** 3.10+ (tested with 3.14 in development; use a venv).
- **Java** 17 or 11 (required by Spark). On macOS with Homebrew: `brew install openjdk@17`, then export `JAVA_HOME` as instructed by `brew info openjdk@17`.
- Network access to download books from Project Gutenberg (first-time corpus build).

---

## Setup

From the project root:

```bash
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
export PYTHONPATH="$(pwd)"          # so `src.*` imports resolve
```

Download and clean the corpus (writes `data/corpus/*.txt`; directory is gitignored by default):

```bash
python data/download_books.py
```

---

## Running analyses (single corpus glob)

All scripts accept an optional path or glob; default is `data/corpus/*.txt`.

| Task | Command |
|------|---------|
| Word count (DataFrame) | `python src/dataframe_api/word_count.py` |
| Word count (SQL) | `python src/spark_sql/word_count_sql.py` |
| Word frequency (DataFrame) | `python src/dataframe_api/word_frequency.py` |
| Word frequency (SQL) | `python src/spark_sql/word_frequency_sql.py` |
| Word pairs (DataFrame) | `python src/dataframe_api/word_pairs.py` |
| Word pairs (SQL) | `python src/spark_sql/word_pairs_sql.py` |
| TF-IDF (DataFrame) | `python src/dataframe_api/tfidf.py` |
| TF-IDF (SQL) | `python src/spark_sql/tfidf_sql.py` |

---

## Benchmarks

Set `PYTHONPATH` to the repo root for each command.

**Baseline (single glob, six jobs)** — `results/benchmark_results.csv`:

```bash
python src/benchmarking/benchmark_runner.py
python src/benchmarking/benchmark_runner.py 'data/corpus/*.txt'   # optional explicit glob
```

**API scaling (three dataset sizes)** — first **5**, **12**, and **20** books when sorted by filename under `data/corpus/`. Runs eight analyses × three sizes (includes TF-IDF). Output: `results/benchmark_scaling.csv`.

```bash
python src/benchmarking/benchmark_scaling.py
```

**Partition tuning** — **WordFrequency** only; varies `spark.sql.shuffle.partitions` over **2, 4, 8, 16** on the full sorted corpus. Output: `results/benchmark_partitions.csv`.

```bash
python src/benchmarking/benchmark_partitions.py
```

**Charts** (requires CSVs from the previous steps):

```bash
python -m src.benchmarking.plot_results
```

Generated files:

- `results/figures/scaling_api_benchmark.png`
- `results/figures/partition_tuning_wordfrequency.png`

Timings are environment-dependent (CPU, JVM warmup, Spark caches). Treat CSVs and PNGs as artifacts you regenerate before submitting or presenting.

---

## Experiment results (example local run)

Numbers below come from one **local `spark-submit`-style run** after warmup (Apple Silicon laptop, full 20-book corpus). Your absolute seconds will differ; **correctness checks** (word totals, top token, top pair, TF-IDF parity) should match when using the same corpus.

### Full corpus (20 books)

| Metric | Value |
|--------|--------|
| Total words (WordCount) | 2,047,705 |
| Top word (WordFrequency) | the (100,764) |
| Top pair (WordPairs) | (the, the) (127,540) |
| Top TF-IDF term (max TF-IDF across docs) | Gregor (~0.016) |

### Scaling benchmark (elapsed seconds, illustrative)

| Size | Files | WordCount DF | WordCount SQL | WordFreq SQL | TF-IDF SQL |
|------|-------|--------------|---------------|--------------|------------|
| small | 5 | ~2.53 | ~1.11 | ~0.05 | ~0.37 |
| medium | 12 | ~0.18 | ~0.17 | ~0.05 | ~0.20 |
| large | 20 | ~0.14 | ~0.12 | ~0.03 | ~0.26 |

The first **small** WordCount runs pay JVM/Spark startup inside the timed region; later sizes reuse a warm session, so relative times are not linear with input size.

### Partition tuning (WordFrequency, 20 books)

With **`spark.sql.shuffle.partitions`** set to **2**, runtimes were highest (~0.66 s DataFrame, ~0.41 s SQL in our sample). Values **4 → 16** were much flatter (~0.03–0.07 s). See `results/benchmark_partitions.csv` and the partition PNG for the exact curve.

---

## Tests (correctness)

We use **pytest** and tiny fixtures under `tests/fixtures/` (no network). Tests assert DataFrame and Spark SQL outputs match for WordCount, WordFrequency, WordPairs, and TF-IDF, and that multi-file paths are passed correctly to Spark.

```bash
source .venv/bin/activate
export PYTHONPATH="$(pwd)"
python -m pytest tests/ -v
```

---

## Repository layout (main pieces)

- `data/download_books.py` — fetch and strip Gutenberg boilerplate.
- `src/readers.py` — shared `read_lines` / `read_wholetext_documents` (supports a **list** of paths for subsets).
- `src/dataframe_api/`, `src/spark_sql/` — analyses including TF-IDF.
- `src/benchmarking/` — runners, corpus subset helpers, plotting.
- `tests/` — pytest suite and fixtures.

---

## New analysis: TF-IDF

Each **file** is one document (`wholetext` read). Per (document, word) we use smoothed IDF: \(\ln\frac{N+1}{df+1}\) times term frequency normalized by document length. We rank terms by **maximum TF-IDF across documents** and report the top 20. The Spark SQL pipeline mirrors the DataFrame pipeline (validated in tests).
