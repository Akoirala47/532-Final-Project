# PySpark Performance Benchmarking Study — Project Report

**Course:** COMPSCI 532 — Systems for Data Science (Spring 2026)  
**Team:** Aayush Koirala, Benson Zheng  

This document is the **single narrative overview** of the project: what we set out to do, what we built, where outputs live, and how to read **every CSV and figure**. Operational commands also appear in `README.md`; this report focuses on **meaning and interpretation**.

---

## 1. Objectives

### 1.1 Original intent (proposal)

We proposed extending the course PySpark homework into a **benchmarking study** over a **large, multi-document text corpus**. Concretely, the proposal called for:

- Scaling **word count**, **word frequency**, and **word pair** analyses beyond a single play-sized file.
- Expressing the same logic in both the **DataFrame API** and **Spark SQL**, then **comparing performance**.
- **API benchmarking** across **at least three dataset sizes**, with **charts**.
- **Partition tuning** (e.g. 2 / 4 / 8 / 16 partitions) to observe effects on shuffle-heavy work.
- A **new analysis** not required by the homework (e.g. TF-IDF or trigrams).
- **Documentation** (setup, results, tests) and a **short recorded presentation** with demo.

### 1.2 Learning goals (why this matters)

The project exercises ideas from the course: distributed execution, the Catalyst optimizer, when APIs agree on results but differ in plans, how **shuffle partition count** interacts with aggregation cost, and how to validate pipelines when scaling data.

---

## 2. What we achieved

### 2.1 Milestone phase (April) — completed

| Area | Outcome |
|------|---------|
| **Data** | ~20 Gutenberg books downloaded, boilerplate stripped, saved under `data/corpus/` (~**2,047,705** words on the full set). Script: `data/download_books.py`. |
| **Validation** | WordCount / WordFrequency / WordPairs run on the corpus; aggregates sanity-checked (e.g. total words, dominant English function words). |
| **Spark SQL** | All three homework analyses reimplemented under `src/spark_sql/` with temp views + SQL; outputs match the DataFrame paths. |
| **Infrastructure** | `time_function` + CSV export in `src/benchmarking/utils.py`; `benchmark_runner.py` runs six timed jobs (3 × DataFrame + 3 × SQL) into `results/benchmark_results.csv`. |

Details match `milestone-goals/milestone_report.md`.

### 2.2 Final phase — technical deliverables completed

| Proposal item | Status | Where it lives |
|---------------|--------|----------------|
| API benchmarking across **3 sizes** + charts | Done | `src/benchmarking/benchmark_scaling.py`, `results/benchmark_scaling.csv`, `results/figures/scaling_api_benchmark.png` |
| Partition tuning **2, 4, 8, 16** | Done | `src/benchmarking/benchmark_partitions.py`, `results/benchmark_partitions.csv`, `results/figures/partition_tuning_wordfrequency.png` |
| **New analysis** (TF-IDF) | Done | `src/dataframe_api/tfidf.py`, `src/spark_sql/tfidf_sql.py`; included in scaling benchmark |
| README + setup + tests | Done | `README.md`, `tests/test_analyses.py` |
| **Presentation** (video) | **Outside repo** — team records separately | N/A in Git |

---

## 3. How the system is organized

### 3.1 High-level data flow

```text
Project Gutenberg URLs
        │
        ▼
data/download_books.py  ──►  data/corpus/*.txt  (cleaned plain text; gitignored by default)
        │
        ├── read as one row per LINE ──► WordCount, WordFrequency, WordPairs
        │         (src/readers.py: read_lines)
        │
        └── read as one row per FILE (whole text) ──► TF-IDF
                  (src/readers.py: read_wholetext_documents)
```

- **Line-oriented** jobs treat each line of each file as input rows (same mental model as the original homework).
- **TF-IDF** treats **each file as one document**, which is appropriate for “which terms characterize which book?” style signals.

### 3.2 Source layout (mental map)

| Path | Role |
|------|------|
| `data/download_books.py` | Fetch + strip Gutenberg headers/footers. |
| `src/readers.py` | Shared Spark reads; supports a **glob string** or a **list of file paths** (required for deterministic subsets). |
| `src/dataframe_api/` | DataFrame implementations: `word_count`, `word_frequency`, `word_pairs`, `tfidf`. |
| `src/spark_sql/` | SQL equivalents: `word_count_sql`, `word_frequency_sql`, `word_pairs_sql`, `tfidf_sql`. |
| `src/benchmarking/corpus_paths.py` | Builds **small / medium / large** path lists (sorted filenames, first 5 / 12 / 20 books). |
| `src/benchmarking/benchmark_runner.py` | Single-glob baseline: 6 jobs → `benchmark_results.csv`. |
| `src/benchmarking/benchmark_scaling.py` | 8 analyses × 3 sizes → `benchmark_scaling.csv`. |
| `src/benchmarking/benchmark_partitions.py` | WordFrequency × shuffle partitions → `benchmark_partitions.csv`. |
| `src/benchmarking/plot_results.py` | Reads the CSVs above and writes PNGs under `results/figures/`. |
| `tests/` | Pytest correctness checks (DataFrame vs SQL agreement on fixtures). |

---

## 4. Analyses (what each job measures)

### 4.1 WordCount

Total count of **non-empty whitespace-separated tokens** over all lines of all input files. Used as a coarse scale check (“did we read the whole corpus?”).

### 4.2 WordFrequency

Global histogram of tokens; reports **top 20** by frequency. Expect heavy English stopwords (**the**, **and**, …) at the top for this corpus.

### 4.3 WordPairs

On **each line**, forms unordered pairs of tokens `(word1, word2)` with positions `pos1 ≤ pos2`, filters empties, aggregates globally, **top 20** pairs. Strong pairs often involve frequent function words and duplicated tokens on the same line (e.g. **(the, the)**).

### 4.4 TF-IDF (new)

- **Document** = one `.txt` file (whole-file read).
- Per document–term: term frequency normalized by **document length**.
- **IDF:** smoothed \(\ln\frac{N+1}{df+1}\), where \(N\) is the number of documents and \(df\) is how many documents contain the term.
- **Ranking:** for each term, take the **maximum TF-IDF across documents**, then sort descending and take **top 20**. That surfaces terms that are **strong in at least one book** (e.g. character names local to a novel), not merely globally frequent words.

DataFrame and SQL pipelines implement the same logic; tests assert matching ranked outputs on small fixtures.

---

## 5. Understanding the results — CSV files

All timings are **wall-clock seconds** around the analysis function (Python `perf_counter`). They depend on machine, JVM warmup, Spark caching, and filesystem; **cross-machine comparison is qualitative**. Correctness is judged from **`result_summary`** (and tests), not from runtime.

### 5.1 `results/benchmark_results.csv`

**Producer:** `src/benchmarking/benchmark_runner.py`  
**Corpus:** typically `data/corpus/*.txt` (single glob; all books).

| Column | Meaning |
|--------|---------|
| `analysis` | WordCount, WordFrequency, or WordPairs. |
| `api_type` | `DataFrame` or `SparkSQL`. |
| `elapsed_seconds` | Time for that single run. |
| `corpus_path` | Input path/glob string used. |
| `result_summary` | Short human-readable sanity output (totals, top token, top pair). |

**How to use it:** quick **single-configuration** comparison of DataFrame vs SQL on the **full** corpus without subset logic.

### 5.2 `results/benchmark_scaling.csv`

**Producer:** `src/benchmarking/benchmark_scaling.py`  
**Design:** For each **dataset_size** (`small`, `medium`, `large`), runs **eight** timed jobs: WordCount, WordFrequency, WordPairs, TF-IDF × (DataFrame + SQL). Subsets are **deterministic**: lexicographically sorted `data/corpus/*.txt`, then **first 5, 12, and 20** paths respectively.

| Column | Meaning |
|--------|---------|
| `dataset_size` | `small` (5 files), `medium` (12), `large` (20), unless fewer books exist. |
| `num_files` | Actual file count for that row. |
| `analysis` | WordCount, WordFrequency, WordPairs, or TFIDF. |
| `api_type` | `DataFrame` or `SparkSQL`. |
| `elapsed_seconds` | Timed run for that subset + analysis + API. |
| `corpus_path` | Semicolon-separated list of input paths (possibly truncated in CSV for length). |
| `result_summary` | Sanity string; **Word totals and tops change with subset** — large should match full-corpus homework numbers when `num_files == 20` and the corpus is complete. |

**How to read it:**

- **Correctness:** For `large` / 20 files, WordCount and dominant tokens should align with the known full-corpus figures (~2.05M words, etc.).
- **Performance:** Compare **SQL vs DataFrame** at the same row; compare **small vs large** to see how costs evolve (note: **first runs** often include more JVM/session overhead, so the **first subset** is not always comparable to later ones in strict proportion).

### 5.3 `results/benchmark_partitions.csv`

**Producer:** `src/benchmarking/benchmark_partitions.py`  
**Design:** Full sorted corpus (all `.txt` files). Before each timed run, sets **`spark.sql.shuffle.partitions`** to **2, 4, 8, or 16**. Runs **WordFrequency** only (DataFrame + SQL) — a shuffle-heavy aggregation representative of tuning exercises.

| Column | Meaning |
|--------|---------|
| `shuffle_partitions` | Value of `spark.sql.shuffle.partitions` during that run. |
| `analysis` | Always WordFrequency here. |
| `api_type` | `DataFrame` or `SparkSQL`. |
| `elapsed_seconds` | Time for that configuration. |
| `num_files` | Number of corpus files (usually 20). |
| `corpus_path` | Semicolon-separated paths (may truncate). |
| `result_summary` | Should be **identical** across partition settings if logic is unchanged (top word and count). |

**How to read it:** If **`result_summary` differs** across partition rows, something is wrong (non-deterministic ordering bug or altered query). If summaries match but times differ, that illustrates **partition count vs shuffle cost** on local Spark.

---

## 6. Understanding the figures — PNG files

**Producer:** `python -m src.benchmarking.plot_results` (needs the CSVs present).

### 6.1 `results/figures/scaling_api_benchmark.png`

- **Rows:** one subplot per **`analysis`** (WordCount, WordFrequency, WordPairs, TFIDF).
- **X-axis:** `small` → `medium` → `large`, labeled with **approximate file counts** taken from the CSV.
- **Y-axis:** `elapsed_seconds` from `benchmark_scaling.csv`.
- **Series:** lines for **`DataFrame`** vs **`SparkSQL`** (legend).

**Interpretation:** See whether one API is systematically faster for a given analysis as data grows, and whether curves flatten (warm caches, amortized overhead) or rise (more data work dominates).

### 6.2 `results/figures/partition_tuning_wordfrequency.png`

- **X-axis:** `shuffle_partitions` (2, 4, 8, 16).
- **Y-axis:** `elapsed_seconds` from `benchmark_partitions.csv`.
- **Series:** DataFrame vs Spark SQL WordFrequency.

**Interpretation:** Very low partition counts can **inflate** shuffle stages on aggregations; higher counts reduce task size but add scheduling overhead — on **local** Spark the curve may be flat after a point. The chart makes that visible for **your** hardware run.

---

## 7. Correctness and regression testing

Automated checks live in `tests/test_analyses.py`:

- WordCount: DataFrame total **equals** SQL total.
- WordFrequency / WordPairs: **Top ranked row** matches between APIs on shared fixtures.
- TF-IDF: **Full ranked lists** (word + rounded score) match between APIs.

Fixtures are tiny controlled texts under `tests/fixtures/` so CI / laptops need **no** downloaded corpus. Running:

```bash
export PYTHONPATH="$(pwd)"
python -m pytest tests/ -v
```

---

## 8. Limitations (honest scope)

- **Local Spark** (`local[*]` via default session) is not a cluster; absolute seconds **do not** predict AWS EMR or Dataproc behavior.
- **JVM warmup** and **session reuse** distort the **first** timed jobs in a session — especially visible when comparing “small” timing to “large” in one script.
- **Tokenization** is naive split-on-space; no stemming, lowercasing, or punctuation normalization (consistent with the homework style).
- **WordPairs** complexity grows with line length; skewed ultra-long lines could dominate — acceptable for Gutenberg prose split across many lines.

---

## 9. Proposal checklist (traceability)

| Deliverable | Evidence |
|-------------|----------|
| Multi-book corpus | `data/download_books.py`, `.gitignore` on `data/corpus/` |
| DataFrame + SQL parity (homework trio) | `src/dataframe_api/*`, `src/spark_sql/*`, tests |
| Baseline timing CSV | `benchmark_results.csv`, `benchmark_runner.py` |
| Three sizes + chart | `benchmark_scaling.csv`, `scaling_api_benchmark.png` |
| Partition sweep + chart | `benchmark_partitions.csv`, `partition_tuning_wordfrequency.png` |
| New analysis | TF-IDF modules + scaling rows + tests |
| Documentation | `README.md`, this `report.md`, pytest |
| Video presentation | Recorded separately per course instructions |

---

## 10. Quick pointer: “I open the repo cold — what do I read first?”

1. **`report.md` (this file)** — objectives, achievement map, how to read artifacts.  
2. **`README.md`** — exact commands, environment, tables of example numbers.  
3. **`project-proposal/proposal.md`** — original commitments.  
4. **`results/*.csv` + `results/figures/*.png`** — raw metrics and plots after running benchmarks.  
5. **`tests/test_analyses.py`** — what “correct” means in code.

---

*Generated as project documentation; regenerate CSVs/PNGs on your machine before submitting if instructors expect fresh timestamps or hardware-specific discussion.*
