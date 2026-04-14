# Project Proposal: PySpark Performance Benchmarking Study

**Team Members:** Aayush Koirala, Benson Zheng

---

## Project Description
We will extend our PySpark text analysis homework into a full-scale benchmarking study. We will scale our word count, word frequency, and word pair analyses to large multi-document corpora, rewrite queries using **Spark SQL**, experiment with **partition tuning**, and measure performance across all approaches with empirical charts.

## Project Relevance
This project directly applies course concepts including:
* Distributed data processing with Spark.
* Query optimization and the DataFrame/SQL execution model.
* The effects of data partitioning on shuffle and performance.
* Extension of hands-on PySpark work from HW3.

---

## Timeline & Milestones

### Milestone Goals (Due 4/15)
* **Data Acquisition:** Download and preprocess a large multi-text corpus (10–50 books from Project Gutenberg) into a unified dataset.
* **Validation:** Run existing WordCount, WordFrequency, and WordPairs scripts on the new corpus and confirm correctness.
* **Refactoring:** Rewrite all three analyses using Spark SQL and verify they produce identical results.
* **Infrastructure:** Set up a timing/benchmarking harness to record runtimes.

### Final Project Goals (Due 5/4)
* **API Benchmarking:** Compare DataFrame API vs. Spark SQL for all three analyses across at least 3 dataset sizes, including runtime charts.
* **Partition Tuning:** Benchmark the effect of partition count (e.g., 2, 4, 8, 16 partitions) on job performance.
* **New Feature:** Implement one new analysis not in the homework (e.g., TF-IDF or trigram frequency).
* **Documentation:** README with setup instructions, experiment results, and test cases.
* **Presentation:** 8–10 minute recorded video presentation with slides and live demo.

---

## Division of Work

| Person | Focus Areas |
| :--- | :--- |
| **Aayush** | Dataset collection, preprocessing pipeline, and corpus scaling. |
| **Benson** | Spark SQL rewrites of all three core analyses. |
| **Aayush** | Benchmarking harness and partition tuning experiments. |
| **Benson** | New analysis implementation (TF-IDF/Trigrams), charts, and README. |