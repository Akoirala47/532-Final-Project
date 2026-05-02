#!/usr/bin/env python3
"""
Plot benchmark CSVs under results/ using matplotlib.

Reads:
  - results/benchmark_scaling.csv
  - results/benchmark_partitions.csv

Writes PNGs to results/figures/
"""

import os
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

def _ensure_pkg_path():
    if PROJECT_ROOT not in sys.path:
        sys.path.insert(0, PROJECT_ROOT)


def plot_scaling(csv_path: str, out_dir: str) -> None:
    import matplotlib.pyplot as plt
    import pandas as pd

    df = pd.read_csv(csv_path)
    order = ["small", "medium", "large"]
    df = df[df["dataset_size"].isin(order)]
    df["dataset_size"] = pd.Categorical(df["dataset_size"], categories=order, ordered=True)
    df = df.sort_values(["analysis", "api_type", "dataset_size"])

    nf_map = (
        df.drop_duplicates(subset=["dataset_size"])
        .set_index("dataset_size")["num_files"]
        .to_dict()
    )
    analyses = sorted(df["analysis"].unique())
    fig, axes = plt.subplots(len(analyses), 1, figsize=(8, 3 * len(analyses)), sharex=True)
    if len(analyses) == 1:
        axes = [axes]

    x_labels = [f"{s}\n({nf_map.get(s, '?')} files)" for s in order]

    for ax, analysis in zip(axes, analyses):
        sub = df[df["analysis"] == analysis]
        for api in sub["api_type"].unique():
            g = sub[sub["api_type"] == api].sort_values("dataset_size")
            xs = range(len(g))
            ax.plot(
                xs,
                g["elapsed_seconds"].values,
                marker="o",
                label=api,
            )
        ax.set_title(analysis)
        ax.set_ylabel("seconds")
        ax.legend()
        ax.grid(True, alpha=0.3)

    axes[-1].set_xticks(range(len(order)))
    axes[-1].set_xticklabels(x_labels)
    axes[-1].set_xlabel("Corpus subset")
    fig.suptitle("API scaling benchmark (elapsed time)")
    fig.tight_layout()
    os.makedirs(out_dir, exist_ok=True)
    dst = os.path.join(out_dir, "scaling_api_benchmark.png")
    fig.savefig(dst, dpi=150)
    plt.close(fig)
    print(f"Wrote {dst}")


def plot_partitions(csv_path: str, out_dir: str) -> None:
    import matplotlib.pyplot as plt
    import pandas as pd

    df = pd.read_csv(csv_path)
    fig, ax = plt.subplots(figsize=(8, 5))
    for api in df["api_type"].unique():
        sub = df[df["api_type"] == api].sort_values("shuffle_partitions")
        ax.plot(
            sub["shuffle_partitions"],
            sub["elapsed_seconds"],
            marker="s",
            linewidth=2,
            label=api,
        )
    ax.set_xlabel("spark.sql.shuffle.partitions")
    ax.set_ylabel("seconds")
    ax.set_title("WordFrequency: partition tuning")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    os.makedirs(out_dir, exist_ok=True)
    dst = os.path.join(out_dir, "partition_tuning_wordfrequency.png")
    fig.savefig(dst, dpi=150)
    plt.close(fig)
    print(f"Wrote {dst}")


def main():
    _ensure_pkg_path()
    results_dir = os.path.join(PROJECT_ROOT, "results")
    fig_dir = os.path.join(results_dir, "figures")
    scaling_csv = os.path.join(results_dir, "benchmark_scaling.csv")
    part_csv = os.path.join(results_dir, "benchmark_partitions.csv")

    if os.path.isfile(scaling_csv):
        plot_scaling(scaling_csv, fig_dir)
    else:
        print(f"Skip scaling plot (missing {scaling_csv})")

    if os.path.isfile(part_csv):
        plot_partitions(part_csv, fig_dir)
    else:
        print(f"Skip partition plot (missing {part_csv})")


if __name__ == "__main__":
    main()
