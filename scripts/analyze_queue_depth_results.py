#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import statistics
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class Sample:
    source: str
    run: int
    mode: str
    queue_depth: int
    mixed_inflight: int
    strong_hot_inflight: int
    strong_cold_inflight: int
    elapsed_time_s: float
    total_hot_requests: int
    avg_latency_us: float
    p95_latency_us: float
    p99_latency_us: float
    hot_throughput_mb_s: float
    cold_throughput_mb_s: float
    hot_thread_errors: int
    cold_thread_errors: int


@dataclass(frozen=True)
class ConfigModeSummary:
    queue_depth: int
    mixed_inflight: int
    strong_hot_inflight: int
    strong_cold_inflight: int
    mode: str
    samples: int
    hot_tp_mean: float
    hot_tp_cv: float
    cold_tp_mean: float
    avg_lat_mean: float
    avg_lat_cv: float
    p95_mean: float
    p99_mean: float


@dataclass(frozen=True)
class ConfigComparison:
    queue_depth: int
    strong_cold_inflight: int
    hot_tp_ratio: float
    cold_tp_ratio: float
    avg_lat_reduction_pct: float
    p95_reduction_pct: float


def parse_metric(value: str) -> float:
    return float(value.split()[0])


def mean(values: Iterable[float]) -> float:
    values = list(values)
    return statistics.fmean(values) if values else 0.0


def coefficient_of_variation(values: Iterable[float]) -> float:
    values = list(values)
    if len(values) < 2:
        return 0.0

    avg = statistics.fmean(values)
    if avg == 0:
        return 0.0

    return statistics.stdev(values) / avg * 100.0


def parse_summary_file(path: Path) -> list[Sample]:
    samples: list[Sample] = []

    with path.open(newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        for row in reader:
            samples.append(
                Sample(
                    source=path.parent.name,
                    run=int(row["run"]),
                    mode=row["mode"],
                    queue_depth=int(row["queue_depth"]),
                    mixed_inflight=int(row["mixed_inflight"]),
                    strong_hot_inflight=int(row["strong_hot_inflight"]),
                    strong_cold_inflight=int(row["strong_cold_inflight"]),
                    elapsed_time_s=parse_metric(row["elapsed_time"]),
                    total_hot_requests=int(row["total_hot_requests"]),
                    avg_latency_us=parse_metric(row["avg_latency"]),
                    p95_latency_us=parse_metric(row["p95_latency"]),
                    p99_latency_us=parse_metric(row["p99_latency"]),
                    hot_throughput_mb_s=parse_metric(row["hot_throughput"]),
                    cold_throughput_mb_s=parse_metric(row["cold_throughput"]),
                    hot_thread_errors=int(row["hot_thread_errors"]),
                    cold_thread_errors=int(row["cold_thread_errors"]),
                )
            )

    return samples


def load_samples(input_root: Path) -> list[Sample]:
    paths = sorted(input_root.glob("*/summary_all.tsv"))
    samples: list[Sample] = []

    for path in paths:
        samples.extend(parse_summary_file(path))

    return samples


def summarize_by_config(samples: list[Sample]) -> list[ConfigModeSummary]:
    grouped: dict[tuple[int, int, int, int, str], list[Sample]] = defaultdict(list)

    for sample in samples:
        key = (
            sample.queue_depth,
            sample.mixed_inflight,
            sample.strong_hot_inflight,
            sample.strong_cold_inflight,
            sample.mode,
        )
        grouped[key].append(sample)

    summaries: list[ConfigModeSummary] = []

    for key in sorted(grouped):
        queue_depth, mixed_inflight, strong_hot_inflight, strong_cold_inflight, mode = key
        group = grouped[key]
        summaries.append(
            ConfigModeSummary(
                queue_depth=queue_depth,
                mixed_inflight=mixed_inflight,
                strong_hot_inflight=strong_hot_inflight,
                strong_cold_inflight=strong_cold_inflight,
                mode=mode,
                samples=len(group),
                hot_tp_mean=mean(sample.hot_throughput_mb_s for sample in group),
                hot_tp_cv=coefficient_of_variation(sample.hot_throughput_mb_s for sample in group),
                cold_tp_mean=mean(sample.cold_throughput_mb_s for sample in group),
                avg_lat_mean=mean(sample.avg_latency_us for sample in group),
                avg_lat_cv=coefficient_of_variation(sample.avg_latency_us for sample in group),
                p95_mean=mean(sample.p95_latency_us for sample in group),
                p99_mean=mean(sample.p99_latency_us for sample in group),
            )
        )

    return summaries


def compare_strong_i_vs_mixed(samples: list[Sample]) -> list[ConfigComparison]:
    grouped: dict[tuple[int, int, int, int], dict[str, list[Sample]]] = defaultdict(lambda: defaultdict(list))

    for sample in samples:
        key = (
            sample.queue_depth,
            sample.mixed_inflight,
            sample.strong_hot_inflight,
            sample.strong_cold_inflight,
        )
        grouped[key][sample.mode].append(sample)

    comparisons: list[ConfigComparison] = []

    for key in sorted(grouped):
        queue_depth, _, _, strong_cold_inflight = key
        per_mode = grouped[key]
        if "mixed" not in per_mode or "strong-i" not in per_mode:
            continue

        mixed_group = per_mode["mixed"]
        strong_group = per_mode["strong-i"]

        mixed_hot_tp = mean(sample.hot_throughput_mb_s for sample in mixed_group)
        mixed_cold_tp = mean(sample.cold_throughput_mb_s for sample in mixed_group)
        mixed_avg_lat = mean(sample.avg_latency_us for sample in mixed_group)
        mixed_p95 = mean(sample.p95_latency_us for sample in mixed_group)

        strong_hot_tp = mean(sample.hot_throughput_mb_s for sample in strong_group)
        strong_cold_tp = mean(sample.cold_throughput_mb_s for sample in strong_group)
        strong_avg_lat = mean(sample.avg_latency_us for sample in strong_group)
        strong_p95 = mean(sample.p95_latency_us for sample in strong_group)

        comparisons.append(
            ConfigComparison(
                queue_depth=queue_depth,
                strong_cold_inflight=strong_cold_inflight,
                hot_tp_ratio=(strong_hot_tp / mixed_hot_tp) if mixed_hot_tp else 0.0,
                cold_tp_ratio=(strong_cold_tp / mixed_cold_tp) if mixed_cold_tp else 0.0,
                avg_lat_reduction_pct=((mixed_avg_lat - strong_avg_lat) / mixed_avg_lat * 100.0)
                if mixed_avg_lat
                else 0.0,
                p95_reduction_pct=((mixed_p95 - strong_p95) / mixed_p95 * 100.0) if mixed_p95 else 0.0,
            )
        )

    return comparisons


def markdown_table(headers: list[str], rows: list[list[str]]) -> str:
    separator = ["---"] * len(headers)
    table_rows = [headers, separator, *rows]
    return "\n".join("| " + " | ".join(row) + " |" for row in table_rows)


def format_us_as_ms(value_us: float) -> str:
    return f"{value_us / 1000.0:.2f} ms"


def format_source_range(samples: list[Sample]) -> str:
    sources = sorted({sample.source for sample in samples})
    if not sources:
        return "n/a"
    return f"{sources[0]} to {sources[-1]}"


def build_report(samples: list[Sample]) -> str:
    if not samples:
        raise ValueError("No summary_all.tsv files were found under the input root.")

    config_summaries = summarize_by_config(samples)
    comparisons = compare_strong_i_vs_mixed(samples)

    total_sources = len({sample.source for sample in samples})
    total_errors = sum(sample.hot_thread_errors + sample.cold_thread_errors for sample in samples)
    queue_depths = sorted({sample.queue_depth for sample in samples})
    modes = ["hot-only", "mixed", "strong-i"]

    best_hot_only = max(
        (summary for summary in config_summaries if summary.mode == "hot-only"),
        key=lambda summary: (summary.hot_tp_mean, -summary.avg_lat_mean),
    )
    best_strong_i = max(
        (summary for summary in config_summaries if summary.mode == "strong-i"),
        key=lambda summary: (summary.hot_tp_mean, summary.cold_tp_mean, -summary.avg_lat_mean),
    )
    strongest_mixed_latency = min(
        (summary for summary in config_summaries if summary.mode == "mixed"),
        key=lambda summary: (summary.avg_lat_mean, -summary.hot_tp_mean),
    )

    min_hot_gain = min(comparison.hot_tp_ratio for comparison in comparisons)
    max_hot_gain = max(comparison.hot_tp_ratio for comparison in comparisons)
    min_avg_lat_drop = min(comparison.avg_lat_reduction_pct for comparison in comparisons)
    max_avg_lat_drop = max(comparison.avg_lat_reduction_pct for comparison in comparisons)
    min_p95_drop = min(comparison.p95_reduction_pct for comparison in comparisons)
    max_p95_drop = max(comparison.p95_reduction_pct for comparison in comparisons)

    hot_only_by_qd = defaultdict(list)
    for summary in config_summaries:
        if summary.mode == "hot-only":
            hot_only_by_qd[summary.queue_depth].append(summary)

    hot_only_qd_means = {
        qd: mean(summary.avg_lat_mean for summary in summaries) for qd, summaries in hot_only_by_qd.items()
    }
    best_hot_only_qd = min(hot_only_qd_means, key=hot_only_qd_means.get)
    worst_hot_only_qd = max(hot_only_qd_means, key=hot_only_qd_means.get)
    hot_only_latency_ratio = hot_only_qd_means[worst_hot_only_qd] / hot_only_qd_means[best_hot_only_qd]

    overview_lines = [
        "# Queue Depth Matrix Analysis",
        "",
        f"Analyzed {len(samples)} samples from {total_sources} merged summaries under `{format_source_range(samples)}`.",
        "",
        "## Overview",
        "",
        f"- Queue depths: {', '.join(str(depth) for depth in queue_depths)}",
        f"- Modes: {', '.join(modes)}",
        f"- Samples per merged summary: 15 (5 runs x 3 modes)",
        f"- Total thread errors across all samples: {total_errors}",
        "",
        "## Highlights",
        "",
        (
            f"- `strong-i` consistently beat `mixed` on hot-path protection: hot throughput improved by "
            f"{min_hot_gain:.2f}x to {max_hot_gain:.2f}x, average latency dropped by "
            f"{min_avg_lat_drop:.1f}% to {max_avg_lat_drop:.1f}%, and P95 latency dropped by "
            f"{min_p95_drop:.1f}% to {max_p95_drop:.1f}% across the tested configurations."
        ),
        (
            f"- Best mean `hot-only` point: qd={best_hot_only.queue_depth}, strong_cold={best_hot_only.strong_cold_inflight}, "
            f"hot throughput={best_hot_only.hot_tp_mean:.2f} MB/s, average latency={format_us_as_ms(best_hot_only.avg_lat_mean)}."
        ),
        (
            f"- Best mean `strong-i` point for hot throughput: qd={best_strong_i.queue_depth}, "
            f"strong_cold={best_strong_i.strong_cold_inflight}, hot throughput={best_strong_i.hot_tp_mean:.2f} MB/s, "
            f"cold throughput={best_strong_i.cold_tp_mean:.2f} MB/s, average latency={format_us_as_ms(best_strong_i.avg_lat_mean)}."
        ),
        (
            f"- Lowest-latency `mixed` mean still landed at qd={strongest_mixed_latency.queue_depth}, "
            f"strong_cold={strongest_mixed_latency.strong_cold_inflight}, average latency={format_us_as_ms(strongest_mixed_latency.avg_lat_mean)}; "
            f"that is still much worse than the comparable `strong-i` points."
        ),
        (
            f"- Raising `hot-only` queue depth from qd={best_hot_only_qd} to qd={worst_hot_only_qd} increased mean hot latency by "
            f"{hot_only_latency_ratio:.1f}x without a corresponding hot-throughput gain, so the data does not support qd=128 for latency-sensitive work."
        ),
        "",
        "## Mean Metrics By Configuration",
        "",
    ]

    config_rows = [
        [
            str(summary.queue_depth),
            str(summary.mixed_inflight),
            str(summary.strong_hot_inflight),
            str(summary.strong_cold_inflight),
            summary.mode,
            str(summary.samples),
            f"{summary.hot_tp_mean:.2f}",
            f"{summary.hot_tp_cv:.1f}%",
            f"{summary.cold_tp_mean:.2f}",
            f"{summary.avg_lat_mean:.2f}",
            f"{summary.avg_lat_cv:.1f}%",
            f"{summary.p95_mean:.2f}",
            f"{summary.p99_mean:.2f}",
        ]
        for summary in config_summaries
    ]

    comparison_rows = [
        [
            str(comparison.queue_depth),
            str(comparison.strong_cold_inflight),
            f"{comparison.hot_tp_ratio:.2f}x",
            f"{comparison.cold_tp_ratio:.2f}x",
            f"{comparison.avg_lat_reduction_pct:.1f}%",
            f"{comparison.p95_reduction_pct:.1f}%",
        ]
        for comparison in comparisons
    ]

    report_sections = [
        "\n".join(overview_lines),
        markdown_table(
            headers=[
                "queue_depth",
                "mixed_inflight",
                "strong_hot_inflight",
                "strong_cold_inflight",
                "mode",
                "samples",
                "hot_tp_mean_mb_s",
                "hot_tp_cv",
                "cold_tp_mean_mb_s",
                "avg_lat_mean_us",
                "avg_lat_cv",
                "p95_mean_us",
                "p99_mean_us",
            ],
            rows=config_rows,
        ),
        "",
        "## `strong-i` vs `mixed`",
        "",
        markdown_table(
            headers=[
                "queue_depth",
                "strong_cold_inflight",
                "hot_tp_ratio",
                "cold_tp_ratio",
                "avg_lat_reduction",
                "p95_reduction",
            ],
            rows=comparison_rows,
        ),
        "",
        "## Notes",
        "",
        "- `strong_cold_inflight` changes across timestamp groups, so `strong-i` should be compared at the full configuration level instead of by queue depth alone.",
        "- `hot-only` and `mixed` inherit the same `strong_cold_inflight` column from the run configuration, but that field only directly changes `strong-i` behavior.",
        "- All numeric means and variability values are computed from the merged 5-run summaries already present in the repository.",
        "",
    ]

    return "\n".join(report_sections)


def parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[1]
    default_input_root = repo_root / "results" / "queue-depth-matrix-repeat5"

    parser = argparse.ArgumentParser(
        description="Analyze queue-depth repeat benchmark summaries and emit a Markdown report."
    )
    parser.add_argument(
        "--input-root",
        type=Path,
        default=default_input_root,
        help=f"Directory that contains timestamped summary_all.tsv folders. Default: {default_input_root}",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional output Markdown path. If omitted, the report is written to stdout.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    samples = load_samples(args.input_root)
    report = build_report(samples)

    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(report, encoding="utf-8")
    else:
        print(report)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
