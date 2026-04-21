#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import statistics
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Sample:
    source: str
    profile: str
    layout: str
    run: int
    queue_depth: int
    mixed_inflight: int
    strong_hot_inflight: int
    strong_cold_inflight: int
    hot_threads: int
    cold_threads: int
    hot_queues: int
    cold_queues: int
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
class DepthMean:
    profile: str
    layout: str
    source_count: int
    queue_depth: int
    mixed_inflight: int
    strong_hot_inflight: int
    strong_cold_inflight: int
    runs: int
    elapsed_time_mean_s: float
    total_hot_requests_mean: float
    avg_latency_mean_us: float
    p95_mean_us: float
    p99_mean_us: float
    hot_throughput_mean_mb_s: float
    cold_throughput_mean_mb_s: float
    hot_thread_errors_sum: int
    cold_thread_errors_sum: int


@dataclass(frozen=True)
class OverallMean:
    profile: str
    layout: str
    source_count: int
    depths: tuple[int, ...]
    runs: int
    elapsed_time_mean_s: float
    total_hot_requests_mean: float
    avg_latency_mean_us: float
    p95_mean_us: float
    p99_mean_us: float
    hot_throughput_mean_mb_s: float
    cold_throughput_mean_mb_s: float
    hot_thread_errors_sum: int
    cold_thread_errors_sum: int


def parse_metric(value: str) -> float:
    return float(value.split()[0])


def mean(values: list[float]) -> float:
    return statistics.fmean(values) if values else 0.0


def layout_sort_key(layout: str) -> tuple[int, int]:
    hot, cold = layout.split(":")
    return int(hot), int(cold)


def detect_profile(queue_depth: int, mixed_inflight: int, strong_hot_inflight: int, strong_cold_inflight: int) -> str:
    if (
        mixed_inflight == queue_depth
        and strong_hot_inflight == queue_depth
        and strong_cold_inflight == queue_depth
    ):
        return "1to1"
    return f"m{mixed_inflight}_h{strong_hot_inflight}_c{strong_cold_inflight}"


def parse_summary_file(path: Path) -> list[Sample]:
    samples: list[Sample] = []
    source = path.parent.parent.name

    with path.open(newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        for row in reader:
            queue_depth = int(row["queue_depth"])
            mixed_inflight = int(row["mixed_inflight"])
            strong_hot_inflight = int(row["strong_hot_inflight"])
            strong_cold_inflight = int(row["strong_cold_inflight"])
            hot_queues = int(row["hot_queues"])
            cold_queues = int(row["cold_queues"])
            layout = f"{hot_queues}:{cold_queues}"

            samples.append(
                Sample(
                    source=source,
                    profile=detect_profile(
                        queue_depth,
                        mixed_inflight,
                        strong_hot_inflight,
                        strong_cold_inflight,
                    ),
                    layout=layout,
                    run=int(row["run"]),
                    queue_depth=queue_depth,
                    mixed_inflight=mixed_inflight,
                    strong_hot_inflight=strong_hot_inflight,
                    strong_cold_inflight=strong_cold_inflight,
                    hot_threads=int(row["hot_threads"]),
                    cold_threads=int(row["cold_threads"]),
                    hot_queues=hot_queues,
                    cold_queues=cold_queues,
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
    samples: list[Sample] = []

    for path in sorted(input_root.glob("*/*/summary_all.tsv")):
        samples.extend(parse_summary_file(path))

    return samples


def build_depth_means(samples: list[Sample]) -> list[DepthMean]:
    groups: dict[tuple[str, str, int, int, int, int], list[Sample]] = defaultdict(list)

    for sample in samples:
        key = (
            sample.profile,
            sample.layout,
            sample.queue_depth,
            sample.mixed_inflight,
            sample.strong_hot_inflight,
            sample.strong_cold_inflight,
        )
        groups[key].append(sample)

    points: list[DepthMean] = []

    for key in sorted(groups, key=lambda item: (item[0], layout_sort_key(item[1]), item[2])):
        profile, layout, queue_depth, mixed_inflight, strong_hot_inflight, strong_cold_inflight = key
        group = groups[key]
        sources = sorted({sample.source for sample in group})

        points.append(
            DepthMean(
                profile=profile,
                layout=layout,
                source_count=len(sources),
                queue_depth=queue_depth,
                mixed_inflight=mixed_inflight,
                strong_hot_inflight=strong_hot_inflight,
                strong_cold_inflight=strong_cold_inflight,
                runs=len(group),
                elapsed_time_mean_s=mean([sample.elapsed_time_s for sample in group]),
                total_hot_requests_mean=mean([sample.total_hot_requests for sample in group]),
                avg_latency_mean_us=mean([sample.avg_latency_us for sample in group]),
                p95_mean_us=mean([sample.p95_latency_us for sample in group]),
                p99_mean_us=mean([sample.p99_latency_us for sample in group]),
                hot_throughput_mean_mb_s=mean([sample.hot_throughput_mb_s for sample in group]),
                cold_throughput_mean_mb_s=mean([sample.cold_throughput_mb_s for sample in group]),
                hot_thread_errors_sum=sum(sample.hot_thread_errors for sample in group),
                cold_thread_errors_sum=sum(sample.cold_thread_errors for sample in group),
            )
        )

    return points


def build_overall_means(samples: list[Sample]) -> list[OverallMean]:
    groups: dict[tuple[str, str], list[Sample]] = defaultdict(list)

    for sample in samples:
        groups[(sample.profile, sample.layout)].append(sample)

    points: list[OverallMean] = []

    for key in sorted(groups, key=lambda item: (item[0], layout_sort_key(item[1]))):
        profile, layout = key
        group = groups[key]
        sources = sorted({sample.source for sample in group})
        depths = tuple(sorted({sample.queue_depth for sample in group}))

        points.append(
            OverallMean(
                profile=profile,
                layout=layout,
                source_count=len(sources),
                depths=depths,
                runs=len(group),
                elapsed_time_mean_s=mean([sample.elapsed_time_s for sample in group]),
                total_hot_requests_mean=mean([sample.total_hot_requests for sample in group]),
                avg_latency_mean_us=mean([sample.avg_latency_us for sample in group]),
                p95_mean_us=mean([sample.p95_latency_us for sample in group]),
                p99_mean_us=mean([sample.p99_latency_us for sample in group]),
                hot_throughput_mean_mb_s=mean([sample.hot_throughput_mb_s for sample in group]),
                cold_throughput_mean_mb_s=mean([sample.cold_throughput_mb_s for sample in group]),
                hot_thread_errors_sum=sum(sample.hot_thread_errors for sample in group),
                cold_thread_errors_sum=sum(sample.cold_thread_errors for sample in group),
            )
        )

    return points


def format_aligned_table(headers: list[str], rows: list[list[str]]) -> list[str]:
    widths = [len(header) for header in headers]

    for row in rows:
        for index, value in enumerate(row):
            widths[index] = max(widths[index], len(value))

    def format_row(row: list[str]) -> str:
        return "  ".join(value.ljust(widths[index]) for index, value in enumerate(row))

    lines = [format_row(headers), format_row(["-" * width for width in widths])]
    lines.extend(format_row(row) for row in rows)
    return lines


def write_tsv(path: Path, headers: list[str], rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(headers)
        writer.writerows(rows)


def write_aligned_text(path: Path, headers: list[str], rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(format_aligned_table(headers, rows)) + "\n", encoding="utf-8")


def build_depth_rows(points: list[DepthMean]) -> tuple[list[str], list[list[str]]]:
    headers = [
        "profile",
        "layout",
        "source_count",
        "queue_depth",
        "mixed_inflight",
        "strong_hot_inflight",
        "strong_cold_inflight",
        "runs",
        "elapsed_time_mean_s",
        "total_hot_requests_mean",
        "avg_latency_mean_us",
        "p95_mean_us",
        "p99_mean_us",
        "hot_throughput_mean_mb_s",
        "cold_throughput_mean_mb_s",
        "hot_thread_errors_sum",
        "cold_thread_errors_sum",
    ]
    rows = [
        [
            point.profile,
            point.layout,
            str(point.source_count),
            str(point.queue_depth),
            str(point.mixed_inflight),
            str(point.strong_hot_inflight),
            str(point.strong_cold_inflight),
            str(point.runs),
            f"{point.elapsed_time_mean_s:.3f}",
            f"{point.total_hot_requests_mean:.2f}",
            f"{point.avg_latency_mean_us:.2f}",
            f"{point.p95_mean_us:.2f}",
            f"{point.p99_mean_us:.2f}",
            f"{point.hot_throughput_mean_mb_s:.2f}",
            f"{point.cold_throughput_mean_mb_s:.2f}",
            str(point.hot_thread_errors_sum),
            str(point.cold_thread_errors_sum),
        ]
        for point in points
    ]
    return headers, rows


def build_overall_rows(points: list[OverallMean]) -> tuple[list[str], list[list[str]]]:
    headers = [
        "profile",
        "layout",
        "source_count",
        "depths",
        "runs",
        "elapsed_time_mean_s",
        "total_hot_requests_mean",
        "avg_latency_mean_us",
        "p95_mean_us",
        "p99_mean_us",
        "hot_throughput_mean_mb_s",
        "cold_throughput_mean_mb_s",
        "hot_thread_errors_sum",
        "cold_thread_errors_sum",
    ]
    rows = [
        [
            point.profile,
            point.layout,
            str(point.source_count),
            ",".join(str(depth) for depth in point.depths),
            str(point.runs),
            f"{point.elapsed_time_mean_s:.3f}",
            f"{point.total_hot_requests_mean:.2f}",
            f"{point.avg_latency_mean_us:.2f}",
            f"{point.p95_mean_us:.2f}",
            f"{point.p99_mean_us:.2f}",
            f"{point.hot_throughput_mean_mb_s:.2f}",
            f"{point.cold_throughput_mean_mb_s:.2f}",
            str(point.hot_thread_errors_sum),
            str(point.cold_thread_errors_sum),
        ]
        for point in points
    ]
    return headers, rows


def build_wide_rows(depth_points: list[DepthMean], overall_points: list[OverallMean]) -> tuple[list[str], list[list[str]]]:
    depths = sorted({point.queue_depth for point in depth_points})
    depth_lookup = {(point.profile, point.layout, point.queue_depth): point for point in depth_points}

    headers = [
        "profile",
        "layout",
        "source_count",
        "runs",
        "overall_hot_tp",
        "overall_cold_tp",
        "overall_avg_lat_us",
        "overall_p99_us",
        "overall_hot_req",
        "overall_hot_err",
    ]

    for depth in depths:
        headers.extend(
            [
                f"qd{depth}_hot_tp",
                f"qd{depth}_cold_tp",
                f"qd{depth}_avg_lat_us",
                f"qd{depth}_p99_us",
                f"qd{depth}_hot_req",
                f"qd{depth}_hot_err",
            ]
        )

    rows: list[list[str]] = []

    for point in overall_points:
        row = [
            point.profile,
            point.layout,
            str(point.source_count),
            str(point.runs),
            f"{point.hot_throughput_mean_mb_s:.2f}",
            f"{point.cold_throughput_mean_mb_s:.2f}",
            f"{point.avg_latency_mean_us:.2f}",
            f"{point.p99_mean_us:.2f}",
            f"{point.total_hot_requests_mean:.2f}",
            str(point.hot_thread_errors_sum),
        ]

        for depth in depths:
            depth_point = depth_lookup.get((point.profile, point.layout, depth))
            if depth_point is None:
                row.extend(["", "", "", "", "", ""])
                continue
            row.extend(
                [
                    f"{depth_point.hot_throughput_mean_mb_s:.2f}",
                    f"{depth_point.cold_throughput_mean_mb_s:.2f}",
                    f"{depth_point.avg_latency_mean_us:.2f}",
                    f"{depth_point.p99_mean_us:.2f}",
                    f"{depth_point.total_hot_requests_mean:.2f}",
                    str(depth_point.hot_thread_errors_sum),
                ]
            )

        rows.append(row)

    return headers, rows


def parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[1]
    default_input_root = repo_root / "results" / "strong-i-queue-layout-repeat5"
    default_output_dir = default_input_root / "analysis"

    parser = argparse.ArgumentParser(
        description="Aggregate strong-i queue-layout summary_all.tsv files into comparison tables."
    )
    parser.add_argument(
        "--input-root",
        type=Path,
        default=default_input_root,
        help=f"Directory that contains timestamp/layout summary_all.tsv files. Default: {default_input_root}",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=default_output_dir,
        help=f"Directory for generated comparison tables. Default: {default_output_dir}",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    samples = load_samples(args.input_root)

    if not samples:
        raise SystemExit(f"No summary_all.tsv files found under {args.input_root}")

    depth_points = build_depth_means(samples)
    overall_points = build_overall_means(samples)

    depth_headers, depth_rows = build_depth_rows(depth_points)
    overall_headers, overall_rows = build_overall_rows(overall_points)
    wide_headers, wide_rows = build_wide_rows(depth_points, overall_points)

    depth_tsv = args.output_dir / "queue_layout_means.tsv"
    depth_txt = args.output_dir / "queue_layout_means_aligned.txt"
    overall_tsv = args.output_dir / "queue_layout_overall.tsv"
    overall_txt = args.output_dir / "queue_layout_overall_aligned.txt"
    wide_tsv = args.output_dir / "queue_layout_wide.tsv"
    wide_txt = args.output_dir / "queue_layout_wide_aligned.txt"

    write_tsv(depth_tsv, depth_headers, depth_rows)
    write_aligned_text(depth_txt, depth_headers, depth_rows)
    write_tsv(overall_tsv, overall_headers, overall_rows)
    write_aligned_text(overall_txt, overall_headers, overall_rows)
    write_tsv(wide_tsv, wide_headers, wide_rows)
    write_aligned_text(wide_txt, wide_headers, wide_rows)

    print(f"Wrote depth means TSV: {depth_tsv}")
    print(f"Wrote depth means aligned TXT: {depth_txt}")
    print(f"Wrote overall TSV: {overall_tsv}")
    print(f"Wrote overall aligned TXT: {overall_txt}")
    print(f"Wrote wide TSV: {wide_tsv}")
    print(f"Wrote wide aligned TXT: {wide_txt}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
