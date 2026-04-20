#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import math
import statistics
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from xml.sax.saxutils import escape


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
class MeanPoint:
    source: str
    queue_depth: int
    mixed_inflight: int
    strong_hot_inflight: int
    strong_cold_inflight: int
    strong_cold_ratio: float
    mode: str
    runs: int
    elapsed_time_mean_s: float
    total_hot_requests_mean: float
    avg_latency_mean_us: float
    avg_latency_std_us: float
    p95_mean_us: float
    p95_std_us: float
    p99_mean_us: float
    p99_std_us: float
    hot_throughput_mean_mb_s: float
    hot_throughput_std_mb_s: float
    cold_throughput_mean_mb_s: float
    cold_throughput_std_mb_s: float
    hot_thread_errors_sum: int
    cold_thread_errors_sum: int


MODE_COLORS = {
    "hot-only": "#D55D3E",
    "mixed": "#2C5F8A",
    "strong-i": "#2E8B57",
}

METRICS = [
    {
        "title": "Hot Throughput",
        "unit": "MB/s",
        "field": "hot_throughput_mean_mb_s",
        "transform": lambda point: point.hot_throughput_mean_mb_s,
        "tick": lambda value: f"{value:.0f}",
    },
    {
        "title": "Cold Throughput",
        "unit": "MB/s",
        "field": "cold_throughput_mean_mb_s",
        "transform": lambda point: point.cold_throughput_mean_mb_s,
        "tick": lambda value: f"{value:.0f}",
    },
    {
        "title": "Average Latency",
        "unit": "ms",
        "field": "avg_latency_mean_us",
        "transform": lambda point: point.avg_latency_mean_us / 1000.0,
        "tick": lambda value: f"{value:.0f}",
    },
    {
        "title": "P95 Latency",
        "unit": "ms",
        "field": "p95_mean_us",
        "transform": lambda point: point.p95_mean_us / 1000.0,
        "tick": lambda value: f"{value:.0f}",
    },
]


def parse_metric(value: str) -> float:
    return float(value.split()[0])


def mean(values: list[float]) -> float:
    return statistics.fmean(values) if values else 0.0


def stddev(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    return statistics.stdev(values)


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
    samples: list[Sample] = []

    for path in sorted(input_root.glob("*/summary_all.tsv")):
        samples.extend(parse_summary_file(path))

    return samples


def build_mean_points(samples: list[Sample]) -> list[MeanPoint]:
    groups: dict[tuple[str, int, int, int, int, str], list[Sample]] = defaultdict(list)

    for sample in samples:
        key = (
            sample.source,
            sample.queue_depth,
            sample.mixed_inflight,
            sample.strong_hot_inflight,
            sample.strong_cold_inflight,
            sample.mode,
        )
        groups[key].append(sample)

    mean_points: list[MeanPoint] = []

    for key in sorted(groups):
        source, queue_depth, mixed_inflight, strong_hot_inflight, strong_cold_inflight, mode = key
        group = groups[key]

        mean_points.append(
            MeanPoint(
                source=source,
                queue_depth=queue_depth,
                mixed_inflight=mixed_inflight,
                strong_hot_inflight=strong_hot_inflight,
                strong_cold_inflight=strong_cold_inflight,
                strong_cold_ratio=strong_cold_inflight / mixed_inflight,
                mode=mode,
                runs=len(group),
                elapsed_time_mean_s=mean([sample.elapsed_time_s for sample in group]),
                total_hot_requests_mean=mean([sample.total_hot_requests for sample in group]),
                avg_latency_mean_us=mean([sample.avg_latency_us for sample in group]),
                avg_latency_std_us=stddev([sample.avg_latency_us for sample in group]),
                p95_mean_us=mean([sample.p95_latency_us for sample in group]),
                p95_std_us=stddev([sample.p95_latency_us for sample in group]),
                p99_mean_us=mean([sample.p99_latency_us for sample in group]),
                p99_std_us=stddev([sample.p99_latency_us for sample in group]),
                hot_throughput_mean_mb_s=mean([sample.hot_throughput_mb_s for sample in group]),
                hot_throughput_std_mb_s=stddev([sample.hot_throughput_mb_s for sample in group]),
                cold_throughput_mean_mb_s=mean([sample.cold_throughput_mb_s for sample in group]),
                cold_throughput_std_mb_s=stddev([sample.cold_throughput_mb_s for sample in group]),
                hot_thread_errors_sum=sum(sample.hot_thread_errors for sample in group),
                cold_thread_errors_sum=sum(sample.cold_thread_errors for sample in group),
            )
        )

    return mean_points


def write_means_tsv(mean_points: list[MeanPoint], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "source",
        "queue_depth",
        "mixed_inflight",
        "strong_hot_inflight",
        "strong_cold_inflight",
        "strong_cold_ratio",
        "mode",
        "runs",
        "elapsed_time_mean_s",
        "total_hot_requests_mean",
        "avg_latency_mean_us",
        "avg_latency_std_us",
        "p95_mean_us",
        "p95_std_us",
        "p99_mean_us",
        "p99_std_us",
        "hot_throughput_mean_mb_s",
        "hot_throughput_std_mb_s",
        "cold_throughput_mean_mb_s",
        "cold_throughput_std_mb_s",
        "hot_thread_errors_sum",
        "cold_thread_errors_sum",
    ]

    with output_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, delimiter="\t", fieldnames=fieldnames)
        writer.writeheader()
        for point in mean_points:
            writer.writerow(
                {
                    "source": point.source,
                    "queue_depth": point.queue_depth,
                    "mixed_inflight": point.mixed_inflight,
                    "strong_hot_inflight": point.strong_hot_inflight,
                    "strong_cold_inflight": point.strong_cold_inflight,
                    "strong_cold_ratio": f"{point.strong_cold_ratio:.2f}",
                    "mode": point.mode,
                    "runs": point.runs,
                    "elapsed_time_mean_s": f"{point.elapsed_time_mean_s:.3f}",
                    "total_hot_requests_mean": f"{point.total_hot_requests_mean:.2f}",
                    "avg_latency_mean_us": f"{point.avg_latency_mean_us:.2f}",
                    "avg_latency_std_us": f"{point.avg_latency_std_us:.2f}",
                    "p95_mean_us": f"{point.p95_mean_us:.2f}",
                    "p95_std_us": f"{point.p95_std_us:.2f}",
                    "p99_mean_us": f"{point.p99_mean_us:.2f}",
                    "p99_std_us": f"{point.p99_std_us:.2f}",
                    "hot_throughput_mean_mb_s": f"{point.hot_throughput_mean_mb_s:.2f}",
                    "hot_throughput_std_mb_s": f"{point.hot_throughput_std_mb_s:.2f}",
                    "cold_throughput_mean_mb_s": f"{point.cold_throughput_mean_mb_s:.2f}",
                    "cold_throughput_std_mb_s": f"{point.cold_throughput_std_mb_s:.2f}",
                    "hot_thread_errors_sum": point.hot_thread_errors_sum,
                    "cold_thread_errors_sum": point.cold_thread_errors_sum,
                }
            )


def build_mean_rows(mean_points: list[MeanPoint]) -> tuple[list[str], list[list[str]]]:
    headers = [
        "source",
        "queue_depth",
        "mixed_inflight",
        "strong_hot_inflight",
        "strong_cold_inflight",
        "strong_cold_ratio",
        "mode",
        "runs",
        "elapsed_time_mean_s",
        "total_hot_requests_mean",
        "avg_latency_mean_us",
        "avg_latency_std_us",
        "p95_mean_us",
        "p95_std_us",
        "p99_mean_us",
        "p99_std_us",
        "hot_throughput_mean_mb_s",
        "hot_throughput_std_mb_s",
        "cold_throughput_mean_mb_s",
        "cold_throughput_std_mb_s",
        "hot_thread_errors_sum",
        "cold_thread_errors_sum",
    ]

    rows = []
    for point in mean_points:
        rows.append(build_mean_row(point))

    return headers, rows


def build_mean_row(point: MeanPoint) -> list[str]:
    return [
        point.source,
        str(point.queue_depth),
        str(point.mixed_inflight),
        str(point.strong_hot_inflight),
        str(point.strong_cold_inflight),
        f"{point.strong_cold_ratio:.2f}",
        point.mode,
        str(point.runs),
        f"{point.elapsed_time_mean_s:.3f}",
        f"{point.total_hot_requests_mean:.2f}",
        f"{point.avg_latency_mean_us:.2f}",
        f"{point.avg_latency_std_us:.2f}",
        f"{point.p95_mean_us:.2f}",
        f"{point.p95_std_us:.2f}",
        f"{point.p99_mean_us:.2f}",
        f"{point.p99_std_us:.2f}",
        f"{point.hot_throughput_mean_mb_s:.2f}",
        f"{point.hot_throughput_std_mb_s:.2f}",
        f"{point.cold_throughput_mean_mb_s:.2f}",
        f"{point.cold_throughput_std_mb_s:.2f}",
        str(point.hot_thread_errors_sum),
        str(point.cold_thread_errors_sum),
    ]


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


def write_aligned_text(mean_points: list[MeanPoint], output_path: Path) -> None:
    headers, rows = build_mean_rows(mean_points)
    lines = format_aligned_table(headers, rows)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_mode_grouped_text(mean_points: list[MeanPoint], output_path: Path) -> None:
    headers, _ = build_mean_rows(mean_points)
    mode_order = ["hot-only", "mixed", "strong-i"]
    lines: list[str] = []

    for index, mode in enumerate(mode_order):
        grouped_points = sorted(
            [point for point in mean_points if point.mode == mode],
            key=lambda point: (point.queue_depth, point.strong_cold_ratio, point.source),
        )
        if not grouped_points:
            continue

        if index > 0 and lines:
            lines.append("")

        lines.append(f"=== {mode} ===")
        lines.extend(format_aligned_table(headers, [build_mean_row(point) for point in grouped_points]))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def is_strong_i_hot_budget_10000(point: MeanPoint) -> bool:
    return point.mode == "strong-i" and abs(point.total_hot_requests_mean - 10000.0) <= 1000.0


def is_strong_i_timed_5s(point: MeanPoint) -> bool:
    return point.mode == "strong-i" and abs(point.elapsed_time_mean_s - 5.0) <= 0.6


def write_filtered_text(
    mean_points: list[MeanPoint],
    output_path: Path,
    title: str,
    predicate,
) -> None:
    headers, _ = build_mean_rows(mean_points)
    filtered_points = sorted(
        [point for point in mean_points if predicate(point)],
        key=lambda point: (point.queue_depth, point.strong_cold_ratio, point.source),
    )

    lines = [title, ""]
    if filtered_points:
        lines.extend(format_aligned_table(headers, [build_mean_row(point) for point in filtered_points]))
    else:
        lines.append("No rows matched.")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def nice_step(max_value: float, tick_count: int = 5) -> float:
    if max_value <= 0:
        return 1.0

    raw = max_value / max(tick_count - 1, 1)
    magnitude = 10 ** math.floor(math.log10(raw))

    for base in (1, 2, 5, 10):
        step = base * magnitude
        if step >= raw:
            return step

    return 10 * magnitude


def metric_ceiling(values: list[float]) -> tuple[float, list[float]]:
    max_value = max(values) if values else 0.0
    step = nice_step(max_value)
    ceiling = step * max(1, math.ceil(max_value / step))
    ticks = [step * index for index in range(int(ceiling / step) + 1)]
    return ceiling, ticks


def svg_text(x: float, y: float, text: str, extra: str = "") -> str:
    return f'<text x="{x:.1f}" y="{y:.1f}" {extra}>{escape(text)}</text>'


def render_svg(mean_points: list[MeanPoint], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    ratios = sorted({point.strong_cold_ratio for point in mean_points})
    queue_depths = sorted({point.queue_depth for point in mean_points})
    modes = ["hot-only", "mixed", "strong-i"]

    width = 1800
    height = 980
    margin_left = 110
    margin_right = 40
    margin_top = 120
    margin_bottom = 70
    col_gap = 24
    row_gap = 42
    cols = len(METRICS)
    rows = len(ratios)

    panel_width = (width - margin_left - margin_right - col_gap * (cols - 1)) / cols
    panel_height = (height - margin_top - margin_bottom - row_gap * (rows - 1)) / rows

    grouped: dict[tuple[float, str], list[MeanPoint]] = defaultdict(list)
    for point in mean_points:
        grouped[(point.strong_cold_ratio, point.mode)].append(point)

    metric_scales = {}
    for metric in METRICS:
        values = [metric["transform"](point) for point in mean_points]
        metric_scales[metric["field"]] = metric_ceiling(values)

    elements: list[str] = []

    elements.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">'
    )
    elements.append(
        """
<style>
  .title { font: 700 28px 'DejaVu Sans', sans-serif; fill: #1d2433; }
  .subtitle { font: 400 14px 'DejaVu Sans', sans-serif; fill: #4f5d75; }
  .panel-title { font: 700 16px 'DejaVu Sans', sans-serif; fill: #24324a; }
  .axis { font: 400 12px 'DejaVu Sans', sans-serif; fill: #39475d; }
  .tick { font: 400 11px 'DejaVu Sans', sans-serif; fill: #53627a; }
  .legend { font: 600 13px 'DejaVu Sans', sans-serif; fill: #24324a; }
</style>
        """.strip()
    )
    elements.append(f'<rect x="0" y="0" width="{width}" height="{height}" fill="#f7f3ea"/>')

    elements.append(svg_text(40, 50, "Queue Depth Mean Metrics", 'class="title"'))
    elements.append(
        svg_text(
            40,
            76,
            "Each point is the mean of 5 runs from one summary_all.tsv batch. Output uses SVG so no plotting dependency is required.",
            'class="subtitle"',
        )
    )

    legend_y = 98
    legend_x = 40
    for mode in modes:
        color = MODE_COLORS[mode]
        elements.append(
            f'<line x1="{legend_x}" y1="{legend_y}" x2="{legend_x + 28}" y2="{legend_y}" stroke="{color}" stroke-width="4" stroke-linecap="round"/>'
        )
        elements.append(f'<circle cx="{legend_x + 14}" cy="{legend_y}" r="5" fill="{color}" />')
        elements.append(svg_text(legend_x + 38, legend_y + 4, mode, 'class="legend"'))
        legend_x += 150

    for row_index, ratio in enumerate(ratios):
        row_top = margin_top + row_index * (panel_height + row_gap)
        row_label = f"strong_cold / mixed = {ratio:.2f}"
        elements.append(svg_text(40, row_top + 24, row_label, 'class="panel-title"'))

        for col_index, metric in enumerate(METRICS):
            panel_left = margin_left + col_index * (panel_width + col_gap)
            panel_top = row_top
            panel_right = panel_left + panel_width
            panel_bottom = panel_top + panel_height

            plot_left = panel_left + 62
            plot_right = panel_right - 18
            plot_top = panel_top + 40
            plot_bottom = panel_bottom - 46
            plot_width = plot_right - plot_left
            plot_height = plot_bottom - plot_top

            y_max, ticks = metric_scales[metric["field"]]

            elements.append(
                f'<rect x="{panel_left:.1f}" y="{panel_top:.1f}" width="{panel_width:.1f}" height="{panel_height:.1f}" rx="14" fill="#fffdf8" stroke="#d9cfbf" stroke-width="1.2"/>'
            )
            elements.append(
                svg_text(panel_left + 16, panel_top + 24, f'{metric["title"]} ({metric["unit"]})', 'class="panel-title"')
            )

            for tick in ticks:
                if y_max == 0:
                    y = plot_bottom
                else:
                    y = plot_bottom - (tick / y_max) * plot_height
                elements.append(
                    f'<line x1="{plot_left:.1f}" y1="{y:.1f}" x2="{plot_right:.1f}" y2="{y:.1f}" stroke="#e7e1d5" stroke-width="1"/>'
                )
                elements.append(svg_text(plot_left - 10, y + 4, metric["tick"](tick), 'class="tick" text-anchor="end"'))

            elements.append(
                f'<line x1="{plot_left:.1f}" y1="{plot_bottom:.1f}" x2="{plot_right:.1f}" y2="{plot_bottom:.1f}" stroke="#8793a5" stroke-width="1.2"/>'
            )
            elements.append(
                f'<line x1="{plot_left:.1f}" y1="{plot_top:.1f}" x2="{plot_left:.1f}" y2="{plot_bottom:.1f}" stroke="#8793a5" stroke-width="1.2"/>'
            )

            for depth_index, queue_depth in enumerate(queue_depths):
                if len(queue_depths) == 1:
                    x = (plot_left + plot_right) / 2
                else:
                    x = plot_left + depth_index * plot_width / (len(queue_depths) - 1)
                elements.append(
                    f'<line x1="{x:.1f}" y1="{plot_bottom:.1f}" x2="{x:.1f}" y2="{plot_bottom + 6:.1f}" stroke="#8793a5" stroke-width="1"/>'
                )
                elements.append(svg_text(x, plot_bottom + 22, str(queue_depth), 'class="axis" text-anchor="middle"'))

            elements.append(svg_text((plot_left + plot_right) / 2, plot_bottom + 40, "Queue Depth", 'class="axis" text-anchor="middle"'))

            for mode in modes:
                color = MODE_COLORS[mode]
                points = sorted(
                    grouped[(ratio, mode)],
                    key=lambda point: point.queue_depth,
                )

                if not points:
                    continue

                polyline_parts = []
                for point in points:
                    x_index = queue_depths.index(point.queue_depth)
                    if len(queue_depths) == 1:
                        x = (plot_left + plot_right) / 2
                    else:
                        x = plot_left + x_index * plot_width / (len(queue_depths) - 1)
                    value = metric["transform"](point)
                    y = plot_bottom if y_max == 0 else plot_bottom - (value / y_max) * plot_height
                    polyline_parts.append(f"{x:.1f},{y:.1f}")

                if len(polyline_parts) >= 2:
                    elements.append(
                        f'<polyline points="{" ".join(polyline_parts)}" fill="none" stroke="{color}" stroke-width="3.2" stroke-linejoin="round" stroke-linecap="round"/>'
                    )

                for point in points:
                    x_index = queue_depths.index(point.queue_depth)
                    if len(queue_depths) == 1:
                        x = (plot_left + plot_right) / 2
                    else:
                        x = plot_left + x_index * plot_width / (len(queue_depths) - 1)
                    value = metric["transform"](point)
                    y = plot_bottom if y_max == 0 else plot_bottom - (value / y_max) * plot_height
                    elements.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4.8" fill="{color}" stroke="#fffdf8" stroke-width="1.4"/>')

    elements.append("</svg>")

    output_path.write_text("\n".join(elements), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[1]
    default_input_root = repo_root / "results" / "queue-depth-matrix-repeat5"
    default_output_dir = default_input_root / "analysis"

    parser = argparse.ArgumentParser(
        description="Compute 5-run means from summary_all.tsv files and render an SVG plot."
    )
    parser.add_argument(
        "--input-root",
        type=Path,
        default=default_input_root,
        help=f"Directory that contains timestamped summary_all.tsv folders. Default: {default_input_root}",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=default_output_dir,
        help=f"Directory for the mean TSV and SVG outputs. Default: {default_output_dir}",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    samples = load_samples(args.input_root)

    if not samples:
        raise SystemExit(f"No summary_all.tsv files found under {args.input_root}")

    mean_points = build_mean_points(samples)

    means_tsv = args.output_dir / "queue_depth_means.tsv"
    means_txt = args.output_dir / "queue_depth_means_aligned.txt"
    means_by_mode_txt = args.output_dir / "queue_depth_means_by_mode.txt"
    strong_i_budget_txt = args.output_dir / "queue_depth_means_strong_i_hot_budget_10000.txt"
    strong_i_timed_txt = args.output_dir / "queue_depth_means_strong_i_timed_5s.txt"
    svg_path = args.output_dir / "queue_depth_metrics.svg"

    write_means_tsv(mean_points, means_tsv)
    write_aligned_text(mean_points, means_txt)
    write_mode_grouped_text(mean_points, means_by_mode_txt)
    write_filtered_text(
        mean_points,
        strong_i_budget_txt,
        "=== strong-i / hot budget ~= 10000 ===",
        is_strong_i_hot_budget_10000,
    )
    write_filtered_text(
        mean_points,
        strong_i_timed_txt,
        "=== strong-i / timed run ~= 5s ===",
        is_strong_i_timed_5s,
    )
    render_svg(mean_points, svg_path)

    print(f"Wrote mean table: {means_tsv}")
    print(f"Wrote aligned mean table: {means_txt}")
    print(f"Wrote mode-grouped mean table: {means_by_mode_txt}")
    print(f"Wrote strong-i hot-budget table: {strong_i_budget_txt}")
    print(f"Wrote strong-i timed-5s table: {strong_i_timed_txt}")
    print(f"Wrote SVG plot: {svg_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
