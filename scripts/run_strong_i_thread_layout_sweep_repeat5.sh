#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
CHILD_SCRIPT="${SCRIPT_DIR}/run_queue_depth_matrix_repeat5.sh"

RUNS=5
OUTPUT_DIR=""
LAYOUTS_CSV="4:12,6:10,8:8"
QUEUE_DEPTHS_CSV="16,32,64,128"
FORWARD_ARGS=()

TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
OUTPUT_DIR_DEFAULT="${ROOT_DIR}/results/strong-i-queue-layout-repeat5/${TIMESTAMP}"

usage() {
    cat <<'EOF'
Usage:
  scripts/run_strong_i_queue_layout_sweep_repeat5.sh [options] [child-options...]

Examples:
  scripts/run_strong_i_queue_layout_sweep_repeat5.sh
  scripts/run_strong_i_queue_layout_sweep_repeat5.sh --layouts 4:12,5:11,6:10
  scripts/run_strong_i_queue_layout_sweep_repeat5.sh --layouts 1:15,2:14,3:13,4:12
  scripts/run_strong_i_queue_layout_sweep_repeat5.sh --layouts 4:12,8:8 --hot-budget 10000 --cold-budget 60000
  scripts/run_strong_i_queue_layout_sweep_repeat5.sh --queue-depths 32,64 --strong-i-1to1

Options:
  --runs N                 Number of repetitions per layout. Default: 5
  --output-dir DIR         Base directory for all layouts
  --layouts LIST           Comma-separated hot:cold queue layouts. Default: 4:12,6:10,8:8
  --queue-depths LIST      Comma-separated queue depths. Default: 16,32,64,128
  -h, --help               Show this help message

Notes:
  1. This helper always runs strong-i only.
  2. Each layout changes strong-i hot/cold queue-pair counts, not producer-thread counts.
  3. Each layout gets its own subdirectory, e.g. DIR/hq4_cq12.
  4. Any remaining options are forwarded to scripts/run_queue_depth_matrix_repeat5.sh.
EOF
}

parse_positive_int() {
    local value="$1"
    [[ "${value}" =~ ^[0-9]+$ ]] && [[ "${value}" -gt 0 ]]
}

validate_layout() {
    local value="$1"
    local hot cold

    if [[ ! "${value}" =~ ^([0-9]+):([0-9]+)$ ]]; then
        return 1
    fi

    hot="${BASH_REMATCH[1]}"
    cold="${BASH_REMATCH[2]}"

    if [[ "${hot}" -le 0 || "${cold}" -lt 0 || $((hot + cold)) -gt 16 ]]; then
        return 1
    fi

    return 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --runs)
            RUNS="$2"
            shift 2
            ;;
        --output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --layouts)
            LAYOUTS_CSV="$2"
            shift 2
            ;;
        --queue-depths)
            QUEUE_DEPTHS_CSV="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            FORWARD_ARGS+=("$1")
            shift
            ;;
    esac
done

if ! parse_positive_int "${RUNS}"; then
    echo "Invalid repetition count: ${RUNS}" >&2
    exit 1
fi

if [[ -z "${OUTPUT_DIR}" ]]; then
    OUTPUT_DIR="${OUTPUT_DIR_DEFAULT}"
fi

if [[ ! -x "${CHILD_SCRIPT}" ]]; then
    echo "Required script is missing or not executable: ${CHILD_SCRIPT}" >&2
    exit 1
fi

IFS=',' read -r -a LAYOUTS <<< "${LAYOUTS_CSV}"
IFS=',' read -r -a QUEUE_DEPTHS <<< "${QUEUE_DEPTHS_CSV}"

if [[ ${#LAYOUTS[@]} -eq 0 ]]; then
    echo "At least one layout is required." >&2
    exit 1
fi

if [[ ${#QUEUE_DEPTHS[@]} -eq 0 ]]; then
    echo "At least one queue depth is required." >&2
    exit 1
fi

mkdir -p "${OUTPUT_DIR}"

for layout in "${LAYOUTS[@]}"; do
    if ! validate_layout "${layout}"; then
        echo "Invalid layout: ${layout}. Use hot:cold queue counts with hot>=1, cold>=0, hot+cold<=16." >&2
        exit 1
    fi

    hot_queues="${layout%%:*}"
    cold_queues="${layout##*:}"
    layout_dir="${OUTPUT_DIR}/hq${hot_queues}_cq${cold_queues}"

    echo
    echo "==== Layout ${layout} ===="
    printf 'Command: %q' "${CHILD_SCRIPT}"
    printf ' %q' \
        --runs "${RUNS}" \
        --output-dir "${layout_dir}" \
        --modes strong-i \
        --hot-queues "${hot_queues}" \
        --cold-queues "${cold_queues}"
    for queue_depth in "${QUEUE_DEPTHS[@]}"; do
        printf ' %q' "${queue_depth}"
    done
    if [[ ${#FORWARD_ARGS[@]} -gt 0 ]]; then
        printf ' %q' "${FORWARD_ARGS[@]}"
    fi
    printf '\n'

    bash "${CHILD_SCRIPT}" \
        --runs "${RUNS}" \
        --output-dir "${layout_dir}" \
        --modes strong-i \
        --hot-queues "${hot_queues}" \
        --cold-queues "${cold_queues}" \
        "${QUEUE_DEPTHS[@]}" \
        "${FORWARD_ARGS[@]}"
done

echo
echo "==== Layout Sweep Done ===="
echo "All layout results saved to: ${OUTPUT_DIR}"
