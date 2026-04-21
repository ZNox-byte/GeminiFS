#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
CHILD_SCRIPT="${SCRIPT_DIR}/run_queue_depth_matrix.sh"

RUNS=5
OUTPUT_DIR=""
FORWARD_ARGS=()
STRONG_I_1TO1_SWEEP=0
STRONG_I_1TO1_BUDGET_SWEEP=0

TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
OUTPUT_DIR_DEFAULT="${ROOT_DIR}/results/queue-depth-matrix-repeat5/${TIMESTAMP}"

usage() {
    cat <<'EOF'
Usage:
  scripts/run_queue_depth_matrix_repeat5.sh [wrapper-options] [child-options...]

Examples:
  scripts/run_queue_depth_matrix_repeat5.sh --hot-budget 10000 --cold-budget 60000 32
  scripts/run_queue_depth_matrix_repeat5.sh --output-dir results/repeat-qd32 32 32 0.5 0.25
  scripts/run_queue_depth_matrix_repeat5.sh --runs 3 --mixed-inflight 32 33
  scripts/run_queue_depth_matrix_repeat5.sh --strong-i-1to1-sweep
  scripts/run_queue_depth_matrix_repeat5.sh --strong-i-1to1-budget-sweep

Wrapper options:
  --runs N                 Number of repetitions. Default: 5
  --output-dir DIR         Base directory for repeated runs
  --strong-i-1to1-sweep    Run qd=16,32,64,128 in strong-i only, with hot/cold inflight = queue_depth
  --strong-i-1to1-budget-sweep
                           Same as --strong-i-1to1-sweep, plus --hot-budget 10000 --cold-budget 60000
  -h, --help               Show this help message

Notes:
  1. All non-wrapper options are forwarded to scripts/run_queue_depth_matrix.sh.
  2. Each repetition writes to DIR/run1, DIR/run2, ..., DIR/runN.
  3. The wrapper also creates DIR/summary_all.tsv with the per-run summaries merged.
  4. --strong-i-1to1-sweep expands to:
       --modes strong-i --strong-i-1to1 16 32 64 128
  5. --strong-i-1to1-budget-sweep expands to:
       --modes strong-i --strong-i-1to1 --hot-budget 10000 --cold-budget 60000 16 32 64 128
EOF
}

parse_positive_int() {
    local value="$1"
    [[ "${value}" =~ ^[0-9]+$ ]] && [[ "${value}" -gt 0 ]]
}

render_aligned_summary() {
    local input_tsv="$1"
    local output_txt="$2"

    if command -v column >/dev/null 2>&1; then
        column -t -s $'\t' "${input_tsv}" > "${output_txt}"
    else
        cp "${input_tsv}" "${output_txt}"
    fi
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
        --strong-i-1to1-sweep)
            STRONG_I_1TO1_SWEEP=1
            shift
            ;;
        --strong-i-1to1-budget-sweep)
            STRONG_I_1TO1_BUDGET_SWEEP=1
            shift
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

if [[ "${STRONG_I_1TO1_SWEEP}" -eq 1 ]] && [[ "${STRONG_I_1TO1_BUDGET_SWEEP}" -eq 1 ]]; then
    echo "Choose either --strong-i-1to1-sweep or --strong-i-1to1-budget-sweep, not both." >&2
    exit 1
fi

if [[ "${STRONG_I_1TO1_SWEEP}" -eq 1 ]]; then
    FORWARD_ARGS=(--modes strong-i --strong-i-1to1 16 32 64 128 "${FORWARD_ARGS[@]}")
fi

if [[ "${STRONG_I_1TO1_BUDGET_SWEEP}" -eq 1 ]]; then
    FORWARD_ARGS=(--modes strong-i --strong-i-1to1 --hot-budget 10000 --cold-budget 60000 16 32 64 128 "${FORWARD_ARGS[@]}")
fi

mkdir -p "${OUTPUT_DIR}"

AGG_SUMMARY="${OUTPUT_DIR}/summary_all.tsv"
AGG_SUMMARY_ALIGNED="${OUTPUT_DIR}/summary_all_aligned.txt"
printf "run\tmode\tqueue_depth\tmixed_inflight\tstrong_hot_inflight\tstrong_cold_inflight\thot_threads\tcold_threads\thot_queues\tcold_queues\telapsed_time\ttotal_hot_requests\tavg_latency\tp95_latency\tp99_latency\thot_throughput\tcold_throughput\thot_thread_errors\tcold_thread_errors\n" > "${AGG_SUMMARY}"

for ((run = 1; run <= RUNS; ++run)); do
    run_dir="${OUTPUT_DIR}/run${run}"

    echo
    echo "==== Repetition ${run}/${RUNS} ===="
    printf 'Forwarded command: %q' "${CHILD_SCRIPT}"
    printf ' %q' --output-dir "${run_dir}"
    if [[ ${#FORWARD_ARGS[@]} -gt 0 ]]; then
        printf ' %q' "${FORWARD_ARGS[@]}"
    fi
    printf '\n'

    bash "${CHILD_SCRIPT}" --output-dir "${run_dir}" "${FORWARD_ARGS[@]}"

    child_summary="${run_dir}/summary.tsv"
    if [[ ! -f "${child_summary}" ]]; then
        echo "Missing child summary file: ${child_summary}" >&2
        exit 1
    fi

    awk -v run="${run}" 'NR > 1 { print run "\t" $0 }' "${child_summary}" >> "${AGG_SUMMARY}"
done

render_aligned_summary "${AGG_SUMMARY}" "${AGG_SUMMARY_ALIGNED}"

echo
echo "==== Repeat Run Done ===="
echo "Per-run results saved to: ${OUTPUT_DIR}"
echo "Merged summary saved to: ${AGG_SUMMARY}"
echo "Aligned summary saved to: ${AGG_SUMMARY_ALIGNED}"
