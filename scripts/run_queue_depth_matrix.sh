#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
BIN_PATH="${ROOT_DIR}/build/bin/nvm-test-bench"
MODULE_DIR="${ROOT_DIR}/module"

DEFAULT_QUEUE_DEPTHS=(64 128)
QUEUE_DEPTHS=()

MIXED_INFLIGHT="${MIXED_INFLIGHT:-32}"
STRONG_HOT_INFLIGHT="${STRONG_HOT_INFLIGHT:-16}"
STRONG_COLD_INFLIGHT="${STRONG_COLD_INFLIGHT:-8}"
RESET_MODULES=1

TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
OUTPUT_DIR_DEFAULT="${ROOT_DIR}/results/queue-depth-matrix/${TIMESTAMP}"
OUTPUT_DIR=""

usage() {
    cat <<'EOF'
Usage:
  scripts/run_queue_depth_matrix.sh [options] [queue_depth...]

Examples:
  scripts/run_queue_depth_matrix.sh
  scripts/run_queue_depth_matrix.sh 64 128
  scripts/run_queue_depth_matrix.sh --mixed-inflight 16 --strong-hot-inflight 16 --strong-cold-inflight 8 64 128

Options:
  --mixed-inflight N         Inflight per queue pair for mixed/hot-only. Default: 32
  --strong-hot-inflight N    Inflight per hot queue pair for strong-i. Default: 16
  --strong-cold-inflight N   Inflight per cold queue pair for strong-i. Default: 8
  --output-dir DIR           Directory for logs and summary output
  --no-reset                 Do not reload snvme modules before each benchmark case
  -h, --help                 Show this help message

Notes:
  1. This script assumes build/bin/nvm-test-bench and module/*.ko are already built.
  2. It runs three modes for each queue depth: hot-only, mixed, strong-i.
  3. The script uses sudo when reloading modules and running the benchmark.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --mixed-inflight)
            MIXED_INFLIGHT="$2"
            shift 2
            ;;
        --strong-hot-inflight)
            STRONG_HOT_INFLIGHT="$2"
            shift 2
            ;;
        --strong-cold-inflight)
            STRONG_COLD_INFLIGHT="$2"
            shift 2
            ;;
        --output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --no-reset)
            RESET_MODULES=0
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            QUEUE_DEPTHS+=("$1")
            shift
            ;;
    esac
done

if [[ ${#QUEUE_DEPTHS[@]} -eq 0 ]]; then
    QUEUE_DEPTHS=("${DEFAULT_QUEUE_DEPTHS[@]}")
fi

if [[ -z "${OUTPUT_DIR}" ]]; then
    OUTPUT_DIR="${OUTPUT_DIR_DEFAULT}"
fi

SUMMARY_FILE="${OUTPUT_DIR}/summary.tsv"

require_file() {
    local path="$1"
    if [[ ! -e "${path}" ]]; then
        echo "Required file not found: ${path}" >&2
        exit 1
    fi
}

extract_metric() {
    local file="$1"
    local key="$2"
    awk -F': ' -v target="${key}" '$1 == target { value=$2 } END { print value }' "${file}"
}

reset_modules() {
    local queue_depth="$1"

    echo "==== Resetting modules for queue depth ${queue_depth} ===="
    pushd "${MODULE_DIR}" >/dev/null

    sudo rmmod snvme 2>/dev/null || true
    sudo rmmod snvme_core 2>/dev/null || true

    sudo modprobe nvme
    echo "Waiting for native NVMe driver to settle..."
    sleep 2

    sudo insmod snvme-core.ko multipath=0
    sudo insmod snvme.ko io_queue_depth="${queue_depth}"

    popd >/dev/null
}

run_case() {
    local mode="$1"
    local queue_depth="$2"
    local log_file="$3"
    local -a cmd

    cmd=(sudo "${BIN_PATH}" "${mode}" --queue-depth "${queue_depth}")

    case "${mode}" in
        mixed|hot-only)
            cmd+=(--mixed-inflight "${MIXED_INFLIGHT}")
            ;;
        strong-i)
            cmd+=(--strong-hot-inflight "${STRONG_HOT_INFLIGHT}" --strong-cold-inflight "${STRONG_COLD_INFLIGHT}")
            ;;
        *)
            echo "Unknown mode: ${mode}" >&2
            exit 1
            ;;
    esac

    echo
    echo "==== Running ${mode} @ qd=${queue_depth} ===="
    printf 'Command:'
    printf ' %q' "${cmd[@]}"
    printf '\n'

    "${cmd[@]}" | tee "${log_file}"
}

append_summary() {
    local mode="$1"
    local queue_depth="$2"
    local log_file="$3"

    local elapsed
    local total_hot_requests
    local avg_latency
    local p95_latency
    local p99_latency
    local hot_tp
    local cold_tp
    local hot_errors
    local cold_errors

    elapsed="$(extract_metric "${log_file}" "Elapsed Time")"
    total_hot_requests="$(extract_metric "${log_file}" "Total Hot Requests")"
    avg_latency="$(extract_metric "${log_file}" "Average Latency")"
    p95_latency="$(extract_metric "${log_file}" "P95 Tail Latency")"
    p99_latency="$(extract_metric "${log_file}" "P99 Tail Latency")"
    hot_tp="$(extract_metric "${log_file}" "Hot Throughput")"
    cold_tp="$(extract_metric "${log_file}" "Cold Throughput")"
    hot_errors="$(extract_metric "${log_file}" "Hot Thread Errors")"
    cold_errors="$(extract_metric "${log_file}" "Cold Thread Errors")"

    printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
        "${mode}" \
        "${queue_depth}" \
        "${MIXED_INFLIGHT}" \
        "${STRONG_HOT_INFLIGHT}" \
        "${STRONG_COLD_INFLIGHT}" \
        "${elapsed}" \
        "${total_hot_requests}" \
        "${avg_latency}" \
        "${p95_latency}" \
        "${p99_latency}" \
        "${hot_tp}" \
        "${cold_tp}" \
        "${hot_errors}" \
        "${cold_errors}" \
        >> "${SUMMARY_FILE}"
}

mkdir -p "${OUTPUT_DIR}"

cat > "${SUMMARY_FILE}" <<'EOF'
mode	queue_depth	mixed_inflight	strong_hot_inflight	strong_cold_inflight	elapsed_time	total_hot_requests	avg_latency	p95_latency	p99_latency	hot_throughput	cold_throughput	hot_thread_errors	cold_thread_errors
EOF

require_file "${BIN_PATH}"
require_file "${MODULE_DIR}/snvme.ko"
require_file "${MODULE_DIR}/snvme-core.ko"

for queue_depth in "${QUEUE_DEPTHS[@]}"; do
    depth_dir="${OUTPUT_DIR}/qd${queue_depth}"
    mkdir -p "${depth_dir}"

    if [[ "${RESET_MODULES}" -eq 1 ]]; then
        reset_modules "${queue_depth}"
    fi
    run_case hot-only "${queue_depth}" "${depth_dir}/hot-only.log"
    append_summary hot-only "${queue_depth}" "${depth_dir}/hot-only.log"

    if [[ "${RESET_MODULES}" -eq 1 ]]; then
        reset_modules "${queue_depth}"
    fi
    run_case mixed "${queue_depth}" "${depth_dir}/mixed.log"
    append_summary mixed "${queue_depth}" "${depth_dir}/mixed.log"

    if [[ "${RESET_MODULES}" -eq 1 ]]; then
        reset_modules "${queue_depth}"
    fi
    run_case strong-i "${queue_depth}" "${depth_dir}/strong-i.log"
    append_summary strong-i "${queue_depth}" "${depth_dir}/strong-i.log"
done

echo
echo "==== Done ===="
echo "Logs saved to: ${OUTPUT_DIR}"
echo "Summary saved to: ${SUMMARY_FILE}"
