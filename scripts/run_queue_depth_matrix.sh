#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
BIN_PATH="${ROOT_DIR}/build/bin/nvm-test-bench"
MODULE_DIR="${ROOT_DIR}/module"

DEFAULT_QUEUE_DEPTHS=(64 128)
QUEUE_DEPTHS=()
DEFAULT_MODES=(hot-only mixed strong-i)
MODES=("${DEFAULT_MODES[@]}")

MIXED_INFLIGHT="${MIXED_INFLIGHT:-32}"
STRONG_HOT_INFLIGHT="${STRONG_HOT_INFLIGHT:-16}"
STRONG_COLD_INFLIGHT="${STRONG_COLD_INFLIGHT:-8}"
HOT_BUDGET="${HOT_BUDGET:-}"
COLD_BUDGET="${COLD_BUDGET:-}"
STRONG_HOT_RATIO=""
STRONG_COLD_RATIO=""
RESET_MODULES=1
STRONG_I_1TO1=0
EXPLICIT_MIXED_INFLIGHT=0
EXPLICIT_STRONG_HOT_INFLIGHT=0
EXPLICIT_STRONG_COLD_INFLIGHT=0

TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
OUTPUT_DIR_DEFAULT="${ROOT_DIR}/results/queue-depth-matrix/${TIMESTAMP}"
OUTPUT_DIR=""

usage() {
    cat <<'EOF'
Usage:
  scripts/run_queue_depth_matrix.sh [options] [queue_depth...]
  scripts/run_queue_depth_matrix.sh <queue_depth> <mixed_inflight> <strong_hot_ratio> <strong_cold_ratio>

Examples:
  scripts/run_queue_depth_matrix.sh
  scripts/run_queue_depth_matrix.sh 64 128
  scripts/run_queue_depth_matrix.sh --mixed-inflight 16 --strong-hot-inflight 16 --strong-cold-inflight 8 64 128
  scripts/run_queue_depth_matrix.sh --hot-budget 10000 --cold-budget 60000 32
  scripts/run_queue_depth_matrix.sh 33 32 0.5 0.25
  scripts/run_queue_depth_matrix.sh --modes strong-i --strong-i-1to1 16 32 64 128

Options:
  --mixed-inflight N         Inflight per queue pair for mixed/hot-only. Default: 32
  --strong-hot-inflight N    Inflight per hot queue pair for strong-i. Default: 16
  --strong-cold-inflight N   Inflight per cold queue pair for strong-i. Default: 8
  --modes LIST               Comma-separated modes to run. Supported: hot-only,mixed,strong-i
  --hot-budget N             Stop after generating N hot requests
  --cold-budget N            Stop after generating N cold requests
  --strong-hot-ratio R       strong-i hot inflight = round(mixed_inflight * R)
  --strong-cold-ratio R      strong-i cold inflight = round(mixed_inflight * R)
  --strong-i-1to1            For each queue depth, set mixed/strong-hot/strong-cold inflight equal to queue_depth
  --output-dir DIR           Directory for logs and summary output
  --no-reset                 Do not reload snvme modules before each benchmark case
  -h, --help                 Show this help message

Notes:
  1. This script assumes build/bin/nvm-test-bench and module/*.ko are already built.
  2. By default it runs three modes for each queue depth: hot-only, mixed, strong-i.
  3. The script uses sudo when reloading modules and running the benchmark.
  4. Compact 4-argument mode means:
       queue_depth, mixed_inflight, strong_hot_ratio, strong_cold_ratio
     For example, "33 32 0.5 0.25" becomes strong-hot=16 and strong-cold=8.
EOF
}

parse_positive_int() {
    local value="$1"
    [[ "${value}" =~ ^[0-9]+$ ]] && [[ "${value}" -gt 0 ]]
}

parse_ratio_value() {
    local value="$1"
    awk -v value="${value}" '
        function fail() {
            exit 1
        }
        BEGIN {
            if (value ~ /^[0-9]+([.][0-9]+)?$/) {
                ratio = value + 0
            } else if (value ~ /^[0-9]+\/[0-9]+$/) {
                split(value, parts, "/")
                if (parts[2] == 0) {
                    fail()
                }
                ratio = parts[1] / parts[2]
            } else {
                fail()
            }

            if (ratio <= 0) {
                fail()
            }

            printf "%.12f\n", ratio
        }
    '
}

scale_inflight_from_ratio() {
    local base="$1"
    local ratio="$2"
    awk -v base="${base}" -v ratio="${ratio}" '
        BEGIN {
            value = int(base * ratio + 0.5)
            if (value < 1) {
                value = 1
            }
            printf "%d\n", value
        }
    '
}

validate_mode() {
    local mode="$1"

    case "${mode}" in
        hot-only|mixed|strong-i)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

parse_mode_list() {
    local value="$1"
    local -a parsed_modes=()
    local item

    IFS=',' read -r -a parsed_modes <<< "${value}"

    if [[ ${#parsed_modes[@]} -eq 0 ]]; then
        echo "Mode list must not be empty." >&2
        exit 1
    fi

    MODES=()
    for item in "${parsed_modes[@]}"; do
        if ! validate_mode "${item}"; then
            echo "Unsupported mode in --modes: ${item}" >&2
            exit 1
        fi
        MODES+=("${item}")
    done
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --mixed-inflight)
            MIXED_INFLIGHT="$2"
            EXPLICIT_MIXED_INFLIGHT=1
            shift 2
            ;;
        --strong-hot-inflight)
            STRONG_HOT_INFLIGHT="$2"
            EXPLICIT_STRONG_HOT_INFLIGHT=1
            shift 2
            ;;
        --strong-cold-inflight)
            STRONG_COLD_INFLIGHT="$2"
            EXPLICIT_STRONG_COLD_INFLIGHT=1
            shift 2
            ;;
        --modes)
            parse_mode_list "$2"
            shift 2
            ;;
        --hot-budget)
            HOT_BUDGET="$2"
            shift 2
            ;;
        --cold-budget)
            COLD_BUDGET="$2"
            shift 2
            ;;
        --strong-hot-ratio)
            STRONG_HOT_RATIO="$2"
            shift 2
            ;;
        --strong-cold-ratio)
            STRONG_COLD_RATIO="$2"
            shift 2
            ;;
        --strong-i-1to1)
            STRONG_I_1TO1=1
            shift
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

if [[ ${#QUEUE_DEPTHS[@]} -eq 4 ]] \
    && [[ "${STRONG_I_1TO1}" -eq 0 ]] \
    && [[ "${EXPLICIT_MIXED_INFLIGHT}" -eq 0 ]] \
    && [[ "${EXPLICIT_STRONG_HOT_INFLIGHT}" -eq 0 ]] \
    && [[ "${EXPLICIT_STRONG_COLD_INFLIGHT}" -eq 0 ]] \
    && [[ -z "${STRONG_HOT_RATIO}" ]] \
    && [[ -z "${STRONG_COLD_RATIO}" ]]; then
    mixed_arg="${QUEUE_DEPTHS[1]}"
    hot_ratio_arg="${QUEUE_DEPTHS[2]}"
    cold_ratio_arg="${QUEUE_DEPTHS[3]}"

    if ! parse_positive_int "${mixed_arg}"; then
        echo "Invalid compact-mode mixed_inflight: ${mixed_arg}" >&2
        exit 1
    fi

    STRONG_HOT_RATIO="$(parse_ratio_value "${hot_ratio_arg}")" || {
        echo "Invalid compact-mode strong_hot_ratio: ${hot_ratio_arg}" >&2
        exit 1
    }
    STRONG_COLD_RATIO="$(parse_ratio_value "${cold_ratio_arg}")" || {
        echo "Invalid compact-mode strong_cold_ratio: ${cold_ratio_arg}" >&2
        exit 1
    }

    MIXED_INFLIGHT="${mixed_arg}"
    EXPLICIT_MIXED_INFLIGHT=1
    QUEUE_DEPTHS=("${QUEUE_DEPTHS[0]}")
fi

if [[ ${#QUEUE_DEPTHS[@]} -eq 0 ]]; then
    QUEUE_DEPTHS=("${DEFAULT_QUEUE_DEPTHS[@]}")
fi

if [[ "${STRONG_I_1TO1}" -eq 1 ]]; then
    if [[ "${EXPLICIT_MIXED_INFLIGHT}" -eq 1 ]] \
        || [[ "${EXPLICIT_STRONG_HOT_INFLIGHT}" -eq 1 ]] \
        || [[ "${EXPLICIT_STRONG_COLD_INFLIGHT}" -eq 1 ]] \
        || [[ -n "${STRONG_HOT_RATIO}" ]] \
        || [[ -n "${STRONG_COLD_RATIO}" ]]; then
        echo "--strong-i-1to1 cannot be combined with explicit inflight or ratio options." >&2
        exit 1
    fi
fi

if ! parse_positive_int "${MIXED_INFLIGHT}"; then
    echo "Invalid mixed inflight: ${MIXED_INFLIGHT}" >&2
    exit 1
fi

if [[ -n "${HOT_BUDGET}" ]] && ! parse_positive_int "${HOT_BUDGET}"; then
    echo "Invalid hot budget: ${HOT_BUDGET}" >&2
    exit 1
fi

if [[ -n "${COLD_BUDGET}" ]] && ! parse_positive_int "${COLD_BUDGET}"; then
    echo "Invalid cold budget: ${COLD_BUDGET}" >&2
    exit 1
fi

if [[ -n "${STRONG_HOT_RATIO}" ]]; then
    STRONG_HOT_RATIO="$(parse_ratio_value "${STRONG_HOT_RATIO}")" || {
        echo "Invalid strong hot ratio." >&2
        exit 1
    }
fi

if [[ -n "${STRONG_COLD_RATIO}" ]]; then
    STRONG_COLD_RATIO="$(parse_ratio_value "${STRONG_COLD_RATIO}")" || {
        echo "Invalid strong cold ratio." >&2
        exit 1
    }
fi

if [[ -n "${STRONG_HOT_RATIO}" ]]; then
    STRONG_HOT_INFLIGHT="$(scale_inflight_from_ratio "${MIXED_INFLIGHT}" "${STRONG_HOT_RATIO}")"
fi

if [[ -n "${STRONG_COLD_RATIO}" ]]; then
    STRONG_COLD_INFLIGHT="$(scale_inflight_from_ratio "${MIXED_INFLIGHT}" "${STRONG_COLD_RATIO}")"
fi

if ! parse_positive_int "${STRONG_HOT_INFLIGHT}"; then
    echo "Invalid strong hot inflight: ${STRONG_HOT_INFLIGHT}" >&2
    exit 1
fi

if ! parse_positive_int "${STRONG_COLD_INFLIGHT}"; then
    echo "Invalid strong cold inflight: ${STRONG_COLD_INFLIGHT}" >&2
    exit 1
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

effective_inflight_values() {
    local queue_depth="$1"
    local mixed="${MIXED_INFLIGHT}"
    local strong_hot="${STRONG_HOT_INFLIGHT}"
    local strong_cold="${STRONG_COLD_INFLIGHT}"

    if [[ "${STRONG_I_1TO1}" -eq 1 ]]; then
        mixed="${queue_depth}"
        strong_hot="${queue_depth}"
        strong_cold="${queue_depth}"
    fi

    printf '%s\t%s\t%s\n' "${mixed}" "${strong_hot}" "${strong_cold}"
}

run_case() {
    local mode="$1"
    local queue_depth="$2"
    local mixed_inflight="$3"
    local strong_hot_inflight="$4"
    local strong_cold_inflight="$5"
    local log_file="$6"
    local -a cmd

    cmd=(sudo "${BIN_PATH}" "${mode}" --queue-depth "${queue_depth}")

    case "${mode}" in
        mixed|hot-only)
            cmd+=(--mixed-inflight "${mixed_inflight}")
            ;;
        strong-i)
            cmd+=(--strong-hot-inflight "${strong_hot_inflight}" --strong-cold-inflight "${strong_cold_inflight}")
            ;;
        *)
            echo "Unknown mode: ${mode}" >&2
            exit 1
            ;;
    esac

    if [[ -n "${HOT_BUDGET}" ]]; then
        cmd+=(--hot-budget "${HOT_BUDGET}")
    fi

    if [[ -n "${COLD_BUDGET}" ]]; then
        cmd+=(--cold-budget "${COLD_BUDGET}")
    fi

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
    local mixed_inflight="$3"
    local strong_hot_inflight="$4"
    local strong_cold_inflight="$5"
    local log_file="$6"

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
        "${mixed_inflight}" \
        "${strong_hot_inflight}" \
        "${strong_cold_inflight}" \
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
    IFS=$'\t' read -r effective_mixed_inflight effective_strong_hot_inflight effective_strong_cold_inflight \
        <<< "$(effective_inflight_values "${queue_depth}")"

    for mode in "${MODES[@]}"; do
        if [[ "${RESET_MODULES}" -eq 1 ]]; then
            reset_modules "${queue_depth}"
        fi

        run_case \
            "${mode}" \
            "${queue_depth}" \
            "${effective_mixed_inflight}" \
            "${effective_strong_hot_inflight}" \
            "${effective_strong_cold_inflight}" \
            "${depth_dir}/${mode}.log"
        append_summary \
            "${mode}" \
            "${queue_depth}" \
            "${effective_mixed_inflight}" \
            "${effective_strong_hot_inflight}" \
            "${effective_strong_cold_inflight}" \
            "${depth_dir}/${mode}.log"
    done
done

echo
echo "==== Done ===="
echo "Logs saved to: ${OUTPUT_DIR}"
echo "Summary saved to: ${SUMMARY_FILE}"
