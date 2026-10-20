#!/usr/bin/env bash
# =============================================================================
# logging.sh — общий helper локальных логов для scripts/*.sh
#
# Подключение (после set -euo pipefail и ROOT=…):
#   # shellcheck source=lib/logging.sh
#   source "$ROOT/scripts/lib/logging.sh"
#   orc_bench_enable_logging "$0" "$@"
#
# Поведение:
#   - Первый (top-level) вызов: создаёт лог, печатает путь, re-exec скрипта
#     через tee (stdout+stderr → файл и консоль).
#   - Вложенный вызов (ORC_BENCH_LOGGING=1): no-op — без повторного tee.
#
# Переменные окружения:
#   ORC_BENCH_LOG_DIR   каталог логов (по умолчанию <repo>/logs)
#   ORC_BENCH_LOG_FILE  явный путь к лог-файлу
#                       (иначе <dir>/<script>-YYYYMMDD-HHMMSS.log)
#   ORC_BENCH_LOGGING=1 уже включено (вложенные скрипты пропускают tee)
# =============================================================================

orc_bench_enable_logging() {
  # Already under a top-level tee (nested script / re-exec child).
  if [[ "${ORC_BENCH_LOGGING:-0}" == "1" ]]; then
    return 0
  fi

  local self="${1:-}"
  shift || true

  if [[ -z "$self" ]]; then
    echo "[logging] ERROR: orc_bench_enable_logging requires script path" >&2
    return 1
  fi

  # Absolute path so re-exec works after cd.
  if [[ "$self" != /* ]]; then
    self="$(cd "$(dirname "$self")" && pwd)/$(basename "$self")"
  fi

  local root="${ROOT:-}"
  if [[ -z "$root" ]]; then
    root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
  fi

  local script_base
  script_base="$(basename "$self")"
  script_base="${script_base%.sh}"

  local log_dir="${ORC_BENCH_LOG_DIR:-${root}/logs}"
  mkdir -p "$log_dir"

  local log_file="${ORC_BENCH_LOG_FILE:-}"
  if [[ -z "$log_file" ]]; then
    log_file="${log_dir}/${script_base}-$(date +%Y%m%d-%H%M%S).log"
  fi
  # Absolute log path for clear messages / nested inheritance.
  if [[ "$log_file" != /* ]]; then
    mkdir -p "$(dirname "$log_file")"
    log_file="$(cd "$(dirname "$log_file")" && pwd)/$(basename "$log_file")"
  else
    mkdir -p "$(dirname "$log_file")"
  fi

  export ORC_BENCH_LOGGING=1
  export ORC_BENCH_LOG_DIR="$log_dir"
  export ORC_BENCH_LOG_FILE="$log_file"

  echo "[logging] log file: $log_file" | tee -a "$log_file"

  local rc=0
  set +e
  # Child inherits ORC_BENCH_LOGGING=1 and skips a second tee.
  "$self" "$@" 2>&1 | tee -a "$log_file"
  rc=${PIPESTATUS[0]}
  set -e

  echo "[logging] finished (exit=${rc}): $log_file" | tee -a "$log_file"
  exit "$rc"
}
