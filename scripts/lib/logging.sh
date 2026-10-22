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
#
# Вместе с этим файлом подключается common.sh (YAML, jar, CRLF, --help).
# =============================================================================

# shellcheck source=common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
# Проверяем вызывающий скрипт ($0), в том числе при вложенном source.
orc_bench_reject_crlf "$0"

orc_bench_enable_logging() {
  # Уже под верхним tee: вложенный скрипт или повторный запуск через re-exec.
  if [[ "${ORC_BENCH_LOGGING:-0}" == "1" ]]; then
    return 0
  fi

  local self="${1:-}"
  shift || true

  if [[ -z "$self" ]]; then
    echo "[logging] ERROR: orc_bench_enable_logging requires script path" >&2
    return 1
  fi

  # Абсолютный путь: повторный запуск после cd не теряет скрипт.
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
  # Абсолютный путь лога — его наследуют вложенные скрипты.
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

  local hdfs_dest=""
  hdfs_dest="$(orc_bench_hdfs_log_dest "$log_file" "$root" || true)"
  local sync_pid=""
  if [[ -n "$hdfs_dest" ]] && command -v hdfs >/dev/null 2>&1; then
    echo "[logging] hdfs log: $hdfs_dest" | tee -a "$log_file"
    orc_bench_hdfs_put_log "$log_file" "$hdfs_dest"
    (
      while true; do
        sleep 30
        orc_bench_hdfs_put_log "$log_file" "$hdfs_dest"
      done
    ) &
    sync_pid=$!
  fi

  local rc=0
  set +e
  # Дочерний процесс получает ORC_BENCH_LOGGING=1 и не открывает второй tee.
  "$self" "$@" 2>&1 | tee -a "$log_file"
  rc=${PIPESTATUS[0]}
  set -e

  if [[ -n "$sync_pid" ]]; then
    kill "$sync_pid" >/dev/null 2>&1 || true
    wait "$sync_pid" >/dev/null 2>&1 || true
    orc_bench_hdfs_put_log "$log_file" "$hdfs_dest"
  fi

  echo "[logging] finished (exit=${rc}): $log_file" | tee -a "$log_file"
  if [[ -n "$hdfs_dest" ]]; then
    orc_bench_hdfs_put_log "$log_file" "$hdfs_dest"
    echo "[logging] hdfs log: $hdfs_dest" | tee -a "$log_file"
  fi
  exit "$rc"
}

# Полный namenode_uri (hdfs://sdp-dzo-lt-omlt). Пусто, если неизвестен.
orc_bench_hdfs_namenode_uri() {
  local root="${1:-}"
  if [[ -z "$root" ]]; then
    root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
  fi
  local yaml="$root/conf/environment.yaml"
  [[ -f "$yaml" ]] || return 0
  local nn
  nn="$(orc_bench_yaml_value namenode_uri "$yaml")"
  [[ -n "$nn" ]] || return 0
  printf '%s\n' "${nn%/}"
}

# Хост из namenode_uri (hdfs://sdp-dzo-lt-omlt → sdp-dzo-lt-omlt). Пусто, если неизвестен.
# hdfs://orc_test как authority нельзя: «_» в хосте, URI.getHost() = null.
orc_bench_hdfs_namenode_host() {
  local nn host
  nn="$(orc_bench_hdfs_namenode_uri "${1:-}" || true)"
  [[ -n "$nn" ]] || return 0
  host="${nn#*://}"
  host="${host%%/*}"
  [[ -n "$host" ]] || return 0
  printf '%s\n' "$host"
}

# hdfs://<namenode>/orc_test
orc_bench_hdfs_bench_root() {
  local nn
  nn="$(orc_bench_hdfs_namenode_uri "${1:-}" || true)"
  [[ -n "$nn" ]] || return 0
  printf '%s/orc_test\n' "$nn"
}

# hdfs://<namenode>/orc_test/results/<host>
orc_bench_hdfs_results_root() {
  local root="${1:-}"
  local nn host
  nn="$(orc_bench_hdfs_namenode_uri "$root" || true)"
  host="$(orc_bench_hdfs_namenode_host "$root" || true)"
  [[ -n "$nn" && -n "$host" ]] || return 0
  printf '%s/orc_test/results/%s\n' "$nn" "$host"
}

# hdfs://<namenode>/orc_test/logs/<host>
orc_bench_hdfs_logs_root() {
  local root="${1:-}"
  local nn host
  nn="$(orc_bench_hdfs_namenode_uri "$root" || true)"
  host="$(orc_bench_hdfs_namenode_host "$root" || true)"
  [[ -n "$nn" && -n "$host" ]] || return 0
  printf '%s/orc_test/logs/%s\n' "$nn" "$host"
}

orc_bench_hdfs_log_dest() {
  local log_file="${1:-}"
  local root="${2:-}"
  [[ -n "$log_file" ]] || return 0
  local logs
  logs="$(orc_bench_hdfs_logs_root "$root" || true)"
  [[ -n "$logs" ]] || return 0
  printf '%s/%s\n' "$logs" "$(basename "$log_file")"
}

orc_bench_hdfs_put_log() {
  local log_file="${1:-}"
  local dest="${2:-}"
  [[ -n "$log_file" && -n "$dest" && -f "$log_file" ]] || return 0
  command -v hdfs >/dev/null 2>&1 || return 0
  hdfs dfs -mkdir -p "$(dirname "$dest")" >/dev/null 2>&1 || true
  hdfs dfs -put -f "$log_file" "$dest" >/dev/null 2>&1 || true
}
