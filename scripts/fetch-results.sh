#!/usr/bin/env bash
# =============================================================================
# fetch-results.sh — упаковка results/<runId> в .tgz для scp с узла
#
# Назначение:
#   На узле submit после прогона (удобно при deploy_mode=client) собирает
#   каталог прогона в архив. При deploy_mode=cluster локальный results/ может
#   быть пуст — см. docs/Запуск на Hadoop кластере.md.
#
# Параметры:
#   -h, --help            показать эту справку
#   --results-dir PATH    каталог с прогонами (иначе ORC_BENCH_RESULTS_DIR
#                         или <repo>/results)
#   --run-id ID           идентификатор прогона (подкаталог в results-dir)
#   --out PATH            путь к выходному .tgz
#                         (иначе <repo>/results-<runId>.tgz)
#   --list                перечислить прогоны в results-dir и выйти
#   --latest              взять самый новый подкаталог как --run-id
#
# Переменные окружения:
#   ORC_BENCH_RESULTS_DIR  каталог results по умолчанию
#
# Логирование (stdout+stderr → файл и консоль через tee):
#   ORC_BENCH_LOG_DIR    каталог логов (по умолчанию <repo>/logs)
#   ORC_BENCH_LOG_FILE   явный путь к лог-файлу
#                        (иначе <dir>/fetch-results-YYYYMMDD-HHMMSS.log)
#   ORC_BENCH_LOGGING=1  уже включено (вложенный вызов без повторного tee)
#
# Примеры (на узле):
#   ./scripts/fetch-results.sh --run-id 2026-10-19T12-00-00.000Z_abcd1234
#   ./scripts/fetch-results.sh --results-dir results --run-id <id> --out /tmp/r.tgz
#   ./scripts/fetch-results.sh --list
#   ./scripts/fetch-results.sh --latest
#   # затем с ноутбука: scp USER@NODE:/path/to/results-<runId>.tgz .
# =============================================================================
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=lib/logging.sh
source "$ROOT/scripts/lib/logging.sh"
orc_bench_enable_logging "$0" "$@"

RESULTS_DIR="${ORC_BENCH_RESULTS_DIR:-$ROOT/results}"
RUN_ID=""
OUT=""
LIST=0
LATEST=0

usage() {
  sed -n '2,35p' "$0" | sed 's/^# \?//'
  exit 2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help) usage ;;
    --results-dir) RESULTS_DIR="$2"; shift 2 ;;
    --run-id) RUN_ID="$2"; shift 2 ;;
    --out) OUT="$2"; shift 2 ;;
    --list) LIST=1; shift ;;
    --latest) LATEST=1; shift ;;
    *) echo "Unknown arg: $1" >&2; usage ;;
  esac
done

list_runs() {
  if [[ ! -d "$RESULTS_DIR" ]]; then
    echo "No results directory: $RESULTS_DIR" >&2
    return 0
  fi
  ls -1 "$RESULTS_DIR" 2>/dev/null || true
}

if [[ "$LIST" -eq 1 ]]; then
  list_runs
  exit 0
fi

if [[ "$LATEST" -eq 1 ]]; then
  if [[ ! -d "$RESULTS_DIR" ]]; then
    echo "ERROR: no results directory: $RESULTS_DIR" >&2
    exit 1
  fi
  RUN_ID="$(ls -1t "$RESULTS_DIR" 2>/dev/null | head -1 || true)"
  if [[ -z "$RUN_ID" ]]; then
    echo "ERROR: no run directories under $RESULTS_DIR" >&2
    exit 1
  fi
  echo "[fetch-results] latest run-id=$RUN_ID"
fi

if [[ -z "$RUN_ID" ]]; then
  echo "ERROR: --run-id is required (or use --latest / --list)" >&2
  if [[ -d "$RESULTS_DIR" ]]; then
    echo "Available under $RESULTS_DIR:" >&2
    list_runs >&2 || true
  fi
  exit 2
fi

SRC="$RESULTS_DIR/$RUN_ID"
if [[ ! -d "$SRC" ]]; then
  echo "ERROR: not a directory: $SRC" >&2
  echo "  Tip: deploy_mode=cluster may leave results on the AM — use client mode" >&2
  echo "  for local results/, or collect yarn logs (runbook §7.4)." >&2
  exit 1
fi

if [[ -z "$OUT" ]]; then
  OUT="$ROOT/results-${RUN_ID}.tgz"
fi

tar -czf "$OUT" -C "$RESULTS_DIR" "$RUN_ID"
echo "[fetch-results] packed $SRC → $OUT"
echo "  scp USER@NODE:$OUT ."
