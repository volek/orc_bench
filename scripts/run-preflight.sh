#!/usr/bin/env bash
# =============================================================================
# run-preflight.sh — удобный запуск фазы preflight через run-benchmark.sh
#
# Назначение:
#   Вызывает run-benchmark.sh с --phase preflight.
#   По умолчанию добавляет --dry-run (проверки по конфигу через spark-submit).
#   Живые пробы HDFS/JDBC/Kerberos — на узле кластера с флагом --live.
#   На машине разработчика: --dry-run-submit.
#
# Параметры:
#   -h, --help         показать эту справку
#   --live             живые пробы (без --dry-run у приложения)
#   --dry-run-submit   только печать строки spark-submit (без submit)
#   остальные args     пробрасываются в run-benchmark.sh
#                      (напр. --env-override, --jar, --deploy-mode, …)
#
# Примеры:
#   ./scripts/run-preflight.sh --env-override conf/environment.local.yaml
#   ./scripts/run-preflight.sh --live --env-override conf/environment.local.yaml
#   ./scripts/run-preflight.sh --dry-run-submit
# =============================================================================
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LIVE=0
DRY_RUN_SUBMIT=0
FORWARD=()

usage() {
  sed -n '2,22p' "$0" | sed 's/^# \?//'
  exit 2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help) usage ;;
    --live) LIVE=1; shift ;;
    --dry-run-submit) DRY_RUN_SUBMIT=1; shift ;;
    *) FORWARD+=("$1"); shift ;;
  esac
done

ARGS=(--phase preflight)
if [[ "$LIVE" -eq 0 ]]; then
  ARGS+=(--dry-run)
  echo "[run-preflight] mode=dry-run (pass --live for live HDFS/JDBC/Kerberos probes)"
else
  echo "[run-preflight] mode=live (requires Kerberos context + cluster access)"
fi

if [[ "$DRY_RUN_SUBMIT" -eq 1 ]]; then
  ARGS+=(--dry-run-submit)
  echo "[run-preflight] dry-run-submit: printing spark-submit line only (no cluster)"
fi

if [[ "$DRY_RUN_SUBMIT" -eq 0 ]] && ! command -v spark-submit >/dev/null 2>&1; then
  echo "[run-preflight] ERROR: spark-submit not in PATH." >&2
  echo "  Run on a cluster node after copying jar+conf+scripts." >&2
  echo "  Local smoke: ./scripts/run-preflight.sh --dry-run-submit" >&2
  echo "  See docs/Запуск на Hadoop кластере.md" >&2
  exit 1
fi

exec "$ROOT/scripts/run-benchmark.sh" "${ARGS[@]}" "${FORWARD[@]}"
