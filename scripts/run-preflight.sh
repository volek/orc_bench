#!/usr/bin/env bash
# =============================================================================
# run-preflight.sh — быстрый preflight на ноде кластера
#
# Назначение:
#   Проверить версии, HDFS capacity, Hive JDBC, capability-матрицу и записать
#   отчёт в results/<runId>/ без генерации данных и без тяжёлых job’ов.
#
# Использование:
#   ./scripts/run-preflight.sh
#   ./scripts/run-preflight.sh --dry-run
#   ./scripts/run-preflight.sh --jar /opt/orc-bench/orc-bench-all.jar \
#       --env-override conf/environment.local.yaml
#
# Параметры скрипта:
#
#   --jar PATH
#       Fat jar orc-bench. По умолчанию:
#       build/libs/orc-bench-0.1.0-SNAPSHOT-all.jar
#
#   --config DIR
#       Каталог YAML-контракта. По умолчанию: conf
#
#   --env-override PATH
#       Overlay для null-полей environment. По умолчанию:
#       conf/environment.local.yaml (если файла нет — не передаётся)
#
#   --results-dir DIR
#       Куда писать SUMMARY/skipped. По умолчанию: results
#
#   --dry-run
#       Не ходить в Hive JDBC / тяжёлые probe’ы; только разбор конфигов
#       и локальные проверки (удобно до открытия сетевых ACL).
#
#   --queue NAME
#       YARN queue для spark-submit. По умолчанию: benchmark
#
#   --help
#       Справка.
# =============================================================================
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# --- значения по умолчанию --------------------------------------------------
JAR="build/libs/orc-bench-0.1.0-SNAPSHOT-all.jar"  # --jar
CONFIG_DIR="conf"                                  # --config
ENV_OVERRIDE="conf/environment.local.yaml"         # --env-override
RESULTS_DIR="results"                              # --results-dir
DRY_RUN=0                                          # --dry-run
QUEUE="benchmark"                                  # --queue

while [[ $# -gt 0 ]]; do
  case "$1" in
    --jar) JAR="$2"; shift 2 ;;
    --config) CONFIG_DIR="$2"; shift 2 ;;
    --env-override) ENV_OVERRIDE="$2"; shift 2 ;;
    --results-dir) RESULTS_DIR="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    --queue) QUEUE="$2"; shift 2 ;;
    --help|-h)
      sed -n '2,45p' "$0"
      exit 0
      ;;
    *)
      echo "Неизвестный параметр: $1 (см. --help)" >&2
      exit 2
      ;;
  esac
done

ARGS=(
  --jar "$JAR"
  --config "$CONFIG_DIR"
  --scale S
  --phase preflight
  --engines spark
  --results-dir "$RESULTS_DIR"
  --queue "$QUEUE"
  --env-override "$ENV_OVERRIDE"
)
# --dry-run: без реальных JDBC/кластерных probe’ов
[[ "$DRY_RUN" -eq 1 ]] && ARGS+=(--dry-run)

exec "$ROOT/scripts/run-benchmark.sh" "${ARGS[@]}"
