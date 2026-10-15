#!/usr/bin/env bash
# =============================================================================
# spark-submit-example.sh — тонкая обёртка над run-benchmark.sh (совместимость)
#
# Рекомендуется использовать напрямую:
#   ./scripts/run-benchmark.sh --help
#
# Этот скрипт сохранён для обратной совместимости со старыми вызовами.
#
# Параметры:
#   $1 (опционально)  PATH к fat jar.
#                     По умолчанию: build/libs/orc-bench-0.1.0-SNAPSHOT-all.jar
#
#   $@ начиная со 2-го — дополнительные флаги, которые пробрасываются в
#                     run-benchmark.sh (например --phase generate --scale S).
#
# Параметры окружения:
#   SCALE         Масштаб S|M|L. По умолчанию: S
#   PHASE         Фаза pipeline. По умолчанию: all
#   ENV_OVERRIDE  Путь к environment.local.yaml.
#                 По умолчанию: conf/environment.local.yaml
# =============================================================================
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# --- $1: путь к jar (опционально) -------------------------------------------
JAR="${1:-$ROOT/build/libs/orc-bench-0.1.0-SNAPSHOT-all.jar}"
if [[ $# -ge 1 ]]; then
  shift
fi

# --- SCALE: масштаб датасета из окружения -----------------------------------
SCALE="${SCALE:-S}"
# --- PHASE: фаза pipeline из окружения --------------------------------------
PHASE="${PHASE:-all}"
# --- ENV_OVERRIDE: overlay null-полей environment ---------------------------
ENV_OVERRIDE="${ENV_OVERRIDE:-$ROOT/conf/environment.local.yaml}"

exec "$ROOT/scripts/run-benchmark.sh" \
  --jar "$JAR" \
  --scale "$SCALE" \
  --phase "$PHASE" \
  --env-override "$ENV_OVERRIDE" \
  "$@"
