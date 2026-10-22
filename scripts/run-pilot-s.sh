#!/usr/bin/env bash
# =============================================================================
# run-pilot-s.sh — Pilot S (масштаб S = 100 GB), узкая матрица экспериментов
#
# Назначение:
#   Читает списки экспериментов из conf/pilot-s.yaml и вызывает run-benchmark.sh
#   с --scale S и нужным --phase. Живые фазы — только на узле Hadoop/SDP.
#   На машине разработчика: --dry-run-submit | --print | --help.
#   Старт: 10×4×16g, deploy_mode=cluster; concurrency 1,5 — калибровать на узле.
#   Живой запуск отвязан от терминала (см. run-benchmark.sh): скрипт выходит
#   после application id, приложение на YARN продолжается.
#
# Параметры:
#   -h, --help              показать эту справку
#   --print                 напечатать план пилота (эксперименты, шаги) и выйти
#   --dry-run-submit        пробросить в run-benchmark.sh (без реального submit)
#   --phase PHASE           generate|materialize|run|report|final|preflight|all
#                           (обязателен, кроме --print)
#   --phase=PHASE           то же в форме key=value
#   остальные аргументы     без разбора пробрасываются в run-benchmark.sh
#                           (напр. --env-override, --jar, …)
#
# Переменные окружения:
#   ORC_BENCH_PILOT_YAML    путь к YAML пилота (по умолчанию conf/pilot-s.yaml)
#
# Логирование (stdout+stderr → файл и консоль через tee):
#   ORC_BENCH_LOG_DIR    каталог логов (по умолчанию <repo>/logs)
#   ORC_BENCH_LOG_FILE   явный путь к лог-файлу
#                        (иначе <dir>/run-pilot-s-YYYYMMDD-HHMMSS.log)
#   ORC_BENCH_LOGGING=1  уже включено — вложенный run-benchmark.sh без повторного tee
#
# Примеры (узел):
#   ./scripts/run-pilot-s.sh --phase generate --env-override conf/environment.local.yaml
#   ./scripts/run-pilot-s.sh --phase materialize --env-override conf/environment.local.yaml
#   ./scripts/run-pilot-s.sh --phase run --env-override conf/environment.local.yaml
#
# Примеры (dev):
#   ./scripts/run-pilot-s.sh --print
#   ./scripts/run-pilot-s.sh --dry-run-submit --phase run
# =============================================================================
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=lib/logging.sh
source "$ROOT/scripts/lib/logging.sh"
orc_bench_enable_logging "$0" "$@"

PILOT_YAML="${ORC_BENCH_PILOT_YAML:-$ROOT/conf/pilot-s.yaml}"
PHASE=""
DRY_RUN_SUBMIT=0
PRINT_ONLY=0
FORWARD=()

usage() {
  orc_bench_print_header "$0"
  exit 2
}

if [[ ! -f "$PILOT_YAML" ]]; then
  echo "[run-pilot-s] ERROR: missing $PILOT_YAML" >&2
  exit 1
fi

MATERIALIZE_EXPS="$(orc_bench_yaml_value experiments_materialize_csv "$PILOT_YAML")"
RUN_EXPS="$(orc_bench_yaml_value experiments_run_csv "$PILOT_YAML")"
MATERIALIZE_EXPS="${MATERIALIZE_EXPS:-MINMAX_SORT,BLOOM_USER,PARTITIONING}"
RUN_EXPS="${RUN_EXPS:-ORC_COLUMN_PRUNING,ORC_PUSHDOWN,MINMAX_SORT,BLOOM_USER,PARTITIONING,SPARK_VECTORIZATION}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help) usage ;;
    --print) PRINT_ONLY=1; shift ;;
    --dry-run-submit) DRY_RUN_SUBMIT=1; shift ;;
    --phase)
      PHASE="$2"
      shift 2
      ;;
    --phase=*)
      PHASE="${1#--phase=}"
      shift
      ;;
    *) FORWARD+=("$1"); shift ;;
  esac
done

if [[ "$PRINT_ONLY" -eq 1 ]]; then
  cat <<EOF
[run-pilot-s] pilot YAML: $PILOT_YAML
[run-pilot-s] scale=S (100 GB)
[run-pilot-s] executor_start=10×4×16g  deploy_mode=cluster
[run-pilot-s] concurrency_start=1,5 (capacity-aware; finalize after trial)
[run-pilot-s] materialize --experiments $MATERIALIZE_EXPS
[run-pilot-s] run         --experiments $RUN_EXPS  --engines spark
[run-pilot-s] sequence on node:
  preflight → generate(S) → materialize → run → report/final
  then fill calibration checklist in docs/Запуск на Hadoop кластере.md §12
EOF
  exit 0
fi

if [[ -z "$PHASE" ]]; then
  echo "[run-pilot-s] ERROR: --phase generate|materialize|run|report|final required (or --print)" >&2
  usage
fi

ARGS=(--scale S --phase "$PHASE")
case "$PHASE" in
  materialize)
    ARGS+=(--experiments "$MATERIALIZE_EXPS")
    ;;
  run)
    ARGS+=(--engines spark --experiments "$RUN_EXPS")
    ;;
  generate|preflight|report|final|all)
    # generate / preflight / report / final: фильтр экспериментов пилота не нужен
    ;;
  *)
    echo "[run-pilot-s] ERROR: unsupported --phase '$PHASE'" >&2
    exit 2
    ;;
esac

if [[ "$DRY_RUN_SUBMIT" -eq 1 ]]; then
  ARGS+=(--dry-run-submit)
  echo "[run-pilot-s] dry-run-submit: printing spark-submit line only (no cluster)"
fi

if [[ "$DRY_RUN_SUBMIT" -eq 0 ]]; then
  orc_bench_require_spark_submit run-pilot-s \
    "Live pilot must run on a cluster node after copying jar+conf+scripts." \
    "Local smoke: ./scripts/run-pilot-s.sh --print" \
    "               ./scripts/run-pilot-s.sh --dry-run-submit --phase run" \
    "See docs/Запуск на Hadoop кластере.md §6.2 / §12" || exit 1
fi

echo "[run-pilot-s] phase=$PHASE scale=S"
exec "$ROOT/scripts/run-benchmark.sh" "${ARGS[@]}" "${FORWARD[@]}"
