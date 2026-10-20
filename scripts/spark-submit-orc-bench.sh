#!/usr/bin/env bash
# =============================================================================
# spark-submit-orc-bench.sh — низкоуровневый spark-submit для orc-bench
#
# Назначение:
#   Прямой вызов spark-submit с --class orcbench.Main на узле Hadoop.
#   Опции submit отделены от аргументов Main разделителем -- :
#     ./scripts/spark-submit-orc-bench.sh [submit opts] -- [main opts]
#   На машине разработчика: --dry-run (печать команды без spark-submit).
#   master всегда yarn (в отличие от run-benchmark.sh, где master настраивается).
#
# Параметры submit (до --):
#   -h, --help              показать эту справку
#   --jar PATH              fat-jar (ORC_BENCH_JAR или путь по умолчанию)
#   --deploy-mode MODE      cluster|client (ORC_BENCH_DEPLOY_MODE / YAML; default cluster)
#   --queue NAME            YARN-очередь (ORC_BENCH_QUEUE / YAML; default default)
#   --num-executors N       число executor'ов (default 10)
#   --executor-cores N      ядра на executor (default 4)
#   --executor-memory SIZE  память executor (default 16g)
#   --principal PRINCIPAL   Kerberos principal
#   --keytab PATH           путь к keytab
#   --dry-run               только напечатать команду и выйти 0
#   --                      дальше аргументы для orcbench.Main
#
# Cluster mode:
#   Локальные --env-override и --metrics-jsonl уходят в spark-submit --files,
#   в Main передаётся basename (файл появляется в cwd контейнера AM).
#
# После -- : флаги Main (--config, --phase, --scale, …)
#
# Переменные окружения:
#   ORC_BENCH_JAR, ORC_BENCH_DEPLOY_MODE, ORC_BENCH_QUEUE,
#   ORC_BENCH_NUM_EXECUTORS, ORC_BENCH_EXECUTOR_CORES, ORC_BENCH_EXECUTOR_MEMORY,
#   ORC_BENCH_PRINCIPAL, ORC_BENCH_KEYTAB, ORC_BENCH_ENV_YAML
#
# Логирование (stdout+stderr → файл и консоль через tee):
#   ORC_BENCH_LOG_DIR    каталог логов (по умолчанию <repo>/logs)
#   ORC_BENCH_LOG_FILE   явный путь к лог-файлу
#                        (иначе <dir>/spark-submit-orc-bench-YYYYMMDD-HHMMSS.log)
#   ORC_BENCH_LOGGING=1  уже включено (вложенный вызов без повторного tee)
#
# Пример:
#   ./scripts/spark-submit-orc-bench.sh --deploy-mode client -- \
#       --config conf --scale S --phase preflight --dry-run
# =============================================================================
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=lib/logging.sh
source "$ROOT/scripts/lib/logging.sh"
orc_bench_enable_logging "$0" "$@"

cd "$ROOT"

JAR="${ORC_BENCH_JAR:-}"
DEPLOY_MODE="${ORC_BENCH_DEPLOY_MODE:-}"
QUEUE="${ORC_BENCH_QUEUE:-}"
NUM_EXECUTORS="${ORC_BENCH_NUM_EXECUTORS:-}"
EXECUTOR_CORES="${ORC_BENCH_EXECUTOR_CORES:-}"
EXECUTOR_MEMORY="${ORC_BENCH_EXECUTOR_MEMORY:-}"
PRINCIPAL="${ORC_BENCH_PRINCIPAL:-}"
KEYTAB="${ORC_BENCH_KEYTAB:-}"
DRY_RUN=0
ENV_YAML="${ORC_BENCH_ENV_YAML:-$ROOT/conf/environment.yaml}"
MAIN_ARGS=()
PARSING_MAIN=0

usage() {
  sed -n '2,45p' "$0" | sed 's/^# \?//'
  exit 2
}

yaml_value() {
  local key="$1"
  [[ -f "$ENV_YAML" ]] || return 0
  grep -E "^[[:space:]]*${key}:" "$ENV_YAML" | head -1 \
    | sed -E "s/.*:[\"']?[[:space:]]*//;s/[\"'][[:space:]]*$//;s/[[:space:]]*$//" || true
}

apply_defaults() {
  if [[ -z "$DEPLOY_MODE" ]]; then
    DEPLOY_MODE="$(yaml_value deploy_mode)"
    DEPLOY_MODE="${DEPLOY_MODE:-cluster}"
  fi
  if [[ -z "$QUEUE" ]]; then
    QUEUE="$(yaml_value queue)"
    QUEUE="${QUEUE:-default}"
  fi
  if [[ -z "$NUM_EXECUTORS" ]]; then
    NUM_EXECUTORS="$(yaml_value executor_instances)"
    NUM_EXECUTORS="${NUM_EXECUTORS:-10}"
  fi
  if [[ -z "$EXECUTOR_CORES" ]]; then
    EXECUTOR_CORES="$(yaml_value executor_cores)"
    EXECUTOR_CORES="${EXECUTOR_CORES:-4}"
  fi
  if [[ -z "$EXECUTOR_MEMORY" ]]; then
    local gib
    gib="$(yaml_value executor_memory_gib)"
    EXECUTOR_MEMORY="${gib:+${gib}g}"
    EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-16g}"
  fi
  if [[ -z "$JAR" ]]; then
    if [[ -f "$ROOT/orc-bench-0.1.0-SNAPSHOT-all.jar" ]]; then
      JAR="$ROOT/orc-bench-0.1.0-SNAPSHOT-all.jar"
    else
      JAR="$ROOT/build/libs/orc-bench-0.1.0-SNAPSHOT-all.jar"
    fi
  fi
  if [[ -z "$PRINCIPAL" ]]; then
    PRINCIPAL="$(yaml_value runner_principal)"
  fi
  if [[ -z "$KEYTAB" ]]; then
    local kt
    kt="$(yaml_value keytab_path)"
    if [[ -n "$kt" ]]; then
      KEYTAB="${kt/#\~/$HOME}"
    fi
  fi
}

while [[ $# -gt 0 ]]; do
  if [[ "$PARSING_MAIN" -eq 1 ]]; then
    MAIN_ARGS+=("$1"); shift; continue
  fi
  case "$1" in
    -h|--help) usage ;;
    --) PARSING_MAIN=1; shift ;;
    --jar) JAR="$2"; shift 2 ;;
    --deploy-mode) DEPLOY_MODE="$2"; shift 2 ;;
    --queue) QUEUE="$2"; shift 2 ;;
    --num-executors) NUM_EXECUTORS="$2"; shift 2 ;;
    --executor-cores) EXECUTOR_CORES="$2"; shift 2 ;;
    --executor-memory) EXECUTOR_MEMORY="$2"; shift 2 ;;
    --principal) PRINCIPAL="$2"; shift 2 ;;
    --keytab) KEYTAB="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    *)
      echo "Unknown submit option: $1 (use -- before Main args)" >&2
      usage ;;
  esac
done

apply_defaults

if [[ ! -f "$JAR" ]]; then
  echo "ERROR: fat jar not found: $JAR" >&2
  echo "  Expected (cluster bundle): $ROOT/orc-bench-0.1.0-SNAPSHOT-all.jar" >&2
  echo "  Expected (git checkout):   $ROOT/build/libs/orc-bench-0.1.0-SNAPSHOT-all.jar" >&2
  echo "  Override: --jar PATH  or  ORC_BENCH_JAR=PATH" >&2
  exit 1
fi

case "$DEPLOY_MODE" in
  cluster|client) ;;
  *)
    echo "ERROR: --deploy-mode must be cluster or client (got: $DEPLOY_MODE)" >&2
    exit 2 ;;
esac

APP_ARGS=()
if [[ ${#MAIN_ARGS[@]} -gt 0 ]]; then
  APP_ARGS=("${MAIN_ARGS[@]}")
fi
# shellcheck source=lib/cluster-dist.sh
source "$ROOT/scripts/lib/cluster-dist.sh"
orc_bench_localize_driver_files
MAIN_ARGS=()
if [[ ${#APP_ARGS[@]} -gt 0 ]]; then
  MAIN_ARGS=("${APP_ARGS[@]}")
fi

CMD=(
  spark-submit
  --master yarn
  --deploy-mode "$DEPLOY_MODE"
  --queue "$QUEUE"
  --num-executors "$NUM_EXECUTORS"
  --executor-cores "$EXECUTOR_CORES"
  --executor-memory "$EXECUTOR_MEMORY"
  --class orcbench.Main
)

if [[ ${#CLUSTER_FILES[@]} -gt 0 ]]; then
  files_csv=""
  for f in "${CLUSTER_FILES[@]}"; do
    if [[ -n "$files_csv" ]]; then
      files_csv+=","
    fi
    files_csv+="$f"
  done
  CMD+=(--files "$files_csv")
fi

if [[ -n "$PRINCIPAL" && -n "$KEYTAB" ]]; then
  CMD+=(--principal "$PRINCIPAL" --keytab "$KEYTAB")
fi

CMD+=("$JAR")
CMD+=("${MAIN_ARGS[@]}")

echo "[spark-submit-orc-bench] deploy_mode=$DEPLOY_MODE queue=$QUEUE jar=$JAR"
if [[ "$DRY_RUN" -eq 1 ]]; then
  printf '  %q' "${CMD[@]}"
  printf '\n'
  exit 0
fi

if ! command -v spark-submit >/dev/null 2>&1; then
  echo "ERROR: spark-submit not in PATH — run on a cluster node." >&2
  echo "  Local smoke: add --dry-run" >&2
  exit 1
fi

exec "${CMD[@]}"
