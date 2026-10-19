#!/usr/bin/env bash
# =============================================================================
# run-benchmark.sh — обёртка spark-submit для orc-bench на узле кластера
#
# Назначение:
#   Формирует и запускает spark-submit с классом orcbench.Main.
#   Живой submit (spark-submit, HDFS, Kerberos) — только на узле Hadoop/SDP.
#   С ноутбука разработчика: --dry-run-submit (печать команды без submit).
#   См. docs/Запуск на Hadoop кластере.md
#
# Значения по умолчанию (CLI/env → conf/environment.yaml → хардкод):
#   master=yarn, deploy_mode=cluster, queue=default, executors 10×4×16g
#
# Параметры spark-submit (опционально; перекрывают YAML/env):
#   -h, --help              показать эту справку
#   --jar PATH              fat-jar (иначе ORC_BENCH_JAR или путь по умолчанию)
#   --master URL            Spark master (иначе ORC_BENCH_MASTER / YAML)
#   --deploy-mode MODE      cluster|client (ORC_BENCH_DEPLOY_MODE / YAML)
#   --queue NAME            YARN-очередь (ORC_BENCH_QUEUE / YAML)
#   --num-executors N       число executor'ов (ORC_BENCH_NUM_EXECUTORS / YAML)
#   --executor-cores N      ядра на executor (ORC_BENCH_EXECUTOR_CORES / YAML)
#   --executor-memory SIZE  память executor, напр. 16g (ORC_BENCH_EXECUTOR_MEMORY)
#   --principal PRINCIPAL   Kerberos principal (ORC_BENCH_PRINCIPAL / YAML)
#   --keytab PATH           путь к keytab (ORC_BENCH_KEYTAB / YAML)
#   --dry-run-submit        только напечатать строку spark-submit и выйти 0
#
# Переменные окружения (те же смыслы, что флаги выше):
#   ORC_BENCH_JAR, ORC_BENCH_MASTER, ORC_BENCH_DEPLOY_MODE, ORC_BENCH_QUEUE,
#   ORC_BENCH_NUM_EXECUTORS, ORC_BENCH_EXECUTOR_CORES, ORC_BENCH_EXECUTOR_MEMORY,
#   ORC_BENCH_PRINCIPAL, ORC_BENCH_KEYTAB, ORC_BENCH_ENV_YAML
#   (YAML по умолчанию: conf/environment.yaml)
#
# Флаги приложения (пробрасываются в orcbench.Main):
#   --config, --scale, --phase, --experiments, --engines, --env-override,
#   --results-dir, --run-id, --metrics-jsonl, --keep-layouts
#   --dry-run, --mock-generate, --mock-materialize, --mock-run, --cleanup-layouts
#   -- …                дальше все аргументы — в Main
#   неизвестные токены тоже добавляются в APP_ARGS
#
# Примеры:
#   ./scripts/run-benchmark.sh --jar ./orc-bench-…-all.jar --phase preflight \
#       --env-override conf/environment.local.yaml
#   ./scripts/run-benchmark.sh --dry-run-submit --phase preflight --scale S
# =============================================================================
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

JAR="${ORC_BENCH_JAR:-}"
MASTER="${ORC_BENCH_MASTER:-}"
DEPLOY_MODE="${ORC_BENCH_DEPLOY_MODE:-}"
QUEUE="${ORC_BENCH_QUEUE:-}"
NUM_EXECUTORS="${ORC_BENCH_NUM_EXECUTORS:-}"
EXECUTOR_CORES="${ORC_BENCH_EXECUTOR_CORES:-}"
EXECUTOR_MEMORY="${ORC_BENCH_EXECUTOR_MEMORY:-}"
PRINCIPAL="${ORC_BENCH_PRINCIPAL:-}"
KEYTAB="${ORC_BENCH_KEYTAB:-}"
DRY_RUN_SUBMIT=0
ENV_YAML="${ORC_BENCH_ENV_YAML:-$ROOT/conf/environment.yaml}"

APP_ARGS=()

usage() {
  sed -n '2,44p' "$0" | sed 's/^# \?//'
  exit 2
}

yaml_value() {
  local key="$1"
  local yaml="${2:-$ENV_YAML}"
  [[ -f "$yaml" ]] || return 0
  grep -E "^[[:space:]]*${key}:" "$yaml" | head -1 \
    | sed -E "s/.*:[\"']?[[:space:]]*//;s/[\"'][[:space:]]*$//;s/[[:space:]]*$//" || true
}

# Prefer CLI/env; else conf/environment.yaml; else hardcoded configs_final defaults.
apply_yaml_defaults() {
  if [[ -z "$MASTER" ]]; then
    MASTER="$(yaml_value master)"
    MASTER="${MASTER:-yarn}"
  fi
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
    if [[ -n "$gib" ]]; then
      EXECUTOR_MEMORY="${gib}g"
    else
      EXECUTOR_MEMORY="16g"
    fi
  fi
  if [[ -z "$JAR" ]]; then
    if [[ -f "$ROOT/orc-bench-0.1.0-SNAPSHOT-all.jar" ]]; then
      JAR="$ROOT/orc-bench-0.1.0-SNAPSHOT-all.jar"
    else
      JAR="$ROOT/build/libs/orc-bench-0.1.0-SNAPSHOT-all.jar"
    fi
  fi
}

load_kerberos_from_yaml() {
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

warn_not_cluster() {
  if ! command -v spark-submit >/dev/null 2>&1; then
    echo "[run-benchmark] WARNING: spark-submit not in PATH." >&2
    echo "  This script is intended for a Hadoop cluster node." >&2
    echo "  On a developer host: use --dry-run-submit, or build/package only;" >&2
    echo "  see docs/Запуск на Hadoop кластере.md" >&2
  fi
  if ! command -v hdfs >/dev/null 2>&1; then
    echo "[run-benchmark] WARNING: hdfs not in PATH (expected on cluster node)." >&2
  fi
  if command -v java >/dev/null 2>&1; then
    local ver
    ver="$(java -version 2>&1 | head -n1 || true)"
    if echo "$ver" | grep -qE '"1[7-9]\.|"2[0-9]\.'; then
      echo "[run-benchmark] WARNING: java looks like 17+: $ver" >&2
      echo "  Prefer JDK 8 on the submit node (environment.yaml: 1.8.0_292)." >&2
    fi
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help) usage ;;
    --jar) JAR="$2"; shift 2 ;;
    --master) MASTER="$2"; shift 2 ;;
    --deploy-mode) DEPLOY_MODE="$2"; shift 2 ;;
    --queue) QUEUE="$2"; shift 2 ;;
    --num-executors) NUM_EXECUTORS="$2"; shift 2 ;;
    --executor-cores) EXECUTOR_CORES="$2"; shift 2 ;;
    --executor-memory) EXECUTOR_MEMORY="$2"; shift 2 ;;
    --principal) PRINCIPAL="$2"; shift 2 ;;
    --keytab) KEYTAB="$2"; shift 2 ;;
    --dry-run-submit) DRY_RUN_SUBMIT=1; shift ;;
    --config|--scale|--phase|--experiments|--engines|--env-override|--results-dir|--keep-layouts|--run-id|--metrics-jsonl)
      APP_ARGS+=("$1" "$2"); shift 2 ;;
    --dry-run|--mock-generate|--mock-materialize|--mock-run|--cleanup-layouts)
      APP_ARGS+=("$1"); shift ;;
    --)
      shift
      APP_ARGS+=("$@")
      break ;;
    *)
      APP_ARGS+=("$1"); shift ;;
  esac
done

apply_yaml_defaults
warn_not_cluster
load_kerberos_from_yaml

if [[ ! -f "$JAR" ]]; then
  echo "ERROR: fat jar not found: $JAR" >&2
  echo "  Build on JDK 8: ./scripts/build.sh  (or copy jar onto this node)" >&2
  exit 1
fi

case "$DEPLOY_MODE" in
  cluster|client) ;;
  *)
    echo "ERROR: --deploy-mode must be cluster or client (got: $DEPLOY_MODE)" >&2
    exit 2 ;;
esac

SUBMIT=(
  spark-submit
  --master "$MASTER"
  --deploy-mode "$DEPLOY_MODE"
  --queue "$QUEUE"
  --num-executors "$NUM_EXECUTORS"
  --executor-cores "$EXECUTOR_CORES"
  --executor-memory "$EXECUTOR_MEMORY"
  --class orcbench.Main
)

if [[ -n "$PRINCIPAL" && -n "$KEYTAB" ]]; then
  if [[ ! -f "$KEYTAB" && "$DRY_RUN_SUBMIT" -eq 0 ]]; then
    echo "WARNING: keytab not found at $KEYTAB (continuing; cluster mode may fail)" >&2
  fi
  SUBMIT+=(--principal "$PRINCIPAL" --keytab "$KEYTAB")
elif [[ "$DEPLOY_MODE" == "cluster" ]]; then
  echo "[run-benchmark] NOTE: no --principal/--keytab; for YARN cluster mode set" >&2
  echo "  ORC_BENCH_PRINCIPAL / ORC_BENCH_KEYTAB or ensure conf/environment.yaml paths exist." >&2
fi

SUBMIT+=("$JAR")
SUBMIT+=("${APP_ARGS[@]}")

echo "[run-benchmark] deploy_mode=$DEPLOY_MODE queue=$QUEUE jar=$JAR"
echo "[run-benchmark] app args: ${APP_ARGS[*]:-}"

if [[ "$DRY_RUN_SUBMIT" -eq 1 ]]; then
  printf '[run-benchmark] dry-run-submit:\n'
  printf '  %q' "${SUBMIT[@]}"
  printf '\n'
  exit 0
fi

if ! command -v spark-submit >/dev/null 2>&1; then
  echo "ERROR: spark-submit not found. Run this script on a cluster node with Spark 3.2." >&2
  echo "  Local smoke without submit: add --dry-run-submit" >&2
  exit 1
fi

exec "${SUBMIT[@]}"
