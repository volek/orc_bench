#!/usr/bin/env bash
# =============================================================================
# run-benchmark.sh — обёртка spark-submit для orc-bench на узле кластера
#
# Назначение:
#   Формирует и запускает spark-submit с классом orcbench.Main.
#   Живой submit (spark-submit, HDFS, Kerberos) — только на узле Hadoop/SDP.
#   Submit отвязан от терминала: скрипт выходит после application id в YARN,
#   ApplicationMaster продолжает работу после обрыва SSH.
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
#   --deploy-mode MODE      только cluster (client отключён; ORC_BENCH_DEPLOY_MODE / YAML)
#   --queue NAME            YARN-очередь (ORC_BENCH_QUEUE / YAML)
#   --num-executors N       число executor'ов (ORC_BENCH_NUM_EXECUTORS / YAML)
#   --executor-cores N      ядра на executor (ORC_BENCH_EXECUTOR_CORES / YAML)
#   --executor-memory SIZE  память executor, напр. 16g (ORC_BENCH_EXECUTOR_MEMORY)
#   --driver-memory SIZE    память драйвера, по умолчанию 4g (ORC_BENCH_DRIVER_MEMORY)
#   --principal PRINCIPAL   Kerberos principal (ORC_BENCH_PRINCIPAL / YAML)
#   --keytab PATH           путь к keytab (ORC_BENCH_KEYTAB / YAML)
#   --name TEXT             имя в YARN App Manager (иначе orc-bench <phase> <scale> …)
#   --dry-run-submit        только напечатать строку spark-submit и выйти 0
#
# Cluster mode (единственный):
#   Локальные --env-override и --metrics-jsonl уходят в spark-submit --files,
#   в Main передаётся basename (файл появляется в cwd контейнера AM).
#   --deploy-mode client и spark.deploy_mode: client в YAML завершают скрипт с ошибкой.
#
# Переменные окружения (те же смыслы, что флаги выше):
#   ORC_BENCH_JAR, ORC_BENCH_MASTER, ORC_BENCH_DEPLOY_MODE, ORC_BENCH_QUEUE,
#   ORC_BENCH_NUM_EXECUTORS, ORC_BENCH_EXECUTOR_CORES, ORC_BENCH_EXECUTOR_MEMORY,
#   ORC_BENCH_DRIVER_MEMORY, ORC_BENCH_PRINCIPAL, ORC_BENCH_KEYTAB, ORC_BENCH_ENV_YAML
#   (YAML по умолчанию: conf/environment.yaml)
#
# Логирование (stdout+stderr → файл и консоль через tee):
#   ORC_BENCH_LOG_DIR    каталог логов (по умолчанию <repo>/logs)
#   ORC_BENCH_LOG_FILE   явный путь к лог-файлу
#                        (иначе <dir>/run-benchmark-YYYYMMDD-HHMMSS.log)
#   ORC_BENCH_LOGGING=1  уже включено (вложенный вызов без повторного tee)
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
# shellcheck source=lib/logging.sh
source "$ROOT/scripts/lib/logging.sh"
orc_bench_enable_logging "$0" "$@"

cd "$ROOT"

JAR="${ORC_BENCH_JAR:-}"
MASTER="${ORC_BENCH_MASTER:-}"
DEPLOY_MODE="${ORC_BENCH_DEPLOY_MODE:-}"
QUEUE="${ORC_BENCH_QUEUE:-}"
NUM_EXECUTORS="${ORC_BENCH_NUM_EXECUTORS:-}"
EXECUTOR_CORES="${ORC_BENCH_EXECUTOR_CORES:-}"
EXECUTOR_MEMORY="${ORC_BENCH_EXECUTOR_MEMORY:-}"
DRIVER_MEMORY="${ORC_BENCH_DRIVER_MEMORY:-4g}"
PRINCIPAL="${ORC_BENCH_PRINCIPAL:-}"
KEYTAB="${ORC_BENCH_KEYTAB:-}"
APP_NAME="${ORC_BENCH_APP_NAME:-}"
DRY_RUN_SUBMIT=0
ENV_YAML="${ORC_BENCH_ENV_YAML:-$ROOT/conf/environment.yaml}"

APP_ARGS=()

usage() {
  orc_bench_print_header "$0"
  exit 2
}

# Если --results-dir не задан, подставляем hdfs://<namenode>/orc_test/results/<host>.
# hdfs://orc_test как authority Hadoop не открывает: в хосте есть «_».
ensure_hdfs_results_dir() {
  local i
  for ((i = 0; i < ${#APP_ARGS[@]}; i++)); do
    if [[ "${APP_ARGS[$i]}" == "--results-dir" ]]; then
      return 0
    fi
  done
  local dest
  dest="$(orc_bench_hdfs_results_root "$ROOT" || true)"
  if [[ -z "$dest" ]]; then
    echo "[run-benchmark] WARNING: namenode_uri is empty; results stay on the AM local disk." >&2
    return 0
  fi
  APP_ARGS+=(--results-dir "$dest")
  echo "[run-benchmark] results-dir=$dest"
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
    --driver-memory) DRIVER_MEMORY="$2"; shift 2 ;;
    --principal) PRINCIPAL="$2"; shift 2 ;;
    --keytab) KEYTAB="$2"; shift 2 ;;
    --name) APP_NAME="$2"; shift 2 ;;
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

orc_bench_apply_submit_defaults "$ENV_YAML"
warn_not_cluster
orc_bench_load_kerberos "$ENV_YAML"
orc_bench_require_jar || exit 1
orc_bench_require_cluster_mode || exit $?

ensure_hdfs_results_dir
APP_YARN_NAME="$(orc_bench_yarn_app_name)"

# shellcheck source=lib/cluster-dist.sh
source "$ROOT/scripts/lib/cluster-dist.sh"
orc_bench_localize_driver_files

SUBMIT=(
  spark-submit
  --master "$MASTER"
  --deploy-mode "$DEPLOY_MODE"
  --queue "$QUEUE"
  --num-executors "$NUM_EXECUTORS"
  --executor-cores "$EXECUTOR_CORES"
  --executor-memory "$EXECUTOR_MEMORY"
  --driver-memory "$DRIVER_MEMORY"
  --name "$APP_YARN_NAME"
  --class orcbench.Main
)

if [[ ${#CLUSTER_FILES[@]} -gt 0 ]]; then
  SUBMIT+=(--files "$(orc_bench_cluster_files_csv)")
fi

if [[ -n "$PRINCIPAL" && -n "$KEYTAB" ]]; then
  if [[ ! -f "$KEYTAB" && "$DRY_RUN_SUBMIT" -eq 0 ]]; then
    echo "WARNING: keytab not found at $KEYTAB (continuing; cluster mode may fail)" >&2
  fi
  SUBMIT+=(--principal "$PRINCIPAL" --keytab "$KEYTAB")
elif [[ "$DEPLOY_MODE" == "cluster" ]]; then
  echo "[run-benchmark] NOTE: no --principal/--keytab; for YARN cluster mode set" >&2
  echo "  ORC_BENCH_PRINCIPAL / ORC_BENCH_KEYTAB or ensure conf/environment.yaml paths exist." >&2
fi

SUBMIT+=(
  --conf spark.yarn.submit.waitAppCompletion=false
)
SUBMIT+=("$JAR")
SUBMIT+=("${APP_ARGS[@]}")

echo "[run-benchmark] yarn name=$APP_YARN_NAME"
echo "[run-benchmark] deploy_mode=$DEPLOY_MODE queue=$QUEUE jar=$JAR"
echo "[run-benchmark] app args: ${APP_ARGS[*]:-}"

if [[ "$DRY_RUN_SUBMIT" -eq 1 ]]; then
  printf '[run-benchmark] dry-run-submit:\n'
  printf '  %q' "${SUBMIT[@]}"
  printf '\n'
  exit 0
fi

orc_bench_require_spark_submit run-benchmark \
  "Run this script on a cluster node with Spark 3.2." \
  "Local smoke without submit: add --dry-run-submit" || exit 1

# shellcheck source=lib/detach-submit.sh
source "$ROOT/scripts/lib/detach-submit.sh"
detach_log="${ORC_BENCH_LOG_FILE:-$ROOT/logs/run-benchmark-detached.log}"
orc_bench_detach_submit "$detach_log" -- "${SUBMIT[@]}"
