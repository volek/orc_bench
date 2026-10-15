#!/usr/bin/env bash
# =============================================================================
# run-benchmark.sh — запуск orc-bench на ноде Hadoop/SDP через spark-submit
#
# Назначение:
#   Оркестрация фаз preflight/generate/materialize/run/final на очереди
#   YARN «benchmark» согласно docs/orc_hdfs_spark_hive_configs_final.
#
# Типовой вызов:
#   ./scripts/run-benchmark.sh \
#     --jar build/libs/orc-bench-0.1.0-SNAPSHOT-all.jar \
#     --scale S \
#     --phase preflight \
#     --env-override conf/environment.local.yaml
#
# Требования на ноде:
#   - Java 8 в PATH / JAVA_HOME
#   - spark-submit (Spark 3.2.1 SDP)
#   - доступ к HDFS и ZooKeeper HS2
#   - каталог conf/ рядом с jar (или путь через --config)
#
# Параметры скрипта (все опциональны, кроме логики по умолчанию):
#
#   --jar PATH
#       Путь к fat jar. По умолчанию:
#       build/libs/orc-bench-0.1.0-SNAPSHOT-all.jar
#
#   --config DIR
#       Каталог YAML-контракта (benchmark/dataset/environment/query-suite/
#       schema). По умолчанию: conf
#
#   --scale S|M|L
#       Масштаб датасета из dataset.yaml:
#         S = 100 GiB, M = 1024 GiB, L = 5120 GiB
#       По умолчанию: S
#
#   --phase NAME
#       Фаза pipeline: preflight | generate | materialize | run | final | all
#       По умолчанию: all
#
#   --experiments LIST
#       Фильтр ID экспериментов через запятую (например
#       BLOOM_USER,COMPRESSION,ORC_PUSHDOWN). Пусто = все из benchmark.yaml
#
#   --engines LIST
#       Список движков: spark,hive_tez,hive_llap
#       По умолчанию: spark,hive_tez,hive_llap
#
#   --env-override PATH
#       YAML overlay для null-полей environment.yaml (namenode, executor_*,
#       llap_* и т.д.). Если файл не существует — флаг не передаётся.
#
#   --retain LIST
#       ID layout’ов, которые оставить при --cleanup (через запятую).
#       Обычно: RAW_ARRAY_BASE или выбранный BEST_ORC
#
#   --cleanup
#       После materialize удалить на HDFS layout’ы вне --retain
#       (экономия места при больших scale)
#
#   --results-dir DIR
#       Локальный каталог отчётов на ноде. По умолчанию: results
#       Внутри появится results/<runId>/{SUMMARY.txt,summary.csv,...}
#
#   --dry-run
#       Только планирование/preflight без реальных Spark/Hive job’ов
#
#   --master URL
#       Spark master для spark-submit. По умолчанию: yarn
#       (как environment.spark.master)
#
#   --deploy-mode MODE
#       client | cluster. По умолчанию: client
#       client нужен, чтобы driver на ноде оркестрировал Hive JDBC локально.
#
#   --queue NAME
#       YARN queue. По умолчанию: benchmark (environment.yarn.queue)
#
#   --driver-memory SIZE
#       Память driver для spark-submit (например 4g). По умолчанию: 4g
#
#   --executor-memory SIZE
#       Память executor. Если не задано — берётся из cluster defaults /
#       env-override внутри приложения.
#
#   --executor-cores N
#       Число ядер executor (опционально).
#
#   --num-executors N
#       Число executor’ов (опционально).
#
#   --help
#       Показать эту справку.
#
# Дополнительные аргументы после «--» уходят напрямую в orcbench.Main.
# =============================================================================
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# --- значения по умолчанию (соответствуют environment.yaml / CLI) -----------
JAR="build/libs/orc-bench-0.1.0-SNAPSHOT-all.jar"   # --jar: fat jar приложения
CONFIG_DIR="conf"                                   # --config: каталог YAML
SCALE="S"                                           # --scale: S|M|L
PHASE="all"                                         # --phase: фаза pipeline
EXPERIMENTS=""                                      # --experiments: фильтр ID
ENGINES="spark,hive_tez,hive_llap"                  # --engines: движки
ENV_OVERRIDE="conf/environment.local.yaml"          # --env-override: overlay
RETAIN=""                                           # --retain: сохранить layout’ы
CLEANUP=0                                           # --cleanup: чистить HDFS
RESULTS_DIR="results"                               # --results-dir: отчёты
DRY_RUN=0                                           # --dry-run: без job’ов
MASTER="yarn"                                       # --master: Spark master
DEPLOY_MODE="client"                                # --deploy-mode: client|cluster
QUEUE="benchmark"                                   # --queue: YARN queue
DRIVER_MEMORY="4g"                                  # --driver-memory
EXECUTOR_MEMORY=""                                  # --executor-memory (опц.)
EXECUTOR_CORES=""                                   # --executor-cores (опц.)
NUM_EXECUTORS=""                                    # --num-executors (опц.)
EXTRA_APP_ARGS=()                                   # аргументы после --

while [[ $# -gt 0 ]]; do
  case "$1" in
    --jar) JAR="$2"; shift 2 ;;
    --config) CONFIG_DIR="$2"; shift 2 ;;
    --scale) SCALE="$2"; shift 2 ;;
    --phase) PHASE="$2"; shift 2 ;;
    --experiments) EXPERIMENTS="$2"; shift 2 ;;
    --engines) ENGINES="$2"; shift 2 ;;
    --env-override) ENV_OVERRIDE="$2"; shift 2 ;;
    --retain) RETAIN="$2"; shift 2 ;;
    --cleanup) CLEANUP=1; shift ;;
    --results-dir) RESULTS_DIR="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    --master) MASTER="$2"; shift 2 ;;
    --deploy-mode) DEPLOY_MODE="$2"; shift 2 ;;
    --queue) QUEUE="$2"; shift 2 ;;
    --driver-memory) DRIVER_MEMORY="$2"; shift 2 ;;
    --executor-memory) EXECUTOR_MEMORY="$2"; shift 2 ;;
    --executor-cores) EXECUTOR_CORES="$2"; shift 2 ;;
    --num-executors) NUM_EXECUTORS="$2"; shift 2 ;;
    --help|-h)
      sed -n '2,90p' "$0"
      exit 0
      ;;
    --)
      shift
      EXTRA_APP_ARGS+=("$@")
      break
      ;;
    *)
      echo "Неизвестный параметр: $1 (см. --help)" >&2
      exit 2
      ;;
  esac
done

if [[ ! -f "$JAR" ]]; then
  echo "ERROR: jar не найден: $JAR" >&2
  echo "Соберите: ./scripts/with-jdk8.sh ./scripts/build.sh" >&2
  exit 1
fi
if [[ ! -d "$CONFIG_DIR" ]]; then
  echo "ERROR: каталог конфигов не найден: $CONFIG_DIR" >&2
  exit 1
fi
command -v spark-submit >/dev/null || {
  echo "ERROR: spark-submit не найден в PATH" >&2
  exit 1
}

# --- аргументы spark-submit -------------------------------------------------
SPARK_ARGS=(
  --master "$MASTER"
  --deploy-mode "$DEPLOY_MODE"
  --queue "$QUEUE"
  --driver-memory "$DRIVER_MEMORY"
  --class orcbench.Main
)
# Опциональные ресурсы executor (если заданы явно)
[[ -n "$EXECUTOR_MEMORY" ]] && SPARK_ARGS+=(--executor-memory "$EXECUTOR_MEMORY")
[[ -n "$EXECUTOR_CORES" ]] && SPARK_ARGS+=(--executor-cores "$EXECUTOR_CORES")
[[ -n "$NUM_EXECUTORS" ]] && SPARK_ARGS+=(--num-executors "$NUM_EXECUTORS")

# --- аргументы orcbench.Main ------------------------------------------------
APP_ARGS=(
  --config "$CONFIG_DIR"
  --scale "$SCALE"
  --phase "$PHASE"
  --engines "$ENGINES"
  --results-dir "$RESULTS_DIR"
)
[[ -n "$EXPERIMENTS" ]] && APP_ARGS+=(--experiments "$EXPERIMENTS")
[[ -n "$RETAIN" ]] && APP_ARGS+=(--retain "$RETAIN")
[[ "$CLEANUP" -eq 1 ]] && APP_ARGS+=(--cleanup)
[[ "$DRY_RUN" -eq 1 ]] && APP_ARGS+=(--dry-run)
# env-override передаём только если файл реально существует
if [[ -n "$ENV_OVERRIDE" && -f "$ENV_OVERRIDE" ]]; then
  APP_ARGS+=(--env-override "$ENV_OVERRIDE")
elif [[ -n "$ENV_OVERRIDE" ]]; then
  echo "WARNING: --env-override файл отсутствует ($ENV_OVERRIDE), пропускаем" >&2
fi
APP_ARGS+=("${EXTRA_APP_ARGS[@]}")

echo "[run-benchmark] jar=$JAR"
echo "[run-benchmark] scale=$SCALE phase=$PHASE engines=$ENGINES"
echo "[run-benchmark] master=$MASTER deploy-mode=$DEPLOY_MODE queue=$QUEUE"

exec spark-submit "${SPARK_ARGS[@]}" "$JAR" "${APP_ARGS[@]}"
