#!/usr/bin/env bash
# =============================================================================
# spark-submit-orc-bench.sh — низкоуровневый вход в тот же submit, что run-benchmark.sh
#
# Назначение:
#   Разбирает опции submit и аргументы Main (разделитель --) и передаёт их
#   в scripts/run-benchmark.sh. Отдельной сборки spark-submit здесь нет:
#   jar, YAML-дефолты, --files, каталог результатов и отвязка от терминала
#   живут в одном месте.
#   Отличия CLI от run-benchmark.sh:
#     --master всегда yarn;
#     флаги orcbench.Main только после --;
#     --dry-run здесь = --dry-run-submit там (печать команды, без submit).
#   Если --results-dir не задан, run-benchmark подставит
#   hdfs://<namenode>/orc_test/results/<host>.
#
# Параметры submit (до --):
#   -h, --help              показать эту справку
#   --jar PATH              fat-jar (ORC_BENCH_JAR или путь по умолчанию)
#   --deploy-mode MODE      только cluster (client отключён)
#   --queue NAME            YARN-очередь
#   --num-executors N       число executor'ов
#   --executor-cores N      ядра на executor
#   --executor-memory SIZE  память executor, напр. 16g
#   --driver-memory SIZE    память драйвера (иначе 4g)
#   --principal PRINCIPAL   Kerberos principal
#   --keytab PATH           путь к keytab
#   --name TEXT             имя в YARN App Manager (иначе orc-bench <phase> <scale> …)
#   --dry-run               только напечатать команду и выйти 0
#   --                      дальше аргументы для orcbench.Main
#
# После -- : флаги Main (--config, --phase, --scale, …)
#
# Переменные окружения: те же, что у run-benchmark.sh
#   (ORC_BENCH_JAR, ORC_BENCH_DEPLOY_MODE, ORC_BENCH_QUEUE, …)
#
# Логирование (stdout+stderr → файл и консоль через tee):
#   ORC_BENCH_LOG_DIR    каталог логов (по умолчанию <repo>/logs)
#   ORC_BENCH_LOG_FILE   явный путь к лог-файлу
#                        (иначе <dir>/spark-submit-orc-bench-YYYYMMDD-HHMMSS.log)
#   ORC_BENCH_LOGGING=1  уже включено — вложенный run-benchmark.sh без повторного tee
#
# Пример:
#   ./scripts/spark-submit-orc-bench.sh --deploy-mode cluster -- \
#       --config conf --scale S --phase preflight --dry-run
# =============================================================================
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=lib/logging.sh
source "$ROOT/scripts/lib/logging.sh"
orc_bench_enable_logging "$0" "$@"

usage() {
  orc_bench_print_header "$0"
  exit 2
}

FORWARD=(--master yarn)
PARSING_MAIN=0

while [[ $# -gt 0 ]]; do
  if [[ "$PARSING_MAIN" -eq 1 ]]; then
    FORWARD+=("$1")
    shift
    continue
  fi
  case "$1" in
    -h|--help) usage ;;
    --) PARSING_MAIN=1; shift ;;
    --jar|--deploy-mode|--queue|--num-executors|--executor-cores|--executor-memory|--driver-memory|--principal|--keytab|--name)
      FORWARD+=("$1" "$2")
      shift 2
      ;;
    --dry-run)
      FORWARD+=(--dry-run-submit)
      shift
      ;;
    *)
      echo "Unknown submit option: $1 (use -- before Main args)" >&2
      usage
      ;;
  esac
done

echo "[spark-submit-orc-bench] передаю запуск в run-benchmark.sh (master=yarn)"
exec "$ROOT/scripts/run-benchmark.sh" "${FORWARD[@]}"
