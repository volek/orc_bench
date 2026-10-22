# =============================================================================
# common.sh — общие функции scripts/*.sh
#
# Подключение (обычно через logging.sh, отдельно не требуется):
#   source "$ROOT/scripts/lib/common.sh"
#
# Здесь: проверка CRLF, чтение скалярного поля YAML, jar и sizing spark-submit,
# запрет client, проверка spark-submit в PATH, печать шапки --help.
# =============================================================================

if [[ -n "${ORC_BENCH_COMMON_LOADED:-}" ]]; then
  return 0
fi
ORC_BENCH_COMMON_LOADED=1

# Завершает скрипт, если файл скопирован с Windows CRLF.
# Иначе bash показывает «: No such file or directory» — \r прячет путь.
orc_bench_reject_crlf() {
  local file="${1:-$0}"
  [[ -f "$file" ]] || return 0
  if grep -q $'\r' "$file" 2>/dev/null; then
    echo "ERROR: $file has Windows CRLF line endings." >&2
    echo "  Fix on this node:  sed -i 's/\\r\$//' scripts/*.sh scripts/lib/*.sh" >&2
    echo "  Or:                dos2unix scripts/*.sh scripts/lib/*.sh" >&2
    exit 1
  fi
}

# Печатает комментарии шапки между shebang и первым исполняемым рядком.
orc_bench_print_header() {
  local file="${1:-$0}"
  awk '
    NR == 1 { next }
    /^#/ { sub(/^# ?/, ""); print; next }
    { exit }
  ' "$file"
}

# Одно скалярное поле YAML: «key: value». Пусто, если файла или ключа нет.
# Двоеточия внутри значения (hdfs://…) не съедаются.
orc_bench_yaml_value() {
  local key="$1"
  local yaml="${2:-}"
  [[ -n "$yaml" && -f "$yaml" ]] || return 0
  grep -E "^[[:space:]]*${key}:" "$yaml" | head -1 \
    | sed -E "s/^[^:]+:[[:space:]]*//;s/[\"']//g;s/[[:space:]]*$//" || true
}

# Пустой JAR → бандл в корне, иначе build/libs.
orc_bench_resolve_jar() {
  if [[ -n "${JAR:-}" ]]; then
    return 0
  fi
  if [[ -f "$ROOT/orc-bench-0.1.0-SNAPSHOT-all.jar" ]]; then
    JAR="$ROOT/orc-bench-0.1.0-SNAPSHOT-all.jar"
  else
    JAR="$ROOT/build/libs/orc-bench-0.1.0-SNAPSHOT-all.jar"
  fi
}

orc_bench_require_jar() {
  orc_bench_resolve_jar
  if [[ -f "$JAR" ]]; then
    return 0
  fi
  echo "ERROR: fat jar not found: $JAR" >&2
  echo "  Expected (cluster bundle): $ROOT/orc-bench-0.1.0-SNAPSHOT-all.jar" >&2
  echo "  Expected (git checkout):   $ROOT/build/libs/orc-bench-0.1.0-SNAPSHOT-all.jar" >&2
  echo "  Override: --jar PATH  or  ORC_BENCH_JAR=PATH" >&2
  echo "  Build on JDK 8: ./scripts/build.sh  (or re-copy jar onto this node)" >&2
  return 1
}

# Пустые MASTER / DEPLOY_MODE / QUEUE / executors заполняет из environment.yaml.
# Уже заданные CLI и env не перезаписывает. Нужны глобальные ROOT и эти переменные.
orc_bench_apply_submit_defaults() {
  local yaml="${1:-${ENV_YAML:-$ROOT/conf/environment.yaml}}"
  if [[ -z "${MASTER:-}" ]]; then
    MASTER="$(orc_bench_yaml_value master "$yaml")"
    MASTER="${MASTER:-yarn}"
  fi
  if [[ -z "${DEPLOY_MODE:-}" ]]; then
    DEPLOY_MODE="$(orc_bench_yaml_value deploy_mode "$yaml")"
    DEPLOY_MODE="${DEPLOY_MODE:-cluster}"
  fi
  if [[ -z "${QUEUE:-}" ]]; then
    QUEUE="$(orc_bench_yaml_value queue "$yaml")"
    QUEUE="${QUEUE:-default}"
  fi
  if [[ -z "${NUM_EXECUTORS:-}" ]]; then
    NUM_EXECUTORS="$(orc_bench_yaml_value executor_instances "$yaml")"
    NUM_EXECUTORS="${NUM_EXECUTORS:-10}"
  fi
  if [[ -z "${EXECUTOR_CORES:-}" ]]; then
    EXECUTOR_CORES="$(orc_bench_yaml_value executor_cores "$yaml")"
    EXECUTOR_CORES="${EXECUTOR_CORES:-4}"
  fi
  if [[ -z "${EXECUTOR_MEMORY:-}" ]]; then
    local gib
    gib="$(orc_bench_yaml_value executor_memory_gib "$yaml")"
    if [[ -n "$gib" ]]; then
      EXECUTOR_MEMORY="${gib}g"
    else
      EXECUTOR_MEMORY="16g"
    fi
  fi
  orc_bench_resolve_jar
}

# Principal и keytab: CLI/env, иначе runner_principal / keytab_path. «~» → $HOME.
orc_bench_load_kerberos() {
  local yaml="${1:-${ENV_YAML:-$ROOT/conf/environment.yaml}}"
  if [[ -z "${PRINCIPAL:-}" ]]; then
    PRINCIPAL="$(orc_bench_yaml_value runner_principal "$yaml")"
  fi
  if [[ -z "${KEYTAB:-}" ]]; then
    local kt
    kt="$(orc_bench_yaml_value keytab_path "$yaml")"
    if [[ -n "$kt" ]]; then
      KEYTAB="${kt/#\~/$HOME}"
    fi
  fi
}

# Единственный допустимый режим — cluster.
orc_bench_require_cluster_mode() {
  case "${DEPLOY_MODE:-}" in
    cluster) return 0 ;;
    client)
      echo "ERROR: client deploy mode is disabled." >&2
      echo "  Submit is cluster-only. Remove spark.deploy_mode: client from" >&2
      echo "  environment.local.yaml and do not pass --deploy-mode client." >&2
      return 2 ;;
    *)
      echo "ERROR: --deploy-mode must be cluster (got: ${DEPLOY_MODE:-})" >&2
      return 2 ;;
  esac
}

# Живой запуск без spark-submit в PATH. $1 — ярлык, дальше строки-подсказки.
orc_bench_require_spark_submit() {
  local tag="${1:-submit}"
  shift || true
  if command -v spark-submit >/dev/null 2>&1; then
    return 0
  fi
  echo "[$tag] ERROR: spark-submit not in PATH." >&2
  local line
  for line in "$@"; do
    echo "  $line" >&2
  done
  return 1
}

# Список CLUSTER_FILES через запятую для spark-submit --files.
orc_bench_cluster_files_csv() {
  local files_csv="" f
  for f in "${CLUSTER_FILES[@]}"; do
    if [[ -n "$files_csv" ]]; then
      files_csv+=","
    fi
    files_csv+="$f"
  done
  printf '%s\n' "$files_csv"
}
