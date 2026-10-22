# =============================================================================
# cluster-dist.sh — доставка локальных файлов драйвера в YARN cluster mode
#
# Подключение (после set -euo pipefail, когда известны DEPLOY_MODE и APP_ARGS):
#   # shellcheck source=lib/cluster-dist.sh
#   source "$ROOT/scripts/lib/cluster-dist.sh"
#   orc_bench_localize_driver_files
#
# Поведение:
#   deploy_mode=cluster: для --env-override и --metrics-jsonl, если путь —
#   локальный файл, добавляет его в CLUSTER_FILES и подменяет аргумент
#   приложения на basename. Spark --files кладёт файл в cwd контейнера AM.
#   Другой deploy_mode скрипты запуска больше не принимают.
#
# Ожидает глобальные: DEPLOY_MODE, APP_ARGS (массив).
# Заполняет: CLUSTER_FILES (массив абсолютных путей).
# =============================================================================

orc_bench_localize_driver_files() {
  CLUSTER_FILES=()
  orc_bench_reject_client_overlay

  if [[ "${DEPLOY_MODE:-}" != "cluster" ]]; then
    return 0
  fi

  local rewritten=()
  local i=0
  local n=${#APP_ARGS[@]}
  while [[ "$i" -lt "$n" ]]; do
    local arg="${APP_ARGS[$i]}"
    if [[ "$arg" == "--env-override" || "$arg" == "--metrics-jsonl" ]]; then
      if [[ $((i + 1)) -ge "$n" ]]; then
        echo "ERROR: $arg requires a path" >&2
        return 2
      fi
      local path="${APP_ARGS[$((i + 1))]}"
      if [[ "$path" == hdfs://* || "$path" == s3://* || "$path" == s3a://* ]]; then
        rewritten+=("$arg" "$path")
      elif [[ -f "$path" ]]; then
        local base abs
        base="$(basename "$path")"
        abs="$(cd "$(dirname "$path")" && pwd)/$base"
        CLUSTER_FILES+=("$abs")
        rewritten+=("$arg" "$base")
        echo "[cluster-dist] файл $abs → cwd AM как $base ($arg)"
      else
        echo "ERROR: $arg file not found on the submit host: $path" >&2
        echo "  In cluster mode the driver runs in a YARN container." >&2
        echo "  A path that exists only as conf/… on the gateway is not visible there." >&2
        return 1
      fi
      i=$((i + 2))
      continue
    fi
    rewritten+=("$arg")
    i=$((i + 1))
  done

  APP_ARGS=()
  if [[ ${#rewritten[@]} -gt 0 ]]; then
    APP_ARGS=("${rewritten[@]}")
  fi
}

# environment.local.yaml обёртка для --deploy-mode не читает.
# client в overlay тоже запрещён: иначе submit останется cluster,
# а файл всё ещё попросит client.
orc_bench_reject_client_overlay() {
  local i=0
  local n=${#APP_ARGS[@]}
  while [[ "$i" -lt "$n" ]]; do
    if [[ "${APP_ARGS[$i]}" == "--env-override" ]]; then
      if [[ $((i + 1)) -ge "$n" ]]; then
        echo "ERROR: --env-override requires a path" >&2
        return 2
      fi
      local path="${APP_ARGS[$((i + 1))]}"
      if [[ -f "$path" ]] && grep -Eq \
        '^[[:space:]]*deploy_mode:[[:space:]]*["'\'']?client["'\'']?[[:space:]]*(#.*)?$' \
        "$path"; then
        echo "ERROR: client deploy mode is disabled." >&2
        echo "  $path sets deploy_mode: client. Submit is cluster-only." >&2
        echo "  Remove that line (environment.yaml already sets deploy_mode: cluster)." >&2
        return 2
      fi
      i=$((i + 2))
      continue
    fi
    i=$((i + 1))
  done
}
