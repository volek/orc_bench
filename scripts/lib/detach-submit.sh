# =============================================================================
# detach-submit.sh — spark-submit вне сессии терминала
#
# Подключение:
#   # shellcheck source=lib/detach-submit.sh
#   source "$ROOT/scripts/lib/detach-submit.sh"
#   orc_bench_detach_submit "$LOG_FILE" -- spark-submit ...
#
# Поведение:
#   Запускает команду в новой сессии (setsid + nohup), не ждёт конца YARN-приложения.
#   Скрипт возвращает управление, когда в логе появился application_<id>,
#   либо когда процесс submit уже завершился. Обрыв SSH после этого
#   не останавливает ApplicationMaster.
# =============================================================================

orc_bench_detach_submit() {
  local log_file="${1:-}"
  shift || true
  if [[ "${1:-}" == "--" ]]; then
    shift
  fi
  if [[ -z "$log_file" || $# -eq 0 ]]; then
    echo "[submit] ERROR: orc_bench_detach_submit LOG_FILE -- command..." >&2
    return 2
  fi

  mkdir -p "$(dirname "$log_file")"
  echo "[submit] detach: log=$log_file" | tee -a "$log_file"

  if command -v setsid >/dev/null 2>&1; then
    setsid nohup "$@" >>"$log_file" 2>&1 </dev/null &
  else
    nohup "$@" >>"$log_file" 2>&1 </dev/null &
  fi
  local pid=$!
  echo "[submit] pid=$pid" | tee -a "$log_file"
  echo "$pid" >"${log_file}.pid"

  local i app
  for ((i = 0; i < 180; i++)); do
    app="$(grep -Eo 'application_[0-9]+_[0-9]+' "$log_file" 2>/dev/null | head -1 || true)"
    if [[ -n "$app" ]]; then
      echo "[submit] YARN $app"
      echo "[submit] скрипт завершается; приложение на кластере продолжает работу."
      echo "[submit] результаты: hdfs://orc_test/results/<namenode>/<runId>/"
      echo "[submit] лог драйвера: hdfs://orc_test/logs/<namenode>/<runId>/driver.log"
      return 0
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "[submit] ERROR: spark-submit завершился до application id. Хвост лога:" >&2
      tail -n 40 "$log_file" >&2 || true
      return 1
    fi
    sleep 2
  done

  echo "[submit] submit ещё идёт (pid=$pid). Application id пока нет в $log_file"
  echo "[submit] скрипт завершается; процесс submit отвязан от этого терминала."
  return 0
}
