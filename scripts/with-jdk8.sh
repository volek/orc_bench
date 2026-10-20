#!/usr/bin/env bash
# =============================================================================
# with-jdk8.sh — запуск команды под JDK 8
#
# Назначение:
#   Находит JDK 8, выставляет JAVA_HOME/PATH и выполняет переданную команду.
#   Целевая версия: java 1.8.0_292 (см. environment.yaml).
#
# Параметры:
#   <command> [args...]   обязательная команда и её аргументы (exec)
#
# Переменные окружения:
#   JAVA_HOME_8           предпочтительный путь к JDK 8 (первый кандидат)
#                         иначе пробуются: ~/.local/jdk/jdk8,
#                         /usr/lib/jvm/java-8-openjdk-amd64,
#                         /usr/lib/jvm/java-1.8.0-openjdk
#
# Логирование (stdout+stderr → файл и консоль через tee):
#   ORC_BENCH_LOG_DIR    каталог логов (по умолчанию <repo>/logs)
#   ORC_BENCH_LOG_FILE   явный путь к лог-файлу
#                        (иначе <dir>/with-jdk8-YYYYMMDD-HHMMSS.log)
#   ORC_BENCH_LOGGING=1  уже включено (вложенный вызов без повторного tee)
#
# Примеры:
#   ./scripts/with-jdk8.sh ./gradlew test
#   ./scripts/with-jdk8.sh java -version
# =============================================================================
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=lib/logging.sh
source "$ROOT/scripts/lib/logging.sh"
orc_bench_enable_logging "$0" "$@"

candidates=(
  "${JAVA_HOME_8:-}"
  "$HOME/.local/jdk/jdk8"
  "/usr/lib/jvm/java-8-openjdk-amd64"
  "/usr/lib/jvm/java-1.8.0-openjdk"
)

JAVA_HOME_RESOLVED=""
for c in "${candidates[@]}"; do
  if [[ -n "$c" && -x "$c/bin/java" ]]; then
    JAVA_HOME_RESOLVED="$c"
    break
  fi
done

if [[ -z "$JAVA_HOME_RESOLVED" ]]; then
  echo "ERROR: JDK 8 not found. Set JAVA_HOME_8=/path/to/jdk8" >&2
  echo "Gradle Java toolchain may still auto-provision JDK 8 for ./gradlew builds." >&2
  exit 1
fi

export JAVA_HOME="$JAVA_HOME_RESOLVED"
export PATH="$JAVA_HOME/bin:$PATH"

echo "[with-jdk8] JAVA_HOME=$JAVA_HOME"
"$JAVA_HOME/bin/java" -version

cd "$ROOT"
if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <command> [args...]" >&2
  exit 2
fi
exec "$@"
