#!/usr/bin/env bash
# =============================================================================
# build.sh — сборка fat-jar (Gradle shadow / clean build)
#
# Назначение:
#   Запускает ./gradlew clean build --no-daemon.
#   Если доступен with-jdk8.sh (найден JDK 8) — сборка через него;
#   иначе Gradle Java toolchain (languageVersion=8).
#
# Параметры: нет (скрипт не принимает флаги и аргументы).
#
# Переменные окружения:
#   косвенно JAVA_HOME_8 — через with-jdk8.sh, если обёртка используется
#
# Логирование (stdout+stderr → файл и консоль через tee):
#   ORC_BENCH_LOG_DIR    каталог логов (по умолчанию <repo>/logs)
#   ORC_BENCH_LOG_FILE   явный путь к лог-файлу
#                        (иначе <dir>/build-YYYYMMDD-HHMMSS.log)
#   ORC_BENCH_LOGGING=1  уже включено — вложенный with-jdk8.sh без повторного tee
#
# Пример:
#   ./scripts/build.sh
# =============================================================================
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=lib/logging.sh
source "$ROOT/scripts/lib/logging.sh"
orc_bench_enable_logging "$0" "$@"

cd "$ROOT"

# Probe with ORC_BENCH_LOGGING=1 so with-jdk8 does not open a second log file.
if ORC_BENCH_LOGGING=1 "$ROOT/scripts/with-jdk8.sh" true 2>/dev/null; then
  exec "$ROOT/scripts/with-jdk8.sh" ./gradlew clean build --no-daemon
fi

echo "[build] JDK 8 wrapper unavailable; using Gradle Java toolchain (languageVersion=8)"
exec ./gradlew clean build --no-daemon
