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
# Пример:
#   ./scripts/build.sh
# =============================================================================
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if "$ROOT/scripts/with-jdk8.sh" true 2>/dev/null; then
  exec "$ROOT/scripts/with-jdk8.sh" ./gradlew clean build --no-daemon
fi

echo "[build] JDK 8 wrapper unavailable; using Gradle Java toolchain (languageVersion=8)"
exec ./gradlew clean build --no-daemon
