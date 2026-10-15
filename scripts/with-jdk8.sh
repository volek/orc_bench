#!/usr/bin/env bash
# =============================================================================
# with-jdk8.sh — запуск произвольной команды под JDK 8
#
# Зачем: environment.yaml фиксирует java 1.8.0_292; сборка Gradle обязана
#        идти на Java 8 (задача verifyJava8 в build.gradle).
#
# Использование:
#   ./scripts/with-jdk8.sh ./gradlew test
#   ./scripts/with-jdk8.sh ./scripts/build.sh
#   JAVA_HOME_8=/opt/jdk1.8.0_292 ./scripts/with-jdk8.sh java -version
#
# Параметры окружения (не позиционные аргументы скрипта):
#   JAVA_HOME_8  — путь к JDK 8. Если не задан, берётся $HOME/.local/jdk/jdk8
#                  (портативная Temurin, скачанная при локальной сборке).
#
# Позиционные аргументы:
#   $@           — команда и её аргументы, которые нужно выполнить уже с
#                  JAVA_HOME/PATH, указывающими на JDK 8.
# =============================================================================
set -euo pipefail

# Корень репозитория (scripts/ → ..)
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# --- JAVA_HOME_8: каталог JDK 8 (bin/java, bin/javac) -----------------------
# Приоритет: явно заданный JAVA_HOME_8 → локальный ~/.local/jdk/jdk8
export JAVA_HOME="${JAVA_HOME_8:-$HOME/.local/jdk/jdk8}"

if [[ ! -x "$JAVA_HOME/bin/java" ]]; then
  echo "ERROR: JDK 8 не найден: $JAVA_HOME/bin/java" >&2
  echo "Укажите JAVA_HOME_8=/path/to/jdk8 или установите JDK 8." >&2
  exit 1
fi

# Подставляем JDK 8 в начало PATH, чтобы gradle/javac брали именно его
export PATH="$JAVA_HOME/bin:$PATH"

echo "[with-jdk8] JAVA_HOME=$JAVA_HOME"
"$JAVA_HOME/bin/java" -version

cd "$ROOT"

# --- $@: команда, которую нужно выполнить под JDK 8 -------------------------
if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <command> [args...]" >&2
  exit 2
fi

exec "$@"
