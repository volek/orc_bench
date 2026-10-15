#!/usr/bin/env bash
# =============================================================================
# build.sh — сборка fat jar orc-bench на JDK 8
#
# Что делает:
#   1) clean — очистка build/
#   2) test  — прогон всех unit-тестов
#   3) shadowJar — сборка uber-jar со всеми implementation-зависимостями
#      (Spark/Hadoop остаются provided и подтягиваются кластером)
#
# Результат:
#   build/libs/orc-bench-0.1.0-SNAPSHOT-all.jar
#
# Использование:
#   ./scripts/with-jdk8.sh ./scripts/build.sh
#   ./scripts/build.sh --skip-tests
#
# Параметры скрипта (флаги):
#   --skip-tests   Не запускать unit-тесты (только clean + shadowJar).
#                  Удобно для быстрой пересборки на ноде, если тесты уже
#                  прогнаны на CI/рабочей станции.
#   --help         Показать эту справку и выйти.
#
# Параметры окружения:
#   JAVA_HOME      Должен указывать на JDK 8 (или запускайте через with-jdk8.sh).
# =============================================================================
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# --- разбор флагов скрипта --------------------------------------------------
SKIP_TESTS=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    # --skip-tests: пропустить Gradle task «test»
    --skip-tests)
      SKIP_TESTS=1
      shift
      ;;
    # --help: краткая справка
    --help|-h)
      sed -n '2,25p' "$0"
      exit 0
      ;;
    *)
      echo "Неизвестный параметр: $1 (см. --help)" >&2
      exit 2
      ;;
  esac
done

if [[ -z "${JAVA_HOME:-}" ]]; then
  echo "WARNING: JAVA_HOME не задан. Рекомендуется: ./scripts/with-jdk8.sh $0" >&2
fi

echo "[build] java=$("$JAVA_HOME/bin/java" -version 2>&1 | head -1 || java -version 2>&1 | head -1)"

GRADLE_ARGS=(clean shadowJar)
# Без --skip-tests добавляем прогон тестов перед упаковкой jar
if [[ "$SKIP_TESTS" -eq 0 ]]; then
  GRADLE_ARGS=(clean test shadowJar)
fi

./gradlew --no-daemon "${GRADLE_ARGS[@]}"

JAR="build/libs/orc-bench-0.1.0-SNAPSHOT-all.jar"
ls -lh "$JAR"
echo "[build] OK → $JAR"
