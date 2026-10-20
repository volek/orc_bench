#!/usr/bin/env bash
# =============================================================================
# kinit-bench.sh — Kerberos-логин (kinit) для orc-bench на узле кластера
#
# Назначение:
#   Выполняет kinit -kt <keytab> <principal>. Principal/keytab берутся из
#   conf/environment.yaml (runner_principal / keytab_path), если не заданы
#   флагами или env. Секреты в скрипт не встроены.
#
# Параметры:
#   -h, --help            показать эту справку
#   --principal PRINCIPAL Kerberos principal
#   --keytab PATH         путь к keytab (~ раскрывается в $HOME)
#   --yaml PATH           YAML с runner_principal / keytab_path
#   --dry-run             только показать команду kinit (без KDC/kinit)
#
# Переменные окружения:
#   ORC_BENCH_ENV_YAML    путь к YAML (по умолчанию conf/environment.yaml)
#   ORC_BENCH_PRINCIPAL   principal (перекрывает YAML)
#   ORC_BENCH_KEYTAB      путь к keytab (перекрывает YAML)
#
# Логирование (stdout+stderr → файл и консоль через tee):
#   ORC_BENCH_LOG_DIR    каталог логов (по умолчанию <repo>/logs)
#   ORC_BENCH_LOG_FILE   явный путь к лог-файлу
#                        (иначе <dir>/kinit-bench-YYYYMMDD-HHMMSS.log)
#   ORC_BENCH_LOGGING=1  уже включено (вложенный вызов без повторного tee)
#
# Примеры (узел):
#   ./scripts/kinit-bench.sh
#   ORC_BENCH_KEYTAB=/path/to.keytab ./scripts/kinit-bench.sh
#   ./scripts/kinit-bench.sh --principal pvm-omltp-sdp@OPSMON.SBT \
#       --keytab "$HOME/keytab/pvm-omltp-sdp.keytab"
#
# Пример (dev, без KDC):
#   ./scripts/kinit-bench.sh --dry-run
# =============================================================================
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=lib/logging.sh
source "$ROOT/scripts/lib/logging.sh"
orc_bench_enable_logging "$0" "$@"

YAML="${ORC_BENCH_ENV_YAML:-$ROOT/conf/environment.yaml}"
PRINCIPAL="${ORC_BENCH_PRINCIPAL:-}"
KEYTAB="${ORC_BENCH_KEYTAB:-}"
DRY_RUN=0

usage() {
  sed -n '2,36p' "$0" | sed 's/^# \?//'
  exit 2
}

yaml_value() {
  local key="$1"
  [[ -f "$YAML" ]] || return 0
  grep -E "^[[:space:]]*${key}:" "$YAML" | head -1 \
    | sed -E "s/.*:[\"']?[[:space:]]*//;s/[\"'][[:space:]]*$//;s/[[:space:]]*$//" || true
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help) usage ;;
    --principal) PRINCIPAL="$2"; shift 2 ;;
    --keytab) KEYTAB="$2"; shift 2 ;;
    --yaml) YAML="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    *) echo "Unknown arg: $1" >&2; usage ;;
  esac
done

if [[ -z "$PRINCIPAL" ]]; then
  PRINCIPAL="$(yaml_value runner_principal)"
fi
if [[ -z "$KEYTAB" ]]; then
  KEYTAB="$(yaml_value keytab_path)"
fi

KEYTAB="${KEYTAB/#\~/$HOME}"

if [[ -z "$PRINCIPAL" || -z "$KEYTAB" ]]; then
  echo "ERROR: need principal and keytab (flags, env, or conf/environment.yaml)." >&2
  exit 1
fi

echo "[kinit-bench] principal=$PRINCIPAL keytab=$KEYTAB"

if [[ "$DRY_RUN" -eq 1 ]]; then
  echo "[kinit-bench] dry-run: would run: kinit -kt $KEYTAB $PRINCIPAL"
  if [[ ! -f "$KEYTAB" ]]; then
    echo "[kinit-bench] dry-run: keytab file not present on this host (expected on node)."
  fi
  exit 0
fi

if [[ ! -f "$KEYTAB" ]]; then
  echo "ERROR: keytab file not found: $KEYTAB" >&2
  echo "  Place the keytab on the node (path from environment.yaml) — do not commit it." >&2
  exit 1
fi

if ! command -v kinit >/dev/null 2>&1; then
  echo "ERROR: kinit not found — run on a Kerberos-enabled cluster node." >&2
  echo "  Local smoke: ./scripts/kinit-bench.sh --dry-run" >&2
  exit 1
fi

kinit -kt "$KEYTAB" "$PRINCIPAL"
echo "[kinit-bench] ticket:"
klist
