#!/usr/bin/env bash
# =============================================================================
# package-for-cluster.sh — локальная сборка tarball (jar + conf + scripts) для scp
#
# Назначение:
#   Упаковывает fat-jar, conf/ (без секретов) и кластерные скрипты в архив
#   для копирования на узел. Безопасно на машине разработчика (без кластера).
#   Не включает: keytab'ы, environment.local.yaml, results/.
#
# Параметры:
#   -h, --help         показать эту справку
#   --out PATH         путь к выходному .tgz
#   --jar PATH         путь к fat-jar (иначе build/libs/orc-bench-…-all.jar)
#   --with-runbook     положить в бандл docs/Запуск на Hadoop кластере.md
#
# Переменные окружения:
#   ORC_BENCH_BUNDLE   путь к .tgz по умолчанию
#                      (иначе dist/orc-bench-cluster-bundle.tgz)
#
# Примеры:
#   ./scripts/package-for-cluster.sh
#   ./scripts/package-for-cluster.sh --out dist/my-bundle.tgz
#   ./scripts/package-for-cluster.sh --with-runbook
# =============================================================================
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

OUT="${ORC_BENCH_BUNDLE:-$ROOT/dist/orc-bench-cluster-bundle.tgz}"
JAR="$ROOT/build/libs/orc-bench-0.1.0-SNAPSHOT-all.jar"
WITH_RUNBOOK=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out) OUT="$2"; shift 2 ;;
    --jar) JAR="$2"; shift 2 ;;
    --with-runbook) WITH_RUNBOOK=1; shift ;;
    -h|--help)
      sed -n '2,24p' "$0" | sed 's/^# \?//'
      exit 2 ;;
    *) echo "Unknown arg: $1" >&2; exit 2 ;;
  esac
done

if [[ ! -f "$JAR" ]]; then
  echo "ERROR: jar not found: $JAR" >&2
  echo "  Run ./scripts/build.sh first (JDK 8)." >&2
  exit 1
fi

mkdir -p "$(dirname "$OUT")"
STAGE="$(mktemp -d)"
cleanup() { rm -rf "$STAGE"; }
trap cleanup EXIT

BUNDLE="$STAGE/orc-bench"
mkdir -p "$BUNDLE/scripts" "$BUNDLE/conf"

cp -a "$JAR" "$BUNDLE/orc-bench-0.1.0-SNAPSHOT-all.jar"
# conf without local overlays / secrets
cp -a "$ROOT/conf/." "$BUNDLE/conf/"
rm -f "$BUNDLE/conf/environment.local.yaml" 2>/dev/null || true
# Defense in depth: never ship keytabs / credential blobs if present under conf/
find "$BUNDLE" \( -name '*.keytab' -o -name '*.ktab' -o -name '*.pem' \
  -o -name '*.p12' -o -name '*.jks' -o -name '*.keystore' \) -type f -delete 2>/dev/null || true

# Cluster-side + packaging scripts only (skip local JDK wrapper noise is OK to include)
for s in \
  run-benchmark.sh \
  run-preflight.sh \
  run-pilot-s.sh \
  spark-submit-orc-bench.sh \
  kinit-bench.sh \
  fetch-results.sh \
  package-for-cluster.sh \
  build.sh \
  with-jdk8.sh
do
  if [[ -f "$ROOT/scripts/$s" ]]; then
    # Strip CR so Windows/autocrlf checkouts cannot poison the node bundle
    # (CRLF → cryptic ": No such file or directory" on exec).
    tr -d '\r' < "$ROOT/scripts/$s" > "$BUNDLE/scripts/$s"
    chmod +x "$BUNDLE/scripts/$s"
  fi
done

if grep -q $'\r' "$BUNDLE"/scripts/*.sh 2>/dev/null; then
  echo "ERROR: CRLF still present in staged scripts after strip - aborting." >&2
  exit 1
fi

if [[ -f "$ROOT/conf/environment.local.yaml.example" ]]; then
  cp -a "$ROOT/conf/environment.local.yaml.example" \
    "$BUNDLE/conf/environment.local.yaml.example"
fi

if [[ "$WITH_RUNBOOK" -eq 1 ]]; then
  mkdir -p "$BUNDLE/docs"
  if [[ -f "$ROOT/docs/Запуск на Hadoop кластере.md" ]]; then
    cp -a "$ROOT/docs/Запуск на Hadoop кластере.md" "$BUNDLE/docs/"
  fi
fi

cat > "$BUNDLE/NODE_README.txt" <<'EOF'
orc-bench cluster bundle (Checkpoint 10)
========================================

On the Hadoop/SDP node (not the developer laptop):

  1. tar -xzf orc-bench-cluster-bundle.tgz && cd orc-bench
  2. cp conf/environment.local.yaml.example conf/environment.local.yaml
     # edit deploy_mode / executor_* if needed (default deploy_mode=cluster)
  3. Ensure Kerberos ticket/principal is already available; keytab at path from
     conf/environment.yaml for spark-submit --principal/--keytab (do not commit it)
  4. ./scripts/run-preflight.sh --env-override conf/environment.local.yaml
  5. ./scripts/run-preflight.sh --live --env-override conf/environment.local.yaml
  6. ./scripts/run-pilot-s.sh --phase generate --env-override conf/environment.local.yaml
  7. ./scripts/run-pilot-s.sh --phase materialize --env-override conf/environment.local.yaml
  8. ./scripts/run-pilot-s.sh --phase run --env-override conf/environment.local.yaml
  9. Fill calibration checklist (runbook §12); finalize executor_* / concurrency for S

If scripts fail with ": No such file or directory" (empty path before colon):
  sed -i 's/\r$//' scripts/*.sh
  # or: dos2unix scripts/*.sh
  Then re-run. Cause is Windows CRLF line endings on *.sh.

Jar layout in this bundle (repo root, not build/libs/):
  ./orc-bench-0.1.0-SNAPSHOT-all.jar

Pilot S experiments: conf/pilot-s.yaml
  materialize: MINMAX_SORT,BLOOM_USER,PARTITIONING
  run: ORC_COLUMN_PRUNING,ORC_PUSHDOWN,MINMAX_SORT,BLOOM_USER,PARTITIONING,SPARK_VECTORIZATION

Switch to client mode (easier local results/):
  ./scripts/run-benchmark.sh --deploy-mode client ...

Defaults: yarn / deploy_mode=cluster / queue=default / executors 10×4×16g

Full ops runbook: docs/Запуск на Hadoop кластере.md (if --with-runbook) or
the repo copy on the developer machine.
EOF

{
  echo "orc-bench-cluster-bundle"
  echo "jar=$(basename "$JAR")"
  echo "built_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "includes=jar,conf,scripts,NODE_README.txt"
  if [[ "$WITH_RUNBOOK" -eq 1 ]]; then
    echo "runbook=yes"
  else
    echo "runbook=no (repackage with --with-runbook to include)"
  fi
} > "$BUNDLE/BUNDLE_MANIFEST.txt"

tar -C "$STAGE" -czf "$OUT" orc-bench

echo "[package-for-cluster] wrote $OUT"
echo "  contents:"
tar -tzf "$OUT" | sed 's/^/    /' | head -40
COUNT="$(tar -tzf "$OUT" | wc -l | tr -d ' ')"
if [[ "$COUNT" -gt 40 ]]; then
  echo "    … ($COUNT entries total)"
fi
echo "  scp to node, then: tar -xzf $(basename "$OUT") && cd orc-bench"
echo "  See NODE_README.txt and docs/Запуск на Hadoop кластере.md"
