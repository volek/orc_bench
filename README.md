# orc-bench

Java + Gradle benchmark orchestrator for ORC / HDFS / Spark 3.2 / Hive (Tez & LLAP).

**Status:** Checkpoints 0–9 **(A) local/implementation** done (CLI phases through `report`/`final` + cluster launch scripts / packaging / runbook). Live cluster validation **(B)** for CP3–CP9 is pending on a Hadoop node. Checkpoint 10 waits for an explicit go-ahead.

> **This development host cannot perform Hadoop-cluster actions** (live Kerberos, HDFS `/orc_test`, `spark-submit` on YARN, Hive JDBC, real generate/run). Local work = code, unit/mock tests, packaging, script `--dry-run-submit`. Cluster work = on-node only. See plan §7.0.

## Docs

| Document | Role |
|---|---|
| [`docs/План разработки тестового приложения ORC HDFS Spark Hive.md`](docs/План%20разработки%20тестового%20приложения%20ORC%20HDFS%20Spark%20Hive.md) | Plan + gated checkpoints (A local / B cluster) |
| [`docs/Запуск на Hadoop кластере.md`](docs/Запуск%20на%20Hadoop%20кластере.md) | **Ops runbook** — copy, phases, CLI, spark-submit, reports, results |
| [`docs/orc_hdfs_spark_hive_configs_final/`](docs/orc_hdfs_spark_hive_configs_final/) | Source-of-truth configs (`hdfs_root=/orc_test`, queue `default`) |

## Build (local / CI)

```bash
./scripts/build.sh
# or: ./gradlew clean test shadowJar
```

Artifact: `build/libs/orc-bench-0.1.0-SNAPSHOT-all.jar`  
Main-Class: `orcbench.Main`

Package for scp to a node (no cluster connection):

```bash
./scripts/package-for-cluster.sh
# → dist/orc-bench-cluster-bundle.tgz
./scripts/package-for-cluster.sh --with-runbook
```

## CLI (local smoke — no live cluster)

```bash
java -cp build/libs/orc-bench-0.1.0-SNAPSHOT-all.jar orcbench.Main --help

# Config-derived capabilities / skip matrix without live HDFS/JDBC
java -cp build/libs/orc-bench-0.1.0-SNAPSHOT-all.jar orcbench.Main \
  --config conf --scale S --phase preflight --dry-run --results-dir results

# Script smoke (prints spark-submit line; no submit)
./scripts/run-benchmark.sh --dry-run-submit --phase preflight --scale S
```

## Cluster node (required for live probes / runs)

After copying jar + `conf/` + `scripts/` to a node (see runbook). Kerberos ticket/principal must already be available on the node:

```bash
./scripts/run-preflight.sh --env-override conf/environment.local.yaml
./scripts/run-preflight.sh --live --env-override conf/environment.local.yaml

# Live generate → /orc_test/S/RAW_ARRAY_BASE/ (+ refs/, manifest/)
./scripts/run-benchmark.sh \
  --jar ./orc-bench-0.1.0-SNAPSHOT-all.jar \
  --scale S --phase generate \
  --env-override conf/environment.local.yaml
```

Default `spark-submit`: `--deploy-mode cluster`, `--queue default`, executors `10×4×16g`. Switch to client with `--deploy-mode client` (easier local `results/`).

Runtime contract: `conf/` (copy of `docs/orc_hdfs_spark_hive_configs_final/`).
