# orc-bench (Java 8 / Gradle)

Fat-jar benchmark orchestrator for ORC / HDFS / Spark 3.2 / Hive 3.1 (Tez/LLAP) on SDP Hadoop.

**Runtime contract** ([`docs/orc_hdfs_spark_hive_configs_final/environment.yaml`](docs/orc_hdfs_spark_hive_configs_final/environment.yaml)):

| Component | Version |
|---|---|
| Java | **1.8** (cluster: 1.8.0_292) |
| Spark | 3.2.1 |
| Hive | 3.1.2 |
| Hadoop | 3.1.3 |
| Tez | 0.9.1 |
| Scala (Spark) | 2.12 |
| Queue | `benchmark` |
| HDFS root | `/benchmarks/audit_orc` |
| Scales | S=100 / M=1024 / L=5120 GiB |

Bundled YAML: `conf/` (synced from `docs/orc_hdfs_spark_hive_configs_final/`).

## Build (JDK 8 required)

```bash
export JAVA_HOME=$HOME/.local/jdk/jdk8   # or any JDK 8
export PATH=$JAVA_HOME/bin:$PATH
./gradlew clean test shadowJar
# → build/libs/orc-bench-0.1.0-SNAPSHOT-all.jar
```

Helper: `./scripts/with-jdk8.sh ./gradlew test shadowJar`

Build fails if Gradle runs on a non-Java-8 JVM (`verifyJava8` task).

## Run / results

Подробная инструкция (фазы, CLI, примеры, разбор отчётов):

**[docs/Запуск orc-bench и получение результатов.md](docs/Запуск%20orc-bench%20и%20получение%20результатов.md)**

Кратко:

```bash
./scripts/with-jdk8.sh ./scripts/build.sh
./scripts/run-preflight.sh --dry-run
./scripts/run-benchmark.sh --scale S --phase all --env-override conf/environment.local.yaml
```

Отчёты: `results/<runId>/` → `SUMMARY.txt`, `summary.csv`, `metrics.jsonl`, `skipped.tsv`.

Скрипты (все параметры с русскими комментариями): `scripts/with-jdk8.sh`, `build.sh`, `run-benchmark.sh`, `run-preflight.sh`.
