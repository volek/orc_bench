# orc-bench

Оркестратор бенчмарка на Java + Gradle для ORC / HDFS / Spark 3.2 / Hive (Tez и LLAP).

**Статус:** чекпоинты 0–10 **(A)** — локальная реализация — выполнены (фазы CLI до `report`/`final`, скрипты кластерного запуска, packaging, runbook, Pilot S: `conf/pilot-s.yaml`, `scripts/run-pilot-s.sh`). Live-валидация на кластере **(B)** для CP3–CP10 — только на ноде Hadoop. Локальный трек (A) по плану завершён.

> **С этой машины разработки нельзя выполнять действия на Hadoop-кластере** (live Kerberos, HDFS `hdfs://orc_test`, `spark-submit` на YARN, Hive JDBC, реальные generate/run). Здесь — код, unit/mock-тесты, упаковка, `--dry-run-submit` у скриптов. Кластерные шаги — только на ноде. См. план §7.0 и runbook.

## Документация

| Документ | Назначение |
|---|---|
| [`docs/План разработки тестового приложения ORC HDFS Spark Hive.md`](docs/План%20разработки%20тестового%20приложения%20ORC%20HDFS%20Spark%20Hive.md) | План и gated-чекпоинты (A локально / B на кластере) |
| [`docs/Запуск на Hadoop кластере.md`](docs/Запуск%20на%20Hadoop%20кластере.md) | **Операционный runbook** — копирование, фазы, CLI, spark-submit, отчёты, сбор results |
| [`docs/orc_hdfs_spark_hive_configs_final/`](docs/orc_hdfs_spark_hive_configs_final/) | Контракт конфигов (`hdfs_root=hdfs://orc_test`, очередь `default`) |

## Сборка (локально / CI)

```bash
./scripts/build.sh
# или: ./gradlew clean test shadowJar
```

Артефакт: `build/libs/orc-bench-0.1.0-SNAPSHOT-all.jar`  
Main-Class: `orcbench.Main`

Упаковка для копирования на ноду (без подключения к кластеру):

```bash
./scripts/package-for-cluster.sh
# → dist/orc-bench-cluster-bundle.tgz
./scripts/package-for-cluster.sh --with-runbook
```

Релизный `dist/orc-bench-cluster-bundle.tgz` хранится в репо (пересборка — тем же скриптом). Остальное под `dist/` в `.gitignore`.

## CLI (локальный smoke — без живого кластера)

```bash
java -cp build/libs/orc-bench-0.1.0-SNAPSHOT-all.jar orcbench.Main --help

# Capabilities / skip-матрица по конфигу, без live HDFS/JDBC
java -cp build/libs/orc-bench-0.1.0-SNAPSHOT-all.jar orcbench.Main \
  --config conf --scale S --phase preflight --dry-run --results-dir results

# Smoke скрипта (печатает строку spark-submit; submit не выполняется)
./scripts/run-benchmark.sh --dry-run-submit --phase preflight --scale S
```

## Нода кластера (нужна для live-проб и прогонов)

После копирования jar + `conf/` + `scripts/` на ноду — см. [runbook](docs/Запуск%20на%20Hadoop%20кластере.md). Kerberos-тикет и principal должны уже быть доступны на ноде:

```bash
./scripts/run-preflight.sh --env-override conf/environment.local.yaml
./scripts/run-preflight.sh --live --env-override conf/environment.local.yaml

# Live generate → hdfs://orc_test/S/RAW_ARRAY_BASE/ (+ refs/, manifest/)
./scripts/run-benchmark.sh \
  --jar ./orc-bench-0.1.0-SNAPSHOT-all.jar \
  --scale S --phase generate \
  --env-override conf/environment.local.yaml
```

По умолчанию `spark-submit`: `--deploy-mode cluster`, `--queue default`, executors `10×4×16g`. Режим client отключён: скрипты его не принимают.

Runtime-контракт: `conf/` (копия `docs/orc_hdfs_spark_hive_configs_final/`).
