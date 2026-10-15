# Запуск orc-bench и получение результатов

Подробная инструкция по сборке fat jar, запуску на ноде SDP-кластера и разбору артефактов benchmark.

Контракт окружения и матрицы экспериментов: [`orc_hdfs_spark_hive_configs_final/`](orc_hdfs_spark_hive_configs_final/).

---

## 1. Требования

| Компонент | Значение (из `environment.yaml`) |
|---|---|
| Java | **1.8** (на кластере: `1.8.0_292`) |
| Spark | 3.2.1 |
| Hive | 3.1.2 |
| Hadoop | 3.1.3 |
| Tez | 0.9.1 |
| Очередь YARN | `benchmark` |
| HDFS root | `/benchmarks/audit_orc` |
| Масштабы датасета | S=100 GiB, M=1024 GiB, L=5120 GiB |

Запуск выполняется **на одной из нод** Hadoop/SDP-кластера через `spark-submit` (driver в `client`-режиме оркестрирует Spark и Hive JDBC).

Права: приложение **не меняет** cluster-wide конфиги (`permission_to_change_cluster_config: false`) и **не сбрасывает** кэши (`permission_to_reset_caches: false`). Эксперименты без capability помечаются `skipped`.

---

## 2. Подготовка

### 2.1. Сборка fat jar (JDK 8)

На машине сборки нужна JDK 8:

```bash
export JAVA_HOME=/path/to/jdk8
export PATH=$JAVA_HOME/bin:$PATH

# или через вспомогательный скрипт:
./scripts/with-jdk8.sh ./scripts/build.sh
```

Артефакт:

```text
build/libs/orc-bench-0.1.0-SNAPSHOT-all.jar
```

### 2.2. Копирование на ноду кластера

Скопируйте на ноду (пример):

```bash
scp build/libs/orc-bench-0.1.0-SNAPSHOT-all.jar user@node:/opt/orc-bench/
scp -r conf user@node:/opt/orc-bench/
scp scripts/run-benchmark.sh user@node:/opt/orc-bench/scripts/
```

На ноде должны быть доступны `spark-submit`, `hadoop`, Java 8 и сеть до ZooKeeper HS2 / YARN.

### 2.3. Локальный override окружения

Скопируйте шаблон и заполните `null` из `environment.yaml`:

```bash
cp conf/environment.local.yaml.example conf/environment.local.yaml
# отредактируйте namenode_uri, executor_*, llap_*, free_space_gib и т.д.
```

Передаётся флагом `--env-override conf/environment.local.yaml`.

---

## 3. Фазы pipeline

| Phase | Что делает |
|---|---|
| `preflight` | Проверки версий/HDFS/JDBC/capability; пишет отчёт; **не** генерирует данные |
| `generate` | Синтетический канонический ORC `RAW_ARRAY_BASE` на HDFS |
| `materialize` | OVAT-перезаписи layout’ов (sort/bloom/stripe/compression/…) |
| `run` | Query suite на Spark и Hive/Tez/LLAP + concurrency |
| `final` | Сравнение `BEST_ORC` × профили SPARK/HIVE/LLAP |
| `all` | Все фазы подряд |

Рекомендуемый порядок первого прогона на кластере:

```text
preflight (dry-run) → preflight → generate (scale=S) → materialize → run → final
```

---

## 4. Параметры CLI приложения

Все флаги передаются **после** jar в `spark-submit` (или через `scripts/run-benchmark.sh`).

| Флаг | По умолчанию | Описание |
|---|---|---|
| `--config` | `conf` | Каталог с YAML-контрактом (`benchmark.yaml`, `dataset.yaml`, …) |
| `--scale` | `S` | Масштаб: `S` / `M` / `L` (100 / 1024 / 5120 GiB) |
| `--phase` | `all` | Фаза: `preflight\|generate\|materialize\|run\|final\|all` |
| `--experiments` | *(все)* | Фильтр ID экспериментов через запятую, напр. `BLOOM_USER,ORC_PUSHDOWN` |
| `--engines` | `spark,hive_tez,hive_llap` | Движки для engine-матрицы |
| `--env-override` | — | YAML-overlay для null-полей environment |
| `--retain` | — | ID layout’ов, которые оставить при `--cleanup` |
| `--cleanup` | off | Удалить на HDFS layout’ы, не входящие в `--retain` |
| `--results-dir` | `results` | Локальный каталог отчётов на ноде |
| `--dry-run` | off | Развернуть матрицу/пробы без Spark/Hive job’ов |
| `--help` | — | Справка picocli |

---

## 5. Примеры запуска

### 5.1. Preflight (без нагрузки)

```bash
./scripts/run-benchmark.sh \
  --jar build/libs/orc-bench-0.1.0-SNAPSHOT-all.jar \
  --scale S \
  --phase preflight \
  --dry-run \
  --env-override conf/environment.local.yaml
```

### 5.2. Генерация датасета S (100 GiB)

```bash
./scripts/run-benchmark.sh \
  --jar build/libs/orc-bench-0.1.0-SNAPSHOT-all.jar \
  --scale S \
  --phase generate \
  --env-override conf/environment.local.yaml
```

Данные появятся в HDFS:

```text
/benchmarks/audit_orc/S/RAW_ARRAY_BASE/
/benchmarks/audit_orc/S/refs/
/benchmarks/audit_orc/S/manifest/
```

### 5.3. Materialize + cleanup

```bash
./scripts/run-benchmark.sh \
  --jar build/libs/orc-bench-0.1.0-SNAPSHOT-all.jar \
  --scale S \
  --phase materialize \
  --experiments COMPRESSION,BLOOM_USER,PARTITIONING,MINMAX_SORT \
  --cleanup \
  --retain RAW_ARRAY_BASE \
  --env-override conf/environment.local.yaml
```

### 5.4. Полный прогон

```bash
./scripts/run-benchmark.sh \
  --jar build/libs/orc-bench-0.1.0-SNAPSHOT-all.jar \
  --scale S \
  --phase all \
  --engines spark,hive_tez,hive_llap \
  --env-override conf/environment.local.yaml \
  --results-dir results
```

### 5.5. Прямой spark-submit (без обёртки)

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --queue benchmark \
  --class orcbench.Main \
  build/libs/orc-bench-0.1.0-SNAPSHOT-all.jar \
  --config conf \
  --scale S \
  --phase preflight \
  --env-override conf/environment.local.yaml
```

---

## 6. Где лежат результаты

После запуска создаётся каталог:

```text
results/<runId>/
  SUMMARY.txt      # сводка runId, число строк, skipped, bestOrc, capabilities
  summary.csv      # агрегаты: avg / p50 / p95 / p99 / SLA / scanRatio
  metrics.jsonl    # каждая измеренная попытка (warmup и measured)
  skipped.tsv      # эксперименты, пропущенные из-за null/capability
```

`runId` печатается в stdout в начале:

```text
[orc-bench] runId=2026-10-15T11-22-01.123Z_ab12cd34 scale=S phase=all
[report] wrote .../results/2026-10-15T11-22-01.123Z_ab12cd34
```

### 6.1. Чтение summary.csv

Колонки:

| Колонка | Смысл |
|---|---|
| `experimentId` / `variantLabel` | Эксперимент и вариант OVAT |
| `engine` | `spark` / `hive_tez` / `hive_llap` |
| `queryId` | ID из `query-suite.yaml` |
| `concurrency` | Уровень параллелизма |
| `cacheState` | `cold` / `warm` / … (не усреднять вместе) |
| `n` | Число measured-повторов |
| `avgMs`, `p50Ms`, `p95Ms`, `p99Ms` | Латентность |
| `slaSuccessRate` | Доля запросов с latency ≤ 3000 ms |
| `scanRatio` | HDFS bytes read / ORC file bytes (если доступно) |
| `resultHash` | Хэш результата для equivalence |

SLA из контракта: **среднее ≤ 3000 ms**; дополнительно смотрите `slaSuccessRate` и p95/p99.

### 6.2. metrics.jsonl

По одной JSON-строке на попытку. Поля включают `latencyMs`, `status` (`ok`/`error`/`skipped`/`dry_run`), `resultHash`, `logicalRows`, `orcFileBytes`, `enginePlan`, `errorMessage`.

Пример выборки ошибок:

```bash
grep '"status":"error"' results/<runId>/metrics.jsonl | head
```

### 6.3. skipped.tsv

```text
SHORT_CIRCUIT/true<TAB>missing capabilities: hdfs_short_circuit_supported,...
HIVE_LLAP/cold<TAB>missing capabilities: llap_daemons_running,...
```

Заполните недостающие поля в `environment.local.yaml` и перезапустите нужную фазу.

### 6.4. HDFS-данные

| Путь | Содержимое |
|---|---|
| `/benchmarks/audit_orc/<scale>/RAW_ARRAY_BASE/` | Базовый ORC |
| `/benchmarks/audit_orc/<scale>/<EXPERIMENT>_…/` | OVAT layout’ы |
| `/benchmarks/audit_orc/<scale>/refs/` | Справочники для JOIN |
| `generated/hive-table.sql` | DDL stub (локально на ноде) |

Проверка:

```bash
hdfs dfs -du -h /benchmarks/audit_orc/S
hdfs dfs -ls /benchmarks/audit_orc/S/RAW_ARRAY_BASE | head
```

---

## 7. Скрипты в `scripts/`

| Скрипт | Назначение |
|---|---|
| [`with-jdk8.sh`](../scripts/with-jdk8.sh) | Выполнить команду под JDK 8 (`JAVA_HOME`) |
| [`build.sh`](../scripts/build.sh) | `clean` + `test` + `shadowJar` |
| [`run-benchmark.sh`](../scripts/run-benchmark.sh) | Основной `spark-submit` на кластере |
| [`run-preflight.sh`](../scripts/run-preflight.sh) | Быстрый preflight (+ optional dry-run) |

У всех скриптов параметры задокументированы **русскими комментариями** в шапке и у каждого флага.

---

## 8. Типовые проблемы

| Симптом | Что проверить |
|---|---|
| `Build must run on Java 8` | `JAVA_HOME` указывает на JDK 8; `./scripts/with-jdk8.sh …` |
| Hive JDBC fail | ZooKeeper URL в `environment.yaml` / override; сеть с ноды |
| Много `skipped` для LLAP | `llap_daemons_running_verified` / live daemons |
| HDFS capacity | `free_space_gib` vs scale (особенно M/L) |
| OOM / долгий generate | Уменьшить scale до `S`; настроить executor_* в override |
| Нет `summary.csv` | Смотрите stderr; фаза могла упасть до `ReportWriter` |

---

## 9. Минимальный чеклист первого дня на кластере

1. Собрать jar на JDK 8.  
2. Скопировать jar + `conf/` на ноду.  
3. Заполнить `conf/environment.local.yaml`.  
4. `./scripts/run-preflight.sh --dry-run` → затем без dry-run.  
5. `phase=generate --scale S`.  
6. `phase=materialize` с узким `--experiments`.  
7. `phase=run` → открыть `results/<runId>/summary.csv`.  
8. При необходимости `phase=final` и сравнить engines на `BEST_ORC`.
