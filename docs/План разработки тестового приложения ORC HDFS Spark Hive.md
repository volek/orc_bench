# План разработки тестового приложения ORC / HDFS / Spark / Hive

| Поле | Значение |
|---|---|
| Статус | **Checkpoint 10 (A: local/pilot prep) выполнен** (2026-10-19) — узкий Pilot S (`conf/pilot-s.yaml`, `scripts/run-pilot-s.sh`) + чеклист калибровки executor/concurrency (runbook §12); CP0–CP9 (A) ранее; **локальный трек (A) по плану завершён**. **(B: cluster)** Pilot S trial + финализация sizing/concurrency — pending на ноде; CP3–CP9 (B) тоже pending |
| Дата | 2026-10-19 |
| Целевой артефакт | Java-приложение (Gradle), запуск через `spark-submit` на ноде SDP-кластера |
| Источники истины | Каталог `docs/orc_hdfs_spark_hive_configs_final/` (первичный контракт); §1 документа `ORC + HDFS + Spark + Hive benchmark.md` |
| Реализация | **Greenfield** — писать с нуля по этому плану (не восстанавливать из git) |
| Ограничение host | **Эта машина разработки НЕ может выполнять действия, предназначенные для Hadoop-кластера** (HDFS, Kerberos live, `spark-submit` на YARN, Hive JDBC, реальные прогоны). См. §7.0 и runbook [`Запуск на Hadoop кластере.md`](Запуск%20на%20Hadoop%20кластере.md) |

---

## 1. Цель и границы

### 1.1. Цель

Разработать тестовое (benchmark) приложение на **Java + Gradle**, которое:

1. Читает контракт конфигурации из YAML/схемы в `docs/orc_hdfs_spark_hive_configs_final/`.
2. Генерирует и материализует на HDFS физические layout’ы ORC (OVAT — one variable at a time).
3. Выполняет фиксированный query suite на **Spark 3.2** и **Hive (Tez / LLAP)** согласно матрице экспериментов.
4. Собирает метрики (latency, scan ratio, HDFS bytes, result hash и др.) и пишет отчёты для проверки вариантов из **§1** benchmark-документа.
5. Запускается **на одной из нод** Hadoop/SDP-кластера через `spark-submit`.

### 1.2. Scope (входит)

- Fat JAR (shadow), CLI фаз: `preflight` → `generate` → `materialize` → `run` → `final`.
- Потребление **всех** файлов `orc_hdfs_spark_hive_configs_final` как первичного контракта.
- Покрытие физических и engine-экспериментов из `benchmark.yaml`, соответствующих карте §1.
- Kerberos-аутентификация (principal/keytab из `environment.yaml`).
- Локальные отчёты на ноде + layout’ы под `hdfs_root` (`/orc_test`).
- Greenfield-реализация Gradle Java-проекта по структуре §6.

### 1.3. Out of scope / non-goals

- Изменение cluster-wide конфигов и сброс кэшей (`permission_to_change_cluster_config: false`, `permission_to_reset_caches: false`).
- Реальные персональные данные (`prohibited_data: real_personal_data`).
- Автоматический rewrite Materialized Views (в среде `materialized_views_supported: false` — эксперимент `HIVE_MATERIALIZED_VIEW` / Q18 → **ожидаемый skip**, подтверждено).
- Centralized HDFS Cache как обязательный прогон (`Configured Cache Capacity = 0` → `HDFS_CACHE.centralized` → **ожидаемый skip**, подтверждено).
- Параллельная разработка «альтернативного» стека (CarbonData и т.п.).
- Восстановление/доработка старого кода из git-истории — не используется; только greenfield.
- **Любая реализация кода до явной команды пользователя** (см. §2).

---

## 2. Ограничения процесса (обязательные правила)

> **Разработка кода НЕ начинается без явной инструкции пользователя.**  
> Настоящий документ — только план. Создание Gradle-проекта, исходников, скриптов и тестов допускается лишь после отдельного подтверждения пользователя на старт реализации.

> **Работа ведётся строго по пунктам (checkpoint’ам).**  
> Переход к следующему пункту плана — **только после подтверждения пользователем**, что предыдущий пункт принят (или явно отложен/изменён). Нельзя «забегать вперёд» и закрывать несколько checkpoint’ов за один шаг без согласия.

Дополнительные правила:

1. При конфликте текстов ориентироваться в первую очередь на **`orc_hdfs_spark_hive_configs_final`**, затем на §1–метрики benchmark-дока. Операционный запуск, CLI и отчёты — [`Запуск на Hadoop кластере.md`](Запуск%20на%20Hadoop%20кластере.md).
2. Недоступные capability → эксперимент `skipped`, а не fail (если `fail_if_required_capability_missing: false`).
3. Не смешивать несколько факторов layout’а в одном измерении (изоляция OVAT из `benchmark.isolation`).
4. **Этот host разработки не выполняет cluster-side действия** (§7.0). Критерии приёмки checkpoint на этом host = только часть **(A)**; часть **(B)** — gated на ноде.

---

## 3. Источники истины

| Приоритет | Источник | Роль |
|:---:|---|---|
| 1 | `docs/orc_hdfs_spark_hive_configs_final/*.yaml` + `schema.orc.txt` | **Первичный контракт** runner’а: среда, датасет, эксперименты, SQL, схема |
| 2 | `docs/ORC + HDFS + Spark + Hive benchmark.md` §1 | Карта вариантов «что проверяем» и приоритеты |
| 3 | Тот же документ §2+ | Метрики, SLA, query intent, методология OVAT (масштабы датасета — по `dataset.yaml` / решению §10, не по устаревшим цифрам §4) |
| 4 | `docs/Запуск на Hadoop кластере.md` | **Операционный runbook**: prereq, copy, фазы, CLI (§13), spark-submit, артефакты отчётов (§8), troubleshooting |

План ориентирован на **configs_final**. Реализация — **greenfield** (новый Gradle Java-проект по §6), без восстановления исходников из git.

---

## 4. Сводка вариантов из §1, которые должно покрыть приложение

Карта из §1 benchmark-документа → эксперименты / профили в `benchmark.yaml`:

| Уровень | Механизм (§1) | Приоритет §1 | Реализация в приложении (`benchmark.yaml`) |
|---|---|--:|---|
| ORC | Column pruning | очень высокий | `ORC_COLUMN_PRUNING` (Q07_NARROW vs Q07_WIDE) |
| ORC | Predicate pushdown | очень высокий | `ORC_PUSHDOWN` (`spark.sql.orc.filterPushdown` false/true; Q02/Q03/Q05/Q06) |
| ORC | min/max statistics | очень высокий | Базовый index ORC + `MINMAX_SORT` (сортировка усиливает skipping) |
| ORC | Bloom Filter (`=`, `IN`) | высокий | `BLOOM_USER`, `BLOOM_FPP`, `NESTED_SCALAR_BLOOM` |
| ORC | `row.index.stride` | средний/высокий | `ROW_INDEX` (5000 / 10000 / 20000 / 50000) |
| ORC | Сортировка данных | очень высокий | `MINMAX_SORT`, `NESTED_SCALAR_SORT` |
| ORC | `stripe.size` | средний | `STRIPE_SIZE` (64 / 128 / 256 MiB) |
| ORC | compression | средний | `COMPRESSION` (ZLIB / SNAPPY / **ZSTD для Spark**; ZSTD исключён из BEST_ORC для Hive до поддержки codec в Hive ORC 1.5.6 — см. §10.1) |
| HDFS/Table | Partitioning | очень высокий | `PARTITIONING`, `DATE_PARTITION_QUERY` (Q01_PARTITIONED, Q06_PARTITIONED) |
| HDFS/Table | Bucketing | выборочно высокий | `BUCKETING` (0 / 32, key `epk_id`, HYBRID; Q11/Q17) |
| HDFS | Compaction / small files | высокий | `FILE_SIZES` (32 / 256 / 1024 MiB) |
| HDFS | Short-circuit read | средний/высокий | `SHORT_CIRCUIT` — только observational (флаг **не mutable** runner’ом; подтверждено) |
| HDFS | Centralized Cache | высокий | `HDFS_CACHE` (cold / naturally_warm / centralized → **centralized = ожидаемый skip**; подтверждено) |
| Spark 3.2 | Native/vectorized ORC | высокий | `SPARK_ORC_READER`, `SPARK_VECTORIZATION`, `SPARK_NESTED_VECTORIZATION` |
| Spark 3.2 | AQE | высокий | `SPARK_AQE` |
| Spark 3.2 | DPP | высокий | `SPARK_DPP` + Q11_DPP |
| Spark 3.2 | CBO/statistics | средний/высокий | `SPARK_CBO` (+ `ANALYZE` в `SPARK_OPTIMIZED`) |
| Spark 3.2 | Broadcast Join | высокий | `SPARK_BROADCAST` |
| Hive | Vectorized execution | высокий | `HIVE_VECTORIZATION` |
| Hive | CBO/statistics | высокий | `HIVE_CBO` |
| Hive | Tez | высокий | `HIVE_TEZ_BASE` (`hive.llap.execution.mode=none`) |
| Hive ≥2.0 | LLAP | очень высокий | `HIVE_LLAP`, `HIVE_LLAP_CACHE`, профили `HIVE_LLAP_COLD/WARM` — **в плане**; готовность **валидируется на реальном кластере** и при необходимости корректируется |
| Hive | Materialized Views | высокий для OLAP | `HIVE_MATERIALIZED_VIEW` → **ожидаемый skip** (подтверждено) |

Дополнительно (есть в configs, усиливает §1):

- Представления nested-данных: `NESTED_REPRESENTATIONS` (RAW_ARRAY / MAP / FLAT / HYBRID) + dialect-specific NP_* запросы.
- Финальное сравнение: `final_comparison` на `BEST_ORC` × engine-профили.

---

## 5. Инвентарь конфигов `orc_hdfs_spark_hive_configs_final` и потребление приложением

### 5.1. Файлы

| Файл | Назначение | Как потребляет приложение |
|---|---|---|
| `environment.yaml` | Факты кластера: Kerberos, версии, HDFS, YARN, Spark, Hive/LLAP, preflight, operations | Preflight-пробы; параметры `spark-submit`; JDBC URL; `hdfs.work_dir`; skip-правила; стартовый executor sizing |
| `benchmark.yaml` | SLA, HDFS root, baseline ORC, representations, physical/engine experiments, metrics, runs, result equivalence | Разворачивание матрицы экспериментов; writer settings; engine profiles; отчётность; кандидаты concurrency (фактический набор — capacity-aware) |
| `dataset.yaml` | Синтетика: seed, scales, колонки, parameters catalog, reference datasets, query samples | Генератор ORC; справочники JOIN; подстановка плейсхолдеров в SQL |
| `query-suite.yaml` | SQL-шаблоны Q01…Q18 + NP_*; dialects; representation_rewrites | Spark/Hive runners; rewrite запросов под representation |
| `schema.orc.txt` | Каноническая ORC-схема RAW_ARRAY | Writer schema; result equivalence projection |

### 5.2. Ключевые knobs из конфигов (ориентир для кода)

**Окружение (`environment.yaml`):**

- Java **1.8**, Spark **3.2.1**, Hive **3.1.2**, Hadoop **3.1.3**, Tez **0.9.1**, Scala **2.12**.
- Kerberos: `pvm-omltp-sdp@OPSMON.SBT`, keytab `~/keytab/pvm-omltp-sdp.keytab`.
- HDFS: `hdfs://sdp-dzo-lt-omlt`, replication **2**, block **128 MiB**, work/root **`/orc_test`**.
- YARN: queue **`default`**, RM HA (namenode-01/02); Effective_Max_Capacity (ориентир первого запуска): **memory 221184 MB (~216 GiB), vCores 67**; **6 datanodes**.
- Spark: `master: yarn`, **`deploy_mode: cluster` только**. Скрипты запуска отклоняют `client`.
- Executor sizing: стартовые значения для первого пробного запуска — см. §8.1; **финальный sizing — после первого trial run**.
- Hive JDBC: ZooKeeper discovery → HS2; LLAP app `llap0`, mode `only`; MV не поддерживаются.
- Short-circuit: supported+enabled, **не mutable** runner’ом (подтверждено).
- Preflight: exact versions, HDFS capacity, feature probe; skip unavailable experiments.
- LLAP: эксперименты остаются в матрице; readiness подтверждается probe’ами на реальном кластере и может быть скорректирована.

**Benchmark (`benchmark.yaml`):**

- `hdfs_root: /orc_test`, SLA **3000 ms**, `warmup_per_query: 5`, `measured_per_query: 30`.
- Concurrency: **не фиксированный MVP-набор**. В YAML могут быть кандидаты уровней; фактические уровни concurrency **настраиваются per dataset scale с учётом ёмкости кластера** (см. §8.2). Не запускать тяжёлые уровни «вслепую» на L без capacity-оценки.
- Physical baseline `RAW_ARRAY_BASE`: SNAPPY, stripe 64 MiB, stride 10000, без bloom/sort/partitions.
- Hive DB `bench`, таблицы `bench.events__{scale}__{experiment}__{variant}`.
- Метрики required: `latency_ms`, `status`, `result_hash`, `logical_rows`, `orc_file_bytes`, `hdfs_bytes_read`, `engine_plan`.
- ZSTD: кандидат compression для Spark; исключён из BEST_ORC на стороне Hive (ORC 1.5.6 codec gap); задел на включение ZSTD для Hive позже.

**Dataset (`dataset.yaml`):**

- Scales (логические GiB / GB по `size_method`): **S=100 / M=500 / L=1024**.
- Seed `123456789`, algorithm `audit_synth_v1`, timestamp range январь 2026 UTC.
- Derived partitions: `event_date = DATE(__time)`, `event_hour = HOUR(__time)`.

**Query suite:** плейсхолдеры `${table}`, `${user_login}`, `${epk_id}`, …; dialects `spark_3_2` / `hive_tez`.

### 5.3. Загрузка конфигов в runtime

Предлагаемый контракт CLI (согласован с документом запуска; детали submit — §8):

```text
--config <dir>          # по умолчанию conf/; на кластере — копия configs_final или symlink
--env-override <yaml>   # overlay для null-полей environment (executor_* и прочие факты ноды)
--scale S|M|L           # 100 / 500 / 1024 GB
--phase preflight|generate|materialize|run|final|all
--experiments <id,...>
--engines spark,hive_tez,hive_llap
--results-dir results
--dry-run
```

Источник по умолчанию для разработки: скопировать/синхронизировать `docs/orc_hdfs_spark_hive_configs_final/` → `conf/` (или читать напрямую с путём `--config docs/orc_hdfs_spark_hive_configs_final`).

---

## 6. Предлагаемая структура проекта (Gradle Java Spark job)

Greenfield-скелет (создаётся только после явного старта Checkpoint 1):

```text
orc_bench/
├── build.gradle / settings.gradle   # Java 8, Shadow JAR, Spark/Hive provided|compileOnly где уместно
├── gradle/wrapper/
├── conf/                            # runtime-копия configs_final (+ environment.local.yaml)
├── scripts/
│   ├── with-jdk8.sh                 # local/CI: JDK 8 wrapper
│   ├── build.sh                     # local/CI: shadowJar
│   ├── package-for-cluster.sh       # local: tarball JAR+conf+scripts для копирования на ноду
│   ├── kinit-bench.sh               # cluster-side: Kerberos из environment.yaml
│   ├── run-benchmark.sh             # cluster-side: spark-submit (deploy_mode=cluster)
│   ├── run-preflight.sh             # cluster-side: phase=preflight
│   ├── spark-submit-orc-bench.sh    # cluster-side: низкоуровневый spark-submit
│   └── fetch-results.sh             # cluster-side helper / подсказка копирования results
├── src/main/java/orcbench/
│   ├── Main.java                    # CLI (picocli или аналог)
│   ├── config/                      # YAML load + merge override
│   ├── preflight/                   # версии, HDFS, JDBC, capability matrix
│   ├── generate/                    # синтетика RAW_ARRAY + refs
│   ├── layout/                      # ExperimentExpander + LayoutMaterializer (OVAT rewrite)
│   ├── engine/spark/                # SparkQueryRunner + SparkConf из profiles
│   ├── engine/hive/                 # HiveJdbcRunner, SchemaDdl
│   ├── metrics/                     # MetricsStore
│   └── report/                      # SUMMARY.txt, summary.csv, metrics.jsonl, skipped.tsv
└── src/test/java/orcbench/          # unit: config expand, templates, generators, reports
```

Артефакт сборки (целевой):

```text
build/libs/orc-bench-0.1.0-SNAPSHOT-all.jar
Main-Class: orcbench.Main
```

Зависимости (ориентир по versions из environment): Spark 3.2.x / Scala 2.12 API для job’а; Hive JDBC; Hadoop client; ORC writer **1.6.12** (как `orc_generator`); SnakeYAML/Jackson YAML; JUnit для тестов. На кластере предпочитать `provided`/`compileOnly` для Spark/Hadoop, чтобы не дублировать jars дистрибутива SDP.

---

## 7. Поэтапный план реализации (checkpoint’ы с подтверждением)

Каждый пункт ниже — **отдельный checkpoint**. После выполнения пункта — демонстрация результата пользователю; **следующий пункт только после явного «ок / дальше»**.

### 7.0. Правило среды: эта машина ≠ Hadoop-кластер

> **На этом host разработки невозможно выполнять действия, предназначенные для Hadoop-кластера.**  
> Сюда относятся: live Kerberos/`kinit` против кластерного KDC, запись/чтение HDFS (`/orc_test`), `spark-submit` на YARN, Hive JDBC к HS2, реальные фазы generate/materialize/run/final, сбор «живых» метрик с кластера.

Поэтому **каждый checkpoint** разделён на:

| Часть | Где | Что входит | Влияет на приёмку checkpoint на этом host? |
|---|---|---|---|
| **(A) Local** | Dev-машина / CI | Код, unit/mock-тесты, packaging (fat JAR), документация, скрипты как артефакты | **Да** — критерии приёмки checkpoint на этом host = только (A) |
| **(B) Cluster** | Нода Hadoop/SDP | Preflight live, HDFS, spark-submit, Hive JDBC, реальные прогоны, сбор results с ноды | **Нет** для закрытия (A); (B) — **gated** отдельной валидацией на ноде (см. runbook) |

Операционный порядок на ноде: [`docs/Запуск на Hadoop кластере.md`](Запуск%20на%20Hadoop%20кластере.md). Скрипты в `scripts/` с пометкой «cluster-side» рассчитаны на запуск **только на ноде**.

---

### Checkpoint 0 — Утверждение плана ✅

**(A) Local — критерии приёмки на этом host**

- [x] Согласовать этот документ, resolved decisions (§10.1) и приоритет MVP (рекомендуется: Spark physical OVAT на scale S → затем Hive).
- [x] Зафиксировать ограничение §7.0 (dev host ≠ cluster).

**(B) Cluster — gated (не требуется для CP0)**

- Нет кластерных действий.

**Статус:** (A) принято 2026-10-19. Код не писался (по определению CP0).  
**Стоп:** следующий шаг — только после команды «начинай Checkpoint 1» — получено.

---

### Checkpoint 1 — Каркас Gradle + CLI skeleton ✅

**(A) Local — критерии приёмки на этом host**

- [x] JDK 8 (bytecode major 52 / toolchain), Shadow JAR, `Main` с разбором флагов и `--help`.
- [x] Копирование `configs_final` → `conf/` (+ `environment.local.yaml.example`).
- [x] Unit-тесты загрузки YAML **без** кластера (`ConfigLoaderTest`, `YamlSupportTest`).
- [x] `./gradlew clean test shadowJar` — BUILD SUCCESSFUL; артефакт `build/libs/orc-bench-0.1.0-SNAPSHOT-all.jar`.

**(B) Cluster — gated (не входит в приёмку CP1 на этом host)**

- Копирование JAR/`conf/` на ноду; smoke `--help` под кластерным JDK 8 — по желанию, на ноде.

**Статус:** (A) выполнено 2026-10-19.  
**Стоп:** подтверждение перед Checkpoint 2 — получено.

---

### Checkpoint 2 — Config model + ExperimentExpander ✅

**(A) Local — критерии приёмки на этом host**

- [x] Модели Java под YAML (benchmark / dataset / environment / query-suite).
- [x] Разворачивание physical/engine experiments в runnable cells.
- [x] Capacity-aware concurrency per scale (конфиг + оценка YARN из YAML, без live YARN API).
- [x] Dry-run печать плана экспериментов; unit-тесты CP2.

**(B) Cluster — gated**

- Сверка capacity-чисел с актуальным Effective_Max_Capacity на ноде — при первом cluster dry-run (не блокирует (A)).

**Статус:** (A) выполнено 2026-10-19.  
**Стоп:** подтверждение перед Checkpoint 3 — получено.

---

### Checkpoint 3 — Preflight

**(A) Local — критерии приёмки на этом host**

- [x] Код проб (`PreflightProbe`, injectable backend / mocks), skip-matrix, `--phase preflight`.
- [x] Unit-тесты на fakes; `./gradlew test shadowJar` локально.
- [x] Скрипты/доки помечают live preflight как **только на ноде** (см. runbook).

**(B) Cluster — gated (обязательная валидация на ноде; не с этого host)**

- [ ] На ноде: Kerberos-контекст уже доступен → `./scripts/run-preflight.sh` (dry-run, затем live).
- [ ] Отчёт capabilities / skip-matrix согласован с ожидаемыми skips (MV, `HDFS_CACHE.centralized`, LLAP по факту probe).
- Проверки версий, HDFS writable `/orc_test`, Kerberos, Hive JDBC, LLAP observational — **только на ноде**.

**Статус:** (A) выполнено 2026-10-19. (B) pending — кластер с этого host недоступен.  
**Стоп:** не начинать Checkpoint 4 без подтверждения пользователя (и желательно после (B) или явного «отложить cluster validation»).

---

### Checkpoint 4 — Dataset generate (scale S = 100 GB) ✅ (A)

**(A) Local — критерии приёмки на этом host**

- [x] Реализация синтетического ORC writer по `schema.orc.txt` + `dataset.yaml` (`DatasetGenerator`, `SynthGenerators`, `EventRow`; live sink — `SparkOrcGenerateBackend`).
- [x] Unit/mock-тесты: alignment arrays, null semantics, row hash sample (без записи на HDFS) — `DatasetGeneratorTest`, `SynthGeneratorsTest`; локальные пути `--dry-run` / `--mock-generate`.
- [x] Packaging: fat JAR включает generate phase; CLI `--phase generate` готов к вызову на ноде.
- [x] `./gradlew clean test shadowJar` — BUILD SUCCESSFUL (2026-10-19).
- **Приёмка (A) не требует** записи в HDFS или запуска generate на кластере.

**(B) Cluster — gated (обязательная валидация на ноде; не с этого host)**

- [ ] На ноде: generate в `{hdfs_root}/{scale}/RAW_ARRAY_BASE/`, `refs/`, `manifest/` (`hdfs_root=/orc_test`) через `spark-submit` / `scripts/run-benchmark.sh --phase generate`.
- [ ] Проверка объёма/манифеста через `hdfs dfs` на ноде.
- См. [`Запуск на Hadoop кластере.md`](Запуск%20на%20Hadoop%20кластере.md).

**Статус:** (A) выполнено 2026-10-19. (B) pending — кластер с этого host недоступен.  
**Стоп** после демонстрации (A) и подтверждения пользователя (перед (B) и/или CP5).

---

### Checkpoint 5 — Layout materialize (OVAT physical) ✅ (A)

**(A) Local — критерии приёмки на этом host**

- [x] Код rewrite-факторов: sort, bloom, fpp, stride, stripe, compression (ZSTD для Spark), partition, file size, representations, nested bloom/sort, bucketing (Hive-compatible) — `LayoutMaterializer` + `SparkMaterializeBackend` / `FakeMaterializeBackend`.
- [x] Unit-тесты логики rewrite / idempotency markers / cleanup policy (mocks FS) — `LayoutMaterializerTest`.
- [x] CLI `--phase materialize` с `--dry-run` / `--mock-materialize` (+ `--cleanup-layouts` / `--keep-layouts`).
- [x] `./gradlew clean test shadowJar` — BUILD SUCCESSFUL (2026-10-19).
- **Без** live materialize на HDFS с этого host.

**(B) Cluster — gated**

- [ ] Materialize на HDFS; markers `{hdfs_root}/state/*.done`; cleanup по `execution_policy` — только на ноде.
- См. [`Запуск на Hadoop кластере.md`](Запуск%20на%20Hadoop%20кластере.md).

**Статус:** (A) выполнено 2026-10-19. (B) pending — кластер с этого host недоступен.  
**Стоп** после (A) + подтверждения (перед CP6 и/или (B)).

---

### Checkpoint 6 — Spark query runner + engine experiments ✅ (A)

**(A) Local — критерии приёмки на этом host**

- [x] Код `SparkQueryRunner`, профили/session overrides, сбор метрик и result hash (unit/mocks где возможно) — `SparkQueryRunner` + `SparkQueryBackend` / `FakeSparkQueryBackend` / `LiveSparkQueryBackend`; `QueryTemplate`; `MetricsStore`.
- [x] Покрытие строк §1 в коде/конфиге: vectorization, AQE, DPP, CBO, broadcast, native/hive ORC (`section1Coverage` + `engine_experiments.spark` / `ORC_PUSHDOWN`).
- [x] CLI `--phase run` с `--dry-run` / `--mock-run`; `./gradlew clean test shadowJar` — BUILD SUCCESSFUL (2026-10-19).
- **Приёмка (A) не включает** `spark-submit` / YARN jobs с этого host.

**(B) Cluster — gated**

- [ ] Прогон query-suite через `spark-submit` (deploy_mode=cluster по умолчанию) на ноде; разбор планов/метрик (`results/<runId>/metrics.jsonl`).
- См. [`Запуск на Hadoop кластере.md`](Запуск%20на%20Hadoop%20кластере.md).

**Статус:** (A) выполнено 2026-10-19. (B) pending — кластер с этого host недоступен.  
**Стоп** после (A) + подтверждения (перед CP7 и/или (B)).

---

### Checkpoint 7 — Hive JDBC (Tez + LLAP) ✅ (A)

**(A) Local — критерии приёмки на этом host**

- [x] Код Hive JDBC runner, DDL helpers, Tez/LLAP профили, equivalence hash rules; unit-тесты с mocks JDBC — `HiveJdbcRunner` + `SchemaDdl` + `HiveJdbcBackend` / `FakeHiveJdbcBackend` / `LiveHiveJdbcBackend`; reuse `QueryTemplate` / `MetricsStore` / `SparkQueryRunner.hashRows`.
- [x] Skip MV (подтверждено); ZSTD для Hive не включать (codec gap 1.5.6) — `applyLocalSkipRules` / `isHiveZstdExcluded`; LLAP observational/skip-aware.
- [x] CLI `--phase run` с `--engines hive_tez` / `hive_llap` (+ spark); `--dry-run` / `--mock-run`; `./gradlew clean test shadowJar` — BUILD SUCCESSFUL (2026-10-19).
- **Без** реального JDBC к HS2 с этого host.

**(B) Cluster — gated**

- [ ] DDL `bench.*`, Tez base, LLAP cold/warm после cluster probe; live JDBC — только на ноде.
- См. [`Запуск на Hadoop кластере.md`](Запуск%20на%20Hadoop%20кластере.md).

**Статус:** (A) выполнено 2026-10-19. (B) pending — кластер с этого host недоступен.  
**Стоп** после (A) + подтверждения (перед CP8 и/или (B)).

---

### Checkpoint 8 — Reporting + final comparison ✅ (A)

**(A) Local — критерии приёмки на этом host**

- [x] Писатели отчётов: `SUMMARY.txt`, `summary.csv`, `metrics.jsonl`, `skipped.tsv` (+ `final_comparison.tsv`) — `ReportWriter` / `ReportPhase`; unit-тесты на фикстурах `fixtures/cp8/metrics.jsonl`.
- [x] Логика выбора `BEST_ORC` (`BestOrcSelector` + `best_orc_rule`: min p50 over Q02/Q03/Q07_NARROW, SLA ≥ 0.95, tie-break partitioned > sorted > baseline); **ZSTD исключён** из BEST_ORC для Hive-shared final_comparison.
- [x] CLI `--phase report` (из metrics.jsonl / `--metrics-jsonl` / `--run-id`) и `--phase final` (BEST_ORC × engine profiles); `--dry-run` локально.
- [x] `./gradlew clean test shadowJar` — BUILD SUCCESSFUL (2026-10-19).
- **Приёмка (A) не требует** полного cluster run.

**(B) Cluster — gated**

- [ ] Отчёты после реального `run`/`final` на ноде; доставка `results/<runId>/` с ноды (см. runbook).

**Статус:** (A) выполнено 2026-10-19. (B) pending — кластер с этого host недоступен.  
**Стоп** после (A) + подтверждения (перед CP9 и/или (B)).

---

### Checkpoint 9 — Скрипты кластерного запуска и dry-run на ноде ✅ (A)

**(A) Local — критерии приёмки на этом host**

- [x] Готовые/обновлённые скрипты в `scripts/` (обёртки spark-submit, preflight, kinit helper, packaging, fetch-results) + runbook [`Запуск на Hadoop кластере.md`](Запуск%20на%20Hadoop%20кластере.md).
- [x] `deploy_mode=cluster` только. Скрипты запуска отклоняют `--deploy-mode client` и `spark.deploy_mode: client`.
- [x] Документация и скрипты **не** предполагают live cluster-side с этого host; локальный smoke: `--dry-run-submit`, `package-for-cluster.sh`.
- [x] Packaging: `dist/orc-bench-cluster-bundle.tgz` (jar + conf + scripts + `NODE_README.txt`; optional `--with-runbook`).

**(B) Cluster — gated**

- [ ] Preflight dry-run → live на реальной ноде; уточнение LLAP readiness; проверка доставки results при `deploy_mode=cluster` (см. runbook §7.4).

**Статус:** (A) выполнено 2026-10-19. (B) pending — кластер с этого host недоступен.  
**Стоп** после (A) + подтверждения (и желательно (B)) перед Checkpoint 10.

---

### Checkpoint 10 — Пилотный прогон S + калибровка sizing/concurrency ✅ (A)

**(A) Local — критерии приёмки на этом host**

- [x] Узкий набор экспериментов Pilot S в конфиге/доках/скриптах: `conf/pilot-s.yaml` (+ `configs_final/pilot-s.yaml`), `scripts/run-pilot-s.sh`, runbook §6.2 — `ORC_COLUMN_PRUNING`, `ORC_PUSHDOWN`, `MINMAX_SORT`, `BLOOM_USER`, `PARTITIONING` + `SPARK_VECTORIZATION` (materialize: rewrite-трио).
- [x] Чеклист калибровки executor_* / concurrency по §8.1–§8.2 — шаблон в [`Запуск на Hadoop кластере.md`](Запуск%20на%20Hadoop%20кластере.md) §12 (заполнение **после** trial на ноде).
- [x] Стартовый sizing в conf: 10×4×16g, `deploy_mode=cluster`, concurrency start 1,5; scales 100/500/1024 GB; Spark ZSTD kept / Hive ZSTD deferred (без изменений политики).
- [x] Локальный smoke: `./scripts/run-pilot-s.sh --print` / `--dry-run-submit` (без live cluster).
- **Приёмка (A) не включает** сам пилотный прогон на кластере.

**(B) Cluster — gated (основная ценность CP10)**

- [ ] Trial на ноде: стартовый sizing §8.1 → финализация `executor_*` и concurrency для S (затем M/L отдельно); заполнить runbook §12.
- [ ] Разбор `results/<runId>/` с пользователем; решение о расширении матрицы / scale M|L.

**Статус:** (A) выполнено 2026-10-19. (B) pending — пилот на ноде; кластер с этого host недоступен.  
**Стоп** после согласования результатов (B) или явного отложения.

---


## 8. Подход к `spark-submit` / запуску на кластере (high-level)

> Подробный ops-runbook (пошагово, на русском): [`docs/Запуск на Hadoop кластере.md`](Запуск%20на%20Hadoop%20кластере.md).  
> Ниже — краткий конспект. Шаги 2–6 выполняются **на ноде кластера**, не на этом host разработки (§7.0).

1. Сборка fat JAR на JDK 8 (**локально / CI** — допустимо на этом host).
2. Копирование на ноду: JAR + `conf/` + `scripts/` (+ optional `environment.local.yaml`).
3. На ноде: Kerberos-контекст / principal уже доступны; для YARN cluster mode — `--principal`/`--keytab` из конфига (runbook §4; явный `kinit` не требуется).
4. Запуск **с ноды**:

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --queue default \
  --num-executors 10 \
  --executor-cores 4 \
  --executor-memory 16g \
  --class orcbench.Main \
  orc-bench-…-all.jar \
  --config conf \
  --scale S \
  --phase preflight \
  --env-override conf/environment.local.yaml
```

5. Driver/AM оркестрирует фазы; Spark-jobs — на YARN; Hive — через JDBC к HS2 (ZooKeeper).
6. Результаты драйвера — в `yarn logs` (§7.4 runbook). Файлы `--results-dir` остаются на узле AM. Данные — под `/orc_test/<scale>/…`.

Рекомендуемый первый день на кластере (пути из configs_final):

```text
preflight (dry-run) → preflight → generate(S=100 GB) → materialize (узкий --experiments) → run → final
```

### 8.1. Executor sizing — первый пробный запуск

Ориентир по кластеру (подтверждённые входные данные):

| Параметр | Значение |
|---|---|
| Datanodes | **6** |
| YARN Effective_Max_Capacity (queue) | **memory: 221184 MB (~216 GiB), vCores: 67** |

**Стартовые рекомендации для первого launch** (с запасом под AM/driver и соседнюю нагрузку; не исчерпывать Effective_Max_Capacity):

| Параметр | Первое значение | Комментарий |
|---|---|---|
| `executor_instances` | **10** | ~1–2 executor’а на datanode в среднем |
| `executor_cores` | **4** | итого ~40 vCores из 67 |
| `executor_memory_gib` | **16** | итого ~160 GiB из ~216 (+ driver/AM ~4–8 GiB) |

Итоговая загрузка ≈ **40 vCores / 160–168 GiB** — ниже потолка 67 / 216, чтобы оставить запас.

**Правило:** эти цифры — только для первого trial. **Финальный sizing утверждается после первого пробного прогона** (по факту YARN, GC, длительности generate/run, конкуренции с LLAP-очередью). Правки — в `environment.yaml` / `environment.local.yaml` и в обёртке `spark-submit`.

### 8.2. Concurrency — dataset- и cluster-aware

- Не использовать фиксированный «MVP-набор» concurrency для всех масштабов.
- Кандидаты в `benchmark.yaml` (`1, 5, 10, 25, 50`) — **верхняя полка возможностей**, а не обязательный список на каждый прогон.
- Фактический набор уровней выбирается **для каждого scale** с учётом:
  - свободной ёмкости YARN (Effective_Max_Capacity, текущая Current Capacity);
  - размера датасета (S=100 / M=500 / L=1024 GB);
  - параллельных executor’ов и LLAP/Tez-нагрузки.
- Практический ориентир: на S начинать с низких уровней (например 1 и 5), расширять после стабильного trial; на M/L — отдельно калибровать, тяжёлые 25/50 только при явном запасе capacity.

---

## 9. Ожидаемые выходы / отчётность

| Артефакт | Содержание |
|---|---|
| `results/<runId>/SUMMARY.txt` | runId, scale, skipped count, bestOrc, capabilities |
| `results/<runId>/summary.csv` | агрегаты avg/p50/p95/p99, SLA success rate, scanRatio |
| `results/<runId>/metrics.jsonl` | каждая попытка (warmup + measured) |
| `results/<runId>/skipped.tsv` | причины skip по capability |
| HDFS `{hdfs_root}/{scale}/…` | RAW_ARRAY_BASE, layout variants, refs, state markers |
| `generated/hive-table.sql` | DDL stub для Hive registration |

Ключевые KPI (из §2 benchmark + `benchmark.metrics`):

- Latency: avg / p50 / p95 / p99; SLA mean ≤ 3000 ms.
- Scan ratio = HDFS bytes read / ORC file bytes (или потенциально доступный объём).
- Equivalence: SHA-256 canonical row hashes across Spark/Hive variants.

---

## 10. Решения и открытые вопросы

### 10.1. Принятые решения (закрытые неоднозначности)

1. **Источник истины / HDFS root / YARN queue:** план следует **`orc_hdfs_spark_hive_configs_final`** (`/orc_test`, queue `default`). Операционный runbook — [`Запуск на Hadoop кластере.md`](Запуск%20на%20Hadoop%20кластере.md) (единый документ запуска / CLI / отчётов).
2. **Масштабы датасета:** во всех контрактных документах — **S=100 / M=500 / L=1024 GB** (логические GiB по `dataset.yaml`).
3. **`deploy_mode`:** только **`cluster`**. Переключение на **`client`** отключено в скриптах запуска.
4. **Executor sizing:** финализация **после первого trial run**. Для первого запуска — рекомендации §8.1 на базе 6 datanodes и Effective_Max_Capacity memory **221184** / vCores **67**.
5. **LLAP:** остаётся в плане (`HIVE_LLAP*`); readiness **подтверждается на реальном кластере** и при необходимости корректируется (observational/skipped до зелёного probe).
6. **Short-circuit / HDFS centralized cache / MV:** подтверждено — `SHORT_CIRCUIT` не mutable runner’ом (observational); `HDFS_CACHE.centralized` и `HIVE_MATERIALIZED_VIEW` — **ожидаемые skips**, приемлемо для покрытия §1.
7. *(слот ответа пользователя)* — подтверждено / закрыто вместе с п.6.
8. **Код:** реализация **с нуля (greenfield)** по этому плану; не restore-from-git.
9. **ZSTD:** путь **Spark ZSTD сохраняем** (эксперимент `COMPRESSION` / Spark writer). Для Hive — **пока не включать** из‑за codec gap ORC **1.5.6**; в плане и конфиге оставить **задел** на добавление ZSTD для Hive, когда поддержка codec позволит. BEST_ORC на стороне Hive ZSTD не использует.
10. **Concurrency:** **не** фиксированный MVP-набор; уровни **настраиваются per dataset / с учётом ёмкости кластера** (§8.2).

### 10.2. Остающиеся открытые вопросы

Существенных блокеров после ответов пользователя **не осталось**.

Неблокирующие уточнения (можно закрывать по ходу checkpoint’ов, без остановки Checkpoint 0):

- Точная доставка локальных отчётов/`results-dir` при устойчивом `deploy_mode=cluster` — практики зафиксированы в runbook §7.4 (CP9 A); **подтвердить на ноде (CP9 B)**.
- Числовые `executor_*` после первого trial (калибровка §8.1 → финал) — шаблон runbook §12; **заполнить на CP10 (B)**.
- Итоговый набор concurrency levels per scale после capacity-проб (§8.2) — старт Pilot S: 1,5; **финал на CP10 (B)**.
- Фактический статус LLAP daemons после чистого probe на кластере (корректировка skip/run для `HIVE_LLAP*`).

---

## 11. Краткий чеклист готовности к старту разработки

- [x] Пользователь утвердил этот план (Checkpoint 0), включая §7.0 (dev host ≠ cluster).
- [x] Критичные решения §10.1 приняты (источник истины, масштабы S/M/L=100/500/1024 GB, deploy_mode=cluster без client, greenfield, skips, ZSTD, concurrency-подход).
- [x] Есть явная команда: «начинай Checkpoint 1» (или эквивалент) — выполнено.
- [x] Checkpoint 1 **(A)** каркас собран и проверен локально (`./gradlew test shadowJar`).
- [x] Checkpoint 2 **(A)**: typed models + ExperimentExpander + capacity-aware concurrency + dry-run plan.
- [x] Checkpoint 3 **(A)**: preflight code + mock tests — выполнен.
- [ ] Checkpoint 3 **(B)**: live `preflight` на ноде (не с этого host) — см. [`Запуск на Hadoop кластере.md`](Запуск%20на%20Hadoop%20кластере.md).
- [x] Явная команда пользователя перед Checkpoint 4 **(A)** — получено; CP4 (A) выполнен.
- [ ] Checkpoint 4 **(B)**: live `generate` на ноде (HDFS `/orc_test/.../RAW_ARRAY_BASE`) — pending.
- [x] Явная команда пользователя перед Checkpoint 5 **(A)** — получено; CP5 (A) выполнен.
- [ ] Checkpoint 5 **(B)**: live `materialize` на ноде (OVAT layouts + `state/*.done`) — pending.
- [x] Явная команда пользователя перед Checkpoint 6 **(A)** — получено; CP6 (A) выполнен.
- [ ] Checkpoint 6 **(B)**: live Spark `run` на ноде (`spark-submit`, metrics.jsonl) — pending.
- [x] Явная команда пользователя перед Checkpoint 7 **(A)** — получено; CP7 (A) выполнен.
- [ ] Checkpoint 7 **(B)**: live Hive JDBC (Tez / LLAP) на ноде — pending.
- [x] Явная команда пользователя перед Checkpoint 8 **(A)** — получено; CP8 (A) выполнен.
- [ ] Checkpoint 8 **(B)**: отчёты после live `run`/`final` на ноде — pending.
- [x] Явная команда пользователя перед Checkpoint 9 **(A)** — получено; CP9 (A) выполнен.
- [ ] Checkpoint 9 **(B)**: preflight dry→live на ноде; доставка results при `deploy_mode=cluster` — pending.
- [x] Явная команда пользователя перед Checkpoint 10 **(A)** — получено; CP10 (A) выполнен.
- [ ] Checkpoint 10 **(B)**: Pilot S trial на ноде + калибровка executor/concurrency (§12 runbook) — pending.

---

*Конец плана. CP0–CP10 **(A)** выполнены (2026-10-19) — **локальный трек разработки (A) завершён**. Cluster validation / Pilot S trial (**B**) — только на ноде. Приёмка на этом host никогда не требует live cluster.*
