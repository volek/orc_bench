# Запуск orc-bench на Hadoop-кластере

Пошаговый операционный runbook: сборка на машине разработки → копирование на ноду → фазы pipeline → сбор результатов.

| | |
|---|---|
| Контракт | [`orc_hdfs_spark_hive_configs_final/`](orc_hdfs_spark_hive_configs_final/) → runtime-копия `conf/` |
| План / checkpoint’ы | [`План разработки тестового приложения ORC HDFS Spark Hive.md`](План%20разработки%20тестового%20приложения%20ORC%20HDFS%20Spark%20Hive.md) (§7.0: **dev host ≠ cluster**) |
| CLI / отчёты | этот документ: §13 (флаги), §8 (артефакты) |

> **Важно.** Команды разделов 3–8 выполняются **на ноде Hadoop/SDP-кластера**.  
> Машина разработки (этот host) подходит только для сборки, unit/mock-тестов и упаковки артефактов (разделы 1–2).  
> С этого host **не** запускать: live Kerberos к кластерному KDC, HDFS, `spark-submit` на YARN, Hive JDBC.

**Зрелость приложения (на момент документа):** реализованы CP0–CP10 **(A)** — фазы CLI + отчёты + скрипты кластерного запуска / packaging / runbook + **Pilot S** (`conf/pilot-s.yaml`, `scripts/run-pilot-s.sh`, чеклист калибровки §12). `deploy_mode=cluster` по умолчанию, переключение на `client`. Live preflight/generate/materialize/run/final / pilot trial на HDFS/YARN/HS2 — **только на ноде** (CP3–CP10 **(B)** pending).

---

## 1. Предварительные требования (на ноде кластера)

| Компонент | Ожидание (из `environment.yaml`) |
|---|---|
| JDK | **1.8** (на кластере ориентир `1.8.0_292`); **не** Java 17 как runtime для submit |
| Spark | 3.2.1 — в `PATH` есть `spark-submit` |
| Hadoop | 3.1.3 — `hdfs`, `hadoop` в `PATH` |
| Hive | 3.1.2 — JDBC к HS2 через ZooKeeper |
| Tez | 0.9.1 |
| YARN queue | **`default`** |
| HDFS root / work | **`/orc_test`** (существует, writable для principal runner’а) |
| Масштабы | S=100 GB, M=500 GB, L=1024 GB (`dataset.yaml`) |
| Kerberos | Тикет / principal уже доступны на ноде; пути principal/keytab в конфиге — для `spark-submit` (см. §4) |
| Сеть | Доступ к NN/RM/ZK HS2 с ноды submit |

Права: приложение **не меняет** cluster-wide конфиги и **не сбрасывает** кэши. Эксперименты без capability → `skipped` (не fail, если `fail_if_required_capability_missing: false`).

### 1.1. Principal и keytab (плейсхолдеры из конфига)

Из `conf/environment.yaml` (не изобретать секреты; путь keytab — как в конфиге). Эти поля нужны **`spark-submit --principal` / `--keytab`** в cluster mode, а не как инструкция «сначала залогиниться вручную»:

```text
runner_principal: pvm-omltp-sdp@OPSMON.SBT
keytab_path:      ~/keytab/pvm-omltp-sdp.keytab
```

На ноде keytab должен лежать по этому пути (или укажите свой путь в `environment.local.yaml` / переменных скрипта — **не коммитьте** реальный keytab в git). Kerberos-тикет / identity runner’а к моменту запуска уже должны быть доступны на ноде (см. §4).

### 1.2. Конфиги на ноде

Минимум в каталоге запуска:

```text
conf/benchmark.yaml
conf/dataset.yaml
conf/environment.yaml
conf/query-suite.yaml
conf/schema.orc.txt
conf/environment.local.yaml   # создать из example на ноде
```

Источник истины: `docs/orc_hdfs_spark_hive_configs_final/` (синхронизирован в `conf/` репозитория).

---

## 2. Сборка на машине разработки (этот host)

Сборка **допустима** локально / в CI:

```bash
# из корня репозитория orc_bench
./scripts/with-jdk8.sh ./scripts/build.sh
# либо: ./gradlew clean test shadowJar
```

Артефакт:

```text
build/libs/orc-bench-0.1.0-SNAPSHOT-all.jar
Main-Class: orcbench.Main
```

Упаковка для копирования (локально):

```bash
./scripts/package-for-cluster.sh
# → dist/orc-bench-cluster-bundle.tgz  (jar + conf/ + scripts/ + NODE_README.txt)
./scripts/package-for-cluster.sh --with-runbook   # + docs/Запуск на Hadoop кластере.md
```

Локальный smoke **без** кластера (CLI / dry-run config / печать submit-линии):

```bash
java -cp build/libs/orc-bench-0.1.0-SNAPSHOT-all.jar orcbench.Main --help
java -cp build/libs/orc-bench-0.1.0-SNAPSHOT-all.jar orcbench.Main \
  --config conf --scale S --phase preflight --dry-run --results-dir results

# Скрипты: синтаксис / help / печать команды без spark-submit
./scripts/run-benchmark.sh --help
./scripts/run-benchmark.sh --dry-run-submit --phase preflight --scale S
./scripts/run-preflight.sh --dry-run-submit
./scripts/spark-submit-orc-bench.sh --dry-run -- --config conf --phase preflight --dry-run
```

---

## 3. Копирование артефактов на ноду

С машины, у которой есть SSH к ноде (пример; подставьте user/host/path):

```bash
# вариант A: tarball
scp dist/orc-bench-cluster-bundle.tgz USER@NODE:/opt/orc-bench/
ssh USER@NODE 'mkdir -p /opt/orc-bench && cd /opt/orc-bench && tar -xzf orc-bench-cluster-bundle.tgz'

# вариант B: по файлам
scp build/libs/orc-bench-0.1.0-SNAPSHOT-all.jar USER@NODE:/opt/orc-bench/
scp -r conf USER@NODE:/opt/orc-bench/
scp -r scripts USER@NODE:/opt/orc-bench/
```

На ноде рабочий каталог (пример): `/opt/orc-bench/`.  
Дальнейшие команды — **в SSH-сессии на ноде**, из этого каталога.

Проверка окружения ноды:

```bash
java -version          # должен быть 1.8.x
which spark-submit hdfs
echo "$JAVA_HOME"
```

---

## 4. Kerberos

Ручной логин (`kinit` / обёртки) **не входит** в процедуру запуска этого runbook. На submit-ноде к моменту live-фаз уже должен быть доступен Kerberos-контекст runner’а (валидный тикет, либо identity, которую обеспечивает окружение / Spark / Hadoop).

Для `spark-submit --deploy-mode cluster` YARN обычно требует пути из конфига:

```text
--principal <runner_principal>
--keytab <абсолютный_путь_к_keytab>
```

Скрипт `run-benchmark.sh` передаёт их, если заданы `ORC_BENCH_PRINCIPAL` / `ORC_BENCH_KEYTAB` или найдены в YAML (`runner_principal`, `keytab_path` — см. §1.1). Убедитесь, что файл keytab на ноде существует по указанному пути.

---

## 5. `environment.local.yaml` / overrides

На ноде:

```bash
cp conf/environment.local.yaml.example conf/environment.local.yaml
# отредактируйте при необходимости: deploy_mode, executor_*, llap_*, free_space_gib, …
```

Типичные правки:

| Поле | Зачем |
|---|---|
| `spark.deploy_mode: client` | Driver на submit-ноде (логи/results проще); по умолчанию в configs — **`cluster`** |
| `spark.executor_*` | Калибровка после первого trial (§8.1 плана) |
| `hdfs.free_space_gib` | Актуальный снимок для preflight capacity |

Передача: `--env-override conf/environment.local.yaml`.

В `deploy_mode=cluster` (значение по умолчанию) `run-benchmark.sh` и `spark-submit-orc-bench.sh` сами добавляют `spark-submit --files` для этого файла и для `--metrics-jsonl`, а в приложение передают только имя файла. Driver в контейнере AM открывает его из своей рабочей директории. В `client` путь `conf/environment.local.yaml` остаётся как есть: процесс идёт на submit-ноде. Прямой `spark-submit` без `--files` файл в контейнер не кладёт — см. §7.3.

---

## 6. Фазы: dry-run vs live

| Phase | Смысл | Где | Статус кода |
|---|---|---|---|
| `preflight` | Версии, HDFS `/orc_test`, Kerberos, JDBC, capabilities → skip-matrix | **Только нода** (live); dry-run config — и локально | CP3 ✅ |
| `generate` | Синтетика `RAW_ARRAY_BASE` на HDFS | Нода (live); локально — `--dry-run` / `--mock-generate` | CP4 (A) ✅; (B) pending |
| `materialize` | OVAT layout’ы из RAW_ARRAY_BASE | Нода (live); локально — `--dry-run` / `--mock-materialize` | CP5 (A) ✅; (B) pending |
| `run` | Query suite Spark + Hive Tez/LLAP | Нода (live); локально — `--dry-run` / `--mock-run` | CP6–CP7 (A) ✅; (B) pending |
| `report` | SUMMARY/CSV/skipped из `metrics.jsonl` | Локально (fixtures) или нода | CP8 (A) ✅ |
| `final` | BEST_ORC × engines + отчёты | Нода (live); локально — dry-run / metrics fixtures | CP8 (A) ✅; (B) pending |
| `all` | Все подряд | Нода (live); локально — dry-run до final | CP8 (A) ✅ |

Рекомендуемый первый день на кластере (когда фазы реализованы):

```text
preflight --dry-run  →  preflight  →  generate(S)  →  materialize (узкий --experiments)  →  run  →  final
```

### 6.1. Сейчас (CP9 scripts + CP8 phases): preflight → … → run → report/final

```bash
cd /opt/orc-bench   # или ваш путь

# dry-run (без live HDFS/JDBC probes — всё же запускайте на ноде для единообразия PATH/конфигов)
./scripts/run-preflight.sh --env-override conf/environment.local.yaml

# live preflight
./scripts/run-benchmark.sh \
  --jar ./orc-bench-0.1.0-SNAPSHOT-all.jar \
  --scale S \
  --phase preflight \
  --env-override conf/environment.local.yaml \
  --results-dir results

# live generate (scale S → /orc_test/S/RAW_ARRAY_BASE/, refs/, manifest/)
./scripts/run-benchmark.sh \
  --jar ./orc-bench-0.1.0-SNAPSHOT-all.jar \
  --scale S \
  --phase generate \
  --env-override conf/environment.local.yaml \
  --results-dir results

# проверка на ноде:
# hdfs dfs -ls /orc_test/S/RAW_ARRAY_BASE
# hdfs dfs -du -h /orc_test/S

# Pilot S materialize / Spark run — предпочтительно через обёртку (§6.2):
./scripts/run-pilot-s.sh --phase materialize --env-override conf/environment.local.yaml
./scripts/run-pilot-s.sh --phase run --env-override conf/environment.local.yaml

# эквивалент вручную (узкая OVAT-матрица CP10):
./scripts/run-benchmark.sh \
  --jar ./orc-bench-0.1.0-SNAPSHOT-all.jar \
  --scale S \
  --phase materialize \
  --experiments MINMAX_SORT,BLOOM_USER,PARTITIONING \
  --env-override conf/environment.local.yaml \
  --results-dir results

./scripts/run-benchmark.sh \
  --jar ./orc-bench-0.1.0-SNAPSHOT-all.jar \
  --scale S \
  --phase run \
  --engines spark \
  --experiments ORC_COLUMN_PRUNING,ORC_PUSHDOWN,MINMAX_SORT,BLOOM_USER,PARTITIONING,SPARK_VECTORIZATION \
  --env-override conf/environment.local.yaml \
  --results-dir results

# проверка на ноде:
# hdfs dfs -ls /orc_test/S
# hdfs dfs -ls /orc_test/state
# ls results/*/metrics.jsonl
# head -n 3 results/*/metrics.jsonl

# (опционально после Pilot S) Hive Tez / LLAP — отдельный прогон, не входит в узкий Spark-пилот:
./scripts/run-benchmark.sh \
  --jar ./orc-bench-0.1.0-SNAPSHOT-all.jar \
  --scale S \
  --phase run \
  --engines hive_tez,hive_llap \
  --experiments HIVE_TEZ_BASE,HIVE_VECTORIZATION,HIVE_CBO,HIVE_LLAP \
  --env-override conf/environment.local.yaml \
  --results-dir results
```

Локально (dev host, без HDFS/YARN/HS2): `./scripts/run-pilot-s.sh --print` / `--dry-run-submit`; или `--phase run --engines hive_tez --dry-run` / `--mock-run`.

Ожидаемые skips (по configs_final): `HIVE_MATERIALIZED_VIEW`, `HDFS_CACHE.centralized`; LLAP — по факту probe на ноде (observational до зелёного probe); Hive ZSTD не требуется (codec gap 1.5.6).

### 6.2. Pilot S (CP10) — узкая матрица + калибровка

Контракт: [`conf/pilot-s.yaml`](../conf/pilot-s.yaml) (копия в `docs/orc_hdfs_spark_hive_configs_final/pilot-s.yaml`). Обёртка: `scripts/run-pilot-s.sh`.

| Что | Значение |
|---|---|
| Scale | **S = 100 GB** |
| Executors (старт §8.1) | **10 × 4 cores × 16g** (`deploy_mode=cluster`) |
| YARN ориентир | 6 DN; Effective_Max **221184 MB / 67 vCores** |
| Concurrency старт (§8.2) | **1, 5** (capacity-aware; финал — после trial) |
| materialize `--experiments` | `MINMAX_SORT,BLOOM_USER,PARTITIONING` |
| run `--experiments` | `ORC_COLUMN_PRUNING,ORC_PUSHDOWN,MINMAX_SORT,BLOOM_USER,PARTITIONING,SPARK_VECTORIZATION` |
| engines (пилот) | `spark` |

Последовательность на ноде:

```text
preflight → generate(S) → materialize (pilot) → run (pilot) → report/final
→ заполнить чеклист калибровки §12 → решить о расширении матрицы / M|L
```

Dev-host smoke (без кластера):

```bash
./scripts/run-pilot-s.sh --print
./scripts/run-pilot-s.sh --dry-run-submit --phase materialize
./scripts/run-pilot-s.sh --dry-run-submit --phase run
```

---

## 7. Примеры `spark-submit`

### 7.1. Через обёртку (рекомендуется)

```bash
# deploy_mode=cluster по умолчанию
./scripts/run-benchmark.sh \
  --jar ./orc-bench-0.1.0-SNAPSHOT-all.jar \
  --config conf \
  --scale S \
  --phase preflight \
  --env-override conf/environment.local.yaml \
  --results-dir results

# переключение на client:
./scripts/run-benchmark.sh --deploy-mode client ...остальные флаги...
```

### 7.2. Низкоуровневый скрипт

```bash
./scripts/spark-submit-orc-bench.sh \
  --jar ./orc-bench-0.1.0-SNAPSHOT-all.jar \
  --deploy-mode cluster \
  -- --config conf --scale S --phase preflight --env-override conf/environment.local.yaml
```

(Аргументы после `--` уходят в `orcbench.Main`.)

### 7.3. Прямой вызов (cluster)

Значения executor — стартовые из §8.1 плана / `environment.yaml` (10×4×16g); queue **`default`**:

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --queue default \
  --num-executors 10 \
  --executor-cores 4 \
  --executor-memory 16g \
  --principal pvm-omltp-sdp@OPSMON.SBT \
  --keytab "$HOME/keytab/pvm-omltp-sdp.keytab" \
  --files conf/environment.local.yaml \
  --class orcbench.Main \
  ./orc-bench-0.1.0-SNAPSHOT-all.jar \
  --config conf \
  --scale S \
  --phase preflight \
  --env-override environment.local.yaml \
  --results-dir results
```

`--files` обязателен при ручном вызове: в контейнере AM нет каталога `conf/` со шлюза. Обёртки из §7.1 и §7.2 делают эту подмену сами. Базовые YAML (`environment.yaml`, `benchmark.yaml`, …) уже лежат в fat-jar и с classpath читаются без `--files`.

### 7.4. Client mode

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --queue default \
  --num-executors 10 \
  --executor-cores 4 \
  --executor-memory 16g \
  --class orcbench.Main \
  ./orc-bench-0.1.0-SNAPSHOT-all.jar \
  --config conf --scale S --phase preflight \
  --env-override conf/environment.local.yaml \
  --results-dir results
```

В **client** режиме `results/` пишется на submit-ноде рядом с CWD — проще забрать отчёты (`./scripts/fetch-results.sh --latest`).

В **cluster** режиме driver на AM: локальный `--results-dir` на submit-ноде часто **пуст** (артефакты внутри контейнера AM). Практики (подтвердить на CP9 **(B)**):

1. **Рекомендуется для отчётов:** `--deploy-mode client` (флаг скрипта или `spark.deploy_mode: client` в `environment.local.yaml`) — `results/<runId>/` сразу на ноде.
2. **Hybrid:** live generate/materialize/run в `cluster`; затем на ноде `--phase report` / `--phase final` с `--metrics-jsonl` / `--run-id`, если metrics уже скопированы на локальный FS.
3. **YARN logs:** `yarn logs -applicationId <appId>` — искать stdout driver’а / пути к metrics (если писались только на AM).
4. **Общий FS (опционально):** если на нодах есть NFS/общий mount — укажите `--results-dir /shared/orc-bench/results` (путь должен быть доступен driver’у в выбранном deploy_mode).

Итог CP9 **(A):** скрипты и runbook готовы; фактическая доставка results при устойчивом `deploy_mode=cluster` — **валидация на ноде (B)**.
---

## 8. Где лежат результаты и как забрать

### 8.1. Локальные отчёты на ноде

```text
results/<runId>/
  SUMMARY.txt
  summary.csv             # после run / report / final (CP8)
  metrics.jsonl
  skipped.tsv
  final_comparison.tsv    # только --phase final (BEST_ORC × engines)
  capabilities.tsv        # preflight
  preflight-notes.txt     # preflight
```

`runId` печатается в stdout в начале запуска.  
Повторный отчёт с уже собранных metrics: `--phase report|final` + `--run-id <id>` и/или `--metrics-jsonl <path>` (fixtures / dump с ноды).

Скопировать на рабочую машину:

```bash
# на ноде — упаковать
./scripts/fetch-results.sh --results-dir results --run-id <runId>
# или: tar -czf results-<runId>.tgz -C results <runId>

# с рабочей машины:
scp USER@NODE:/opt/orc-bench/results-<runId>.tgz .
```

### 8.2. Поля артефактов отчёта

| Файл | Назначение / ключевые поля |
|---|---|
| `metrics.jsonl` | Одна строка JSON на измерение: `runId`, `experimentId`, `variantLabel`, `engine`, `queryId`, `repetition`, `warmup`, `concurrency`, `cacheState`, `latencyMs`, `status`, `resultHash`, `logicalRows`, `orcFileBytes`, `hdfsBytesRead`, … |
| `summary.csv` | Агрегаты по ячейке: `experimentId,variantLabel,engine,queryId,concurrency,cacheState,n,avgMs,p50Ms,p95Ms,p99Ms,slaSuccessRate,scanRatio,resultHash,slaPass`. SLA: latency ≤ **3000 ms** (`sla_mean_latency_ms` / `sla_success_threshold_ms`); `slaPass` = доля успехов ≥ 0.95. `scanRatio` ≈ HDFS bytes read / ORC file bytes. |
| `skipped.tsv` | `experiment` + `reason` (capability / probe / policy). |
| `SUMMARY.txt` | Сводка прогона: counts, `slaThresholdMs`, `slaGateOverall`, блок **BEST_ORC** (`experimentId`, `layoutPath`, `meanP50Ms`, `meanSlaSuccessRate`, `slaPassed`, `tieBreakClass`), ячейки final, notes. |
| `final_comparison.tsv` | После `final`: `engineProfile`, `engine`, `layoutPath` (BEST_ORC layout), `queries`, `skipReason`. |

**BEST_ORC:** выбор layout по `best_orc_rule` (objective queries, SLA-gate, tie-break); Hive-shared рекомендации **не** берут ZSTD (codec gap ORC 1.5.6).

### 8.3. Данные на HDFS

Корень: **`/orc_test`** (`benchmark.hdfs_root` / `hdfs.work_dir`).

| Путь | Содержимое (после CP4+) |
|---|---|
| `/orc_test/<scale>/RAW_ARRAY_BASE/` | Базовый ORC |
| `/orc_test/<scale>/<EXPERIMENT>_…/` | OVAT layout’ы |
| `/orc_test/<scale>/refs/` | Справочники JOIN |
| `/orc_test/state/*.done` | Idempotency markers |

Проверка **на ноде**:

```bash
hdfs dfs -ls /orc_test
hdfs dfs -du -h /orc_test/S
```

---

## 9. Troubleshooting

| Симптом | Что проверить |
|---|---|
| `UnsupportedClassVersion` / странные ошибки Spark | На ноде для submit используется **Java 8**, не 17. `JAVA_HOME` → JDK 8; `spark-submit` подхватывает его |
| `Build must run on Java 8` (локально) | `./scripts/with-jdk8.sh …` или toolchain Gradle |
| Classpath / `ClassNotFoundException` | Fat JAR shadow; на кластере Spark/Hadoop — `provided`; не смешивать чужие Spark jars |
| `GSSException` / Kerberos | Есть ли валидный Kerberos-контекст на ноде; путь keytab из YAML; для cluster mode — `--principal`/`--keytab` |
| Queue rejected | В configs queue = **`default`** (не `benchmark`). Проверьте `--queue` и `environment.yaml` |
| HDFS `Permission denied` / нет `/orc_test` | Principal имеет write на `/orc_test`; `hdfs dfs -ls /orc_test` |
| HDFS capacity / preflight fail | `hdfs.free_space_gib` vs scale (S/M/L); актуальный снимок в `environment.local.yaml` |
| Hive JDBC fail | ZK URL из `environment.yaml`; сеть с ноды; ticket ещё валиден |
| Много `skipped` LLAP | `llap_daemons_running_verified` / live probe; ожидаемо до зелёного probe |
| Нет `results/` после cluster mode | Driver на AM — см. §7.4; `--deploy-mode client` или hybrid report; подтвердить на CP9 (B) |
| `spark-submit` / live Kerberos на laptop | Не запускать live с этого host; локально только `--dry-run-submit` / packaging |
| Phase not implemented | Все фазы CLI (A) готовы CP1–CP8; live на ноде — CP3–CP9 (B) |

---

## 10. Чеклист первого дня на ноде

1. [ ] JAR собран на JDK 8 (на этом host / CI).  
2. [ ] На ноду скопированы jar + `conf/` + `scripts/` (в т.ч. `pilot-s.yaml`, `run-pilot-s.sh`).  
3. [ ] Создан `conf/environment.local.yaml`.  
4. [ ] Kerberos-контекст / principal доступны на ноде; keytab по пути из конфига (для cluster `spark-submit`, §4).  
5. [ ] `java -version` = 1.8.x; `spark-submit` и `hdfs` в PATH.  
6. [ ] `./scripts/run-preflight.sh` (dry-run) → затем live preflight.  
7. [ ] Разбор `results/<runId>/` (capabilities / skipped / SUMMARY — §8.2).  
8. [ ] Pilot S (CP10 B): `generate` → `run-pilot-s.sh` materialize/run → report/final; заполнить §12.  
9. [ ] Забрать `results/<runId>/` через `fetch-results.sh` / scp.

---

## 11. Связанные скрипты (`scripts/`)

| Скрипт | Где запускать | Назначение |
|---|---|---|
| `with-jdk8.sh` | Dev / CI | Команда под JDK 8 |
| `build.sh` | Dev / CI | `clean` + `test` + `shadowJar` |
| `package-for-cluster.sh` | Dev | Tarball для scp на ноду |
| `kinit-bench.sh` | *(опционально)* | Helper вне основного flow; runbook **не** требует явного `kinit` (§4) |
| `run-benchmark.sh` | **Нода** | Основной `spark-submit` wrapper |
| `run-preflight.sh` | **Нода** | Быстрый `--phase preflight` |
| `run-pilot-s.sh` | **Нода** (smoke: `--print` / `--dry-run-submit` на dev) | Pilot S узкая матрица (CP10) |
| `spark-submit-orc-bench.sh` | **Нода** | Низкоуровневый spark-submit |
| `fetch-results.sh` | **Нода** (или локально, если results уже есть) | Упаковка `results/<runId>`; `--list` / `--latest` |

Все cluster-side скрипты:

- по умолчанию **`deploy_mode=cluster`**, queue **`default`**, executors **10×4×16g** (из `environment.yaml` / overrides);
- **не** содержат секретов — только пути/имена из конфигов;
- на developer host поддерживают **`--dry-run-submit` / `--dry-run`** (печать команды без submit);
- при отсутствии `spark-submit`/`hdfs` печатают WARNING/ERROR и отсылают к этому runbook;
- пишут локальные логи (stdout+stderr через `tee`) в **`logs/<script>-YYYYMMDD-HHMMSS.log`** рядом с корнем репо/бандла; каталог создаётся автоматически. Переопределение: `ORC_BENCH_LOG_DIR`, `ORC_BENCH_LOG_FILE`. Вложенные вызовы (`run-preflight` → `run-benchmark`, `build` → `with-jdk8`) не делают повторный tee (`ORC_BENCH_LOGGING=1`).

---

## 12. Чеклист калибровки executor_* / concurrency (CP10)

Шаблон **заполняется на ноде после** Pilot S trial (не на developer host). Опирается на план §8.1–§8.2 и `conf/pilot-s.yaml`.

### 12.1. Стартовые значения (до trial)

| Параметр | Старт | Источник |
|---|---|---|
| Scale | S = 100 GB | `dataset.yaml` |
| `executor_instances` | 10 | §8.1 / `environment.yaml` |
| `executor_cores` | 4 | §8.1 |
| `executor_memory_gib` | 16 | §8.1 |
| `deploy_mode` | cluster | configs_final |
| YARN Effective_Max | 221184 MB (~216 GiB), 67 vCores; 6 DN | подтверждённые факты |
| Concurrency старт | 1, 5 | §8.2 / pilot-s; candidates ceiling 1…50 в `benchmark.yaml` |
| Pilot experiments | см. §6.2 / `pilot-s.yaml` | CP10 |

Ожидаемая загрузка старта ≈ **40 vCores / 160–168 GiB** (запас до 67 / 216).

### 12.2. Наблюдения после trial (заполнить на ноде)

| Поле | Значение |
|---|---|
| Дата / `runId` | ________________ |
| YARN Current Capacity (queue `default`) mem / vCores | ________ / ________ |
| Concurrent apps / LLAP queue contention | ________________ |
| generate(S) wall time | ________ |
| materialize (pilot) wall time | ________ |
| run (pilot) wall time / p50 notes | ________ |
| GC / spill / OO M на executors | ________________ |
| `results/<runId>/` доставлен? (cluster vs client, §7.4) | ________________ |
| Skipped / failed cells | ________________ |

### 12.3. Финальные значения для S (после согласования)

| Параметр | Финал S | Примечание |
|---|---|---|
| `executor_instances` | ________ | правка → `environment.local.yaml` |
| `executor_cores` | ________ | |
| `executor_memory_gib` | ________ | |
| Concurrency levels (S) | ________ | не слепой набор 1…50; capacity-aware |
| `deploy_mode` (cluster / client) | ________ | client — если нужна локальная `results/` |

### 12.4. Решение о расширении

| Вопрос | Решение |
|---|---|
| Расширить physical/engine матрицу beyond Pilot S? | yes / no / later — ________ |
| Калибровать M = 500 GB отдельно? | yes / no / later — ________ |
| Калибровать L = 1024 GB отдельно? | yes / no / later — ________ |
| Тяжёлые concurrency 25/50 на S? | только при явном запасе capacity |

> **Правило:** финальный sizing и concurrency утверждаются **после** trial. M/L не наследуют слепо значения S.

---

## 13. Справочник CLI (`orcbench.Main`)

Флаги передаются **после** jar в `spark-submit` (или через `scripts/run-benchmark.sh` / `run-pilot-s.sh`). Источник: `CliOptions`.

| Флаг | По умолчанию | Описание |
|---|---|---|
| `--config` | `conf` | Каталог YAML-контракта |
| `--env-override` | — | Overlay для `environment` (обычно `conf/environment.local.yaml`) |
| `--scale` | `S` | `S` / `M` / `L` (100 / 500 / 1024 GB) |
| `--phase` | `all` | `preflight\|generate\|materialize\|run\|report\|final\|all` |
| `--experiments` | *(все)* | Фильтр ID экспериментов (comma-separated) |
| `--engines` | `spark,hive_tez,hive_llap` | Движки (comma-separated) |
| `--results-dir` | `results` | Каталог отчётов → `results/<runId>/` |
| `--run-id` | — | Существующий `results/<runId>/` для `--phase report\|final` |
| `--metrics-jsonl` | — | Явный путь к `metrics.jsonl` (fixtures / dump с ноды) |
| `--dry-run` | off | План / config-probes без live Spark/Hive/HDFS writes |
| `--mock-generate` | off | In-memory `FakeGenerateBackend` (local/CI, без HDFS) |
| `--mock-materialize` | off | In-memory `FakeMaterializeBackend` (local/CI) |
| `--mock-run` | off | Fake Spark/Hive backends (local/CI, без YARN/HS2) |
| `--cleanup-layouts` | off | После materialize удалить неretained rewrite layout’ы |
| `--keep-layouts` | — | ID / имена layout’ов, сохраняемые при cleanup |
| `--help` / `--version` | — | Справка / версия (picocli) |

Скриптовые флаги обёрток (не уходят в `Main`): `--jar`, `--deploy-mode`, `--queue`, `--dry-run-submit`, `--principal` / `--keytab` / env `ORC_BENCH_*` — см. `run-benchmark.sh --help`.
