# Запуск orc-bench на Hadoop-кластере

Инструкция для прогона Pilot S (масштаб S = 100 GB) и забора результатов. Команды шагов 2–10 выполняются в SSH-сессии на узле кластера, из каталога с распакованным бандлом (ниже — `/opt/orc-bench`). Сборка и упаковка — на машине разработки.

| | |
|---|---|
| Контракт | [`orc_hdfs_spark_hive_configs_final/`](orc_hdfs_spark_hive_configs_final/) → runtime-копия `conf/` |
| Узкая матрица | [`conf/pilot-s.yaml`](../conf/pilot-s.yaml), обёртка `scripts/run-pilot-s.sh` |
| План | [`План разработки тестового приложения ORC HDFS Spark Hive.md`](План%20разработки%20тестового%20приложения%20ORC%20HDFS%20Spark%20Hive.md) |

Живой `spark-submit` идёт только в `deploy_mode=cluster`. Скрипты завершаются с ошибкой на `--deploy-mode client` и на `deploy_mode: client` в `environment.local.yaml`.

---

## Куда что пишется

Имена ниже — для текущего `conf/environment.yaml` (`namenode_uri: hdfs://sdp-dzo-lt-omlt`). Хост в путях результатов и логов берётся из этого URI. Каталог данных — `/orc_test` на этом nameservice. Адрес `hdfs://orc_test/...` Hadoop не открывает: символ `_` в имени хоста даёт пустой host (`Incomplete HDFS URI, no host`).

| Что | Путь |
|---|---|
| Базовый ORC, scale S | `hdfs://sdp-dzo-lt-omlt/orc_test/S/RAW_ARRAY_BASE/` |
| Справочники и манифест | `hdfs://sdp-dzo-lt-omlt/orc_test/S/refs/`, `hdfs://sdp-dzo-lt-omlt/orc_test/S/manifest/` |
| Layout’ы materialize | `hdfs://sdp-dzo-lt-omlt/orc_test/S/<эксперимент>_<вариант>/` |
| Маркеры «layout уже записан» | `hdfs://sdp-dzo-lt-omlt/orc_test/state/S__<эксперимент>__<вариант>.done` |
| Отчёты прогона | `hdfs://sdp-dzo-lt-omlt/orc_test/results/sdp-dzo-lt-omlt/<runId>/` |
| Лог драйвера (растёт, пока жив YARN) | `hdfs://sdp-dzo-lt-omlt/orc_test/logs/sdp-dzo-lt-omlt/<runId>/driver.log` |
| Лог скрипта submit | локально `logs/<скрипт>-YYYYMMDD-HHMMSS.log`, копия `hdfs://sdp-dzo-lt-omlt/orc_test/logs/sdp-dzo-lt-omlt/<имя файла>` |

В `benchmark.yaml` поле `hdfs_root` и в `environment.yaml` поле `work_dir` равны `/orc_test`. На кластере приложение дописывает nameservice и получает `hdfs://sdp-dzo-lt-omlt/orc_test`. Каталог отчётов по умолчанию скрипт подставляет сам (`hdfs://sdp-dzo-lt-omlt/orc_test/results/sdp-dzo-lt-omlt`). Флаг `--results-dir` нужен, только если отчёты надо положить в другое место. Явный `hdfs://orc_test/...` драйвер переписывает на nameservice сам.

Данные generate и отчёты — разные деревья. Очистка одного дерева другое не трогает.

Живой запуск отвязан от терминала. Скрипт печатает `application_<id>` и завершается; ApplicationMaster на YARN продолжает работу после обрыва SSH. В App Manager колонка Name — имя этой фазы, не класс `orcbench.Main`: `orc-bench <фаза> <масштаб>`, а если переданы — движки и список экспериментов. Пример прогона: `orc-bench run S spark ORC_COLUMN_PRUNING,ORC_PUSHDOWN,MINMAX_SORT,BLOOM_USER,PARTITIONING,SPARK_VECTORIZATION`. Своё имя задаётся `--name` или `ORC_BENCH_APP_NAME`. `runId` появляется уже внутри драйвера. Его каталог виден так:

```bash
hdfs dfs -ls hdfs://sdp-dzo-lt-omlt/orc_test/logs/sdp-dzo-lt-omlt
hdfs dfs -ls hdfs://sdp-dzo-lt-omlt/orc_test/results/sdp-dzo-lt-omlt
```

Свежий подкаталог и есть `<runId>`. В начале `driver.log` есть строка `[orc-bench] resolved resultsDir=… runId=…`.

---

## Шаг 0. Сборка на машине разработки

```bash
cd /path/to/orc_bench
./scripts/with-jdk8.sh ./scripts/build.sh
```

Артефакт: `build/libs/orc-bench-0.1.0-SNAPSHOT-all.jar`.

Упаковка для узла:

```bash
./scripts/package-for-cluster.sh --with-runbook
# → dist/orc-bench-cluster-bundle.tgz
#   jar в корне бандла, conf/, scripts/ (включая scripts/lib/), этот runbook
```

Проверка скриптов без кластера:

```bash
./scripts/run-pilot-s.sh --print
./scripts/run-pilot-s.sh --dry-run-submit --phase run
./scripts/run-preflight.sh --dry-run-submit
```

Локальный smoke приложения (отчёты остаются в локальном каталоге `results/`):

```bash
java -cp build/libs/orc-bench-0.1.0-SNAPSHOT-all.jar orcbench.Main \
  --config conf --scale S --phase preflight --dry-run --results-dir results
```

---

## Шаг 1. Копирование на узел

```bash
scp dist/orc-bench-cluster-bundle.tgz USER@NODE:/opt/orc-bench/
ssh USER@NODE 'mkdir -p /opt/orc-bench && cd /opt/orc-bench && tar -xzf orc-bench-cluster-bundle.tgz && cd orc-bench'
```

Дальше все команды — на узле, каталог `/opt/orc-bench/orc-bench` (корень распакованного бандла; если копировали файлы вручную, это `/opt/orc-bench`).

Проверка:

```bash
java -version          # 1.8.x
which spark-submit hdfs
test -f ./orc-bench-0.1.0-SNAPSHOT-all.jar && echo jar-ok
```

Если скрипт падает с `: No such file or directory` и пустым путём до двоеточия, в файлах Windows-окончания строк:

```bash
sed -i 's/\r$//' scripts/*.sh scripts/lib/*.sh
```

---

## Шаг 2. Конфиг и Kerberos на узле

```bash
cp conf/environment.local.yaml.example conf/environment.local.yaml
```

В `environment.local.yaml` оставьте `deploy_mode: cluster`. Числа executor’ов на первый прогон: **10 × 4 × 16g**, очередь **`default`**. После прогона их правят по чеклисту §12.

Keytab должен лежать по пути из `conf/environment.yaml`:

```text
runner_principal: pvm-omltp-sdp@OPSMON.SBT
keytab_path:      ~/keytab/pvm-omltp-sdp.keytab
```

`run-benchmark.sh` сам передаёт `--principal` и `--keytab` в `spark-submit`, если эти поля заданы. Файл keytab в git не кладут.

Обёртка в cluster mode сама добавляет `spark-submit --files` для `--env-override` и `--metrics-jsonl` и в приложение отдаёт только имя файла: драйвер читает его из рабочей директории контейнера. Прямой `spark-submit` без `--files` этот файл в контейнер не кладёт.

---

## Шаг 3. Очистка данных на HDFS

Перед **первым** прогоном на пустом `hdfs://sdp-dzo-lt-omlt/orc_test` удалять нечего. Проверка:

```bash
hdfs dfs -ls hdfs://sdp-dzo-lt-omlt/orc_test
hdfs dfs -du -h hdfs://sdp-dzo-lt-omlt/orc_test/S
hdfs dfs -ls hdfs://sdp-dzo-lt-omlt/orc_test/state
```

Очистка нужна, когда данные уже есть и их надо записать заново. Удаление уходит в корзину HDFS. Флаг `-skipTrash` добавляйте после того, как `hdfs dfs -ls` показал ровно те пути, которые собираетесь стереть.

### 3.1. Полный пересъём scale S

Generate перезаписывает `RAW_ARRAY_BASE`, `refs/` и `manifest/` (`SaveMode.Overwrite`). Старые layout’ы и маркеры при этом остаются. Повторный materialize видит `*.done` и **пропускает** уже отмеченные layout’ы, даже если базовый ORC уже другой.

Перед новым generate, если базовый набор надо пересобрать вместе с layout’ами:

```bash
hdfs dfs -rm -r hdfs://sdp-dzo-lt-omlt/orc_test/S
hdfs dfs -rm -r hdfs://sdp-dzo-lt-omlt/orc_test/state/S__*
```

Каталоги `hdfs://sdp-dzo-lt-omlt/orc_test/results/` и `hdfs://sdp-dzo-lt-omlt/orc_test/logs/` этой командой не затрагиваются.

### 3.2. Переписать один layout

Маркер и каталог layout’а удаляют парой. Иначе materialize либо пропустит запись (маркер на месте), либо оставит старые файлы.

```bash
hdfs dfs -ls hdfs://sdp-dzo-lt-omlt/orc_test/S
hdfs dfs -ls hdfs://sdp-dzo-lt-omlt/orc_test/state

hdfs dfs -rm -r hdfs://sdp-dzo-lt-omlt/orc_test/S/<каталог_layout>
hdfs dfs -rm hdfs://sdp-dzo-lt-omlt/orc_test/state/S__<эксперимент>__<вариант>.done
```

`RAW_ARRAY_BASE`, `refs/` и `manifest/` при этом оставляют.

### 3.3. Снять лишние layout’ы и оставить базовый ORC

Флаг materialize `--cleanup-layouts` удаляет под `hdfs://sdp-dzo-lt-omlt/orc_test/<scale>/` каталоги перезаписи, кроме `RAW_ARRAY_BASE`, `refs/`, `manifest/` и имён из `--keep-layouts`. Маркеры в `hdfs://sdp-dzo-lt-omlt/orc_test/state/` он не удаляет. После такой очистки следующий materialize снова пропустит эксперимент, пока соответствующий `*.done` лежит на месте. Чтобы layout записался заново, удалите и маркер (§3.2).

Пример: оставить layout’ы `MINMAX_SORT`, остальные rewrite-каталоги scale S удалить в конце materialize:

```bash
./scripts/run-pilot-s.sh --phase materialize \
  --env-override conf/environment.local.yaml \
  --cleanup-layouts --keep-layouts MINMAX_SORT
```

### 3.4. Старые отчёты и логи

На данные бенчмарка это не влияет. Удаляйте конкретный прогон, когда каталог больше не нужен:

```bash
hdfs dfs -rm -r hdfs://sdp-dzo-lt-omlt/orc_test/results/sdp-dzo-lt-omlt/<runId>
hdfs dfs -rm -r hdfs://sdp-dzo-lt-omlt/orc_test/logs/sdp-dzo-lt-omlt/<runId>
```

Лог скрипта submit лежит файлом рядом с каталогами `<runId>`, не внутри них: `hdfs dfs -ls hdfs://sdp-dzo-lt-omlt/orc_test/logs/sdp-dzo-lt-omlt`.

---

## Шаг 4. Preflight

Сначала прогон по конфигу (без живых проб HDFS и JDBC):

```bash
./scripts/run-preflight.sh --env-override conf/environment.local.yaml
```

Затем живые пробы (запись в `hdfs://sdp-dzo-lt-omlt/orc_test`, Kerberos, Hive JDBC):

```bash
./scripts/run-preflight.sh --live --env-override conf/environment.local.yaml
```

Скрипт печатает `application_<id>` и возвращает оболочку. Статус:

```bash
yarn application -status application_<id>
```

Дождитесь `SUCCEEDED`. Отчёт preflight — свой `<runId>`:

```text
hdfs://sdp-dzo-lt-omlt/orc_test/results/sdp-dzo-lt-omlt/<runId>/capabilities.tsv
hdfs://sdp-dzo-lt-omlt/orc_test/results/sdp-dzo-lt-omlt/<runId>/preflight-notes.txt
hdfs://sdp-dzo-lt-omlt/orc_test/results/sdp-dzo-lt-omlt/<runId>/skipped.tsv
```

Ожидаемые пропуски по контракту: `HIVE_MATERIALIZED_VIEW`, `HDFS_CACHE.centralized`. LLAP до зелёного probe остаётся observational. Дальше идите, когда preflight завершился `SUCCEEDED` и в `preflight-notes.txt` нет отказа записи в `hdfs://sdp-dzo-lt-omlt/orc_test`.

---

## Шаг 5. Generate (scale S)

Если на HDFS уже лежит старый scale S и его надо заменить вместе с layout’ами — сначала §3.1.

```bash
./scripts/run-pilot-s.sh --phase generate \
  --env-override conf/environment.local.yaml
```

Дождитесь `SUCCEEDED`. Проверка объёма:

```bash
hdfs dfs -ls hdfs://sdp-dzo-lt-omlt/orc_test/S/RAW_ARRAY_BASE
hdfs dfs -ls hdfs://sdp-dzo-lt-omlt/orc_test/S/refs
hdfs dfs -ls hdfs://sdp-dzo-lt-omlt/orc_test/S/manifest
hdfs dfs -du -h hdfs://sdp-dzo-lt-omlt/orc_test/S
```

Ориентир: логический объём S = 100 GB, порядка 89 млн строк. Каталог `RAW_ARRAY_BASE` должен быть непустым. Лог драйвера этой фазы: `hdfs dfs -tail hdfs://sdp-dzo-lt-omlt/orc_test/logs/sdp-dzo-lt-omlt/<runId>/driver.log`. В конце ожидается `[orc-bench] OK — generate`.

Повторный generate без удаления `hdfs://sdp-dzo-lt-omlt/orc_test/S` перезаписывает базу, справочники и манифест. Layout’ы и маркеры остаются прежними — для нового materialize выполните §3.1 или §3.2.

---

## Шаг 6. Materialize (Pilot S)

Пишет три семейства layout’ов поверх `RAW_ARRAY_BASE`: `MINMAX_SORT`, `BLOOM_USER`, `PARTITIONING`.

```bash
./scripts/run-pilot-s.sh --phase materialize \
  --env-override conf/environment.local.yaml
```

Проверка:

```bash
hdfs dfs -ls hdfs://sdp-dzo-lt-omlt/orc_test/S
hdfs dfs -ls hdfs://sdp-dzo-lt-omlt/orc_test/state
```

Рядом с `RAW_ARRAY_BASE` появляются каталоги экспериментов. В `hdfs://sdp-dzo-lt-omlt/orc_test/state/` — файлы `S__MINMAX_SORT__…done`, `S__BLOOM_USER__…done`, `S__PARTITIONING__…done`. Повторный materialize с теми же маркерами новые ORC не пишет: в `driver.log` строка `[materialize] skip (marker)`. Чтобы записать layout заново, удалите пару «каталог + маркер» (§3.2) и запустите эту команду снова.

Эквивалент без обёртки пилота:

```bash
./scripts/run-benchmark.sh \
  --scale S --phase materialize \
  --experiments MINMAX_SORT,BLOOM_USER,PARTITIONING \
  --env-override conf/environment.local.yaml
```

---

## Шаг 7. Прогон запросов (Pilot S)

Движок `spark`, эксперименты:

`ORC_COLUMN_PRUNING`, `ORC_PUSHDOWN`, `MINMAX_SORT`, `BLOOM_USER`, `PARTITIONING`, `SPARK_VECTORIZATION`.

`ORC_COLUMN_PRUNING` и `ORC_PUSHDOWN` читают `RAW_ARRAY_BASE`. `SPARK_VECTORIZATION` отдельный layout не создаёт. На каждую измеряемую ячейку: 5 прогревов и 30 замеров (`benchmark.yaml`). Прогон длинный: скрипт submit завершится сразу после `application_<id>`, приложение останется в YARN.

```bash
./scripts/run-pilot-s.sh --phase run \
  --env-override conf/environment.local.yaml
```

Сразу запишите `application_<id>` из вывода. Найдите `<runId>`:

```bash
hdfs dfs -ls hdfs://sdp-dzo-lt-omlt/orc_test/logs/sdp-dzo-lt-omlt
```

Дальше, пока приложение в состоянии `RUNNING`:

```bash
yarn application -status application_<id>
hdfs dfs -tail -f hdfs://sdp-dzo-lt-omlt/orc_test/logs/sdp-dzo-lt-omlt/<runId>/driver.log
```

`metrics.jsonl` появляется после первой завершённой ячейки и дописывается по одной ячейке:

```bash
hdfs dfs -tail hdfs://sdp-dzo-lt-omlt/orc_test/results/sdp-dzo-lt-omlt/<runId>/metrics.jsonl
```

В логе драйвера на каждую ячейку есть `[run] spark <эксперимент>/<вариант>` и `[run] appended N rows to …/metrics.jsonl`. Успешный конец фазы:

```text
[run] complete attempts=…
[orc-bench] OK — run complete
```

`yarn application -status` при этом показывает финальный статус `SUCCEEDED`. Локальный `logs/run-pilot-s-*.log` и его копия на HDFS содержат вывод submit до появления `application_<id>`. После выхода скрипта они больше не растут. Ход запросов смотрите в `driver.log`.

Этот `<runId>` понадобится на шаге 8. Прогон `run` и прогон `preflight` — разные приложения и разные `<runId>`.

Hive Tez / LLAP в Pilot S не входят. Отдельный прогон, когда Spark-пилот уже разобран:

```bash
./scripts/run-benchmark.sh \
  --scale S --phase run \
  --engines hive_tez,hive_llap \
  --experiments HIVE_TEZ_BASE,HIVE_VECTORIZATION,HIVE_CBO,HIVE_LLAP \
  --env-override conf/environment.local.yaml
```

---

## Шаг 8. Отчёт и сравнение

`report` и `final` читают уже записанный `metrics.jsonl`. Запросы заново не исполняют. Им передают `<runId>` шага 7. Каталог отчётов тот же, скрипт подставит его сам.

```bash
./scripts/run-pilot-s.sh --phase report \
  --run-id <runId> \
  --env-override conf/environment.local.yaml

./scripts/run-pilot-s.sh --phase final \
  --run-id <runId> \
  --env-override conf/environment.local.yaml
```

После `SUCCEEDED` в `hdfs://sdp-dzo-lt-omlt/orc_test/results/sdp-dzo-lt-omlt/<runId>/`:

| Файл | Когда появляется |
|---|---|
| `metrics.jsonl` | Во время шага 7, по ячейке |
| `SUMMARY.txt` | `report` и `final` |
| `summary.csv` | `report` и `final` |
| `skipped.tsv` | `report` и `final` |
| `final_comparison.tsv` | `final` (BEST_ORC и профили движков) |

Посмотреть с узла, не забирая каталог целиком:

```bash
hdfs dfs -cat hdfs://sdp-dzo-lt-omlt/orc_test/results/sdp-dzo-lt-omlt/<runId>/SUMMARY.txt
hdfs dfs -tail hdfs://sdp-dzo-lt-omlt/orc_test/results/sdp-dzo-lt-omlt/<runId>/summary.csv
```

`SUMMARY.txt` содержит порог SLA 3000 ms, долю успехов и блок **BEST_ORC** (`experimentId`, `layoutPath`, `meanP50Ms`). Hive-рекомендация ZSTD не выбирается (кодек ORC 1.5.6 на Hive).

---

## Шаг 9. Забрать результаты с кластера

На узле. Скрипт сам делает `hdfs dfs -get`, если локального `results/<runId>` ещё нет, и пакует архив:

```bash
./scripts/fetch-results.sh --run-id <runId>
# → /opt/orc-bench/orc-bench/results-<runId>.tgz
```

Список прогонов, которые уже лежат локально:

```bash
./scripts/fetch-results.sh --list
./scripts/fetch-results.sh --latest
```

С машины разработки:

```bash
scp USER@NODE:/opt/orc-bench/orc-bench/results-<runId>.tgz .
tar -tzf results-<runId>.tgz
```

Ручной вариант без архива:

```bash
mkdir -p results
hdfs dfs -get hdfs://sdp-dzo-lt-omlt/orc_test/results/sdp-dzo-lt-omlt/<runId> results/
```

Лог драйвера в каталог результатов не входит. Его забирают отдельно:

```bash
hdfs dfs -get \
  hdfs://sdp-dzo-lt-omlt/orc_test/logs/sdp-dzo-lt-omlt/<runId>/driver.log \
  driver-<runId>.log
```

После завершения приложения тот же текст есть в `yarn logs -applicationId application_<id>`.

---

## Шаг 10. Что считать результатом прогона

1. YARN-приложение шага 7 в статусе `SUCCEEDED`.
2. В `driver.log` есть `[orc-bench] OK — run complete`.
3. `metrics.jsonl` непустой, `summary.csv` и `SUMMARY.txt` лежат в том же `<runId>` после шага 8.
4. В `SUMMARY.txt` заполнен блок BEST_ORC либо явная пометка, что метрик для выбора не хватило.
5. `skipped.tsv` разобран: пропуски по capability ожидаемы, ошибки ячеек — нет.
6. Чеклист §12 заполнен по этому `<runId>` (время generate / materialize / run, sizing, решение про M/L).

Пока приложение `RUNNING`, промежуточный результат — растущие `driver.log` и `metrics.jsonl`. Локальный каталог `results/` на submit-узле до шага 9 пуст.

---

## Состав Pilot S

| Что | Значение |
|---|---|
| Scale | S = 100 GB |
| Executors, старт | 10 × 4 × 16g, `deploy_mode=cluster`, очередь `default` |
| Concurrency, старт | 1 и 5 |
| materialize | `MINMAX_SORT,BLOOM_USER,PARTITIONING` |
| run | `ORC_COLUMN_PRUNING,ORC_PUSHDOWN,MINMAX_SORT,BLOOM_USER,PARTITIONING,SPARK_VECTORIZATION` |
| Движок пилота | `spark` |

Порядок на узле: шаг 3 при необходимости → preflight → generate → materialize → run → report → final → fetch → §12.

---

## Поля отчёта

| Файл | Содержание |
|---|---|
| `metrics.jsonl` | Одна строка JSON на измерение: `runId`, `experimentId`, `variantLabel`, `engine`, `queryId`, `repetition`, `warmup`, `concurrency`, `cacheState`, `latencyMs`, `status`, `resultHash`, `logicalRows`, `orcFileBytes`, `hdfsBytesRead` |
| `summary.csv` | Агрегат ячейки: `n`, `avgMs`, `p50Ms`, `p95Ms`, `p99Ms`, `slaSuccessRate`, `scanRatio`, `resultHash`, `slaPass`. SLA: latency ≤ 3000 ms, `slaPass` при доле успехов ≥ 0.95 |
| `skipped.tsv` | Эксперимент и причина пропуска |
| `SUMMARY.txt` | Счётчики, SLA, блок BEST_ORC |
| `final_comparison.tsv` | Профили движков на layout BEST_ORC |
| `capabilities.tsv`, `preflight-notes.txt` | Только фаза preflight |

---

## Скрипты

| Скрипт | Где | Назначение |
|---|---|---|
| `with-jdk8.sh`, `build.sh` | Машина разработки | Сборка fat-jar на JDK 8 |
| `package-for-cluster.sh` | Машина разработки | Архив jar + `conf/` + `scripts/` |
| `run-benchmark.sh` | Узел | Единственная сборка `spark-submit` |
| `run-pilot-s.sh` | Узел | Pilot S: `--phase` и списки из `pilot-s.yaml`, дальше вызывает `run-benchmark.sh` |
| `run-preflight.sh` | Узел | `--phase preflight`; без `--live` добавляет `--dry-run` |
| `spark-submit-orc-bench.sh` | Узел | Тот же submit. Аргументы `Main` только после `--`, флаг печати команды — `--dry-run`, master всегда `yarn` |
| `fetch-results.sh` | Узел | `hdfs dfs -get` каталога прогона и упаковка в `.tgz` |
| `kinit-bench.sh` | Узел, по желанию | `kinit -kt` из `runner_principal` / `keytab_path`. Основной путь этого runbook на него не опирается |
| `lib/common.sh` | библиотека | YAML, jar, sizing, CRLF, шапка `--help` |
| `lib/logging.sh` | библиотека | Tee в `logs/` и копия в `hdfs://sdp-dzo-lt-omlt/orc_test/logs/<namenode>/` |
| `lib/cluster-dist.sh` | библиотека | Локальные `--env-override` и `--metrics-jsonl` → `spark-submit --files` |
| `lib/detach-submit.sh` | библиотека | `setsid`/`nohup`, выход после `application_<id>` |

Низкоуровневый вызов с тем же поведением, что `run-benchmark.sh`:

```bash
./scripts/spark-submit-orc-bench.sh \
  --deploy-mode cluster -- \
  --config conf --scale S --phase preflight \
  --env-override conf/environment.local.yaml
```

Прямой `spark-submit` (обёртка это собирает сама; `--files` обязателен вручную):

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --queue default \
  --num-executors 10 \
  --executor-cores 4 \
  --executor-memory 16g \
  --driver-memory 4g \
  --name "orc-bench preflight S" \
  --principal pvm-omltp-sdp@OPSMON.SBT \
  --keytab "$HOME/keytab/pvm-omltp-sdp.keytab" \
  --conf spark.yarn.submit.waitAppCompletion=false \
  --files conf/environment.local.yaml \
  --class orcbench.Main \
  ./orc-bench-0.1.0-SNAPSHOT-all.jar \
  --config conf --scale S --phase preflight \
  --env-override environment.local.yaml \
  --results-dir hdfs://sdp-dzo-lt-omlt/orc_test/results/sdp-dzo-lt-omlt
```

`--conf spark.yarn.submit.waitAppCompletion=false` нужен, чтобы клиент submit вернулся после постановки приложения в YARN.

---

## Если что-то пошло не так

| Симптом | Что сделать |
|---|---|
| `application_<id>` есть, SSH уже закрыт | `yarn application -status application_<id>` и `hdfs dfs -tail` лога драйвера. Submit-сессия приложению не нужна |
| Долго нет `metrics.jsonl` | Файл создаётся после первой ячейки. Смотрите `driver.log`: строка `[run] spark …` означает, что ячейка ещё считается |
| `Java heap space` / `TaskResultLost` на Q07 | Старый jar вызывает `collect()` всего результата на драйвер. Нужен jar, где число строк и хеш считаются на экзекуторах. Память драйвера в submit — 4g (`--driver-memory`) |
| Materialize сразу `skip (marker)` | Маркер уже есть. Для перезаписи — §3.2 |
| Generate прошёл, materialize пишет, что нет `RAW_ARRAY_BASE` | Проверьте `hdfs dfs -ls hdfs://sdp-dzo-lt-omlt/orc_test/S/RAW_ARRAY_BASE` |
| `Permission denied` на `hdfs://sdp-dzo-lt-omlt/orc_test` | Права principal’а: `hdfs dfs -ls hdfs://sdp-dzo-lt-omlt/orc_test` и пробная запись preflight `--live` |
| `client deploy mode is disabled` | В `environment.local.yaml` стоит `deploy_mode: cluster`, флаг `--deploy-mode client` не передаётся |
| `User did not initialize spark context` | На узле старый jar. Заново скопируйте `orc-bench-0.1.0-SNAPSHOT-all.jar` и перезапустите фазу |
| `UnsupportedClassVersion` | `java -version` на узле — 1.8.x, `JAVA_HOME` указывает на JDK 8 |
| Нет локального `results/<runId>` | Для cluster mode это нормально до шага 9. Каталог на HDFS |
| Preflight: мало места | `hdfs.free_space_gib` в `environment.local.yaml` против фактического `hdfs dfs -df -h` |
| Много skipped по LLAP | Для Pilot S движок — spark. LLAP смотрят отдельным прогоном после зелёного probe |
| Очередь отклонила приложение | Очередь `default` (`environment.yaml` / `--queue`) |

Повтор упавшей фазы: дождитесь конечного статуса YARN (`FAILED` / `KILLED`), посмотрите хвост `driver.log`, устраните причину. Generate можно запустить снова (перезапись базы). Materialize и run безопасны для повтора: materialize пропускает готовые маркеры, run создаёт новый `<runId>` и новый `metrics.jsonl`. Старый отчёт при этом остаётся в своём каталоге.

---

## 12. Чеклист калибровки после Pilot S

Заполняется на узле по `<runId>` шага 7.

### 12.1. Старт

| Параметр | Старт |
|---|---|
| Scale | S = 100 GB |
| `executor_instances` / `cores` / `memory_gib` | 10 / 4 / 16 |
| `deploy_mode` | cluster |
| YARN | 6 datanode, Effective_Max 221184 MB (~216 GiB), 67 vCores |
| Concurrency | 1, 5 |

Оценка старта: около 40 vCores и 160–168 GiB.

### 12.2. Наблюдения

| Поле | Значение |
|---|---|
| Дата / `runId` | ________________ |
| YARN Current Capacity (`default`) mem / vCores | ________ / ________ |
| generate(S), время | ________ |
| materialize, время | ________ |
| run, время и p50 | ________ |
| GC / spill / OOM | ________________ |
| Каталог отчёта | `hdfs://sdp-dzo-lt-omlt/orc_test/results/sdp-dzo-lt-omlt/<runId>/` |
| Skipped / failed | ________________ |

### 12.3. Что записать в `environment.local.yaml`

| Параметр | Финал S |
|---|---|
| `executor_instances` | ________ |
| `executor_cores` | ________ |
| `executor_memory_gib` | ________ |
| Concurrency для S | ________ |
| `deploy_mode` | cluster |

### 12.4. Расширение

| Вопрос | Решение |
|---|---|
| Матрица шире Pilot S | yes / no / later — ________ |
| Отдельная калибровка M = 500 GB | yes / no / later — ________ |
| Отдельная калибровка L = 1024 GB | yes / no / later — ________ |
| Concurrency 25 и 50 на S | только при свободной ёмкости очереди |

M и L калибруют отдельно. Числа S на них сами не переносятся. Перед M или L повторите шаги 3–9 со `--scale M` или `--scale L` и своей очисткой `hdfs://sdp-dzo-lt-omlt/orc_test/M` или `hdfs://sdp-dzo-lt-omlt/orc_test/L` (маркеры `M__*` / `L__*`). Обёртка `run-pilot-s.sh` всегда ставит `--scale S`; для M/L вызывайте `run-benchmark.sh --scale M`.

---

## Справочник флагов `orcbench.Main`

Флаги передаются через `run-benchmark.sh` / `run-pilot-s.sh` (у `spark-submit-orc-bench.sh` — после `--`).

| Флаг | По умолчанию | Смысл |
|---|---|---|
| `--config` | `conf` | Каталог YAML |
| `--env-override` | — | Overlay, обычно `conf/environment.local.yaml` |
| `--scale` | `S` | `S` 100 GB, `M` 500 GB, `L` 1024 GB |
| `--phase` | `all` | `preflight`, `generate`, `materialize`, `run`, `report`, `final`, `all` |
| `--experiments` | все | Список id через запятую. Пилот подставляет свой |
| `--engines` | `spark,hive_tez,hive_llap` | Пилот на `run` подставляет `spark` |
| `--results-dir` | `hdfs://sdp-dzo-lt-omlt/orc_test/results/sdp-dzo-lt-omlt` на YARN | Корень отчётов. `<runId>` добавляет приложение |
| `--run-id` | новый id | Для `report` и `final` — id прогона шага 7 |
| `--metrics-jsonl` | — | Явный файл метрик вместо `<results>/<runId>/metrics.jsonl` |
| `--dry-run` | выкл | План без записи HDFS и без замеров |
| `--cleanup-layouts` | выкл | После materialize удалить лишние layout’ы (§3.3) |
| `--keep-layouts` | — | Что оставить при `--cleanup-layouts` |
| `--mock-generate`, `--mock-materialize`, `--mock-run` | выкл | Память процесса, для локальных тестов |
| `--help`, `--version` | — | Справка |

Флаги обёртки (в `Main` не попадают): `--jar`, `--deploy-mode`, `--queue`, `--num-executors`, `--executor-cores`, `--executor-memory`, `--driver-memory`, `--principal`, `--keytab`, `--name`, `--dry-run-submit`. Переменные с тем же смыслом: `ORC_BENCH_JAR`, `ORC_BENCH_DEPLOY_MODE`, `ORC_BENCH_QUEUE`, `ORC_BENCH_NUM_EXECUTORS`, `ORC_BENCH_EXECUTOR_CORES`, `ORC_BENCH_EXECUTOR_MEMORY`, `ORC_BENCH_DRIVER_MEMORY`, `ORC_BENCH_PRINCIPAL`, `ORC_BENCH_KEYTAB`, `ORC_BENCH_APP_NAME`, `ORC_BENCH_ENV_YAML`, `ORC_BENCH_LOG_DIR`, `ORC_BENCH_LOG_FILE`. `--name` — подпись в YARN App Manager; без него скрипт собирает `orc-bench <фаза> <масштаб>`. `--driver-memory` по умолчанию `4g`.
