# Запуск orc-bench на Hadoop-кластере

Инструкция для Pilot S индекса SM_UKO и для отдельного Hive-трека. Команды шагов 2–10 выполняются в SSH-сессии на узле, из каталога с распакованным бандлом (ниже — `/opt/orc-bench/orc-bench`). Сборка и упаковка — на машине разработки.

Pilot S — один час старой эпохи, ключ `2025-04-14--12`, цель около 9,7 ГБ сжатых ZLIB и около 262 ГиБ логических. Час новой эпохи из `observed-orc-profile.yaml` (ключ `2026-06-28--09`, 19 159 061 строк, 5,34 ГБ, 7 файлов) — справочный профиль. Генератор на него объём S не равняет.

Живой `spark-submit` идёт в `deploy_mode=cluster`. Скрипты завершаются с ошибкой на `--deploy-mode client` и на `deploy_mode: client` в `environment.local.yaml`.

| | |
|---|---|
| Контракт | `conf/` (копия пакета measured-profile; снимок рядом — `docs/orc_hdfs_spark_hive_configs_final/`) |
| Узкая матрица | `conf/pilot-s.yaml`, обёртка `scripts/run-pilot-s.sh` |

---

## Что измеряет этот прогон

Четыре эксперимента Spark на scale S, затем один финальный профиль `SPARK_COMMON` на выбранном layout.

| Фаза | Что исполняется | Новый ORC |
|---|---|---|
| generate | `RAW_ARRAY_BASE`, `refs/`, `manifest/` | да, базовая копия |
| materialize | `BLOOM_SESSION`: пустой bloom и bloom по `session` | да, две копии |
| run | `ORC_COLUMN_PRUNING`, `ORC_PUSHDOWN`, `BLOOM_SESSION`, `SPARK_VECTORIZATION` | нет |
| report | агрегаты и блок BEST_ORC по уже записанным метрикам | нет |
| final | 10 запросов `SPARK_COMMON` на layout BEST_ORC | нет |

Базовая раскладка уже часовая (`key=yyyy-MM-dd--HH`), ZLIB, цель файла 912 МиБ, цель страйпа 64 МиБ, строки пишутся с сортировкой по `__time`. Эти свойства в пилоте не варьируются. Монотонность `__time` в справочном футере не доказана; генератор сортирует запись, проверка «время только растёт» в приёмку не входит.

Порог приемлемого времени — 7 200 000 мс (2 часа) на измерение. Доля успехов для `slaPass` — не ниже 0,95. 3000 мс в конфиге — ориентир для `Q_SESSION_EQ` после того, как bloom по `session` уменьшил чтение. Этот ориентир ворота SLA не открывает и не закрывает.

Протокол каждой ячейки: 5 прогревов (`warmup=true`, в `summary.csv` не входят) и 30 замеров (`warmup=false`, `status=ok`). Перцентиль — элемент с индексом `ceil(p·n)−1`. У ячеек пилота concurrency = 1. Уровни 5 и 9 в конфиге относятся к `SPARK_BASE`; этот эксперимент в Pilot S не запускается.

Литералы запросов. Generate и run — разные приложения YARN. Манифест выборок generate в run не подхватывается, подставляются заглушки (`session` = `copy2_0`, `userLogin` = `userLogin0` и соседние). Равенство по такой заглушке может вернуть 0 строк. Сравнение раскладок — стабильный `resultHash` одного и того же SQL и `hdfsBytesRead` / `scanRatio`. Число строк из `expect_rows` в `query-suite.yaml` описывает боевой шаблон, а не эту заглушку. `Q_ABSENT_PARAM` пустой и на боевых литералах: параметра `STAFF_LOGIN` в индексе нет. `Q_UNFILTERED` читает час целиком, заглушки на него не влияют.

`hdfsBytesRead` у Spark — сумма `bytesRead` входных метрик задач. `orcFileBytes` — сумма длин файлов layout. В `summary.csv` поле `scanRatio` = `hdfsBytesRead / orcFileBytes`. Ноль — снятое значение. Пустое поле — счётчик не пришёл (так бывает у Hive, если в журнале Tez нет `HDFS_BYTES_READ`).

---

## Куда что пишется

Имена — для `namenode_uri: hdfs://sdp-dzo-lt-omlt`. Каталог данных — `/orc_test`. Адрес `hdfs://orc_test/...` Hadoop не открывает: символ `_` в имени хоста даёт пустой host.

| Что | Путь |
|---|---|
| Базовый ORC, scale S | `hdfs://sdp-dzo-lt-omlt/orc_test/S/RAW_ARRAY_BASE/` |
| Справочники и манифест | `hdfs://sdp-dzo-lt-omlt/orc_test/S/refs/`, `.../S/manifest/` |
| Bloom, пустой список | `hdfs://sdp-dzo-lt-omlt/orc_test/S/BLOOM_SESSION_bloom_columns=__` |
| Bloom по `session` | `hdfs://sdp-dzo-lt-omlt/orc_test/S/BLOOM_SESSION_bloom_columns=_session_` |
| Маркеры | `hdfs://sdp-dzo-lt-omlt/orc_test/state/S__BLOOM_SESSION__….done` |
| Отчёты прогона | `hdfs://sdp-dzo-lt-omlt/orc_test/results/sdp-dzo-lt-omlt/<runId>/` |
| Лог драйвера | `hdfs://sdp-dzo-lt-omlt/orc_test/logs/sdp-dzo-lt-omlt/<runId>/driver.log` |
| Лог скрипта submit | локально `logs/<скрипт>-YYYYMMDD-HHMMSS.log`, копия в том же дереве `logs/` |

Имена каталогов bloom — результат санитизации меток `bloom_columns=[]` и `bloom_columns=[session]`. Если `hdfs dfs -ls` покажет другое имя, ориентир — префикс `BLOOM_SESSION_bloom_columns_` и строка `[materialize]` в `driver.log`.

`hdfs_root` и `work_dir` равны `/orc_test`. На кластере приложение дописывает nameservice. Каталог отчётов скрипт подставляет сам. `--results-dir` нужен только для другого корня.

Данные generate и отчёты — разные деревья. Очистка одного другое не трогает.

Скрипт печатает `application_<id>` и завершается. ApplicationMaster продолжает работу после обрыва SSH. В App Manager имя фазы выглядит как `orc-bench run S spark ORC_COLUMN_PRUNING,ORC_PUSHDOWN,BLOOM_SESSION,SPARK_VECTORIZATION`. `runId` появляется внутри драйвера:

```bash
hdfs dfs -ls hdfs://sdp-dzo-lt-omlt/orc_test/logs/sdp-dzo-lt-omlt
hdfs dfs -ls hdfs://sdp-dzo-lt-omlt/orc_test/results/sdp-dzo-lt-omlt
```

Свежий подкаталог и есть `<runId>`. В начале `driver.log` есть строка `[orc-bench] resolved resultsDir=… runId=…`.

Порядок колонок ORC (он же порядок в `conf/schema.orc.txt` и в Hive DDL):

`metamodelHash`, `session`, `sourceSystem`, `module`, `userName`, `metamodelVersion`, `userLogin`, `createdAt`, `requestId`, `userNode`, `name`, `id`, `nodeId`, `tags`, `paramName`, `paramValue`, `paramOldValue`, `archiveInternalId`, `archiveInsertTime`, `__time`.

Файлы, записанные прежним jar, начинались с `userLogin` и заканчивались `requestId`. Новая Hive-таблица на них не сажается. Следующий generate идёт после удаления scale S.

---

## Шаг 0. Сборка на машине разработки

```bash
cd /path/to/orc_bench
./scripts/with-jdk8.sh ./scripts/build.sh
./scripts/package-for-cluster.sh
# → dist/orc-bench-cluster-bundle.tgz
```

Артефакт jar: `build/libs/orc-bench-0.1.0-SNAPSHOT-all.jar`. В бандле — jar, `conf/`, `scripts/`. Этот runbook попадает в бандл только с флагом `--with-runbook`.

Проверка скриптов без кластера:

```bash
./scripts/run-pilot-s.sh --print
./scripts/run-pilot-s.sh --dry-run-submit --phase run
./scripts/run-preflight.sh --dry-run-submit
```

Локальный smoke (отчёты в локальном `results/`):

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

Дальше все команды — на узле, каталог `/opt/orc-bench/orc-bench`.

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

В `environment.local.yaml` оставьте `deploy_mode: cluster`. Активный размер: **10 × 4 × 16g**, overhead 512m, driver **1 × 4g**, очередь **`default`**. Одно приложение — 41 vCore и около 169 ГБ. Очередь (около 216 ГиБ и 67 vCores, контейнер не больше 45 ГиБ) вмещает одно такое приложение.

Keytab — по полям `conf/environment.yaml`:

```text
runner_principal: pvm-omltp-sdp@OPSMON.SBT
keytab_path:      ~/keytab/pvm-omltp-sdp.keytab
```

`run-benchmark.sh` передаёт `--principal` и `--keytab`, если поля заданы. Keytab в бандл не входит.

Обёртка сама добавляет `spark-submit --files` для `--env-override` и `--metrics-jsonl`. Прямой `spark-submit` без `--files` этот файл в контейнер не кладёт.

---

## Шаг 3. Очистка данных на HDFS перед generate

Существующий scale S записан прежним порядком колонок. Перед generate этого jar его удаляют целиком, вместе с маркерами. Иначе materialize увидит `*.done` и оставит старые файлы bloom рядом с новой базой, а Hive DDL нового порядка укажет на чужой футер.

```bash
hdfs dfs -ls hdfs://sdp-dzo-lt-omlt/orc_test
hdfs dfs -du -h hdfs://sdp-dzo-lt-omlt/orc_test/S
hdfs dfs -ls hdfs://sdp-dzo-lt-omlt/orc_test/state
```

Удаление уходит в корзину. `-skipTrash` добавляйте после того, как `hdfs dfs -ls` показал ровно те пути, которые собираетесь стереть.

```bash
hdfs dfs -rm -r hdfs://sdp-dzo-lt-omlt/orc_test/S
hdfs dfs -rm -r hdfs://sdp-dzo-lt-omlt/orc_test/state/S__*
```

Каталоги `results/` и `logs/` эта команда не трогает. Промежуточные отчёты прошлых прогонов остаются.

Пустой `hdfs://sdp-dzo-lt-omlt/orc_test` чистить не нужно: `hdfs dfs -ls` просто ничего не покажет в `S/`.

Между generate, materialize, run, report и final данные S и каталог `<runId>` не удаляют. Final читает тот же `metrics.jsonl` и тот же ORC.

### 3.1. Переписать один layout

Маркер и каталог удаляют парой.

```bash
hdfs dfs -ls hdfs://sdp-dzo-lt-omlt/orc_test/S
hdfs dfs -ls hdfs://sdp-dzo-lt-omlt/orc_test/state

hdfs dfs -rm -r hdfs://sdp-dzo-lt-omlt/orc_test/S/<каталог_layout>
hdfs dfs -rm hdfs://sdp-dzo-lt-omlt/orc_test/state/S__<эксперимент>__<вариант>.done
```

`RAW_ARRAY_BASE`, `refs/` и `manifest/` при этом оставляют, если меняется только bloom.

### 3.2. Снять лишние layout’ы и оставить базу

`--cleanup-layouts` удаляет под `.../<scale>/` каталоги перезаписи, кроме `RAW_ARRAY_BASE`, `refs/`, `manifest/` и имён из `--keep-layouts`. Маркеры в `state/` он не удаляет. Следующий materialize снова пропустит эксперимент, пока `*.done` лежит на месте.

Для этого пилота оставляют оба каталога `BLOOM_SESSION_*`. Флаг нужен, если на том же scale остались layout’ы другого прогона (`MINMAX_SORT`, `COMPRESSION` и т.п.).

### 3.3. Старые отчёты и логи

На данные бенчмарка это не влияет. Удаляйте конкретный прогон, когда каталог больше не нужен и архив уже забран:

```bash
hdfs dfs -rm -r hdfs://sdp-dzo-lt-omlt/orc_test/results/sdp-dzo-lt-omlt/<runId>
hdfs dfs -rm -r hdfs://sdp-dzo-lt-omlt/orc_test/logs/sdp-dzo-lt-omlt/<runId>
```

Лог скрипта submit лежит файлом рядом с каталогами `<runId>`: `hdfs dfs -ls hdfs://sdp-dzo-lt-omlt/orc_test/logs/sdp-dzo-lt-omlt`.

---

## Шаг 4. Preflight

Сначала прогон по конфигу, без живых проб HDFS и JDBC:

```bash
./scripts/run-preflight.sh --env-override conf/environment.local.yaml
```

Затем живые пробы (запись в `hdfs://sdp-dzo-lt-omlt/orc_test`, Kerberos, Hive JDBC):

```bash
./scripts/run-preflight.sh --live --env-override conf/environment.local.yaml
```

```bash
yarn application -status application_<id>
```

Дождитесь `SUCCEEDED`. У preflight свой `<runId>`, это не `<runId>` шага 7.

Ожидаемые файлы:

```text
hdfs://sdp-dzo-lt-omlt/orc_test/results/sdp-dzo-lt-omlt/<runId>/capabilities.tsv
hdfs://sdp-dzo-lt-omlt/orc_test/results/sdp-dzo-lt-omlt/<runId>/preflight-notes.txt
hdfs://sdp-dzo-lt-omlt/orc_test/results/sdp-dzo-lt-omlt/<runId>/skipped.tsv
```

Ожидаемые пропуски по контракту: `HIVE_MATERIALIZED_VIEW`, `HDFS_CACHE.centralized`. LLAP до зелёного probe остаётся observational и пилот Spark не блокирует. Дальше идите, когда статус `SUCCEEDED` и в `preflight-notes.txt` нет отказа записи в `hdfs://sdp-dzo-lt-omlt/orc_test`.

Профилирование (`profiling_stage.enabled: false`), проверка baseline по справочному часу и `profiling_validation` в этом preflight не включаются и отдельным шагом не запускаются.

---

## Шаг 5. Generate (scale S)

После шага 3:

```bash
./scripts/run-pilot-s.sh --phase generate \
  --env-override conf/environment.local.yaml
```

Дождитесь `SUCCEEDED`. В конце `driver.log` — `[orc-bench] OK — generate`.

Проверка:

```bash
hdfs dfs -ls hdfs://sdp-dzo-lt-omlt/orc_test/S/RAW_ARRAY_BASE
hdfs dfs -ls hdfs://sdp-dzo-lt-omlt/orc_test/S/refs
hdfs dfs -ls hdfs://sdp-dzo-lt-omlt/orc_test/S/manifest
hdfs dfs -du -h hdfs://sdp-dzo-lt-omlt/orc_test/S/RAW_ARRAY_BASE
```

Ожидаемый результат:

- `RAW_ARRAY_BASE` непустой. Сжатый объём около 9,7 ГБ (репликация этот `du` умножает, смотрите и число реплик). Логический ориентир ~262 ГиБ. Это средний час старой эпохи, не 19 159 061 строк справочного часа.
- Партиция `key=2025-04-14--12`.
- Сжатие ZLIB. Цель страйпа 64 МиБ, цель файла 912 МиБ. Точное число файлов и точный размер страйпа в футере могут отличаться от цели; требование «ровно 7 файлов» к этому часу не относится.
- Порядок полей в футере совпадает со списком выше. Безымянные заполнители обрываются на индексе 20.
- `userLogin` у событий MONITORING остаётся пустой строкой.
- В `refs/` есть справочник модулей. В `manifest/` — описание генерации. Эти файлы run для подстановки литералов не читает.
- Маркеров bloom ещё нет: materialize впереди.

Повторный generate без удаления `S/` перезаписывает базу, справочники и манифест (`SaveMode.Overwrite`). Старые каталоги bloom и их маркеры остаются. После смены порядка колонок так не делают: сначала шаг 3.

---

## Шаг 6. Materialize (Pilot S)

Пишет две копии `BLOOM_SESSION` поверх базы. Часовой ключ и сортировка по `__time` копируются из baseline.

```bash
./scripts/run-pilot-s.sh --phase materialize \
  --env-override conf/environment.local.yaml
```

Эквивалент:

```bash
./scripts/run-benchmark.sh \
  --scale S --phase materialize \
  --experiments BLOOM_SESSION \
  --env-override conf/environment.local.yaml
```

Проверка:

```bash
hdfs dfs -ls hdfs://sdp-dzo-lt-omlt/orc_test/S
hdfs dfs -ls hdfs://sdp-dzo-lt-omlt/orc_test/state
hdfs dfs -du -h hdfs://sdp-dzo-lt-omlt/orc_test/S
```

Ожидаемый результат:

- Два непустых каталога с префиксом `BLOOM_SESSION_bloom_columns_`. Объём каждого близок к `RAW_ARRAY_BASE`: bloom добавляет индекс, а не второй набор строк.
- Два маркера `S__BLOOM_SESSION__….done`.
- В `driver.log` две строки `[materialize] BLOOM_SESSION/bloom_columns=…`.
- Повтор с теми же маркерами новые ORC не пишет: `[materialize] skip (marker)`. Чтобы записать заново — §3.1 и эта команда снова.
- `RAW_ARRAY_BASE` на месте и не переписан.

На диске после этого шага три полных копии часа. С репликацией 2 это около 60 ГБ. Для M и L вторая копия считается отдельно, в разделе про большие масштабы.

---

## Шаг 7. Прогон запросов (Pilot S)

```bash
./scripts/run-pilot-s.sh --phase run \
  --env-override conf/environment.local.yaml
```

Эквивалент:

```bash
./scripts/run-benchmark.sh \
  --scale S --phase run \
  --engines spark \
  --experiments ORC_COLUMN_PRUNING,ORC_PUSHDOWN,BLOOM_SESSION,SPARK_VECTORIZATION \
  --env-override conf/environment.local.yaml
```

Запишите `application_<id>`. Пока приложение `RUNNING`:

```bash
yarn application -status application_<id>
hdfs dfs -tail hdfs://sdp-dzo-lt-omlt/orc_test/logs/sdp-dzo-lt-omlt/<runId>/driver.log
hdfs dfs -tail hdfs://sdp-dzo-lt-omlt/orc_test/results/sdp-dzo-lt-omlt/<runId>/metrics.jsonl
```

`metrics.jsonl` появляется после первой завершённой ячейки и растёт на 35 строк (5 прогревов + 30 замеров) на каждый запрос каждого варианта. В `driver.log` строка `[run] spark …` означает, что ячейка ещё считается.

Ожидаемый объём файла после `SUCCEEDED`:

| Эксперимент | Варианты | Запросы | Строк в metrics.jsonl |
|---|---|---|---|
| `ORC_COLUMN_PRUNING` | один, читает `RAW_ARRAY_BASE` | `Q_SESSION_EQ`, `Q_SESSION_EQ_NARROW` | 70 |
| `ORC_PUSHDOWN` | `false`, `true`, оба читают базу | `Q_SESSION_ILIKE`, `Q_SESSION_EQ`, `Q_EPK_SPARK`, `Q_USERLOGIN` | 280 |
| `BLOOM_SESSION` | `bloom_columns=[]`, `bloom_columns=[session]` | те же четыре | 280 |
| `SPARK_VECTORIZATION` | `false`, `true`, оба читают базу | `Q_SESSION_ILIKE`, `Q_FULLTEXT`, `Q_EPK_SPARK` | 210 |

Итого 840 строк, если ни одна ячейка не ушла в `error` / `skipped`. В конце `driver.log` — `[orc-bench] OK — run complete`. Этот `<runId>` нужен шагам 8 и 9. Отчёт на этом шаге ещё не пишется: нет `SUMMARY.txt` и `summary.csv`.

Общее для всех четырёх экспериментов, если ячейка `status=ok`:

- `orcFileBytes` заполнен и одинаков у запросов одного layout.
- `hdfsBytesRead` — число, в том числе ноль.
- `resultHash` одного `queryId` совпадает у вариантов, которые читают те же строки и ту же проекцию. У `Q_SESSION_EQ` и `Q_SESSION_EQ_NARROW` хеши разные: проекции разные.
- Прогревы в агрегате шага 8 отсутствуют. В `summary.csv` у такой ячейки `n=30`.

### 7.1. `ORC_COLUMN_PRUNING`

Оба запроса фильтруют `session = '…'` и читают базу. Широкий — `SELECT *`, узкий — `session`, `createdAt`, `` `__time` ``.

Ожидание: `logicalRows` совпадает. `hdfsBytesRead` узкого запроса меньше широкого, если отсечение колонок сработало. `p50` узкого меньше широкого при том же предикате. Боевые запросы архива остаются `SELECT *`; узкая проекция в финал не входит.

### 7.2. `ORC_PUSHDOWN`

Один и тот же SQL на базе при `spark.sql.orc.filterPushdown` false и true. Базовый профиль пилота этот флаг держит включённым. Пара false/true — сравнение, она базовый профиль сама не переключает.

Ожидание: `resultHash` пары совпадает. У `Q_SESSION_EQ` вариант `true` может прочитать меньше байт. У `Q_SESSION_ILIKE` предикат `ILIKE` в pushdown не отдаётся, байты двух вариантов близки. `Q_EPK_SPARK` и `Q_USERLOGIN` смотрят по отдельности: знак может отличаться от равенства по `session`. Вывод пилота — таблица пар в `summary.csv` (`scanRatio`, `p50Ms`), а не глобальное выключение pushdown.

### 7.3. `BLOOM_SESSION`

Четыре запроса на двух копиях. Решение по bloom принимают по этой таблице, а не по октябрьскому прогону плоской раскладки.

| Запрос | Что ждать от bloom по `session` |
|---|---|
| `Q_SESSION_EQ` | `hdfsBytesRead` и `scanRatio` на варианте `[session]` ниже, чем на `[]`, если фильтр страйпов сработал. Задержка может снизиться слабее, чем байты. При заглушке `copy2_0`, которой в данных нет, рабочий bloom имеет право пропустить все страйпы: байты падают к чтению индексов, строк 0. Это признак фильтра, а не признак боевой выборки в 3–8 строк. |
| `Q_SESSION_ILIKE` | `ILIKE` bloom не использует. Байты остаются близки к пустому bloom. Падение байт здесь означает разницу файлов, а не пользу фильтра. |
| `Q_EPK_SPARK`, `Q_USERLOGIN` | Предикат не по `session`. Байты двух копий близки. `userLogin` целью bloom не является. |

Вариант оставляют, если у `Q_SESSION_EQ` байты упали, а три остальных запроса не получили большого роста `p50`. Если байты равенства не упали, в финал уходит пустой bloom (или база, если выбор BEST_ORC пуст — см. шаг 9). 3000 мс после этого сравнения — ориентир, который записывают в чеклист, если `p50` `Q_SESSION_EQ` на варианте `[session]` его достиг. Ворота 2 часа при этом остаются прежними.

### 7.4. `SPARK_VECTORIZATION`

`spark.sql.orc.enableVectorizedReader` false и true на базе. Базовый профиль держит векторный читатель включённым. Пара это значение не переписывает.

Ожидание: `resultHash` пары совпадает, `orcFileBytes` совпадает. `p50` варианта `true` на `Q_SESSION_ILIKE`, `Q_FULLTEXT` и `Q_EPK_SPARK` — то, что сравнивают. Итог записывают в чеклист. Пока отдельного решения по этому прогону нет, финал идёт с векторным читателем базового профиля.

---

## Промежуточный сбор после run

Делайте его после `SUCCEEDED` шага 7 и до шага 9. На HDFS данные S и каталог `<runId>` остаются.

`fetch-results.sh` забирает каталог с HDFS только если локального `results/<runId>` ещё нет, и всегда пакует то, что лежит локально. Поэтому имя архива задают явно, а перед повторным забором локальную копию удаляют.

```bash
rm -rf results/<runId>
./scripts/fetch-results.sh --run-id <runId> --out results-<runId>-after-run.tgz
```

В архиве: `metrics.jsonl` (около 840 строк) и нет ещё строк `SPARK_COMMON`. `SUMMARY.txt` появится после шага 8, если report запускают отдельно; после одного только run его нет.

Снимок посередине `RUNNING` — частичный файл. Его кладут в другое локальное имя, чтобы не занять путь `results/<runId>`:

```bash
mkdir -p /tmp/orc-snap
hdfs dfs -get \
  hdfs://sdp-dzo-lt-omlt/orc_test/results/sdp-dzo-lt-omlt/<runId> \
  /tmp/orc-snap/<runId>-partial
```

Полный промежуточный архив — только после `SUCCEEDED`.

Лог драйвера в архив результатов не входит:

```bash
hdfs dfs -get \
  hdfs://sdp-dzo-lt-omlt/orc_test/logs/sdp-dzo-lt-omlt/<runId>/driver.log \
  driver-<runId>-run.log
```

---

## Шаг 8. Отчёт по ячейкам пилота

`report` запросы не исполняет. Он читает `metrics.jsonl` указанного `<runId>`.

```bash
./scripts/run-pilot-s.sh --phase report \
  --run-id <runId> \
  --env-override conf/environment.local.yaml
```

Дождитесь `SUCCEEDED`. В том же каталоге `<runId>` появляются `SUMMARY.txt`, `summary.csv`, `skipped.tsv`.

Ожидаемый результат:

- В `summary.csv` по одной строке на запрос варианта с `n=30`, `p50Ms`, `p95Ms`, `p99Ms`, `slaSuccessRate`, `scanRatio`, `resultHash`, `slaPass`.
- `slaPass` истинен при доле успехов ≥ 0,95 внутри порога 7 200 000 мс.
- Блок BEST_ORC в `SUMMARY.txt`: `experimentId=BLOOM_SESSION` и одна из двух меток bloom. Выбор — меньший средний `p50` запросов `Q_SESSION_EQ` при concurrency 1 и доле успехов ≥ 0,95. `ORC_COLUMN_PRUNING` и `ORC_PUSHDOWN` в кандидаты layout не входят: они не пишут свою копию. ZSTD в этом пилоте нет.
- Если ни одна ячейка `Q_SESSION_EQ` не прошла, в `SUMMARY.txt` будет пометка, что кандидата нет. Финал тогда читает `RAW_ARRAY_BASE` и пишет это в лог.
- `skipped.tsv` пуст или содержит только ожидаемые пропуски capability. Строки `error` в `metrics.jsonl` разбирают до финала.

Этот report можно пропустить: шаг 9 сам пересобирает те же файлы уже вместе со строками финала. Отдельный report полезен, чтобы увидеть BEST_ORC до долгого финального прогона.

---

## Шаг 9. Финал `SPARK_COMMON`

`--phase final` исполняет запросы, затем пишет отчёт. Это отдельное приложение YARN на том же `<runId>`. Данные S и `metrics.jsonl` шага 7 должны лежать на HDFS. Локальный `results/<runId>` на финал не влияет.

```bash
./scripts/run-pilot-s.sh --phase final \
  --run-id <runId> \
  --env-override conf/environment.local.yaml
```

В `driver.log` сначала строка `[final] loaded N prior rows` (N около 840), затем либо

```text
[final] BEST_ORC=BLOOM_SESSION/<метка> layout=...
```

либо

```text
[final] BEST_ORC layout unknown, using baseline .../S/RAW_ARRAY_BASE
```

Дальше 10 запросов профиля `SPARK_COMMON`, concurrency 1, те же 5 прогревов и 30 замеров:

`Q_SESSION_ILIKE`, `Q_SESSION_EQ`, `Q_EPK_SPARK`, `Q_FULLTEXT`, `Q_USERNODE`, `Q_USERLOGIN`, `Q_MODULE_NAME_EPK_FILE`, `Q_OTHER_PARAM`, `Q_ABSENT_PARAM`, `Q_UNFILTERED`.

`Q_SESSION_EQ_NARROW` в этот список не входит.

Ожидаемый результат после `SUCCEEDED`:

- В `metrics.jsonl` к строкам пилота добавлены 350 строк с `experimentId=SPARK_COMMON` (10 × 35). Файл переписывается целиком из объединённого набора, дублей пилота нет.
- `final_comparison.tsv`: заголовок `engineProfile, engine, queryId, layoutPath, n, p50Ms, scanRatio, resultHash, skipReason`. Десять строк измерений, `engineProfile=SPARK_COMMON`, `n=30`, заполненные `p50Ms` и `scanRatio`. Пустые `p50Ms` при пустом `skipReason` означают, что запросы не исполнялись (так бывает у `--dry-run`, не у живого финала).
- `SUMMARY.txt` и `summary.csv` пересобраны по пилоту и финалу вместе.
- `resultHash` финального `Q_SESSION_EQ` совпадает с хешем того же запроса на том же layout в шаге 7.
- `Q_ABSENT_PARAM`: `logicalRows=0`.
- `Q_UNFILTERED`: чтение всего часа, `logicalRows` — полный объём generate. Это самая длинная ячейка финала вместе с `Q_MODULE_NAME_EPK_FILE` и `Q_FULLTEXT`. Порог одной попытки — 2 часа. Превышение даёт провал SLA этой попытки и не отменяет уже снятые ячейки пилота.
- Профиль финала один: `SPARK_COMMON` (векторизация и pushdown как в базовом Spark). Пять октябрьских профилей, включая LLAP, этим шагом не запускаются.

`--dry-run` на final план печатает и запросы не исполняет. В `final_comparison.tsv` тогда остаются строки назначения без `p50Ms`.

---

## Шаг 10. Забрать полный результат

Локальная копия после промежуточного сбора устарела. Её удаляют, каталог на HDFS не трогают.

```bash
rm -rf results/<runId>
./scripts/fetch-results.sh --run-id <runId> --out results-<runId>-after-final.tgz
hdfs dfs -get \
  hdfs://sdp-dzo-lt-omlt/orc_test/logs/sdp-dzo-lt-omlt/<runId>/driver.log \
  driver-<runId>-final.log
```

Список и последний локальный каталог:

```bash
./scripts/fetch-results.sh --list
./scripts/fetch-results.sh --latest
```

С машины разработки:

```bash
scp USER@NODE:/opt/orc-bench/orc-bench/results-<runId>-after-run.tgz .
scp USER@NODE:/opt/orc-bench/orc-bench/results-<runId>-after-final.tgz .
tar -tzf results-<runId>-after-final.tgz
```

После `yarn logs -applicationId application_<id>` тот же текст драйвера доступен из YARN, пока логи приложения хранятся.

Что считать прогоном закрытым:

1. YARN шагов 7 и 9 в статусе `SUCCEEDED`.
2. В логе run есть `[orc-bench] OK — run complete`. В логе final есть `[final] BEST_ORC=…` или явный откат на `RAW_ARRAY_BASE`, затем обычное завершение отчёта.
3. Промежуточный архив содержит метрики пилота. Полный архив содержит их и строки `SPARK_COMMON`, плюс `SUMMARY.txt`, `summary.csv`, `final_comparison.tsv`.
4. В `final_comparison.tsv` у десяти запросов заполнены `n`, `p50Ms`, `scanRatio`.
5. `skipped.tsv` разобран.
6. Чеклист в конце этого файла заполнен. Решение по bloom записано по `scanRatio` `Q_SESSION_EQ`, а не только по строке BEST_ORC.

---

## Очистка после закрытого прогона

Пока идёт сравнение и возможен повтор финала, дерево `S/` оставляют.

Когда место нужно под следующий масштаб, а архивы `after-run` и `after-final` уже скопированы с узла:

```bash
hdfs dfs -rm -r hdfs://sdp-dzo-lt-omlt/orc_test/S
hdfs dfs -rm -r hdfs://sdp-dzo-lt-omlt/orc_test/state/S__*
```

Отчёты удаляют отдельно и только после `scp` (§3.3). Удаление `S/` архив результатов не восстанавливает: в архиве метрики, не ORC.

---

## Hive-трек, отдельно от Pilot S

Пилот Spark этот трек не ждёт. База Hive — Tez и векторизация (`hive.llap.execution.mode=none` у `HIVE_TEZ_BASE`). LLAP — отдельное наблюдение после зелёного probe, базовым профилем он не становится.

Запросы SM_UKO справочник модулей не читают. Приложение всё равно создаёт базу `bench` и таблицу `bench.module_dictionary`, затем делает `DESCRIBE`. Успех виден в `driver.log`. Если `DESCRIBE` не прошёл, пропускаются только запросы, в SQL которых есть `module_dictionary`. Остальные запросы трека идут дальше. Имя таблицы в Hive — два идентификатора в кавычках (`bench` и `module_dictionary`), а не один идентификатор с точкой.

Запускать после generate того масштаба, который читаете. DDL указывает на новые файлы. На ORC прежнего порядка колонок этот трек не направляют.

```bash
./scripts/run-benchmark.sh \
  --scale S --phase run \
  --engines hive_tez \
  --experiments HIVE_VECTORIZATION,HIVE_TEZ_BASE \
  --env-override conf/environment.local.yaml
```

`HIVE_CBO` в контракте исключён (`excluded_from_sm_uko`, список запросов пуст). В `skipped.tsv` для него ожидается причина исключения. Строк измерений CBO нет.

Ожидание по ячейкам, которые реально исполняются:

| Эксперимент | Запросы | Что смотреть |
|---|---|---|
| `HIVE_VECTORIZATION` | `Q_SESSION_EQ`, `Q_USERLOGIN`, `Q_EPK_HIVE`, значения false и true | хеш пары совпадает; базовый профиль векторизацию не выключает по одной паре |
| `HIVE_TEZ_BASE` | `Q_SESSION_LIKE_HIVE`, `Q_SESSION_EQ`, `Q_USERLOGIN`, `Q_UNFILTERED` | concurrency 1 — измерение нагрузки. Уровни 5 и 9 приложение ставит только этому эксперименту |

Concurrency больше 1 делит Tez-сессию пула HS2. Клиент пул не выключает. Приложение дополнительно пытается выставить `tez.am.container.reuse.enabled=false` и `hive.prewarm.enabled=false`. Если в ошибке слота есть `DAG_KILL`, `DAG_TERMINATE` или `FileNotFound` на `_tez_session_dir` / `hive-hcatalog-core.jar`, вся группа этого concurrency из `summary.csv` выбрасывается, включая удачные слоты той же группы. В `SUMMARY.txt` появляется заметка `concurrency load omitted: DAG_KILL or missing Tez session jar`. Такая строка не является пропускной способностью. В отчёте остаётся concurrency 1.

`hdfsBytesRead` у Hive берётся из журнала запроса (`HDFS_BYTES_READ`). Если счётчика в журнале нет, поле пустое, ноль туда не пишется.

LLAP, когда probe зелёный:

```bash
./scripts/run-benchmark.sh \
  --scale S --phase run \
  --engines hive_llap \
  --experiments HIVE_LLAP \
  --env-override conf/environment.local.yaml
```

Запросы: `Q_SESSION_LIKE_HIVE`, `Q_SESSION_EQ`, `Q_USERLOGIN`, `Q_UNFILTERED`, `Q_FULLTEXT`, состояния cold и warm. Пока probe не зелёный, ячейки пропускаются по `llap_daemons_running` / `llap_task_execution_verified`, и это ожидаемый skip, а не провал Spark-пилота.

Отчёт Hive — тот же `--phase report --run-id` на `<runId>` этого приложения. В BEST_ORC он попадает только если в метриках есть `Q_SESSION_EQ` на layout-кандидате. Для чтения уже записанного `RAW_ARRAY_BASE` без своих копий блок BEST_ORC может остаться пустым: смотрите `summary.csv`.

---

## Что этим пакетом не запускают

- Набор октября `Q01`–`Q10`, плоский каталог, `event_date` без `event_hour`, ворота 3 секунды на `Q02`/`Q03`/`Q04`. Другой контракт и другой jar-профиль.
- `profiling-suite.yaml` и `spark_queries.sql`. Флаг `profiling_stage.enabled` остаётся `false`. Включать его для этого пилота не нужно: исполнителя профайлинга в приложении нет.
- `baseline_validation` и `profiling_validation`. Оба выключены. Справочный час с новым ключом не является целью размера S.
- Эксперименты вне списка пилота: `MINMAX_SORT`, `BLOOM_FPP`, `ROW_INDEX`, `STRIPE_SIZE`, `COMPRESSION`, `SPARK_AQE`, `SPARK_ORC_READER`, `SPARK_NESTED_VECTORIZATION`. Их можно поставить отдельным `--experiments` после разбора S. `SPARK_DPP`, `SPARK_CBO`, `SPARK_BROADCAST` исключены из SM_UKO.
- Смешивать в одном `--phase run` движки `spark` и `hive_tez`. У них разные `<runId>`, если запускать двумя командами, и так и нужно.

---

## Scale M и L

Те же фазы и те же четыре эксперимента Spark. `run-pilot-s.sh` всегда ставит `--scale S`, поэтому здесь `run-benchmark.sh`.

M — сутки старой эпохи, 24 часа начиная с `2025-04-14`. Ориентир около 233 ГБ сжатых, логический объём ~6291 ГиБ. С репликацией 2 одна копия около 466 ГБ. Bloom — вторая такая копия.

L — 120 часов, `2025-04-14` включительно и `2025-04-19` не включая. Ориентир около 1165 ГБ сжатых. С репликацией 2 одна копия около 2,3 ТиБ. Бюджет тестовых данных около 3 ТиБ с репликами. База L и bloom L вместе в этот бюджет не входят. Перед L снимают S и M, если они ещё лежат, и смотрят `hdfs dfs -df -h`.

Перед generate каждого масштаба, если каталог уже есть, его удаляют так же, как S: порядок колонок должен совпасть с новым jar.

```bash
hdfs dfs -rm -r hdfs://sdp-dzo-lt-omlt/orc_test/M
hdfs dfs -rm -r hdfs://sdp-dzo-lt-omlt/orc_test/state/M__*
./scripts/run-benchmark.sh --scale M --phase generate \
  --env-override conf/environment.local.yaml
./scripts/run-benchmark.sh --scale M --phase materialize \
  --experiments BLOOM_SESSION \
  --env-override conf/environment.local.yaml
./scripts/run-benchmark.sh --scale M --phase run \
  --engines spark \
  --experiments ORC_COLUMN_PRUNING,ORC_PUSHDOWN,BLOOM_SESSION,SPARK_VECTORIZATION \
  --env-override conf/environment.local.yaml
```

Для L те же команды с `--scale L` и путями `L` / `L__*`. Промежуточный fetch и final — как у S, со своим `<runId>`. Уровни concurrency 5 и 9 на этих экспериментах по-прежнему не измеряются: это не `SPARK_BASE`.

M и L запускают после разбора архива S. Числа S на них сами не переносятся.

---

## Поля отчёта

| Файл | Содержание |
|---|---|
| `metrics.jsonl` | Строка JSON на измерение: `runId`, `experimentId`, `variantLabel`, `engine`, `queryId`, `repetition`, `warmup`, `concurrency`, `cacheState`, `latencyMs`, `status`, `resultHash`, `logicalRows`, `orcFileBytes`, `hdfsBytesRead` |
| `summary.csv` | Агрегат ячейки `status=ok` и `warmup=false`: `n`, `avgMs`, `p50Ms`, `p95Ms`, `p99Ms`, `slaSuccessRate`, `scanRatio`, `resultHash`, `slaPass`. Группа concurrency>1 с `DAG_KILL` или пропавшим jar сессии Tez в файл не попадает |
| `skipped.tsv` | Эксперимент и причина пропуска |
| `SUMMARY.txt` | Счётчики, порог 7 200 000 мс, блок BEST_ORC, заметка про выброшенный concurrency Hive |
| `final_comparison.tsv` | Одна строка на запрос `SPARK_COMMON`: профиль, движок, `queryId`, путь layout, `n`, `p50Ms`, `scanRatio`, `resultHash`, `skipReason` |
| `capabilities.tsv`, `preflight-notes.txt` | Только фаза preflight |

---

## Скрипты

| Скрипт | Где | Назначение |
|---|---|---|
| `with-jdk8.sh`, `build.sh` | Машина разработки | Fat-jar на JDK 8 |
| `package-for-cluster.sh` | Машина разработки | Архив jar + `conf/` + `scripts/`. Runbook — с `--with-runbook` |
| `run-benchmark.sh` | Узел | Единственная сборка `spark-submit` |
| `run-pilot-s.sh` | Узел | Pilot S: `--phase` и списки из `pilot-s.yaml`, дальше `run-benchmark.sh`. Масштаб всегда S |
| `run-preflight.sh` | Узел | `--phase preflight`; без `--live` добавляет `--dry-run` |
| `spark-submit-orc-bench.sh` | Узел | Тот же submit. Аргументы `Main` после `--`, печать команды — `--dry-run`, master всегда `yarn` |
| `fetch-results.sh` | Узел | `hdfs dfs -get`, если локального каталога нет, и упаковка в `.tgz` |
| `kinit-bench.sh` | Узел, по желанию | `kinit -kt`. Основной путь runbook на него не опирается |
| `lib/common.sh` | библиотека | YAML, jar, sizing, шапка `--help` |
| `lib/logging.sh` | библиотека | Tee в `logs/` и копия на HDFS |
| `lib/cluster-dist.sh` | библиотека | Локальные `--env-override` и `--metrics-jsonl` → `--files` |
| `lib/detach-submit.sh` | библиотека | `setsid`/`nohup`, выход после `application_<id>` |

Низкоуровневый вызов:

```bash
./scripts/spark-submit-orc-bench.sh \
  --deploy-mode cluster -- \
  --config conf --scale S --phase preflight \
  --env-override conf/environment.local.yaml
```

Прямой `spark-submit` (обёртка собирает это сама; `--files` обязателен вручную):

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --queue default \
  --num-executors 10 \
  --executor-cores 4 \
  --executor-memory 16g \
  --driver-cores 1 \
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

`--conf spark.yarn.submit.waitAppCompletion=false` нужен, чтобы клиент вернулся после постановки приложения в YARN.

---

## Если что-то пошло не так

| Симптом | Что сделать |
|---|---|
| `application_<id>` есть, SSH уже закрыт | `yarn application -status application_<id>` и `hdfs dfs -tail` лога драйвера |
| Долго нет `metrics.jsonl` | Файл создаётся после первой ячейки. Смотрите `driver.log`: `[run] spark …` значит, ячейка ещё считается |
| `Java heap space` / `TaskResultLost` на `SELECT *` | На узле старый jar: число строк и хеш должны считаться на экзекуторах. Память драйвера профиля common — 2g |
| Generate обрывается на `rows≈37509381`, в `driver.log` нет `[fatal]` | Задача копила всю партицию (~3,75 млн строк) в список на экзекуторе 3g. Нужен jar, где `RowPartitionMapper` отдаёт строки по одной. Повторный generate — после удаления `S/` |
| Hive читает колонки не с тех позиций | Файлы записаны прежним порядком. Шаг 3, затем generate заново. Новую DDL на старый каталог не вешают |
| Materialize сразу `skip (marker)` | Маркер уже есть. Для перезаписи — §3.1 |
| Generate прошёл, materialize пишет, что нет `RAW_ARRAY_BASE` | `hdfs dfs -ls hdfs://sdp-dzo-lt-omlt/orc_test/S/RAW_ARRAY_BASE` |
| `Permission denied` на `hdfs://sdp-dzo-lt-omlt/orc_test` | Права principal: `hdfs dfs -ls` и пробная запись preflight `--live` |
| `client deploy mode is disabled` | В `environment.local.yaml` стоит `deploy_mode: cluster` |
| `User did not initialize spark context` | На узле старый jar. Заново скопируйте `orc-bench-0.1.0-SNAPSHOT-all.jar` |
| `UnsupportedClassVersion` | `java -version` на узле — 1.8.x |
| Нет локального `results/<runId>` | Для cluster mode это нормально до fetch. Каталог на HDFS |
| Второй fetch собрал архив без финала | Локальный `results/<runId>` уже был. `rm -rf results/<runId>` и fetch с новым `--out`. Каталог на HDFS не удалять |
| Preflight: мало места | `hdfs.free_space_gib` в `environment.local.yaml` против `hdfs dfs -df -h` |
| Много skipped по LLAP | Для Pilot S движок — spark. LLAP — отдельная команда после зелёного probe |
| В `summary.csv` нет concurrency 5 у Hive | Группа снята из-за `DAG_KILL` или пропавшего jar сессии. Это не пропускная способность. Остаётся concurrency 1 и заметка в `SUMMARY.txt` |
| `DESCRIBE bench.module_dictionary` не прошёл | Запросы SM_UKO справочник не используют и продолжаются. Почините DDL до любого SQL, где таблица есть в тексте |
| Финал написал путь и пустые `p50Ms` | Запускали `--dry-run` либо BEST_ORC не найден и чтение базы не стартовало. В живом логе ищите `[final] BEST_ORC` |
| Очередь отклонила приложение | Очередь `default` |
| Все `Q_SESSION_EQ` вернули 0 строк | Ожидаемо для заглушки `copy2_0` в отдельном `--phase run`. Сравнивайте хеш и `hdfsBytesRead` |

Повтор упавшей фазы: дождитесь `FAILED` или `KILLED`, посмотрите хвост `driver.log`. Generate после шага 3 можно запустить снова. Materialize и run безопасны для повтора: materialize пропускает готовые маркеры, run создаёт новый `<runId>`. Final повторяют с тем же `--run-id`, когда `metrics.jsonl` пилота на месте. Повторный final дописывает измерения `SPARK_COMMON` в этот каталог.

---

## Чеклист после Pilot S

### Старт

| Параметр | Старт |
|---|---|
| Scale | S, час `2025-04-14--12`, ~9,7 ГБ сжатых |
| Executors | 10 / 4 / 16g, overhead 512m, driver 1 / 4g |
| `deploy_mode` | cluster, очередь `default` |
| Concurrency ячеек пилота | 1 |
| Порядок колонок | футер `schema.orc.txt`, старый `S/` удалён до generate |

### Наблюдения

| Поле | Значение |
|---|---|
| `<runId>` шага 7 | ________________ |
| `du` `RAW_ARRAY_BASE` | ________________ |
| generate, время YARN | ________________ |
| materialize, время и два каталога bloom | ________________ |
| run, время; строк в `metrics.jsonl` | ________________ |
| `scanRatio` `Q_SESSION_EQ` на bloom `[]` и на `[session]` | ________________ / ________________ |
| `p50` узкого и широкого `Q_SESSION_EQ` | ________________ / ________________ |
| Pushdown true против false: где байты упали | ________________ |
| Векторизация true против false: `p50` трёх запросов | ________________ |
| BEST_ORC (`experimentId` / метка / путь) | ________________ |
| Финал: 10 строк `final_comparison.tsv` с `n=30` | yes / no |
| `p50` финального `Q_SESSION_EQ` (ориентир 3000 мс — отдельно от ворот 2 ч) | ________________ |
| GC / spill / OOM | ________________ |
| Промежуточный архив | `results-<runId>-after-run.tgz` |
| Полный архив | `results-<runId>-after-final.tgz` |

### Решение

| Вопрос | Запись |
|---|---|
| Bloom по `session` уменьшил `scanRatio` у `Q_SESSION_EQ` | yes / no |
| `Q_SESSION_ILIKE` байты сохранил | yes / no |
| Финал шёл по layout из лога `[final] BEST_ORC` | путь: ________________ |
| Масштаб M | yes / no / later |
| Масштаб L | yes / no / later |
| Hive-трек отдельно | yes / no / later |

---

## Справочник флагов `orcbench.Main`

Флаги передаются через `run-benchmark.sh` / `run-pilot-s.sh` (у `spark-submit-orc-bench.sh` — после `--`).

| Флаг | По умолчанию | Смысл |
|---|---|---|
| `--config` | `conf` | Каталог YAML |
| `--env-override` | — | Overlay, обычно `conf/environment.local.yaml` |
| `--scale` | `S` | `S` один час (~9,7 ГБ), `M` сутки (~233 ГБ), `L` 120 часов (~1165 ГБ) |
| `--phase` | `all` | `preflight`, `generate`, `materialize`, `run`, `report`, `final`, `all`. Живой `final` исполняет `SPARK_COMMON` |
| `--experiments` | все подходящие | Список id через запятую. Пилот подставляет свой |
| `--engines` | `spark,hive_tez,hive_llap` | Пилот на `run` подставляет `spark` |
| `--results-dir` | `hdfs://sdp-dzo-lt-omlt/orc_test/results/sdp-dzo-lt-omlt` на YARN | Корень отчётов. `<runId>` добавляет приложение |
| `--run-id` | новый id | Для `report` и `final` — id прогона шага 7 |
| `--metrics-jsonl` | — | Явный файл метрик вместо `<results>/<runId>/metrics.jsonl` |
| `--dry-run` | выкл | План без записи HDFS и без замеров. Final при этом запросы не исполняет |
| `--cleanup-layouts` | выкл | После materialize удалить лишние layout’ы (§3.2) |
| `--keep-layouts` | — | Что оставить при `--cleanup-layouts` |
| `--mock-generate`, `--mock-materialize`, `--mock-run` | выкл | Память процесса, для локальных тестов |
| `--help`, `--version` | — | Справка |

Флаги обёртки (в `Main` не попадают): `--jar`, `--deploy-mode`, `--queue`, `--num-executors`, `--executor-cores`, `--executor-memory`, `--driver-memory`, `--principal`, `--keytab`, `--name`, `--dry-run-submit`. Переменные с тем же смыслом: `ORC_BENCH_JAR`, `ORC_BENCH_DEPLOY_MODE`, `ORC_BENCH_QUEUE`, `ORC_BENCH_NUM_EXECUTORS`, `ORC_BENCH_EXECUTOR_CORES`, `ORC_BENCH_EXECUTOR_MEMORY`, `ORC_BENCH_DRIVER_MEMORY`, `ORC_BENCH_PRINCIPAL`, `ORC_BENCH_KEYTAB`, `ORC_BENCH_APP_NAME`, `ORC_BENCH_ENV_YAML`, `ORC_BENCH_LOG_DIR`, `ORC_BENCH_LOG_FILE`. `--name` — подпись в YARN. `--driver-memory` у обёртки по умолчанию может быть `4g`; для этого пилота в `environment.local.yaml` оставляют 2g профиля common.
