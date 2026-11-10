# Проблемы производительности подсистемы хранения Единого аудита

Контекст: рост нагрузки 6,000 → 2,500,000 запросов/мес (417×) при требованиях цЮЗА.
Источники: профиль нагрузки PROD (180 дней), helm-конфигурации ARQS PROD, код ARQS/abyss.

---

## P-001. Ограничение скорости сабмита в YARN — 5 запросов за тик

**Статус:** подтверждено
**Критичность:** критическая
**Репозиторий:** arqs (archive-query)

### Симптом

Пропускная способность сабмита: ~5 запросов/мин = ~216,000/мес.
Требуется: 2,500,000/мес. Дефицит: ~12×.

### Цепочка сабмита

```
Пользователь → BUSE → ARQS API (POST /async/tasks) → статус NEW в БД
  ↓
ArchiveQuerySubmitScheduler.submitQueries()     [@Scheduled fixedDelay=60s, @SchedulerLock 1 инстанс/zone]
  ↓
maxQueriesPerTick = 5                            [pvm.service.spark.submit.thread-pool-size:5, default]
  ↓
SparkQueryApplicationYarnLauncher.launchAsync()  [FixedThreadPool(5) — тот же параметр]
  ↓
awaitSubmitted() — poll YARN RM каждые 500ms     [timeout=60с]
  ↓
YARN Resource Manager
```

### Параметры

| Параметр | Значение | Источник | Код |
|---|---|---|---|
| `querySubmitIntervalSec` | 60 | `values-arqs-archive-query.yaml:673` (`submit_interval_sec: '60'`) | `ArchiveQuerySubmitScheduler.java:144` |
| `pvm.service.spark.submit.thread-pool-size` | 5 (Java default) | **не задан в PROD helm** | `ArchiveQuerySubmitScheduler.java:117`, `SparkConfiguration.java:32` |
| `applicationSubmissionTimeoutSec` | 60 | `values-arqs-archive-query.yaml` (`application_submission_timeout_sec: '60'`) | `SparkQueryApplicationYarnLauncher.java:225` |
| `@SchedulerLock` | 1 инстанс/zone | `ArchiveQuerySubmitScheduler.java:149` | ShedLock, БД-блокировка |
| `scheduling_pool_size` | 10 | `values-arqs-archive-query.yaml:585` | Spring `@Scheduled` pool (не лимитирует сабмит) |

### Почему 5

`maxQueriesPerTick` и `sparkSubmitThreadPoolSize` — **один и тот же параметр** (`pvm.service.spark.submit.thread-pool-size`).

Комментарий в коде (`ArchiveQuerySubmitScheduler.java:177-179`):
> За тик запускаем не больше maxQueriesPerTick запросов, остальные уйдут на следующем тике,
> иначе хвост очереди ждал бы свободный поток пула и ложно падал по таймауту.

Логика: если запустить 50 запросов в пул из 5 потоков, 45 встанут в очередь. `awaitSubmits` ждёт с общим дедлайном 60с. Если первые 5 займут по 10с, оставшиеся могут не успеть — ложный таймаут. Поэтому `maxQueriesPerTick = sparkSubmitThreadPoolSize` — каждый запрос сразу получает поток.

### Можно ли просто увеличить?

**Да, параметр не задан в PROD helm — нужно только добавить.** Но есть вторичные эффекты:

| Эффект | Оценка | Блокер? |
|---|---|---|
| Нагрузка на YARN RM | N concurrent HTTP POST — RM держит сотни | нет |
| HDFS staging (upload driver JAR) | N concurrent загрузок JAR в `spark.yarn.stagingDir` | нет (JAR один, переиспользуется) |
| Heap ARQS | каждый поток держит SparkYarnClient + FileSystem ~100MB | при 50 потоках = ~5GB — нужен `-Xmx` ARQS |
| БД: `getQueriesByStatusesAndZoneId` | SELECT всех non-final запросов за тик | при росте очереди — тяжёлый SELECT, нужен LIMIT |
| `fixedDelay=60s` | даже с pool=50 → 50/мин = 2.16M/мес | нужно ещё уменьшить interval до 10-15с |

### Расчёт целевых значений

Целевая нагрузка: 2,500,000/мес, пик 8ч/день → 10,417/ч = 174/мин.

| `submit_interval_sec` | `thread-pool-size` | Запросов/мин | Запросов/мес | Достаточно? |
|---|---|---|---|---|
| 60 | 5 (тек.) | 5 | 216K | нет (12× дефицит) |
| 60 | 50 | 50 | 2.16M | почти (1.16× дефицит) |
| 60 | 100 | 100 | 4.32M | да (1.7× запас) |
| 15 | 50 | 200 | 8.64M | да (3.5× запас) |
| 10 | 30 | 180 | 7.78M | да (3.1× запас) |

Рекомендация: `thread-pool-size=50`, `submit_interval_sec=15` → 200/мин = 8.64M/мес (3.5× запас).

### Что нужно сделать

1. Добавить `pvm.service.spark.submit.thread-pool-size` в PROD helm `application.properties` (сейчас только Java default).
2. Уменьшить `submit_interval_sec` с 60 до 15.
3. Проверить `-Xmx` ARQS-приложения (50 потоков × ~100MB).
4. Добавить LIMIT в `getQueriesByStatusesAndZoneId` (брать не больше N новых за тик из БД).
5. Убедиться, что ShedLock не блокирует при `fixedDelay=15s` (`lockAtMostFor` = 10m — может устареть).

### Снято ограничение?

Частично. Даже без ограничения сабмита остаются:
- **P-002** — YARN concurrency (8,333 одновременных Spark-приложений)
- **P-003** — длительность запроса (10.5ч → 30 мин, 21× ускорение)

---

## P-002. Недостаточная параллельность YARN-кластера

**Статус:** подтверждено
**Критичность:** критическая
**Репозиторий:** arqs (archive-query)

### Симптом

При целевом SLA (80% ≤30 мин, 20% ≤2ч, avg 48 мин) и пиковой нагрузке 8ч/день:
- нужно **8,333 одновременных Spark-приложений** в YARN
- требуется **122K cores, 210 TB RAM**

Большой кластер (200 nodes × 32 cores × 256GB) = 6,400 cores. **Перегрузка 19×.**

### Ресурсы на один Spark-запрос (PROD)

| Параметр | Значение | Источник |
|---|---|---|
| `spark.executor.instances` | 4 (база) | `application.properties:485` |
| `spark.executor.cores` | 2 | `application.properties:489` |
| `spark.executor.memory` | 3g + 512m overhead | `application.properties:491,500` |
| `spark.driver.cores` | 1 | `application.properties:497` |
| `spark.driver.memory` | 2g | `application.properties:498` |
| **Итого (базовый)** | **9 cores, 16 GB** | |
| **Итого (масштаб 2×, complexity 100-200)** | **17 cores, 30 GB** | `executor_scaling: threshold=100, coeff=2` |
| **Итого (масштаб 3×, complexity >200)** | **25 cores, 44 GB** | `executor_scaling: threshold=200, coeff=3` |

Масштабирование включается по complexity: `ARRAY_POSITION` coefficient=2, `volume_step=500MB`, `volume_coefficient=0.5`.

### Расчёт (Little's Law: L = λ × W)

| Сценарий | Длительность | Приложений (8ч пик) | Cores | RAM |
|---|---|---|---|---|
| Текущий цЮЗА (10.5ч) | 37,800с | 109,375 | 1,596,875 | 2,756 TB |
| Целевой SLA (48 мин) | 2,880с | 8,333 | 121,667 | 210 TB |
| Целевой SLA 80% (30 мин) | 1,800с | 5,208 | 76,042 | 131 TB |

### Что нужно сделать

Требуется масштабирование кластера YARN + снижение длительности запроса (P-003).
Без P-003 кластер нереализуем (1.6M cores). С P-003 — 122K cores (~19 больших кластеров, или fewer с оптимизацией запросов).

---

## P-003. Длительность поиска — 10.5ч при требовании ≤30 мин

**Статус:** подтверждено
**Критичность:** критическая
**Репозиторий:** arqs, abyss

### Симптом

Текущая длительность поиска цЮЗА: avg 10.5ч, median 12ч.
Требование: 80% ≤30 мин, 20% ≤2ч. Нужно ускорение **21×**.

### Профиль нагрузки (PROD, 180 дней)

| Метрика | Значение |
|---|---|
| Median (BigData) | 182с (3 мин) |
| Median (сентябрь, аномальный пик) | 1,119с (19 мин) |
| p90 (BigData) | 9,065с (2.5 ч) |
| Max | 604,812с (~7 сут) |
| Запросов со сканом >1 TB | 3,988 (сентябрь: 3,390) |

### Bottleneck: ARRAY_POSITION по массивам в ORC

6,131 запросов (23%) ищут по `paramValue[ARRAY_POSITION(paramName, 'target') - 1]`.
ORC-индексы не работают по массивам — **full row scan** каждой строки.
64% этих запросов без сужения по module — median 1,552–2,189с.
С фильтром по module: median 86с (ускорение 18×).

### Паттерны запросов (сентябрь 2026)

| Паттерн | Доля | Median, с | Характер |
|---|---|---|---|
| ARRAY_LOOKUP (поиск по параметрам) | 38% | 115 | Full row scan по массивам |
| FULL_TEXT_LIKE (по всем колонкам) | 14% | 0 | Скан 16+ колонок |
| LIKE_single (по конкретному полю) | 28% | — | Точечный поиск |
| TAIL (хвост таблицы) | 1% | 67 | Без WHERE |
| TIME_RANGE_ONLY | 7% | 98 | Только интервал |

### Что нужно сделать

1. **Материализация массивов** `paramName → paramValue` — вытянутая таблица с bloom-filter / predicate pushdown. Ускорение до 10× для 38% запросов.
2. **Обязательный фильтр по module** в UI — ускорение до 18× для тяжёлых запросов.
3. **Bloom-filter на колонки** в ORC — `OrcConf.BLOOM_FILTER_COLUMNS` (частично настроено на time field).
4. **Ограничение скана >1 TB** — защита от массовых выгрузок.
5. **Индексация атрибутов цЮЗА** (9 новых идентификаторов: uza_authentification_id и др.) — для Режима 1 (новый фонд).

---

## P-004. Динамическая schema не поддерживается в archive_service

**Статус:** подтверждено
**Критичность:** средняя
**Репозиторий:** abyss

TODO в `ArchiveFlowBuilderImpl.java:819`: "dynamic schema now is not supported".
DATE-поля в dynamic fields не конвертируются в UTC (`ArchiveFlowBuilderImpl.java:417`).

Влияет на добавление новых атрибутов цЮЗА (9 идентификаторов) в события аудита.

---

## P-005. PartitionGranularity.value не используется

**Статус:** подтверждено
**Критичность:** низкая
**Репозиторий:** abyss

`PartitionGranularity.java:16-17`: TODO Chernov — "algorithm for granularity with value not clear".
Только HOUR/DAY/WEEK/MONTH без множителя (нельзя "каждые 2 часа").

---

## P-006. DLQ для HdfsOrc sink — workaround

**Статус:** подтверждено
**Критичность:** средняя
**Репозиторий:** abyss

`ObjectNodeVectorizer.java:86-93` (PVM-32669): ошибки векторизации обрабатываются через проброс исключения наверх, а не через штатный DLQ. Риск потери данных при ошибке записи в ORC.

---

## Сводка барьеров для целевой нагрузки 2.5M запросов/мес

| Барьер | Дефицит | Можно решить конфигурацией? |
|---|---|---|
| P-001: Сабмит в YARN | 12× | да (helm-параметры) |
| P-002: YARN concurrency | 19× | нет (масштаб кластера + P-003) |
| P-003: Длительность поиска | 21× | нет (архитектурные изменения) |

Все три нужно решать одновременно. Без P-001 остальные не имеют смысла (очередь копится). Без P-003 кластер YARN нереализуем.

---

# Варианты оптимизации поисковых запросов и масштабирования

## Уровень 1 — Конфигурация (быстрые wins, без изменения кода)

| # | Оптимизация | Эффект | Что менять | Риск |
|---|---|---|---|---|
| **1a** | **Пул сабмита** `thread-pool-size=50`, `submit_interval_sec=15` | 5/мин → 200/мин (40×) | helm `values-arqs-archive-query.yaml` | Heap ARQS +5GB, SELECT БД |
| **1b** | **Параметры Spark** `executor.instances=8`, `executor.cores=4`, `executor.memory=6g` | ~2× throughput на запрос | `application.properties:485-491` | Больше ресурсов на запрос |
| **1c** | **AQE + advisoryPartitionSize** | Уже включён, но размер партиции может быть неоптимален | helm `spark.adaptive.*` | низкий |
| **1d** | **`bucketSize` и `parallelism`** — дефолты 20 и 5 | `parallelism=10` → 2× параллельных бакетов внутри Spark | `SparkProperties.java:51-52` или в параметрах запроса | Heap executor |
| **1e** | **`spark.sql.adaptive.coalescePartitions`** | меньше партиций при малом результате → меньше overhead | helm `spark.adaptive.coalescePartitions.*` | низкий |

## Уровень 2 — Архитектурные изменения (код)

| # | Оптимизация | Эффект | Что делать | Сложность |
|---|---|---|---|---|
| **2a** | **Материализация массивов параметров** | ARRAY_LOOKUP: full row scan → index lookup. **10× для 38% запросов** | Flink: доп. sink с вытянутой таблицей `event_id, param_name, param_value` + bloom. ARQS: `ARRAY_POSITION` → JOIN | высокая |
| **2b** | **Индексация атрибутов цЮЗА** | Режим 1: поиск по 9 ID за <30с вместо скана | Bloom-filter / secondary index на `uza_authentification_id` и др. | средняя |
| **2c** | **Multi-index поиск** | 1 запрос → группа источников (~120 индексов) вместо 120 отдельных | `SparkSqlQueryParallelExecutor`: множественные `sourcePath` в одном Spark-приложении | средняя |
| **2d** | **Bloom-filter на ORC** | Predicate pushdown: skip stripes по `module`, `name` без чтения row data | `orcConfiguration` → `OrcConf.BLOOM_FILTER_COLUMNS` на `module, name, paramName, __time` | низкая |
| **2e** | **Сортировка ORC по `module, name`** | Stripe-level skip по module: min/max в stripe metadata | `orcConfiguration` → `orc.sort.columns = module, name` + backfill | средняя |
| **2f** | **Обязательный фильтр `module`** | 18× для тяжёлых запросов (1,552с → 86с) | BUSE: в UI требовать choice module. ARQS: reject без module (feature-flag) | низкая |
| **2g** | **Ограничение скана >1 TB** | Защита кластера от массовых выгрузок (3,988 запросов >1 TB) | `ComplexityProperties`: hard-fail при `volume_bytes > threshold` | низкая |
| **2h** | **Параллельный сабмит с нескольких инстансов** | Снять ShedLock-ограничение (1 инстанс/zone) | Шардинг по project/hash вместо `@SchedulerLock` | средняя |

## Уровень 3 — Масштабирование инфраструктуры

| # | Оптимизация | Эффект | Что делать |
|---|---|---|---|
| **3a** | **Масштаб YARN-кластера** | Больше одновременных Spark-приложений | При SLA 48 мин: 122K cores = ~19 кластеров. При 2a+2b: ~3K cores = 1 кластер |
| **3b** | **Capacity Scheduler** | Изоляция цЮЗА-нагрузки от обычной | YARN: очередь `uza` с гарантированным % ресурсов |
| **3c** | **Отдельный ARQS для цЮЗА** | Hard-изоляция сабмита, БД, metrics | Развёртывание второго ARQS с `zoneId=UZA` |
| **3d** | **Кэш результатов** | Идемпотентные запросы — instant | Redis/PG: `hash(SQL+params+sources) → result_path` |

## Эффект комбинаций

| Комбинация | Длительность | Concurrency | Кластер |
|---|---|---|---|
| Текущее | 10.5ч | 109K приложений | 1.6M cores — нереализуемо |
| **1a** (сабмит) | 10.5ч | 109K | 1.6M cores — кластер нереализуемо |
| **1a + 2f** (module) | ~1ч (18×) | 10.4K | 152K cores — ~24 кластера |
| **1a + 2f + 2a** (материализация) | ~6 мин (100×) | 1.0K | 15K cores — ~3 кластера |
| **1a + 2f + 2a + 2b** (индексы цЮЗА) | ~30с (1260×) | 87 | 1.3K cores — 1 кластер |

---

# Детальные планы выбранных направлений

## 2a. Материализация массивов параметров

### Проблема

Событие аудита хранит параметры в массивах `paramName[]` / `paramValue[]` внутри ORC. Поиск:
```sql
paramValue[ARRAY_POSITION(paramName, 'targetField') - 1] LIKE '?'
```
ORC-индексы не работают по элементам массива — **full row scan** каждой строки. Это 38% запросов (сентябрь 2026), median 115с, max 350,026с.

### Вариант A — «Вытянутая» таблица рядом с ORC

При записи события в HDFS (Flink job) дополнительно писать вторую ORC-таблицу — по одной строке на каждый параметр:

```
Таблица событий (текущая):       Таблица параметров (новая):
┌──────────┬──────────┬──────────────┐    ┌──────────┬────────────┬─────────────┐
│ __uuid   │ module   │ paramName[]  │    │ __uuid   │ param_name │ param_value │
│ event_1  │ payment  │ [payId,amt]  │    │ event_1  │ payId      │ 123456789   │
└──────────┴──────────┴──────────────┘    │ event_1  │ amt        │ 1000.00     │
                                          └──────────┴────────────┴─────────────┘
```

**Поиск:** вместо `ARRAY_POSITION` — JOIN:
```sql
SELECT e.* FROM events e
JOIN params p ON e.__uuid = p.__uuid
WHERE p.param_name = 'payId' AND p.param_value LIKE '123%'
```

**Эффект:**
- Bloom-filter на `param_name` → skip stripes без нужного параметра
- Predicate pushdown на `param_value` → сканирование только нужных строк
- Ускорение: **~10×** (оценка из профиля нагрузки)

**Реализация:**
1. **Flink flow_service** — новый processing node после `CopyAuditParams`: `FlattenParamsNode` — разворачивает массивы в плоские строки. Новый sink: второй `HdfsOrcSinkNode` с путём `<source_path>/_params/` и схемой `(__uuid, param_name, param_value, __time)`.
2. **ARQS** — `ArchiveQueryCompiler`: если SQL содержит `ARRAY_POSITION`, переписывать на JOIN по `_params`-таблице. Или новый query type.
3. **Backfill** — одноразовая Materialization существующих данных (Spark job: read ORC → explode → write _params ORC).

**Сложность:** высокая (flow_service + arqs + backfill).

### Вариант B — Secondary index в Solr

Вместо ORC-таблицы — индексировать `paramName → paramValue` в Solr (оперативный поиск). Уже есть `SolrSinkNode`.

**Эффект:** мгновенный поиск по параметрам (секунды). Но Solr имеет ограничение по объёму (оперативный поиск), не подходит для архива (5 лет TTL).

**Когда:** для «свежих» данных (оперативный период), не для архивного поиска.

### Рекомендация: Вариант A (ORC-таблица) для архива + Вариант B (Solr) для оперативного.

### Trade-offs

**Объём хранения vs скорость:**
- Среднее число параметров на событие: ~10
- Дополнительный объём: +10× строк, но каждая строка меньше (3 колонки vs 57)
- Реальный overhead хранения: **+15-25%** к объёму ORC

**Write-time vs read-time:**
- Flink пишет 2 файла вместо 1. Дополнительная нагрузка: разворачивание массивов + второй `StreamingFileSink`.
- Рост latency записи: ~20-30%. Но запись — асинхронная, не влияет на SLA приёма событий.

**Backfill:**

| Вариант | Плюс | Минус |
|---|---|---|
| Backfill всех 5 лет | Единый интерфейс поиска | Долгий Spark-job (недели), +25% хранения |
| Только новые данные (с Q1 2027) | Нет backfill | Два пути поиска: новый → JOIN, старый → ARRAY_POSITION |
| Backfill по приоритету (топ-таблицы) | `sbolpro_confirm` (20% нагрузки) — первая | Частичный, inconsistent |

**Рекомендация:** гибрид — новые данные с Q1 2027 (без backfill) + backfill топ-5 таблиц по нагрузке. Исторический фонд (Режим 2) использует `ARRAY_POSITION` — он и так медленнее, но там другие требования (фоновая разметка, SLA 24ч).

---

## 2b. Индексация атрибутов цЮЗА

### Проблема

CR1 Режим 1 требует поиск по 9 новым идентификаторам:
`uza_authentification_id`, `uza_authorization_id`, `uza_ufs_history_operation_id`, `uza_operation_id`, `uza_confirmation_id`, `epk_id`, `operation_date`, `mobilenumber`, `aimodelid`

Сейчас это — параметры в массивах `paramName/paramValue`. Поиск = `ARRAY_POSITION` = full row scan.

### Вариант A — Bloom-filter на `paramName` (дешёвый)

Добавить `paramName` в `BLOOM_FILTER_COLUMNS` при записи ORC. Bloom-filter хранится в stripe metadata — Spark skip stripe, если `paramName='uza_authentification_id'` отсутствует в bloom.

**Эффект:** skip ~90% stripes (если 9 цЮЗА-ID встречаются в <10% событий). Но **не устраняет** full row scan для оставшихся stripes — bloom на массивах работает по наличию значения, не по позиции.

**Реализация:** `ArchiveFlowBuilderImpl.java:622-623` — добавить `paramName` к bloom-filter columns. Только конфигурация + backfill.

### Вариант B — Вынесение цЮЗА-ID в отдельные колонки

При записи события: если `paramName` совпадает с одним из 9 цЮЗА-ID, скопировать значение в отдельную статическую колонку. Bloom-filter на эти колонки + predicate pushdown.

**Эффект:** точный index lookup вместо array scan. **~100×** для цЮЗА-запросов.

**Реализация:**
1. **Schema** — добавить 9 статических полей в `HdfsSinkMessageSchema` (nullable, заполняются только при наличии).
2. **Flink** — новый processing node `ExtractUzaIdsNode`: если paramName ∈ {9 ID}, копировать в соответствующую статическую колонку.
3. **ARQS** — для Режима 1 генерировать SQL по статическим колонкам вместо `ARRAY_POSITION`.
4. **Backfill** — Spark job: read existing ORC → extract цЮЗА-ID → write new ORC с доп. колонками.

**Сложность:** средняя (flow_service + arqs + schema migration + backfill).

### Вариант C — Отдельный индекс (Solr или отдельный ORC)

Отдельный индекс `uza_id → event_uuid` для быстрого lookup. При поиске: сначала найти `event_uuid` по `uza_id`, потом вычитать событие из основного ORC.

**Эффект:** двухфазный поиск, но каждый этап быстрый. Подходит для Режима 3 (размеченные данные — уже отдельный индекс).

### Рекомендация: Вариант B для нового фонда (с 01.01.2027) + Вариант A (bloom на paramName) для исторического.

### Trade-offs

**9 статических колонок vs гибкость:**

| Плюс | Минус |
|---|---|
| Bloom-filter + predicate pushdown — точный lookup | Список ID зафиксирован в schema. Расширение (до 120 атрибутов) — migration + backfill |
| 0 накладных для null (ORC) | 9 колонок × 224 проекта × 2000 индексов — изменение schema всех индексов |

**Bloom на `paramName` (2b-A) vs отдельные колонки (2b-B):**

| | Bloom на paramName (A) | Отдельные колонки (B) |
|---|---|---|
| Эффект | skip ~90% stripes, row scan оставшихся | точный index lookup, ~100× |
| Сложность | только конфиг ORC writer | schema migration + Flink node + backfill |
| Гибкость | любой параметр | только 9 зафиксированных ID |
| Backfill | rewrite (bloom при записи) | rewrite (колонки при записи) |

---

## 2d + 2e. Bloom-filter и сортировка ORC

### Текущее состояние

`ArchiveFlowBuilderImpl.java:622-623`:
```java
List<String> bloomFilterColumns = Collections.singletonList(choseTimeFieldName(archivePrimaryTimeField));
orcConfiguration.put(OrcConf.BLOOM_FILTER_COLUMNS.getAttribute(), String.join(",", bloomFilterColumns));
```
Bloom-filter **только на time field**. Сортировка ORC не настроена (данные пишутся в порядке поступления из Kafka).

`orcConfiguration` — это `Map<String,String>`, пробрасывается в `OrcBulkWriterFactory` как `Properties` (`HdfsOrcFlowJobSinkNodeBuilder.java:65,91-94`). Любые ORC-параметры можно добавить без изменения кода — только конфигурация.

### Bloom-filter

Добавить колонки:
```java
bloomFilterColumns = List.of(timeField, "module", "name", "paramName");
```

Дополнительно:
- `orc.bloom.filter.fpp` — false positive probability (default 0.05). Для `module` можно 0.01.
- `orc.create.index` — включить row group indexes.

**Backfill:** bloom-filter создаётся **при записи**. Существующие ORC-файлы не имеют bloom на `module/name`. Нужен rewrite или применять только к новым данным.

### Сортировка ORC

`orc.sort.columns` — колонки, по которым ORC writer сортирует данные внутри файла. Stripe metadata хранит min/max — Spark skip'ает stripe если искомое значение вне диапазона.

```
orc.sort.columns = module,name
```

**Эффект:** при `WHERE module = 'payment-service'` — skip всех stripes где min/max `module` не содержит `payment-service`. Для 224 проектов: каждый stripe содержит ~1-5 модулей → skip 95-99% stripes.

**Реализация:**
1. `orcConfiguration.put("orc.sort.columns", "module,name")` в `ArchiveFlowBuilderImpl.java`.
2. `OrcBulkWriterFactory` (Flink) — проверить поддержку `orc.sort.columns` (Apache ORC >= 1.6).
3. **Backfill** — критический: сортировка применяется при записи. Существующие файлы неотсортированы.

### Trade-offs

**Bloom без sort vs bloom + sort:**

| | Bloom без sort | Bloom + sort |
|---|---|---|
| Stripe skip по `module` | bloom: FP ~5% → skip ~90% | min/max: FP ~0% → skip ~99% |
| Доп. нагрузка при записи | минимальная (bloom — cheap) | сортировка — буфер + sort |
| Backfill | rewrite для bloom | rewrite для sort (то же) |

Bloom без sort уже даёт ~10× (skip 90%). Sort добавляет ещё ~2× (skip 99% vs 90%).

**Влияние сортировки на запись:**
- `StreamingFileSink` (Flink) — данные буферизуются, сортируются, потом пишутся.
- `bucketCheckIntervalMillis = 10,000` (10с) — буфер 10с данных.
- При 10K событий/сек × 10с = 100K строк. Sort O(n log n) ~1.7M операций — приемлемо.
- При 100K событий/сек → 1M строк → ~20M операций + ~2GB heap — нужен контроль.

**Backfill:**

| Вариант | Эффект на старые данные | Стоимость |
|---|---|---|
| Без backfill | Bloom/sort только на новых данных. Старые — full scan. Два пути в ARQS. | 0 |
| Backfill всех | Единый интерфейс. Rewrite 5 лет ORC — недели Spark-джобов. | высокая |
| Backfill топ-таблиц | `sbolpro_confirm` (20%) + 4 таблицы = ~50% нагрузки. | средняя, ~1-2 недели |

**Рекомендация:** bloom на `module, name, paramName` — сразу для новых данных (конфиг, без кода). Sort — после оценки Flink heap. Backfill — топ-5 таблиц, остальное — lazy rewrite при первом запросе.

---

## 3a + 3b. Масштабирование YARN

### Расчёт целевого кластера

При комбинации оптимизаций (2a + 2b-B + 2d+2e):

| Оптимизация | Длительность до | Длительность после | Эффект |
|---|---|---|---|
| Текущее | 10.5ч (37,800с) | — | — |
| + Bloom + sort (2d+2e) | 37,800с | ~3,780с (10×) | skip 90% stripes |
| + Материализация (2a) | 3,780с | ~380с (10×) | array scan → index lookup |
| + цЮЗА-ID колонки (2b-B) | 380с | ~30с (12×) | точный lookup по ID |
| **Итог** | **37,800с** | **~30с** | **1,260×** |

**Concurrency при 30с avg, 8ч пик:**
- λ = 2,500,000 / (30 × 8 × 3600) = 289 запросов/с
- L = 289 × 30 = **208 одновременных приложений**
- Ресурсы: 208 × 14.6 cores = **3,037 cores**, 208 × 25.8 GB = **5.4 TB RAM**

**Сравнение:**

| Кластер | Cores | RAM | Достаточно? |
|---|---|---|---|
| 1 × (100 nodes × 32 cores × 128GB) | 3,200 | 12.8 TB | **да** (с запасом) |
| 2 × (50 nodes × 32 cores × 128GB) | 3,200 | 12.8 TB | **да** |

### Capacity Scheduler

```
Queue: uza    (50% гарантировано, 100% max)
Queue: regular (50% гарантировано, 100% max)
```

- цЮЗА-запросы → queue `uza` (через `spark.yarn.queue` в `SparkJobConfig`)
- Обычные запросы → queue `regular`
- При отсутствии нагрузки в одной очереди — ресурсы переходят другой (elastic)

**Реализация:** `application.properties` ARQS — добавить `spark.yarn.queue` per query type. Или в `ArchiveQuerySubmitScheduler` — выбирать queue по признаку цЮЗА-запроса.

### При SLA 48 мин (без 2b, только 2a + 2d + 2e)

| Длительность | Concurrency | Cores | RAM | Кластер |
|---|---|---|---|---|
| 380с (6 мин) | 2,632 | 38,427 | 68 TB | ~6 кластеров (100×32) |
| 180с (3 мин) | 1,316 | 19,213 | 34 TB | ~3 кластера |

### Trade-offs

**Оптимизировать vs масштабировать:**

| Подход | Кластер | Стоимость инфра | Срок |
|---|---|---|---|
| Только масштаб (без 2a/2b) | ~1.6M cores = 250 кластеров | нереализуемо | — |
| Масштаб после 2d+2e (bloom+sort) | ~152K cores = 24 кластера | очень дорого | Q1 2027 |
| Масштаб после 2a (материализация) | ~15K cores = 3 кластера | приемлемо | Q1 2027 |
| Масштаб после 2a+2b (цЮЗА-ID) | ~3K cores = 1 кластер | оптимально | Q3 2027 |

Масштабирование без оптимизации запросов — экономически нереализуемо. Оптимизация (2a+2b) снижает потребность в кластере **в 500×** (1.6M → 3K cores).

**Capacity Scheduler vs отдельный ARQS:**

| | Capacity Scheduler (один ARQS, две очереди) | Отдельный ARQS для цЮЗА |
|---|---|---|
| Изоляция | soft (preemption по очередям) | hard (отдельный сервис, БД, metrics) |
| Сложность | YARN config | развёртывание + helm + БД |
| Совместимость | один API, один код | два инстанса, синхронизация schema |
| При отказе | обе очереди в одном RM | цЮЗА-ARQS независим |

**Рекомендация:** Capacity Scheduler для Q1-Q3 2027 (быстрее). Отдельный ARQS — если цЮЗА-нагрузка превысит 50% общей (прогноз 2029: 20% — не превысит).

---

# Порядок выката

```
Q4 2026 (RnD):
  2d: Bloom-filter на module, name, paramName (новые данные)     ← конфиг, без кода
  2e: Sort ORC по module, name (новые данные)                    ← конфиг + оценка Flink heap
  Backfill топ-5 таблиц                                          ← Spark job

Q1 2027 (API поиска цЮЗА):
  2a: Материализация массивов (новые данные)                     ← flow_service + arqs
  2b-A: Bloom на paramName (исторический фонд)                   ← конфиг
  3b: Capacity Scheduler (queue uza)                             ← YARN config
  1a: Пул сабмита 5→50, interval 60→15                           ← helm

Q3 2027 (Разметка):
  2b-B: цЮЗА-ID статические колонки (новый фонд с 01.01.2027)    ← flow_service + arqs + schema
  Backfill топ-5 таблиц (цЮЗА-ID extraction)                     ← Spark job

Q4 2027 (Уничтожение):
  Финальная оценка кластера (1 кластер достаточно)
```

### Этапность масштабирования

| Этап | Оптимизации | Длительность | Кластер | Срок |
|---|---|---|---|---|
| Q4 2026 (RnD) | Bloom + sort (2d+2e) | ~1ч (10×) | ~24 кластера — нереализуемо | только новых данных |
| Q1 2027 | + Материализация (2a) | ~6 мин (100×) | ~3 кластера | API поиска цЮЗА |
| Q3 2027 | + цЮЗА-ID колонки (2b-B) | ~30с (1260×) | 1 кластер | разметка |
| Q4 2027 | Финал | 30с | 1 кластер + Capacity Scheduler | уничтожение |

### Зависимости и критический путь

- **2a (материализация)** — нужен **до** Q1 2027, иначе API поиска цЮЗА не удовлетворит SLA даже с масштабированием.
- **2d (bloom)** — можно делать **параллельно**, не блокирует 2a.
- **2b-B** — после 2a, т.к. использует ту же схему расширения статических полей.
- **1a (сабмит)** — независим, можно делать в любой момент.

### Главный риск

Backfill. Если нельзя остановить приём событий на время backfill — нужен дизайн «двойной записи» (Flink пишет в старую и новую schema параллельно до полного перехода).
