

## 1. Итоговая карта вариантов для тестирования

Я бы оставил такой набор:

|Уровень|Механизм|Что проверяем|Приоритет|
|---|---|---|--:|
|ORC|Column pruning|чтение только необходимых колонок|очень высокий|
|ORC|Predicate pushdown|передача фильтров в ORC reader|очень высокий|
|ORC|min/max statistics|skipping stripe/row group|очень высокий|
|ORC|Bloom Filter|`=`, `IN`|высокий|
|ORC|`row.index.stride`|детализация row-group index|средний/высокий|
|ORC|сортировка данных|увеличение эффективности min/max|очень высокий|
|ORC|`stripe.size`|размер единицы ORC scan|средний|
|ORC|compression|CPU ↔ I/O компромисс|средний|
|HDFS/Table|Partitioning|исключение файлов/директорий|очень высокий|
|HDFS/Table|Bucketing|JOIN/equality|выборочно высокий|
|HDFS|Compaction / small files|file-listing, scheduling, I/O|высокий|
|HDFS|Short-circuit read|локальный DataNode access|средний/высокий|
|HDFS|Centralized Cache|повторные запросы|высокий|
|Spark 3.2|Native/vectorized ORC reader|decode/CPU|высокий|
|Spark 3.2|AQE|shuffle/join/skew|высокий|
|Spark 3.2|DPP|partition pruning через JOIN|высокий|
|Spark 3.2|CBO/statistics|выбор плана|средний/высокий|
|Spark 3.2|Broadcast Join|исключение shuffle|высокий|
|Hive|Vectorized execution|CPU/decode|высокий|
|Hive|CBO/statistics|JOIN/order/plan|высокий|
|Hive|Tez|DAG execution, меньше промежуточного HDFS I/O|высокий|
|Hive ≥2.0|LLAP|persistent execution + ORC cache + I/O|очень высокий|
|Hive|Materialized Views|повторяющиеся aggregation queries|высокий для OLAP|


---

# 2. Что именно должно быть целью benchmark

Для SLA:

> **среднее время запроса ≤ 3 с**

главная метрика — не только `query duration`.

Нужно измерять минимум:

|Метрика|Зачем|
|---|---|
|AVG latency|ваш SLA|
|p50|типовая latency|
|p95|стабильность|
|p99|хвостовые задержки|
|HDFS bytes read|эффективность pruning|
|ORC rows read|эффективность pruning|
|partitions read|partition pruning|
|files opened|small-files overhead|
|CPU time|эффективность vectorization/compression|
|shuffle read/write|JOIN/GROUP BY|
|spilled bytes|дефицит памяти|
|records returned|нормализация результата|
|planning time|Hive/Spark optimizer overhead|
|execution time|собственно выполнение|
|cache hit ratio|LLAP/HDFS cache|

Ключевой KPI для ORC:

```text
Scan ratio =
physical bytes read
────────────────────
total bytes potentially accessible
```

Например:

```text
Partition = 200 GB
HDFS read = 800 MB

scan ratio = 800 MB / 200 GB
           ≈ 0.4 %
```

Чем ближе типовые selective queries к малым долям процента, тем реалистичнее SLA 3 секунды.

---

# 3. Не начинать со всех оптимизаций одновременно

Если сразу сделать:

```text
partitioning
+ sorting
+ bloom
+ stride=5000
+ vectorization
+ cache
+ AQE
```

и получить:

```text
20 s → 2.4 s
```

вы не узнаете, что именно дало результат.

Поэтому рекомендую схему:

```text
Baseline
   │
   ├─ + Optimization A
   │       ↓
   │     measure
   │
   ├─ + Optimization B
   │       ↓
   │     measure
   │
   ...
   │
   ▼
Best combination
```

---

# 4. Фиксированный тестовый dataset

Все тесты должны проводиться на **одном логическом dataset**.

Для вашего сценария разумная структура:

```text
event_date       DATE/TIMESTAMP

field_high       STRING   -- high cardinality
field_medium     STRING   -- medium cardinality
field_low        STRING   -- low cardinality

msg              STRING   -- ~1000 chars

id               STRING/BIGINT
```

Особенно важно сохранить **одно и то же распределение данных**, поскольку Bloom/min-max сильно зависят от cardinality и физического порядка значений.

Согласованные масштабы датасета (логически, по `dataset.yaml` / configs_final):

```text
Dataset S    ~100 GB    — layout-факторы, пилотный прогон
Dataset M    ~500 GB    — top-N подтверждение
Dataset L    ~1024 GB   — максимум; финальный SLA
```

Причём финальное решение по SLA принимать только по `L`. Между масштабами удаляйте ненужные физические копии layout’ов: каждый вариант — отдельный каталог на HDFS.

---

# 5. Набор эталонных запросов

Нужно сделать фиксированный query suite.

### Q1 — partition pruning

```sql
SELECT count(*)
FROM events
WHERE event_date = DATE '2026-09-01';
```

Проверяет:

```text
Partition pruning
+
ORC scan
```

---

### Q2 — equality high cardinality

```sql
SELECT event_date, field_high
FROM events
WHERE event_date = DATE '2026-09-01'
  AND field_high = 'VALUE_123456';
```

Проверяет:

```text
partition pruning
+
min/max
+
Bloom
+
sorting
```

Это один из наиболее важных benchmark-запросов.

---

### Q3 — equality medium cardinality

```sql
SELECT event_date, field_medium
FROM events
WHERE event_date = DATE '2026-09-01'
  AND field_medium = 'CATEGORY_123';
```

Проверяет эффективность Bloom при меньшей cardinality.

---

### Q4 — equality low cardinality

```sql
SELECT count(*)
FROM events
WHERE event_date = DATE '2026-09-01'
  AND field_low = 'ERROR';
```

Полезен для доказательства, что Bloom filter не обязательно эффективен на low-cardinality колонке.

---

### Q5 — `IN`

```sql
SELECT *
FROM events
WHERE event_date = DATE '2026-09-01'
  AND field_high IN ('A', 'B', 'C', 'D');
```

Основной сценарий Bloom.

---

### Q6 — range

Для числовой/date колонки:

```sql
SELECT count(*)
FROM events
WHERE event_time >= ...
  AND event_time < ...;
```

Проверяем:

```text
partition pruning
+
min/max
+
sorting
```

Bloom здесь не является основным механизмом.

---

### Q7 — column pruning

```sql
SELECT event_date, field_high
FROM events
WHERE event_date = DATE '2026-09-01';
```

против:

```sql
SELECT *
FROM events
WHERE event_date = DATE '2026-09-01';
```

Очень важно из-за большого `msg`.

---

### Q8 — aggregation

```sql
SELECT field_low, count(*)
FROM events
WHERE event_date = DATE '2026-09-01'
GROUP BY field_low;
```

Проверяет:

```text
scan
CPU
aggregation
shuffle
```

---

### Q9 — более тяжёлый GROUP BY

```sql
SELECT field_medium, count(*)
FROM events
WHERE event_date BETWEEN ...
GROUP BY field_medium;
```

Здесь уже интересно сравнивать:

```text
Spark AQE
vs
Hive/Tez
vs
Hive/LLAP
```

---

### Q10 — JOIN

Например:

```sql
SELECT e.field_medium, count(*)
FROM events e
JOIN dictionary d
  ON e.field_medium = d.id
WHERE e.event_date = DATE '2026-09-01'
  AND d.type = 'X'
GROUP BY e.field_medium;
```

Проверяет:

```text
Broadcast
AQE
DPP
CBO
Hive map join
```

---

# 6. Этап A — чистый ORC baseline

Создаём исходную таблицу:

```text
ORC
без Bloom
без sorting
row.index.stride = 10000
standard stripe
standard compression
```

Но без искусственного отключения штатных ORC indexes.

ORC изначально содержит lightweight indexes и способен пропускать row groups по predicates. Формат ORC появился в Hive 0.11 и с самого начала проектировался именно вокруг columnar storage и встроенных lightweight indexes. ([Apache Hive](https://hive.apache.org/docs/latest/language/languagemanual-orc/?utm_source=chatgpt.com "Apache Hive : LanguageManual ORC"))

Получаем:

```text
ORC-BASE
```

Это точка `T0`.

---

# 7. Этап B — Column pruning

Сравнить:

```sql
SELECT *
```

и:

```sql
SELECT field_high
```

при одном и том же `WHERE`.

Особенно показательным будет `msg ~1000 chars`.

Регистрировать:

```text
HDFS bytes read
CPU
query time
```

Spark 3.2 отдельно улучшил ORC column selection: release notes отмечают передачу списка требуемых columns в task configuration для уменьшения чтения ORC. ([Apache Spark](https://spark.apache.org/releases/spark-release-3-2-0.html?utm_source=chatgpt.com "Spark Release 3.2.0 | Apache Spark"))

---

# 8. Этап C — Predicate pushdown

Два варианта:

```text
C0 — pushdown OFF
C1 — pushdown ON
```

для Q2/Q3/Q5/Q6.

Проверить execution plan.

Для Spark:

```text
spark.sql.orc.filterPushdown
```

и native ORC reader.

Цель:

```text
C1 HDFS bytes read
        <
C0 HDFS bytes read
```

---

# 9. Этап D — Partitioning

Сделать три физические версии одной таблицы:

```text
D0 — no partitioning

D1 — PARTITIONED BY event_date

D2 — PARTITIONED BY event_date + hour
```

Но `D2` имеет смысл только если workload действительно активно работает с часовыми интервалами.

Тесты:

```text
Q1
Q2
Q3
Q6
Q8
```

Измерять:

```text
partitions selected
files selected
planning time
HDFS bytes
latency
```

---

# 10. Этап E — сортировка данных

Очень важный эксперимент.

Создать:

```text
E0
unsorted
```

```text
E1
sorted by field_high
```

```text
E2
sorted by field_medium
```

```text
E3
sorted by event_time, field_high
```

На Q2/Q3/Q6 сравнить:

```text
HDFS bytes read
row groups read
latency
```

Главный вопрос:

> насколько сортировка повышает эффективность встроенного min/max pruning?

---

# 11. Этап F — Bloom Filter

Создать четыре варианта:

```text
F0
Bloom OFF
```

```text
F1
Bloom(field_high)
```

```text
F2
Bloom(field_medium)
```

```text
F3
Bloom(field_high, field_medium, field_low)
```

ORC Bloom indexes поддерживаются начиная с Hive 1.2.0; на каждую указанную колонку создаётся `BLOOM_FILTER` stream с entry для каждой row group. ([Apache ORC](https://orc.apache.org/specification/ORCv2/?utm_source=chatgpt.com "Evolving Draft for ORC Specification v2"))

Тестировать:

```text
Q2 high
Q3 medium
Q4 low
Q5 IN
```

Собирать дополнительно:

```text
ORC size
index overhead
write time
```

Важно не только:

```text
query latency
```

но и:

```text
+5% storage?
+10%?
+20%?
```

---

# 12. Этап G — FPP Bloom Filter

Например:

```text
G1  FPP = 0.05
G2  FPP = 0.01
G3  FPP = 0.001
```

Стандартное значение текущего ORC:

```text
0.01
```

([Apache ORC](https://orc.apache.org/docs/spark-config.html?utm_source=chatgpt.com "Spark Configuration"))

Сравнить:

```text
index size
file size
query latency
false-positive efficiency
```

Я бы ожидал, что этот параметр даст меньший эффект, чем правильный выбор Bloom columns, но это нужно подтвердить benchmark.

---

# 13. Этап H — `row.index.stride`

Сделать:

```text
H1 = 5 000
H2 = 10 000
H3 = 20 000
H4 = 50 000
```

Default:

```text
10 000
```

([Apache ORC](https://orc.apache.org/docs/core-java-config.html?utm_source=chatgpt.com "ORC Java configuration"))

Тестировать прежде всего:

```text
Q2
Q3
Q5
```

Ожидаемая зависимость:

```text
stride ↓
   │
   ├─ точнее pruning
   ├─ больше indexes
   └─ больше metadata


stride ↑
   │
   ├─ меньше index overhead
   └─ более грубый pruning
```

---

# 14. Этап I — stripe size

Проверить, например:

```text
64 MB
128 MB
256 MB
```

Текущее default ORC:

```text
64 MiB
```

([Apache ORC](https://orc.apache.org/docs/core-java-config.html?utm_source=chatgpt.com "ORC Java configuration"))

Проверяются:

```text
selective scan
sequential scan
parallelism
metadata overhead
file size
```

Особенно важно не путать:

```text
ORC stripe size

и

ORC file size

и

HDFS block size
```

Это разные уровни.

---

# 15. Этап J — compression

Если версия ORC/Spark позволяет, сравнить минимум:

```text
ZLIB
SNAPPY
ZSTD
```

Spark 3.2 обновил bundled Apache ORC до **1.6.11** и добавил для ORC поддержку ZSTD/LZ4. ([Apache Spark](https://spark.apache.org/releases/spark-release-3-2-0.html?utm_source=chatgpt.com "Spark Release 3.2.0 | Apache Spark"))

Измерять:

```text
file size
HDFS bytes
decompression CPU
query latency
write latency
```

Это очень полезный benchmark:

```text
более сильная compression
       ↓
меньше HDFS I/O

но

больше CPU
```

При I/O-bound workload более сильная компрессия иногда ускоряет запрос.

---

# 16. Этап K — small files / compaction

Создать три layout:

```text
K1
много маленьких файлов
```

```text
K2
средние файлы
```

```text
K3
крупные файлы
```

Например benchmark-значения можно взять:

```text
~32 MB
~256 MB
~1 GB
```

Это **не рекомендация для production**, а удобные точки эксперимента.

Измерять:

```text
NameNode/file listing
planning
task count
HDFS open operations
query duration
```

---

# 17. Этап L — Spark ORC execution

После выбора лучшего физического ORC layout его фиксируем.

Получаем:

```text
BEST_ORC
```

Дальше не меняем данные и тестируем execution.

Для Spark 3.2:

```text
L0 native ORC, vectorized OFF

L1 native ORC, vectorized ON
```

Spark ORC native reader по умолчанию vectorized; Spark 3.2 добавил поддержку nested columns в ORC vectorized reader. ([Apache ORC](https://orc.apache.org/docs/spark-config.html?utm_source=chatgpt.com "Spark Configuration"))

---

# 18. Этап M — Spark AQE

Сравнить:

```text
M0 AQE OFF

M1 AQE ON
```

Запросы:

```text
Q8 GROUP BY
Q9 GROUP BY
Q10 JOIN
```

Spark **3.2 включил AQE по умолчанию**. В этой версии также улучшена интеграция Dynamic Partition Pruning с AQE и оптимизация skew join. ([Apache Spark](https://spark.apache.org/releases/spark-release-3-2-0.html?utm_source=chatgpt.com "Spark Release 3.2.0 | Apache Spark"))

Измерять:

```text
query time
shuffle bytes
shuffle partitions
spill
skew
physical plan
```

---

# 19. Этап N — Dynamic Partition Pruning

Использовать Q10 и partitioned fact table.

Сравнить:

```text
N0 DPP OFF
N1 DPP ON
```

Главная метрика:

```text
количество прочитанных partitions
```

и:

```text
HDFS bytes read
```

Spark 3.2 имеет специальные изменения по поддержке DPP совместно с AQE. ([Apache Spark](https://spark.apache.org/releases/spark-release-3-2-0.html?utm_source=chatgpt.com "Spark Release 3.2.0 | Apache Spark"))

---

# 20. Этап O — Spark CBO

Сначала:

```sql
ANALYZE TABLE ...
COMPUTE STATISTICS;

ANALYZE TABLE ...
COMPUTE STATISTICS FOR COLUMNS ...;
```

Затем:

```text
O0 CBO OFF
O1 CBO ON
```

Запросы:

```text
Q9
Q10
```

Проверяем не только latency:

```text
Join order
Join strategy
estimated rows
actual rows
```

---

# 21. Этап P — HDFS short-circuit read

Два режима:

```text
P0
short-circuit OFF
```

```text
P1
short-circuit ON
```

Но эксперимент имеет смысл только когда:

```text
Spark/Hive worker
и
DataNode

расположены совместно.
```

Сравнить:

```text
HDFS read latency
CPU
network
query time
```

---

# 22. Этап Q — HDFS Centralized Cache

Очень важен отдельный **cold/warm benchmark**.

```text
Q0
cold HDFS
```

```text
Q1
OS/HDFS naturally warmed
```

```text
Q2
HDFS Centralized Cache
```

Для всех следующих тестов обязательно явно писать:

```text
COLD
или
WARM
```

Иначе результаты LLAP/Spark/HDFS будут несопоставимы.

---

# 23. Теперь Hive baseline

После выбора одного:

```text
BEST_ORC
```

его надо читать одновременно Spark и Hive.

Получаем:

```text
             SAME ORC DATA
                   │
          ┌────────┴────────┐
          ▼                 ▼
       Spark 3.2          Hive
```

Это очень важно: **не создавать отдельные данные специально под Hive**, иначе benchmark потеряет чистоту.

---

# 24. Hive — первая конфигурация

Сначала:

```text
Hive + Tez
без LLAP
```

Я бы именно Tez использовал как baseline Hive для LLAP comparison.

Причина: LLAP сам по себе execution engine не является; официальная архитектура Hive описывает Tez AM как orchestrator, а части query исполняются LLAP daemons. ([Apache Hive](https://hive.apache.org/development/desingdocs/llap/?utm_source=chatgpt.com "Apache Hive : LLAP"))

Получаем:

```text
H0 = Hive + Tez
```

---

# 25. Hive Vectorization

Сравнить:

```text
H1
Hive/Tez
vectorization OFF
```

```text
H2
Hive/Tez
vectorization ON
```

Hive имеет:

```text
hive.vectorized.execution.enabled
```

с Hive 0.13.0. ([Apache Hive](https://hive.apache.org/docs/latest/admin/adminmanual-configuration/?utm_source=chatgpt.com "Apache Hive : AdminManual Configuration"))

Официальные Hive recommendations для Spark/Hive workloads также включают включение vectorized execution и CBO. ([Apache Hive](https://hive.apache.org/docs/latest/admin/hive-on-spark-getting-started/?utm_source=chatgpt.com "Apache Hive : Hive on Spark: Getting Started"))

---

# 26. Hive CBO

Собрать statistics:

```sql
ANALYZE TABLE ...
COMPUTE STATISTICS;

ANALYZE TABLE ...
COMPUTE STATISTICS FOR COLUMNS;
```

Hive хранит column statistics, включая:

```text
LOW_VALUE
HIGH_VALUE
NUM_NULLS
NUM_DISTINCTS
```

([Apache Hive](https://hive.apache.org/development/desingdocs/column-statistics-in-hive/?utm_source=chatgpt.com "Apache Hive : Column Statistics in Hive"))

Сравнить:

```text
HC0 Hive CBO OFF
HC1 Hive CBO ON
```

на:

```text
Q8
Q9
Q10
```

Hive CBO использует statistics, в частности для join ordering и выбора алгоритма JOIN. ([Apache Hive](https://hive.apache.org/docs/latest/user/cost-based-optimization-in-hive/?utm_source=chatgpt.com "Apache Hive : Cost-based optimization in Hive"))

---

# 27. Основной эксперимент — LLAP

Теперь:

```text
HL0
Hive + Tez
```

против:

```text
HL1
Hive + Tez + LLAP
```

LLAP появился в **Hive 2.0.0**. ([Apache Hive](https://hive.apache.org/development/desingdocs/llap/?utm_source=chatgpt.com "Apache Hive : LLAP"))

Архитектурно:

```text
HiveServer2
     │
     ▼
   Tez AM
     │
     ├─────────────┐
     ▼             ▼
LLAP daemon     YARN container
     │
     ├─ ORC cache
     ├─ metadata
     ├─ filter
     ├─ projection
     ├─ partial aggregate
     └─ selected joins
```

LLAP может выполнять filter, projection, transformations, partial aggregates, sorting, bucketing и некоторые joins. ([Apache Hive](https://hive.apache.org/development/desingdocs/llap/?utm_source=chatgpt.com "Apache Hive : LLAP"))

---

# 28. LLAP нужно тестировать минимум в двух состояниях

Это принципиально.

### LLAP Cold

```text
restart / clear LLAP cache

query
```

Получаем:

```text
Hive+LLAP cold
```

### LLAP Warm

```text
query
query
query
...
```

Получаем:

```text
Hive+LLAP warm
```

LLAP имеет специальный low-level ORC cache; конфигурация включает:

```text
hive.llap.io.enabled

hive.llap.io.cache.orc.size

hive.llap.io.memory.size
```

Эти параметры появились в Hive 2.0.0. ([Apache Hive](https://hive.apache.org/docs/latest/user/configuration-properties/?utm_source=chatgpt.com "Apache Hive : Configuration Properties"))

Поэтому одно измерение LLAP совершенно недостаточно.

---

# 29. LLAP cache-size benchmark

Проверить, например:

```text
25% hot working set
50%
100%
```

Не относительно всего 5-ТБ массива, а относительно **реального hot working set**.

Например:

```text
обычный запрос работает с 200 GB/day
```

тогда benchmark cache:

```text
50 GB
100 GB
200 GB
```

Главный вопрос:

> насколько latency становится стабильной при повторных interactive queries?

---

# 30. Обязательное финальное сравнение Spark и Hive

После всех локальных tuning experiments создать финальные профили.

Например:

```text
S0
Spark baseline
```

```text
S1
Spark optimized
```

```text
H0
Hive + Tez
```

```text
H1
Hive + Tez + vectorization + CBO
```

```text
H2
Hive + Tez + LLAP COLD
```

```text
H3
Hive + Tez + LLAP WARM
```

Все работают на **одних и тех же optimized ORC files**.

---

# 31. Финальная benchmark-матрица

Я бы свёл всё примерно к такой таблице:

|ID|Data layout|Engine|Optimization|
|---|---|---|---|
|B0|ORC baseline|Spark|baseline|
|B1|B0|Spark|column pruning|
|B2|B0|Spark|predicate pushdown|
|B3|partitioned|Spark|partition pruning|
|B4|sorted|Spark|min/max|
|B5|sorted|Spark|Bloom high|
|B6|sorted|Spark|Bloom high+medium|
|B7|sorted|Spark|stride 5k|
|B8|sorted|Spark|stride 10k|
|B9|sorted|Spark|stride 20k|
|B10|optimized|Spark|vectorized|
|B11|optimized|Spark|AQE|
|B12|optimized|Spark|DPP|
|B13|optimized|Spark|CBO|
|B14|optimized|Spark|HDFS short-circuit|
|B15|optimized|Spark|HDFS cache|
|H0|optimized|Hive/Tez|baseline|
|H1|optimized|Hive/Tez|vectorized|
|H2|optimized|Hive/Tez|vectorized + CBO|
|H3|optimized|Hive/Tez/LLAP|cold|
|H4|optimized|Hive/Tez/LLAP|warm|

Это уже достаточно управляемое количество конфигураций.

---

# 32. Но каждый вариант надо прогнать много раз

Один запуск не считается benchmark.

Для каждой пары:

```text
configuration × query
```

я бы делал:

```text
3–5 warm-up
+
20–30 measured executions
```

Для очень тяжёлых запросов можно уменьшить количество, но для целевого SLA в 3 секунды 20–30 наблюдений вполне оправданы.

Не усреднять cold и warm вместе.

---

# 33. Concurrency benchmark

После single-query теста обязательно:

```text
1 concurrent query

5

10

25

50
```

или до фактической ожидаемой нагрузки.

Получаем матрицу:

|Engine|Concurrent|AVG|p50|p95|p99|
|---|--:|--:|--:|--:|--:|
|Spark|1|||||
|Spark|5|||||
|Spark|10|||||
|Hive/LLAP|1|||||
|Hive/LLAP|5|||||
|Hive/LLAP|10|||||

LLAP как раз рассчитан на concurrent interactive workload: daemon позволяет параллельно исполнять fragments нескольких queries/sessions. ([Apache Hive](https://hive.apache.org/development/desingdocs/llap/?utm_source=chatgpt.com "Apache Hive : LLAP"))

---

# 34. Я бы добавил отдельный SLA-показатель

Для каждого варианта:

```text
SLA Success Rate =
queries with latency <= 3 sec
───────────────────────────
all queries
× 100 %
```

Например:

```text
AVG = 2.4 s
p95 = 7.8 s
SLA success = 83 %
```

Такой результат я бы **не считал удовлетворительным**, несмотря на среднее меньше трёх секунд.

Полезнее получить:

```text
AVG       = 1.7 s
p95       = 2.6 s
p99       = 3.5 s
<=3 s     = 98.4 %
```

Тогда характер поведения системы виден гораздо лучше.

---

## 35. Итоговая последовательность R&D

Получается такой pipeline:

```text
PHASE 1
ORC baseline
     │
     ▼
PHASE 2
Partitioning
     │
     ▼
PHASE 3
Sorting
     │
     ▼
PHASE 4
Bloom
     │
     ▼
PHASE 5
stride / stripe / compression
     │
     ▼
PHASE 6
Compaction
     │
     ▼
 BEST PHYSICAL ORC
     │
     ├───────────────────────────┐
     ▼                           ▼
PHASE 7                     PHASE 8
Spark 3.2                   Hive/Tez
vectorized                  vectorized
AQE                         CBO
DPP                            │
CBO                            ▼
     │                     Hive/LLAP
     │                     cold/warm
     │                           │
     └──────────────┬────────────┘
                    ▼
                 PHASE 9
           concurrency benchmark
                    │
                    ▼
                 PHASE 10
              SLA ≤ 3 sec
```

Главный принцип здесь — **сначала найти оптимальную физическую организацию ORC один раз, а затем на совершенно одинаковых ORC-файлах сравнить Spark 3.2 и Hive/Tez/LLAP**. Тогда можно будет корректно ответить отдельно на два вопроса: сколько ускорения даёт сам правильный ORC-layout и сколько дополнительно даёт query engine/LLAP.

И я бы особо выделил **Hive 2.0.0 как минимальную версию для LLAP**, но если задача — реально проводить R&D сейчас, версию Hive надо выбирать не просто «≥2.0», а исходя из совместимости с вашим Hadoop/YARN/Tez-стеком. Сам факт появления LLAP в 2.0.0 подтверждён официальной документацией Apache Hive. ([Apache Hive](https://hive.apache.org/development/desingdocs/llap/?utm_source=chatgpt.com "Apache Hive : LLAP"))