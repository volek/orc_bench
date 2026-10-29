-- =====================================================================================================================
-- Профилирование индекса на проде для генерации тестовых данных НТ.
--
-- Плейсхолдеры (заменить перед запуском):
--   <INDEX_PATH>    - полный путь к каталогу индекса в HDFS: <архив>/<проект>/<индекс>
--   <PARTITION_KEY> - значение key "средней" партиции без "key=" (медианная по размеру, см. вывод hdfs_stats.sh):
--                     дневные партиции - формат yyyy-MM--dd, например 2026-09--15,
--                     часовые - формат yyyy-MM-dd--HH, например 2026-09-15--12
--   <RANGE_KEYS>    - glob по нескольким партициям для раздела 9, например {2026-09--13,2026-09--14,2026-09--15}
--                     или 2026-09-15--* (все часы суток для часовых партиций)
--
--   sed -e 's#<INDEX_PATH>#hdfs:///archive/proj/idx#g' -e 's#<PARTITION_KEY>#2026-09--15#g' \
--       -e 's#<RANGE_KEYS>#{2026-09--13,2026-09--14,2026-09--15}#g' spark_queries.sql > q.sql
--
-- Запуск (сначала hdfs_stats.sh):
--   a) spark-sql -f q.sql > spark_queries_result.txt
--   b) spark-shell -i run_spark_queries.scala   (выполняет q.sql и печатает результаты)
--
-- КОНФИДЕНЦИАЛЬНОСТЬ: значения EPK_ID, логинов, имён пользователей, сессий и значения параметров не выводятся -
-- только количества, длины и распределения. Выводятся значения справочных полей: name, module, metamodelVersion,
-- userNode, tags и имена параметров (paramName).
--
-- ОБЪЁМ ЧТЕНИЯ: индекс целиком не читается и не листится. Разделы 1-8 читают одну партицию целиком,
-- раздел 9 - только перечисленные в <RANGE_KEYS> партиции. Каталог индекса указан лишь как basePath, чтобы Spark
-- распознал колонку key. Количество строк во всём индексе оценивается без чтения данных:
--   rows_index ~ rows_total (запрос 1.1) / bytes(партиции) * bytes(индекса)   (байты - из hdfs_stats.sh)
--
-- Каждый запрос заново читает партицию. Если партиция большая и на кластере есть место, раскомментировать
-- CACHE TABLE ниже.
-- =====================================================================================================================


-- =====================================================================================================================
-- 0. Размеры индекса и партиций, параметры ORC - см. hdfs_stats.sh (выполняется в shell до этого файла,
--    по его результатам выбирается <PARTITION_KEY> - партиция с медианным размером)
-- =====================================================================================================================


SET spark.sql.sources.partitionColumnTypeInference.enabled=false;
SET spark.sql.ansi.enabled=false;

-- Читается только каталог выбранной партиции, basePath нужен, чтобы в данных была колонка key
CREATE OR REPLACE TEMPORARY VIEW part_raw USING orc
OPTIONS (path '<INDEX_PATH>/key=<PARTITION_KEY>', basePath '<INDEX_PATH>');

CREATE OR REPLACE TEMPORARY VIEW p AS
SELECT *,
       input_file_name() AS src_file,
       -- размер строки в JSON-представлении: struct(*) берёт все колонки партиции, какой бы ни была схема
       -- (плюс колонка key - около 25 символов, без src_file/epk/row_json_len)
       length(to_json(struct(*))) AS row_json_len,
       CASE WHEN array_position(paramName, 'EPK_ID') > 0
            THEN paramValue[CAST(array_position(paramName, 'EPK_ID') - 1 AS INT)] END AS epk
FROM part_raw;

-- Параметры событий в "плоском" виде: одна строка на параметр события
CREATE OR REPLACE TEMPORARY VIEW pp AS
SELECT name, module, pos, pn AS param_name, paramValue[pos] AS pv, paramOldValue[pos] AS pov
FROM p LATERAL VIEW posexplode(paramName) t AS pos, pn;

-- Кеширование партиции, чтобы не перечитывать её каждым запросом (на диск executor'ов, в несжатом виде -
-- место нужно в несколько раз больше размера партиции в HDFS):
--   CACHE TABLE p OPTIONS ('storageLevel' 'DISK_ONLY')


-- =====================================================================================================================
-- 1. Общие характеристики партиции
-- =====================================================================================================================
SELECT count(*)                                AS rows_total,
       count(DISTINCT src_file)                AS orc_files,
       min(createdAt)                          AS min_createdAt,
       max(createdAt)                          AS max_createdAt,
       min(`__time`)                           AS min_time,
       max(`__time`)                           AS max_time,
       approx_count_distinct(name)             AS distinct_names,
       approx_count_distinct(module)           AS distinct_modules,
       approx_count_distinct(userLogin)        AS distinct_userLogin,
       approx_count_distinct(userName)         AS distinct_userName,
       approx_count_distinct(session)          AS distinct_session,
       approx_count_distinct(userNode)         AS distinct_userNode,
       approx_count_distinct(metamodelVersion) AS distinct_metamodelVersion,
       approx_count_distinct(epk)              AS distinct_epk,
       approx_count_distinct(archiveInternalId) AS distinct_archiveInternalId
FROM p;

-- 1.2 Доля NULL / пустых значений по полям
SELECT count(*)                                                     AS rows_total,
       sum(IF(userLogin IS NULL, 1, 0))                             AS null_userLogin,
       sum(IF(createdAt IS NULL, 1, 0))                             AS null_createdAt,
       sum(IF(session IS NULL, 1, 0))                               AS null_session,
       sum(IF(userNode IS NULL, 1, 0))                              AS null_userNode,
       sum(IF(module IS NULL, 1, 0))                                AS null_module,
       sum(IF(name IS NULL, 1, 0))                                  AS null_name,
       sum(IF(userName IS NULL, 1, 0))                              AS null_userName,
       sum(IF(metamodelVersion IS NULL, 1, 0))                      AS null_metamodelVersion,
       sum(IF(tags IS NULL, 1, 0))                                  AS null_tags,
       sum(IF(size(tags) = 0, 1, 0))                                AS empty_tags,
       sum(IF(paramName IS NULL, 1, 0))                             AS null_paramName,
       sum(IF(size(paramName) = 0, 1, 0))                           AS empty_paramName,
       sum(IF(paramOldValue IS NULL, 1, 0))                         AS null_paramOldValue,
       sum(IF(size(paramOldValue) = 0, 1, 0))                       AS empty_paramOldValue,
       sum(IF(size(paramName) <> size(paramValue), 1, 0))           AS name_value_size_mismatch,
       sum(IF(size(paramOldValue) > 0 AND size(paramName) <> size(paramOldValue), 1, 0)) AS name_oldvalue_size_mismatch,
       sum(IF(epk IS NULL, 1, 0))                                   AS without_epk,
       sum(IF(archiveInternalId IS NULL, 1, 0))                     AS null_archiveInternalId,
       sum(IF(archiveInsertTime IS NULL, 1, 0))                     AS null_archiveInsertTime,
       sum(IF(`__time` IS NULL, 1, 0))                              AS null_time
FROM p;

-- 1.3 Размер строки (байты в JSON-представлении) - для оценки "тяжести" событий
SELECT percentile_approx(row_json_len, array(0.1, 0.5, 0.9, 0.99, 0.999)) AS row_json_len_p10_50_90_99_999,
       max(row_json_len)                                                AS row_json_len_max,
       avg(row_json_len)                                                AS row_json_len_avg
FROM p;

-- 1.4 Количество строк в ORC-файлах партиции
SELECT count(*)                                              AS files,
       min(cnt)                                              AS rows_min,
       percentile_approx(cnt, array(0.1, 0.5, 0.9))          AS rows_p10_50_90,
       max(cnt)                                              AS rows_max,
       avg(cnt)                                              AS rows_avg
FROM (SELECT src_file, count(*) AS cnt FROM p GROUP BY src_file) t;


-- =====================================================================================================================
-- 2. Типы событий и модули
-- =====================================================================================================================
-- 2.1 Типы событий (name)
SELECT name, count(*) AS cnt, round(100 * count(*) / sum(count(*)) OVER (), 4) AS pct
FROM p
GROUP BY name
ORDER BY cnt DESC;

-- 2.2 Модули (module)
SELECT module, count(*) AS cnt, round(100 * count(*) / sum(count(*)) OVER (), 4) AS pct
FROM p
GROUP BY module
ORDER BY cnt DESC;

-- 2.3 Сочетания module x name + характеристики параметров для каждого сочетания
SELECT module, name,
       count(*)                                            AS cnt,
       round(100 * count(*) / sum(count(*)) OVER (), 4)    AS pct,
       min(size(paramName))                                AS params_min,
       percentile_approx(size(paramName), 0.5)             AS params_p50,
       max(size(paramName))                                AS params_max,
       sum(IF(epk IS NOT NULL, 1, 0))                      AS with_epk,
       sum(IF(size(paramOldValue) > 0, 1, 0))              AS with_old_values,
       avg(row_json_len)                     AS avg_row_json_len
FROM p
GROUP BY module, name
ORDER BY cnt DESC;


-- =====================================================================================================================
-- 3. Массивы параметров
-- =====================================================================================================================
-- 3.1 Размеры массивов
SELECT percentile_approx(size(paramName), array(0.1, 0.5, 0.9, 0.99, 0.999))  AS paramName_size_p10_50_90_99_999,
       max(size(paramName))                                                  AS paramName_size_max,
       avg(size(paramName))                                                  AS paramName_size_avg,
       percentile_approx(size(paramOldValue), array(0.5, 0.9, 0.99))         AS paramOldValue_size_p50_90_99,
       max(size(paramOldValue))                                              AS paramOldValue_size_max,
       percentile_approx(size(tags), array(0.5, 0.9, 0.99))                  AS tags_size_p50_90_99,
       max(size(tags))                                                       AS tags_size_max
FROM p;

-- 3.2 Гистограмма размера массива paramName
SELECT CASE WHEN s <= 10 THEN lpad(CAST(s AS STRING), 5, '0')
            WHEN s <= 50 THEN '00011-00050'
            WHEN s <= 100 THEN '00051-00100'
            WHEN s <= 500 THEN '00101-00500'
            WHEN s <= 1000 THEN '00501-01000'
            ELSE '01001+' END AS params_count_bucket,
       count(*) AS cnt
FROM (SELECT size(paramName) AS s FROM p) t
GROUP BY 1
ORDER BY 1;

-- 3.3 Суммарная длина значений параметров в событии (символы)
SELECT percentile_approx(total_len, array(0.1, 0.5, 0.9, 0.99, 0.999)) AS values_total_len_p10_50_90_99_999,
       max(total_len)                                                 AS values_total_len_max
FROM (SELECT aggregate(paramValue, 0L, (acc, x) -> acc + coalesce(length(x), 0)) AS total_len FROM p) t;

-- 3.4 Каталог параметров: частота, длины значений, кардинальность (без самих значений)
SELECT param_name,
       count(*)                                                AS occurrences,
       round(100 * count(*) / (SELECT count(*) FROM p), 4)     AS pct_of_events,
       approx_count_distinct(pv)                               AS distinct_values,
       sum(IF(pv IS NULL, 1, 0))                               AS null_values,
       sum(IF(pv = '', 1, 0))                                  AS empty_values,
       min(length(pv))                                         AS len_min,
       percentile_approx(length(pv), array(0.5, 0.9, 0.99))    AS len_p50_90_99,
       max(length(pv))                                         AS len_max,
       avg(length(pv))                                         AS len_avg,
       sum(IF(pov IS NOT NULL, 1, 0))                          AS with_old_value,
       percentile_approx(pos + 1, array(0.5, 0.9))             AS position_p50_90,
       sum(IF(pv RLIKE '^[0-9]+$', 1, 0))                      AS numeric_values,
       sum(IF(pv RLIKE '^\\s*[\\[{]', 1, 0))                   AS json_like_values
FROM pp
GROUP BY param_name
ORDER BY occurrences DESC;

-- 3.4.1 Гистограмма длин значений параметров (символы), все параметры
SELECT CASE WHEN pv IS NULL          THEN '0) null'
            WHEN length(pv) < 100    THEN '1) < 100'
            WHEN length(pv) < 1000   THEN '2) 100 - 1K'
            WHEN length(pv) < 10000  THEN '3) 1K - 10K'
            WHEN length(pv) < 100000 THEN '4) 10K - 100K'
            WHEN length(pv) < 1000000 THEN '5) 100K - 1M'
            ELSE '6) >= 1M' END                   AS value_len_bucket,
       count(*)                                    AS values_cnt,
       sum(length(pv))                             AS chars_total,
       approx_count_distinct(param_name)           AS distinct_params
FROM pp
GROUP BY 1
ORDER BY 1;

-- 3.4.2 Гистограмма длин значений по каждому параметру (в каких параметрах "длинные" значения)
SELECT param_name,
       count(*)                                                        AS values_cnt,
       sum(IF(length(pv) < 100, 1, 0))                                 AS lt_100,
       sum(IF(length(pv) >= 100 AND length(pv) < 1000, 1, 0))          AS `100_1K`,
       sum(IF(length(pv) >= 1000 AND length(pv) < 10000, 1, 0))        AS `1K_10K`,
       sum(IF(length(pv) >= 10000 AND length(pv) < 100000, 1, 0))      AS `10K_100K`,
       sum(IF(length(pv) >= 100000 AND length(pv) < 1000000, 1, 0))    AS `100K_1M`,
       sum(IF(length(pv) >= 1000000, 1, 0))                            AS ge_1M,
       sum(length(pv))                                                 AS chars_total,
       max(length(pv))                                                 AS len_max
FROM pp
GROUP BY param_name
ORDER BY chars_total DESC;

-- 3.4.3 Гистограмма суммарной длины значений параметров в событии (символы)
SELECT CASE WHEN total_len < 1000    THEN '1) < 1K'
            WHEN total_len < 10000   THEN '2) 1K - 10K'
            WHEN total_len < 100000  THEN '3) 10K - 100K'
            WHEN total_len < 1000000 THEN '4) 100K - 1M'
            ELSE '5) >= 1M' END                        AS event_values_len_bucket,
       count(*)                                         AS events_cnt,
       percentile_approx(params_cnt, array(0.5, 0.9))   AS params_cnt_p50_90
FROM (SELECT size(paramName) AS params_cnt,
             aggregate(paramValue, 0L, (acc, x) -> acc + coalesce(length(x), 0)) AS total_len
      FROM p) t
GROUP BY 1
ORDER BY 1;

-- 3.4.4 "Форма" значений параметров - определяет оператор поиска, который сформирует UI (см. fullTextSearch.md):
--       digits / no_letters (без букв) -> LIKE, letters (есть буквы) -> ILIKE
SELECT param_name,
       count(*)                                                                     AS values_cnt,
       sum(IF(pv IS NULL OR pv = '', 1, 0))                                         AS null_or_empty,
       sum(IF(pv RLIKE '^[0-9]+$', 1, 0))                                           AS digits,
       sum(IF(pv NOT RLIKE '^[0-9]+$' AND lower(pv) = upper(pv) AND pv <> '', 1, 0)) AS no_letters,
       sum(IF(lower(pv) <> upper(pv), 1, 0))                                        AS letters,
       sum(IF(pv RLIKE '^\\s*[\\[{]', 1, 0))                                     AS json_like
FROM pp
GROUP BY param_name
ORDER BY values_cnt DESC;

-- 3.5 Повторяющиеся имена параметров внутри одного события (влияет на ARRAY_POSITION - берётся первое вхождение)
SELECT param_name, count(*) AS events_with_duplicates
FROM (SELECT archiveInternalId, param_name, count(*) AS c
      FROM (SELECT archiveInternalId, pn AS param_name FROM p LATERAL VIEW explode(paramName) t AS pn) x
      GROUP BY archiveInternalId, param_name
      HAVING count(*) > 1) d
GROUP BY param_name
ORDER BY events_with_duplicates DESC
LIMIT 100;

-- 3.6 Наборы параметров по модулям и типам событий (module x name x param_name), топ-5000
--     Фильтры "module + name + параметр" - одно из самых частых сочетаний в запросах с прода.
SELECT module, name, param_name,
       count(*)                                    AS occurrences,
       approx_count_distinct(pv)                   AS distinct_values,
       percentile_approx(length(pv), 0.5)          AS len_p50,
       max(length(pv))                             AS len_max
FROM pp
GROUP BY module, name, param_name
ORDER BY occurrences DESC
LIMIT 5000;

-- 3.7 Количество уникальных "наборов" имён параметров (насколько стабильна структура событий одного типа)
SELECT name,
       approx_count_distinct(concat_ws(',', array_sort(paramName))) AS distinct_param_sets,
       count(*)                                                     AS cnt
FROM p
GROUP BY name
ORDER BY cnt DESC;


-- =====================================================================================================================
-- 4. EPK_ID (ключ клиента) - только распределения, без значений
-- =====================================================================================================================
-- 4.1 Наличие, кардинальность, позиция в массиве, длина значения
SELECT count(*)                                                                   AS rows_total,
       sum(IF(epk IS NOT NULL, 1, 0))                                             AS rows_with_epk,
       approx_count_distinct(epk)                                                 AS distinct_epk,
       percentile_approx(array_position(paramName, 'EPK_ID'), array(0.5, 0.9, 0.99)) AS epk_position_p50_90_99,
       max(array_position(paramName, 'EPK_ID'))                                   AS epk_position_max,
       percentile_approx(length(epk), array(0.01, 0.5, 0.99))                     AS epk_len_p1_50_99,
       sum(IF(epk RLIKE '^[0-9]+$', 1, 0))                                        AS epk_numeric
FROM p;

-- 4.2 Событий на один EPK_ID в партиции
SELECT count(*)                                                       AS distinct_epk,
       min(cnt)                                                       AS events_min,
       percentile_approx(cnt, array(0.5, 0.75, 0.9, 0.99, 0.999, 0.9999)) AS events_p50_75_90_99_999_9999,
       max(cnt)                                                       AS events_max,
       avg(cnt)                                                       AS events_avg
FROM (SELECT epk, count(*) AS cnt FROM p WHERE epk IS NOT NULL GROUP BY epk) t;

-- 4.3 Гистограмма "событий на EPK_ID" (сколько клиентов имеют N событий)
SELECT CASE WHEN cnt = 1 THEN '000001'
            WHEN cnt <= 5 THEN '000002-5'
            WHEN cnt <= 10 THEN '000006-10'
            WHEN cnt <= 50 THEN '000011-50'
            WHEN cnt <= 100 THEN '000051-100'
            WHEN cnt <= 1000 THEN '000101-1000'
            WHEN cnt <= 10000 THEN '001001-10000'
            ELSE '010001+' END AS events_per_epk,
       count(*) AS epk_cnt,
       sum(cnt) AS events
FROM (SELECT epk, count(*) AS cnt FROM p WHERE epk IS NOT NULL GROUP BY epk) t
GROUP BY 1
ORDER BY 1;

-- 4.4 Топ-50 самых "тяжёлых" клиентов (только количества, без EPK_ID)
SELECT cnt, distinct_names, distinct_modules
FROM (SELECT epk, count(*) AS cnt, count(DISTINCT name) AS distinct_names, count(DISTINCT module) AS distinct_modules
      FROM p WHERE epk IS NOT NULL GROUP BY epk) t
ORDER BY cnt DESC
LIMIT 50;

-- 4.5 Типов событий на один EPK_ID
SELECT percentile_approx(n, array(0.5, 0.9, 0.99)) AS names_per_epk_p50_90_99, max(n) AS names_per_epk_max
FROM (SELECT epk, count(DISTINCT name) AS n FROM p WHERE epk IS NOT NULL GROUP BY epk) t;

-- 4.6 Доля событий с EPK_ID по типам событий
SELECT name, count(*) AS cnt, sum(IF(epk IS NOT NULL, 1, 0)) AS with_epk
FROM p
GROUP BY name
ORDER BY cnt DESC;

-- 4.7 Связь EPK_ID и userLogin (сколько EPK на логин и логинов на EPK)
SELECT percentile_approx(n, array(0.5, 0.9, 0.99)) AS epk_per_login_p50_90_99, max(n) AS epk_per_login_max
FROM (SELECT userLogin, count(DISTINCT epk) AS n FROM p WHERE epk IS NOT NULL GROUP BY userLogin) t;

SELECT percentile_approx(n, array(0.5, 0.9, 0.99)) AS login_per_epk_p50_90_99, max(n) AS login_per_epk_max
FROM (SELECT epk, count(DISTINCT userLogin) AS n FROM p WHERE epk IS NOT NULL GROUP BY epk) t;


-- =====================================================================================================================
-- 5. Остальные поля
-- =====================================================================================================================
-- 5.1 Справочные поля: значения и количества
SELECT metamodelVersion, count(*) AS cnt FROM p GROUP BY metamodelVersion ORDER BY cnt DESC;

SELECT userNode, count(*) AS cnt FROM p GROUP BY userNode ORDER BY cnt DESC LIMIT 200;

SELECT tag, count(*) AS cnt FROM p LATERAL VIEW explode(tags) t AS tag GROUP BY tag ORDER BY cnt DESC LIMIT 200;

-- 5.2 Поля с персональными данными: только распределения "событий на значение" и длины
SELECT 'userLogin' AS field, count(*) AS distinct_values,
       percentile_approx(cnt, array(0.5, 0.9, 0.99, 0.999)) AS events_per_value_p50_90_99_999, max(cnt) AS max_events,
       percentile_approx(len, array(0.5, 0.99)) AS len_p50_99
FROM (SELECT userLogin AS v, count(*) AS cnt, max(length(userLogin)) AS len FROM p GROUP BY userLogin) t
UNION ALL
SELECT 'userName', count(*), percentile_approx(cnt, array(0.5, 0.9, 0.99, 0.999)), max(cnt), percentile_approx(len, array(0.5, 0.99))
FROM (SELECT userName AS v, count(*) AS cnt, max(length(userName)) AS len FROM p GROUP BY userName) t
UNION ALL
SELECT 'session', count(*), percentile_approx(cnt, array(0.5, 0.9, 0.99, 0.999)), max(cnt), percentile_approx(len, array(0.5, 0.99))
FROM (SELECT session AS v, count(*) AS cnt, max(length(session)) AS len FROM p GROUP BY session) t
UNION ALL
SELECT 'archiveInternalId', count(*), percentile_approx(cnt, array(0.5, 0.9, 0.99, 0.999)), max(cnt), percentile_approx(len, array(0.5, 0.99))
FROM (SELECT archiveInternalId AS v, count(*) AS cnt, max(length(archiveInternalId)) AS len FROM p GROUP BY archiveInternalId) t;

-- 5.3 Связи между полями: сессий на логин, событий на сессию, логинов на узел
SELECT percentile_approx(n, array(0.5, 0.9, 0.99)) AS sessions_per_login_p50_90_99, max(n) AS sessions_per_login_max
FROM (SELECT userLogin, count(DISTINCT session) AS n FROM p GROUP BY userLogin) t;

SELECT userNode, count(DISTINCT userLogin) AS logins, count(DISTINCT session) AS sessions, count(*) AS cnt
FROM p GROUP BY userNode ORDER BY cnt DESC LIMIT 100;

SELECT module, count(DISTINCT userLogin) AS logins, count(DISTINCT userNode) AS nodes, count(DISTINCT metamodelVersion) AS versions
FROM p GROUP BY module ORDER BY logins DESC;


-- =====================================================================================================================
-- 6. Время
-- =====================================================================================================================
-- 6.1 Распределение событий внутри партиции (по часам или минутам createdAt)
SELECT date_format(createdAt, 'yyyy-MM-dd HH') AS hh, count(*) AS cnt
FROM p GROUP BY 1 ORDER BY 1;

-- 6.2 Соотношение служебных времён: __time vs createdAt, задержка попадания в архив
SELECT sum(IF(`__time` = createdAt, 1, 0))                                                     AS time_eq_createdAt,
       percentile_approx(unix_timestamp(`__time`) - unix_timestamp(createdAt), array(0.01, 0.5, 0.99)) AS time_minus_created_sec_p1_50_99,
       percentile_approx(unix_timestamp(archiveInsertTime) - unix_timestamp(createdAt), array(0.5, 0.9, 0.99)) AS insert_lag_sec_p50_90_99,
       max(unix_timestamp(archiveInsertTime) - unix_timestamp(createdAt))                       AS insert_lag_sec_max
FROM p;

-- 6.3 Какое поле времени определяет партицию: доля событий вне диапазона партиции
--     Время приводится к формату ключа: дневные партиции yyyy-MM--dd, часовые yyyy-MM-dd--HH
SELECT sum(IF(date_format(`__time`, f) <> k, 1, 0))           AS time_outside_partition,
       sum(IF(date_format(createdAt, f) <> k, 1, 0))          AS createdAt_outside_partition,
       sum(IF(date_format(archiveInsertTime, f) <> k, 1, 0))  AS insertTime_outside_partition,
       count(*)                                               AS rows_total
FROM p
CROSS JOIN (SELECT '<PARTITION_KEY>' AS k,
                   IF(length('<PARTITION_KEY>') <= 11, 'yyyy-MM--dd', 'yyyy-MM-dd--HH') AS f) pk;


-- =====================================================================================================================
-- 7. Физическая упорядоченность данных в файлах (влияет на эффективность predicate pushdown в ORC)
-- =====================================================================================================================
-- 7.1 Сколько файлов партиции содержат один и тот же EPK_ID / name (если 1 - данные кластеризованы)
SELECT percentile_approx(files, array(0.5, 0.9, 0.99)) AS files_per_epk_p50_90_99, max(files) AS files_per_epk_max
FROM (SELECT epk, count(DISTINCT src_file) AS files FROM p WHERE epk IS NOT NULL GROUP BY epk) t;

SELECT name, count(DISTINCT src_file) AS files, count(*) AS cnt
FROM p GROUP BY name ORDER BY cnt DESC LIMIT 50;

-- 7.2 Отсортированы ли данные внутри файлов по времени (диапазон __time на файл)
SELECT percentile_approx(span_sec, array(0.1, 0.5, 0.9)) AS file_time_span_sec_p10_50_90
FROM (SELECT src_file, unix_timestamp(max(`__time`)) - unix_timestamp(min(`__time`)) AS span_sec
      FROM p GROUP BY src_file) t;


-- =====================================================================================================================
-- 8. Проверка "типовых" запросов на реальной партиции (время выполнения смотреть в выводе / Spark UI)
-- =====================================================================================================================
-- 8.1 Селективность фильтра по EPK_ID: сколько строк вернёт запрос по "медианному" и "тяжёлому" клиенту
SELECT percentile_approx(cnt, array(0.5, 0.99)) AS rows_returned_p50_99, max(cnt) AS rows_returned_max
FROM (SELECT epk, count(*) AS cnt FROM p WHERE epk IS NOT NULL GROUP BY epk) t;


-- =====================================================================================================================
-- 9. [ТЯЖЁЛЫЕ] Анализ по нескольким партициям (только перечисленным в <RANGE_KEYS>)
-- =====================================================================================================================
-- Читаются колонки paramName и paramValue - это основной объём данных, поэтому суммарный размер партиций в
-- <RANGE_KEYS> стоит ограничить (оценить по partitions.tsv из hdfs_stats.sh), например 3-7 дневных партиций
-- или 24-72 часовых. Количество строк по всем партициям индекса здесь не считается - см. оценку в заголовке файла.

-- 9.1 Сколько партиций за период содержит событие одного и того же клиента
--     (нужно, чтобы правильно "размазать" EPK_ID по партициям при генерации)
CREATE OR REPLACE TEMPORARY VIEW range_raw USING orc
OPTIONS (path '<INDEX_PATH>/key=<RANGE_KEYS>', basePath '<INDEX_PATH>');

CREATE OR REPLACE TEMPORARY VIEW pr AS
SELECT key,
       CASE WHEN array_position(paramName, 'EPK_ID') > 0
            THEN paramValue[CAST(array_position(paramName, 'EPK_ID') - 1 AS INT)] END AS epk
FROM range_raw;

SELECT count(*)                                                AS distinct_epk_in_period,
       percentile_approx(parts, array(0.5, 0.9, 0.99))        AS partitions_per_epk_p50_90_99,
       max(parts)                                              AS partitions_per_epk_max,
       percentile_approx(cnt, array(0.5, 0.9, 0.99, 0.999))   AS events_per_epk_in_period_p50_90_99_999,
       max(cnt)                                                AS events_per_epk_in_period_max
FROM (SELECT epk, count(DISTINCT key) AS parts, count(*) AS cnt FROM pr WHERE epk IS NOT NULL GROUP BY epk) t;

-- 9.2 Изменчивость состава событий между партициями периода
SELECT key, approx_count_distinct(epk) AS distinct_epk, count(*) AS cnt
FROM pr GROUP BY key ORDER BY key;
