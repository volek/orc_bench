package orcbench.generate;

import orcbench.model.Models.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.to_date;

public final class DatasetGenerator {
    private static final long APPROX_BYTES_PER_ROW = 1200L;

    private DatasetGenerator() {}

    public static long estimateRowCount(long targetLogicalGib) {
        return Math.max(1L, (targetLogicalGib * 1024L * 1024L * 1024L) / APPROX_BYTES_PER_ROW);
    }

    public static QuerySampleManifest generate(SparkSession spark, LoadedConfig cfg, String scale, boolean dryRun) throws Exception {
        DatasetContract ds = cfg.dataset;
        ScaleSpec scaleSpec = ds.scales.get(scale);
        if (scaleSpec == null) {
            throw new IllegalArgumentException("Unknown scale " + scale);
        }
        long rowCount = ds.generation.rowCount != null ? ds.generation.rowCount : estimateRowCount(scaleSpec.targetLogicalGib);
        String hdfsRoot = cfg.benchmark.benchmark.hdfsRoot;
        String basePath = hdfsRoot + "/" + scale + "/" + cfg.benchmark.physicalBaseline.id;
        String refsPath = hdfsRoot + "/" + scale + "/refs";
        String manifestPath = hdfsRoot + "/" + scale + "/manifest";

        System.out.println("[generate] scale=" + scale + " targetGiB=" + scaleSpec.targetLogicalGib
                + " rows≈" + rowCount + " path=" + basePath);

        if (dryRun) {
            System.out.println("[generate] dry-run: skip ORC materialization");
            QuerySampleManifest m = new QuerySampleManifest();
            m.partitionDates = Collections.singletonList(
                    Defaults.getOrElse(cfg.queries.querySuite.defaults, "date_value", "2026-01-15"));
            return m;
        }

        long seed = ds.generation.seed;
        String algo = ds.generation.algorithmVersion;
        long fromMs = Instant.parse(ds.generation.timestampFrom).toEpochMilli();
        long toMs = Instant.parse(ds.generation.timestampToExclusive).toEpochMilli();
        int batch = ds.generation.batchRows;
        List<String> paramKeys = new ArrayList<>(ds.parameters.catalog.keySet());
        Collections.sort(paramKeys);

        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("userLogin", DataTypes.StringType, false),
                DataTypes.createStructField("createdAt", DataTypes.TimestampType, false),
                DataTypes.createStructField("session", DataTypes.StringType, false),
                DataTypes.createStructField("userNode", DataTypes.StringType, false),
                DataTypes.createStructField("module", DataTypes.StringType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("userName", DataTypes.StringType, false),
                DataTypes.createStructField("metamodelVersion", DataTypes.StringType, false),
                DataTypes.createStructField("tags", DataTypes.createArrayType(DataTypes.StringType), false),
                DataTypes.createStructField("paramOldValue", DataTypes.createArrayType(DataTypes.StringType), true),
                DataTypes.createStructField("paramName", DataTypes.createArrayType(DataTypes.StringType), false),
                DataTypes.createStructField("paramValue", DataTypes.createArrayType(DataTypes.StringType), false),
                DataTypes.createStructField("archiveInternalId", DataTypes.StringType, false),
                DataTypes.createStructField("archiveInsertTime", DataTypes.TimestampType, false),
                DataTypes.createStructField("__time", DataTypes.TimestampType, false)
        });

        OrcBaseline orc = cfg.benchmark.physicalBaseline.orc;
        int numPartitions = Math.max(1, (int) Math.min(2000, rowCount / Math.max(1, batch * 64L)));

        final Map<String, ColumnGenerator> catalog = ds.parameters.catalog;
        final Map<String, Map<String, Object>> jsonTemplates = ds.jsonTemplates;
        final List<String> paramKeysFinal = paramKeys;
        final long seedF = seed;
        final String algoF = algo;
        final long fromMsF = fromMs;
        final long toMsF = toMs;

        // Spark Java exposes range().javaRDD() as JavaRDD<Long>
        JavaRDD<Row> rdd = spark.range(0L, rowCount).repartition(numPartitions).javaRDD()
                .mapPartitions(new FlatMapFunction<Iterator<Long>, Row>() {
                    @Override
                    public Iterator<Row> call(Iterator<Long> iter) {
                        List<Row> out = new ArrayList<>();
                        while (iter.hasNext()) {
                            long rowId = iter.next();
                            out.add(buildEventRow(rowId, seedF, algoF, fromMsF, toMsF, paramKeysFinal, catalog, jsonTemplates));
                        }
                        return out.iterator();
                    }
                });

        Dataset<Row> df = spark.createDataFrame(rdd, schema);
        org.apache.spark.sql.DataFrameWriter<Row> writer = df.write().mode(SaveMode.Overwrite)
                .option("compression", orc.compression.toLowerCase())
                .option("orc.stripe.size", Long.toString(orc.stripeSizeMib * 1024L * 1024L))
                .option("orc.create.index", Boolean.toString(orc.createIndex))
                .option("orc.row.index.stride", Integer.toString(orc.rowIndexStride));
        if (!orc.bloomColumns.isEmpty()) {
            writer = writer
                    .option("orc.bloom.filter.columns", String.join(",", orc.bloomColumns))
                    .option("orc.bloom.filter.fpp", Double.toString(orc.bloomFpp));
        }
        writer.format("orc").save(basePath);
        System.out.println("[generate] wrote RAW_ARRAY_BASE to " + basePath);

        writeReferenceTables(spark, ds, refsPath);
        QuerySampleManifest manifest = buildQuerySamples(spark, basePath, ds, cfg);
        writeManifest(spark, manifest, manifestPath);
        validate(spark, basePath, ds, rowCount);
        return manifest;
    }

    private static final class Defaults {
        static String getOrElse(Map<String, String> m, String k, String d) {
            String v = m.get(k);
            return v == null ? d : v;
        }
    }

    static Row buildEventRow(
            long rowId, long seed, String algo, long fromMs, long toMs,
            List<String> paramKeys, Map<String, ColumnGenerator> catalog,
            Map<String, Map<String, Object>> jsonTemplates) {
        String userLogin = SynthGenerators.prefixedId(seed, rowId, "userLogin", algo, "userLogin", 1_000_000L);
        long createdAtMs = SynthGenerators.timestampUniform(seed, rowId, "createdAt", algo, fromMs, toMs);
        Timestamp createdAt = new Timestamp(createdAtMs);
        String session = SynthGenerators.prefixedId(seed, rowId, "session", algo, "copy2_", 5_000_000L);
        String userNode = SynthGenerators.dictionary(seed, rowId, "userNode", algo, "userNode", 1000L);
        String module = SynthGenerators.dictionary(seed, rowId, "module", algo, "module_", 500L);
        String name = SynthGenerators.weightedPick(seed, rowId, "name", algo,
                Arrays.asList("LOGON", "LOGOFF", "ACTION"), Arrays.asList(0.6, 0.25, 0.15));
        String userName = SynthGenerators.prefixedId(seed, rowId, "userName", algo, "userName", 1_000_000L);
        String metamodelVersion = SynthGenerators.dictionary(seed, rowId, "metamodelVersion", algo, "metamodelVersion", 1000L);

        List<String> names = new ArrayList<>();
        List<String> values = new ArrayList<>();
        for (String key : paramKeys) {
            names.add(key);
            values.add(generateParam(catalog.get(key), seed, rowId, key, algo, name, session, jsonTemplates));
        }

        String archiveId = SynthGenerators.uuidDeterministic(seed, rowId, "archiveInternalId", algo);
        int offset = SynthGenerators.bounded(seed, rowId, "archiveInsertTime", algo, 2L) == 0 ? 0 : 120;
        Timestamp archiveInsert = new Timestamp(createdAtMs + offset * 1000L);

        return RowFactory.create(
                userLogin, createdAt, session, userNode, module, name, userName, metamodelVersion,
                toSeq(Collections.<String>emptyList()),
                null,
                toSeq(names),
                toSeq(values),
                archiveId, archiveInsert, createdAt
        );
    }

    private static scala.collection.Seq<String> toSeq(List<String> list) {
        return JavaConverters.asScalaBufferConverter(list).asScala().toList();
    }

    private static String generateParam(ColumnGenerator g, long seed, long rowId, String key, String algo,
                                        String eventName, String session,
                                        Map<String, Map<String, Object>> jsonTemplates) {
        if (g == null) return key + "_missing";
        switch (g.generator) {
            case "uuid_deterministic":
                return SynthGenerators.uuidDeterministic(seed, rowId, key, algo);
            case "copy_column":
                if ("name".equals(g.source)) return eventName;
                if ("session".equals(g.source)) return session;
                return SynthGenerators.prefixedId(seed, rowId, key, algo, "copy_", 1000L);
            case "prefixed_id":
                return SynthGenerators.prefixedId(seed, rowId, key, algo,
                        g.prefix == null ? "" : g.prefix, g.cardinality == null ? 1_000_000L : g.cardinality);
            case "dictionary":
                return SynthGenerators.dictionary(seed, rowId, key, algo,
                        g.prefix == null ? "" : g.prefix, g.cardinality == null ? 100L : g.cardinality);
            case "documentation_ip_v4":
                return SynthGenerators.documentationIpv4(seed, rowId, key, algo);
            case "fake_token":
                return SynthGenerators.fakeToken(seed, rowId, key, algo,
                        g.prefix == null ? "token_" : g.prefix, g.cardinality == null ? 1_000_000L : g.cardinality);
            case "json_template":
                String templateKey = g.template != null ? g.template : "mobile_sdk_v1";
                return SynthGenerators.jsonMobileSdk(seed, rowId, algo,
                        jsonTemplates.getOrDefault(templateKey, Collections.emptyMap()));
            default:
                return SynthGenerators.prefixedId(seed, rowId, key, algo, g.generator + "_", 1000L);
        }
    }

    private static void writeReferenceTables(SparkSession spark, DatasetContract ds, String refsPath) {
        ReferenceDataset module = ds.referenceDatasets.get("module_dictionary");
        if (module != null && module.enabled) {
            int n = module.rowCount == null ? 500 : module.rowCount;
            List<Row> rows = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                String t = i % 3 == 0 ? "A" : (i % 3 == 1 ? "B" : "C");
                rows.add(RowFactory.create("module_" + i, t));
            }
            spark.createDataFrame(rows, new StructType(new StructField[]{
                    DataTypes.createStructField("id", DataTypes.StringType, false),
                    DataTypes.createStructField("type", DataTypes.StringType, false)
            })).write().mode(SaveMode.Overwrite).format("orc").save(refsPath + "/module_dictionary");
        }

        ReferenceDataset epk = ds.referenceDatasets.get("epk_dictionary");
        if (epk != null && epk.enabled) {
            int n = epk.rowCount == null ? 100000 : epk.rowCount;
            List<Row> rows = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                rows.add(RowFactory.create("epk_" + i, i % 2 == 0 ? "A" : "B"));
            }
            spark.createDataFrame(rows, new StructType(new StructField[]{
                    DataTypes.createStructField("id", DataTypes.StringType, false),
                    DataTypes.createStructField("type", DataTypes.StringType, false)
            })).write().mode(SaveMode.Overwrite).format("orc").save(refsPath + "/epk_dictionary");
        }

        ReferenceDataset dates = ds.referenceDatasets.get("partition_dates");
        if (dates != null && dates.enabled) {
            List<Row> rows = new ArrayList<>();
            for (int d = 0; d < 31; d++) {
                String day = String.format("2026-01-%02d", d + 1);
                rows.add(RowFactory.create(day, d % 2 == 0 ? "peak" : "offpeak"));
            }
            spark.createDataFrame(rows, new StructType(new StructField[]{
                    DataTypes.createStructField("event_date", DataTypes.StringType, false),
                    DataTypes.createStructField("category", DataTypes.StringType, false)
            })).withColumn("event_date", to_date(col("event_date")))
                    .write().mode(SaveMode.Overwrite).format("orc").save(refsPath + "/partition_dates");
        }
    }

    private static QuerySampleManifest buildQuerySamples(SparkSession spark, String basePath, DatasetContract ds, LoadedConfig cfg) {
        Dataset<Row> df = spark.read().format("orc").load(basePath);
        int sampleN = ds.querySamples.valuesPerCase;
        QuerySampleManifest manifest = new QuerySampleManifest();

        List<String> userLogins = takeDistinct(df, "userLogin", sampleN * 4);
        List<String> sessions = takeDistinct(df, "session", sampleN * 4);

        df.createOrReplaceTempView("events_sample");
        List<String> epks = spark.sql(
                "SELECT DISTINCT element_at(paramValue, array_position(paramName, 'EPK_ID')) AS epk "
                        + "FROM events_sample WHERE array_contains(paramName, 'EPK_ID') LIMIT 200")
                .javaRDD().map(r -> r.getString(0)).filter(s -> s != null).collect();
        List<String> ips = spark.sql(
                "SELECT DISTINCT element_at(paramValue, array_position(paramName, 'IP_ADDRESS')) AS ip "
                        + "FROM events_sample WHERE array_contains(paramName, 'IP_ADDRESS') LIMIT 200")
                .javaRDD().map(r -> r.getString(0)).filter(s -> s != null).collect();
        List<String> channels = spark.sql(
                "SELECT DISTINCT element_at(paramValue, array_position(paramName, 'CHANNEL_TYPE')) AS ch "
                        + "FROM events_sample WHERE array_contains(paramName, 'CHANNEL_TYPE') LIMIT 200")
                .javaRDD().map(r -> r.getString(0)).filter(s -> s != null).collect();

        manifest.samples.put("userLogin", bucket(userLogins, ds.querySamples.cases, sampleN));
        manifest.samples.put("session", bucket(sessions, ds.querySamples.cases, sampleN));
        manifest.samples.put("EPK_ID", bucket(epks, ds.querySamples.cases, sampleN));
        manifest.samples.put("SESSION_ID", bucket(sessions, ds.querySamples.cases, sampleN));
        manifest.samples.put("IP_ADDRESS", bucket(ips, ds.querySamples.cases, sampleN));
        manifest.samples.put("CHANNEL_TYPE", bucket(channels, ds.querySamples.cases, sampleN));
        Map<String, List<String>> device = new HashMap<>();
        device.put("present", Arrays.asList("iOS", "Android"));
        device.put("absent", Collections.singletonList("UNKNOWN_OS"));
        device.put("selective", Collections.singletonList("iOS"));
        device.put("common", Collections.singletonList("Android"));
        manifest.samples.put("DeviceSystemName", device);
        String dateValue = cfg.queries.querySuite.defaults.getOrDefault("date_value", "2026-01-15");
        manifest.partitionDates = Collections.singletonList(dateValue);
        return manifest;
    }

    private static List<String> takeDistinct(Dataset<Row> df, String colName, int n) {
        return df.select(colName).distinct().limit(n).javaRDD().map(r -> r.getString(0)).collect();
    }

    private static Map<String, List<String>> bucket(List<String> values, List<String> cases, int sampleN) {
        Map<String, List<String>> out = new HashMap<>();
        List<String> present = values.subList(0, Math.min(sampleN, values.size()));
        List<String> selective = values.subList(Math.min(sampleN, values.size()),
                Math.min(sampleN * 2, values.size()));
        List<String> common = values.subList(0, Math.min(3, values.size()));
        List<String> absent = new ArrayList<>();
        for (int i = 0; i < sampleN; i++) absent.add("ABSENT_" + i + "_NOT_IN_DATA");
        for (String c : cases) {
            switch (c) {
                case "present": out.put(c, new ArrayList<>(present)); break;
                case "absent": out.put(c, absent); break;
                case "selective":
                    out.put(c, selective.isEmpty() ? new ArrayList<>(present.subList(0, Math.min(1, present.size()))) : new ArrayList<>(selective));
                    break;
                case "common": out.put(c, new ArrayList<>(common)); break;
                default: out.put(c, new ArrayList<>(present));
            }
        }
        return out;
    }

    private static void writeManifest(SparkSession spark, QuerySampleManifest manifest, String path) throws Exception {
        List<Row> rows = new ArrayList<>();
        for (Map.Entry<String, Map<String, List<String>>> e : manifest.samples.entrySet()) {
            for (Map.Entry<String, List<String>> c : e.getValue().entrySet()) {
                for (String v : c.getValue()) {
                    rows.add(RowFactory.create(e.getKey(), c.getKey(), v));
                }
            }
        }
        spark.createDataFrame(rows, new StructType(new StructField[]{
                DataTypes.createStructField("key", DataTypes.StringType, false),
                DataTypes.createStructField("case_name", DataTypes.StringType, false),
                DataTypes.createStructField("value", DataTypes.StringType, false)
        })).coalesce(1).write().mode(SaveMode.Overwrite).option("header", "true").csv(path + "/query_samples");

        Path localDir = Paths.get("generated", "manifest");
        Files.createDirectories(localDir);
        StringBuilder sb = new StringBuilder("{\n");
        for (Map.Entry<String, Map<String, List<String>>> e : manifest.samples.entrySet()) {
            sb.append("  \"").append(e.getKey()).append("\": {\n");
            for (Map.Entry<String, List<String>> c : e.getValue().entrySet()) {
                sb.append("    \"").append(c.getKey()).append("\": [");
                for (int i = 0; i < c.getValue().size(); i++) {
                    if (i > 0) sb.append(',');
                    sb.append('"').append(c.getValue().get(i).replace("\"", "\\\"")).append('"');
                }
                sb.append("],\n");
            }
            sb.append("  },\n");
        }
        sb.append("  \"partitionDates\": [");
        for (int i = 0; i < manifest.partitionDates.size(); i++) {
            if (i > 0) sb.append(',');
            sb.append('"').append(manifest.partitionDates.get(i)).append('"');
        }
        sb.append("]\n}\n");
        Files.write(localDir.resolve("query_samples.json"), sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static void validate(SparkSession spark, String basePath, DatasetContract ds, long expectedRows) {
        Dataset<Row> df = spark.read().format("orc").load(basePath);
        long count = df.count();
        if (ds.validation.checkRowCount && count != expectedRows) {
            String msg = "row count mismatch: expected=" + expectedRows + " actual=" + count;
            if (ds.validation.failOnViolation) throw new IllegalStateException(msg);
            System.out.println("[validate] WARN " + msg);
        }
        if (ds.validation.checkNameValueAlignment) {
            long bad = df.filter(size(col("paramName")).notEqual(size(col("paramValue")))).limit(1).count();
            if (bad > 0) {
                String msg = "paramName/paramValue length mismatch";
                if (ds.validation.failOnViolation) throw new IllegalStateException(msg);
                System.out.println("[validate] WARN " + msg);
            }
        }
        System.out.println("[validate] OK rows=" + count);
    }
}
