package orcbench.generate;

import orcbench.model.Models.OrcWriteSettings;
import orcbench.model.Models.QuerySampleManifest;
import orcbench.model.Models.ReferenceDataset;
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

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.to_date;

/**
 * Cluster-side ORC writer via Spark. Requires {@code spark-submit} classpath
 * (Spark is compileOnly — not bundled in the fat JAR). Do not invoke against
 * live HDFS from a developer laptop (plan §7.0 / Checkpoint 4 (B)).
 */
public final class SparkOrcGenerateBackend implements GenerateBackend {

    private final SparkSession spark;

    public SparkOrcGenerateBackend(SparkSession spark) {
        if (spark == null) {
            throw new IllegalArgumentException("SparkSession is required for live generate");
        }
        this.spark = spark;
    }

    @Override
    public void writeRawArray(
            String basePath,
            long rowCount,
            int parallelismHint,
            EventRowFactory factory,
            OrcWriteSettings orc) {
        int partitions = Math.max(1, parallelismHint);

        // Static FlatMapFunction: anonymous inner classes capture SparkOrcGenerateBackend
        // (holds SparkSession) and fail with Task not serializable on YARN.
        JavaRDD<Row> rdd = spark.range(0L, rowCount).repartition(partitions).javaRDD()
                .mapPartitions(new RowPartitionMapper(factory));

        Dataset<Row> df = spark.createDataFrame(rdd, eventSchema());
        org.apache.spark.sql.DataFrameWriter<Row> writer = df.write().mode(SaveMode.Overwrite)
                .option("compression", orc.compression == null
                        ? "snappy" : orc.compression.toLowerCase(Locale.ROOT))
                .option("orc.stripe.size", Long.toString(Math.max(1, orc.stripeSizeMib) * 1024L * 1024L))
                .option("orc.create.index", Boolean.toString(orc.createIndex))
                .option("orc.row.index.stride", Integer.toString(orc.rowIndexStride));
        if (orc.bloomColumns != null && !orc.bloomColumns.isEmpty()) {
            writer = writer
                    .option("orc.bloom.filter.columns", String.join(",", orc.bloomColumns))
                    .option("orc.bloom.filter.fpp", Double.toString(orc.bloomFpp));
        }
        writer.format("orc").save(basePath);
        System.out.println("[generate] wrote RAW_ARRAY_BASE to " + basePath);
    }

    @Override
    public void writeReferences(
            String refsPath,
            Map<String, ReferenceDataset> datasets,
            long seed,
            String algo) {
        if (datasets == null) {
            return;
        }
        ReferenceDataset module = datasets.get("module_dictionary");
        if (module != null && module.enabled) {
            int n = module.rowCount == null ? 500 : module.rowCount;
            writeIdType(refsPath + "/module_dictionary", RefBuilders.moduleDictionary(n));
        }
        ReferenceDataset epk = datasets.get("epk_dictionary");
        if (epk != null && epk.enabled) {
            int n = epk.rowCount == null ? 100_000 : epk.rowCount;
            writeIdType(refsPath + "/epk_dictionary", RefBuilders.epkDictionary(n));
        }
        ReferenceDataset dates = datasets.get("partition_dates");
        if (dates != null && dates.enabled) {
            int cats = 5;
            List<DateRefRow> rows = RefBuilders.partitionDates(31, cats);
            List<Row> sparkRows = new ArrayList<>();
            for (DateRefRow r : rows) {
                sparkRows.add(RowFactory.create(r.eventDate, r.category));
            }
            spark.createDataFrame(sparkRows, new StructType(new StructField[]{
                    DataTypes.createStructField("event_date", DataTypes.StringType, false),
                    DataTypes.createStructField("category", DataTypes.StringType, false)
            })).withColumn("event_date", to_date(col("event_date")))
                    .write().mode(SaveMode.Overwrite).format("orc")
                    .save(refsPath + "/partition_dates");
        }
        System.out.println("[generate] wrote refs under " + refsPath);
    }

    @Override
    public void writeManifest(String manifestPath, QuerySampleManifest manifest) throws Exception {
        List<Row> rows = new ArrayList<>();
        if (manifest != null && manifest.samples != null) {
            for (Map.Entry<String, Map<String, List<String>>> e : manifest.samples.entrySet()) {
                for (Map.Entry<String, List<String>> c : e.getValue().entrySet()) {
                    for (String v : c.getValue()) {
                        rows.add(RowFactory.create(e.getKey(), c.getKey(), v));
                    }
                }
            }
        }
        spark.createDataFrame(rows, new StructType(new StructField[]{
                DataTypes.createStructField("key", DataTypes.StringType, false),
                DataTypes.createStructField("case_name", DataTypes.StringType, false),
                DataTypes.createStructField("value", DataTypes.StringType, false)
        })).coalesce(1).write().mode(SaveMode.Overwrite).option("header", "true")
                .csv(manifestPath + "/query_samples");

        // Also drop a local JSON copy next to the submit cwd for ops convenience
        Path localDir = Paths.get("generated", "manifest");
        Files.createDirectories(localDir);
        Files.write(localDir.resolve("query_samples.json"),
                toJson(manifest).getBytes(StandardCharsets.UTF_8));
        System.out.println("[generate] wrote manifest to " + manifestPath
                + " and local generated/manifest/query_samples.json");
    }

    @Override
    public long countRows(String basePath) {
        return spark.read().format("orc").load(basePath).count();
    }

    private void writeIdType(String path, List<RefRow> rows) {
        List<Row> sparkRows = new ArrayList<>();
        for (RefRow r : rows) {
            sparkRows.add(RowFactory.create(r.id, r.type));
        }
        spark.createDataFrame(sparkRows, new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.StringType, false),
                DataTypes.createStructField("type", DataTypes.StringType, false)
        })).write().mode(SaveMode.Overwrite).format("orc").save(path);
    }

    /**
     * Executor-side mapper: only serializable {@link EventRowFactory} state, no outer backend.
     */
    static final class RowPartitionMapper
            implements FlatMapFunction<Iterator<Long>, Row>, Serializable {
        private static final long serialVersionUID = 1L;
        private final EventRowFactory factory;

        RowPartitionMapper(EventRowFactory factory) {
            this.factory = factory;
        }

        @Override
        public Iterator<Row> call(Iterator<Long> iter) {
            List<Row> out = new ArrayList<>();
            while (iter.hasNext()) {
                out.add(toSparkRow(factory.create(iter.next())));
            }
            return out.iterator();
        }
    }

    static StructType eventSchema() {
        return new StructType(new StructField[]{
                DataTypes.createStructField("userLogin", DataTypes.StringType, false),
                DataTypes.createStructField("createdAt", DataTypes.TimestampType, false),
                DataTypes.createStructField("session", DataTypes.StringType, false),
                DataTypes.createStructField("userNode", DataTypes.StringType, false),
                DataTypes.createStructField("module", DataTypes.StringType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("userName", DataTypes.StringType, false),
                DataTypes.createStructField("metamodelVersion", DataTypes.StringType, false),
                DataTypes.createStructField("tags", DataTypes.createArrayType(DataTypes.StringType), false),
                DataTypes.createStructField("paramOldValue",
                        DataTypes.createArrayType(DataTypes.StringType, true), true),
                DataTypes.createStructField("paramName", DataTypes.createArrayType(DataTypes.StringType), false),
                DataTypes.createStructField("paramValue", DataTypes.createArrayType(DataTypes.StringType), false),
                DataTypes.createStructField("archiveInternalId", DataTypes.StringType, false),
                DataTypes.createStructField("archiveInsertTime", DataTypes.TimestampType, false),
                DataTypes.createStructField("__time", DataTypes.TimestampType, false)
        });
    }

    static Row toSparkRow(EventRow e) {
        return RowFactory.create(
                e.userLogin,
                new Timestamp(e.createdAtMs),
                e.session,
                e.userNode,
                e.module,
                e.name,
                e.userName,
                e.metamodelVersion,
                toSeq(e.tags),
                toSeqNullable(e.paramOldValue),
                toSeq(e.paramName),
                toSeq(e.paramValue),
                e.archiveInternalId,
                new Timestamp(e.archiveInsertTimeMs),
                new Timestamp(e.timeMs)
        );
    }

    private static scala.collection.Seq<String> toSeq(List<String> list) {
        return JavaConverters.asScalaBufferConverter(list).asScala().toList();
    }

    private static scala.collection.Seq<String> toSeqNullable(List<String> list) {
        List<String> copy = new ArrayList<>(list == null ? java.util.Collections.<String>emptyList() : list);
        return JavaConverters.asScalaBufferConverter(copy).asScala().toList();
    }

    private static String toJson(QuerySampleManifest manifest) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        if (manifest != null && manifest.samples != null) {
            for (Map.Entry<String, Map<String, List<String>>> e : manifest.samples.entrySet()) {
                sb.append("  \"").append(e.getKey()).append("\": {\n");
                for (Map.Entry<String, List<String>> c : e.getValue().entrySet()) {
                    sb.append("    \"").append(c.getKey()).append("\": [");
                    List<String> vals = c.getValue();
                    for (int i = 0; i < vals.size(); i++) {
                        if (i > 0) {
                            sb.append(',');
                        }
                        sb.append('"').append(vals.get(i).replace("\"", "\\\"")).append('"');
                    }
                    sb.append("],\n");
                }
                sb.append("  },\n");
            }
        }
        sb.append("  \"partitionDates\": [");
        if (manifest != null && manifest.partitionDates != null) {
            for (int i = 0; i < manifest.partitionDates.size(); i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append('"').append(manifest.partitionDates.get(i)).append('"');
            }
        }
        sb.append("]\n}\n");
        return sb.toString();
    }
}
