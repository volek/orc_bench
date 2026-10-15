package orcbench.layout;

import orcbench.model.Models.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.spark.sql.functions.array_position;
import static org.apache.spark.sql.functions.arrays_zip;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.element_at;
import static org.apache.spark.sql.functions.get_json_object;
import static org.apache.spark.sql.functions.hour;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.map_from_arrays;
import static org.apache.spark.sql.functions.to_date;

public final class LayoutMaterializer {
    private LayoutMaterializer() {}

    public static List<ExperimentInstance> materialize(
            SparkSession spark, LoadedConfig cfg, String scale,
            List<ExperimentInstance> instances, boolean dryRun, boolean cleanup, List<String> retain) throws Exception {

        String baselineId = cfg.benchmark.physicalBaseline.id;
        String hdfsRoot = cfg.benchmark.benchmark.hdfsRoot;
        String basePath = hdfsRoot + "/" + scale + "/" + baselineId;
        List<String> flatCols = cfg.benchmark.representations.containsKey("FLAT")
                ? cfg.benchmark.representations.get("FLAT").columns
                : Arrays.asList("epk_id", "session_id", "operation_uid", "channel_type",
                "vsp_code", "osb_code", "tb_code", "ip_address", "device_system_name");

        Set<String> rewriteKinds = new HashSet<>(Arrays.asList("rewrite", "hive_compatible_write", "baseline"));
        List<ExperimentInstance> toWrite = new ArrayList<>();
        for (ExperimentInstance i : instances) {
            if (i.skipReason == null && i.layoutPath != null
                    && rewriteKinds.contains(i.kind) && !i.layoutPath.equals(basePath)) {
                toWrite.add(i);
            }
        }
        System.out.println("[materialize] rewrite layouts to write: " + toWrite.size());
        if (dryRun) {
            for (ExperimentInstance i : toWrite) {
                System.out.println("  dry-run would write " + i.layoutPath + " (" + i.variantLabel + ")");
            }
            return instances;
        }

        Dataset<Row> baseDf = spark.read().format("orc").load(basePath);
        List<String> ddlBuf = new ArrayList<>();

        for (ExperimentInstance inst : toWrite) {
            System.out.println("[materialize] " + inst.experimentId + "/" + inst.variantLabel + " -> " + inst.layoutPath);
            Dataset<Row> df = applyRepresentation(baseDf, inst.representation, flatCols);

            if (!inst.partitionColumns.isEmpty()) {
                if (inst.partitionColumns.contains("event_date")) {
                    df = df.withColumn("event_date", to_date(col("__time")));
                }
                if (inst.partitionColumns.contains("event_hour")) {
                    df = df.withColumn("event_hour", hour(col("__time")));
                }
            }
            if (!inst.sorting.isEmpty()) {
                org.apache.spark.sql.Column[] sortCols = new org.apache.spark.sql.Column[inst.sorting.size()];
                for (int i = 0; i < inst.sorting.size(); i++) sortCols[i] = col(inst.sorting.get(i));
                df = df.repartition(col(inst.sorting.get(0))).sortWithinPartitions(sortCols);
            }

            long approxBytes = estimateBytes(spark, basePath);
            long targetBytes = inst.targetFileSizeMib * 1024L * 1024L;
            int parts = Math.max(1, (int) (approxBytes / Math.max(targetBytes, 1L)));
            if (inst.bucketCount <= 0) {
                df = df.repartition(parts);
            }

            OrcWriteSettings orc = inst.orcSettings;
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

            if (inst.bucketCount > 0 && inst.bucketKey != null) {
                writer.bucketBy(inst.bucketCount, inst.bucketKey)
                        .sortBy(inst.bucketKey)
                        .option("path", inst.layoutPath)
                        .format("orc")
                        .saveAsTable("tmp_bucket_" + inst.experimentId.toLowerCase() + "_" + System.nanoTime());
                df.write().mode(SaveMode.Overwrite).format("orc").save(inst.layoutPath);
            } else if (!inst.partitionColumns.isEmpty()) {
                writer.partitionBy(inst.partitionColumns.toArray(new String[0])).format("orc").save(inst.layoutPath);
            } else {
                writer.format("orc").save(inst.layoutPath);
            }
            ddlBuf.add(generateDdl(inst, inst.layoutPath));
        }

        writeDdlFile(cfg.benchmark.benchmark.generatedHiveDdl, ddlBuf);
        if (cleanup) cleanupLayouts(hdfsRoot, scale, baselineId, instances, retain);
        return instances;
    }

    private static Dataset<Row> applyRepresentation(Dataset<Row> base, String representation, List<String> flatCols) {
        switch (representation) {
            case "RAW_ARRAY":
                return base;
            case "PAIRS":
                return base.withColumn("params",
                        arrays_zip(col("paramName"), col("paramValue"))
                                .cast("array<struct<name:string,value:string>>"));
            case "MAP":
                return base.withColumn("params", map_from_arrays(col("paramName"), col("paramValue")));
            case "FLAT":
                return extractScalars(base, flatCols, false);
            case "HYBRID":
                return extractScalars(base, flatCols, true);
            default:
                System.out.println("[materialize] unknown representation " + representation + "; keeping RAW_ARRAY");
                return base;
        }
    }

    private static Dataset<Row> extractScalars(Dataset<Row> base, List<String> flatCols, boolean keepOriginal) {
        Dataset<Row> df = base;
        for (String colName : flatCols) {
            if ("device_system_name".equals(colName)) {
                df = df.withColumn("device_system_name",
                        get_json_object(
                                element_at(col("paramValue"), array_position(col("paramName"), lit("MOBILE_SDK_DATA"))),
                                "$.DeviceSystemName"));
            } else {
                String key = paramKey(colName);
                df = df.withColumn(colName,
                        element_at(col("paramValue"), array_position(col("paramName"), lit(key))));
            }
        }
        if (!keepOriginal) {
            df = df.drop("paramName", "paramValue", "paramOldValue", "tags");
        }
        return df;
    }

    private static String paramKey(String colName) {
        switch (colName) {
            case "epk_id": return "EPK_ID";
            case "session_id": return "SESSION_ID";
            case "operation_uid": return "OPERATION_UID";
            case "channel_type": return "CHANNEL_TYPE";
            case "vsp_code": return "VSP_CODE";
            case "osb_code": return "OSB_CODE";
            case "tb_code": return "TB_CODE";
            case "ip_address": return "IP_ADDRESS";
            default: return colName.toUpperCase();
        }
    }

    private static long estimateBytes(SparkSession spark, String path) {
        try {
            Path p = new Path(path);
            FileSystem fs = p.getFileSystem(spark.sparkContext().hadoopConfiguration());
            long sum = 0L;
            org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus> it = fs.listFiles(p, true);
            while (it.hasNext()) sum += it.next().getLen();
            return Math.max(sum, 1L);
        } catch (Exception e) {
            return 1L;
        }
    }

    private static String generateDdl(ExperimentInstance inst, String path) {
        String table = "events_" + inst.experimentId.toLowerCase() + "_" + Integer.toHexString(inst.variantLabel.hashCode());
        String partitions = "";
        if (!inst.partitionColumns.isEmpty()) {
            List<String> parts = new ArrayList<>();
            for (String c : inst.partitionColumns) parts.add(c + " STRING");
            partitions = "PARTITIONED BY (" + String.join(", ", parts) + ")";
        }
        return "CREATE EXTERNAL TABLE IF NOT EXISTS " + table
                + "\nSTORED AS ORC\nLOCATION '" + path + "'\n" + partitions + ";\n";
    }

    private static void writeDdlFile(String relative, List<String> ddls) throws Exception {
        java.nio.file.Path path = Paths.get(relative);
        if (path.getParent() != null) Files.createDirectories(path.getParent());
        StringBuilder sb = new StringBuilder();
        for (String d : ddls) sb.append(d).append('\n');
        Files.write(path, sb.toString().getBytes(StandardCharsets.UTF_8));
        System.out.println("[materialize] wrote DDL stub " + relative + " (" + ddls.size() + " tables)");
    }

    private static void cleanupLayouts(String hdfsRoot, String scale, String baselineId,
                                       List<ExperimentInstance> instances, List<String> retain) throws Exception {
        Set<String> retainSet = new HashSet<>(retain);
        retainSet.add(baselineId);
        Path root = new Path(hdfsRoot + "/" + scale);
        FileSystem fs = FileSystem.get(root.toUri(), new Configuration());
        if (!fs.exists(root)) return;
        for (FileStatus st : fs.listStatus(root)) {
            String name = st.getPath().getName();
            boolean keep = retainSet.contains(name) || "refs".equals(name) || "manifest".equals(name);
            if (!keep) {
                for (ExperimentInstance i : instances) {
                    if (i.layoutPath != null && i.layoutPath.endsWith("/" + name) && retainSet.contains(i.experimentId)) {
                        keep = true;
                        break;
                    }
                }
            }
            if (st.isDirectory() && !keep && !retain.isEmpty()) {
                System.out.println("[cleanup] deleting " + st.getPath());
                fs.delete(st.getPath(), true);
            }
        }
    }
}
