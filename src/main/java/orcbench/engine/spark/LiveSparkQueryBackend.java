package orcbench.engine.spark;

import orcbench.engine.ResultDigest;
import orcbench.generate.SparkSessionSupport;
import orcbench.io.HdfsPaths;
import orcbench.model.Models.LoadedConfig;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import org.apache.spark.util.TaskCompletionListener;

import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * SparkSession-backed query backend for cluster {@code spark-submit} runs.
 * Do not use from the development host against a live YARN/HDFS cluster.
 */
public final class LiveSparkQueryBackend implements SparkQueryBackend {

    private final SparkSession spark;
    private final boolean ownsSession;

    public LiveSparkQueryBackend(SparkSession spark, boolean ownsSession) {
        if (spark == null) {
            throw new IllegalArgumentException("spark session is required");
        }
        this.spark = spark;
        this.ownsSession = ownsSession;
    }

    /**
     * Create a session from environment + execution_baselines.spark, then wrap it.
     * Intended for use under {@code spark-submit} on a cluster node.
     */
    public static LiveSparkQueryBackend create(LoadedConfig cfg, String appName) {
        SparkSession session = createSession(cfg, appName);
        // Never stop the session ourselves in cluster mode: SparkContext.stop() races the
        // ApplicationMaster shutdown hook and marks the app FAILED before SUCCEEDED.
        boolean owns = !SparkSessionSupport.runningInsideYarnCluster();
        return new LiveSparkQueryBackend(session, owns);
    }

    public static SparkSession createSession(LoadedConfig cfg, String appName) {
        if (!SparkSessionSupport.sparkAvailable()) {
            throw new IllegalStateException(
                    "Spark is not on the classpath. Live --phase run requires spark-submit "
                            + "on a Hadoop cluster node, or use --dry-run / --mock-run.");
        }
        SparkSession.Builder builder = SparkSession.builder()
                .appName(appName == null ? "orc-bench-run" : appName);
        if (cfg != null && cfg.environment != null && cfg.environment.environment != null) {
            if (cfg.environment.environment.spark != null) {
                String master = System.getProperty("spark.master",
                        cfg.environment.environment.spark.master);
                if (master != null && !master.isEmpty()
                        && System.getProperty("spark.master") == null
                        && System.getenv("SPARK_ENV_LOADED") == null) {
                    // Under spark-submit, master is already set; only set when local/dev.
                    builder.master(master);
                }
                if (cfg.environment.environment.spark.enableHiveSupport == null
                        || Boolean.TRUE.equals(cfg.environment.environment.spark.enableHiveSupport)) {
                    builder.enableHiveSupport();
                }
                if (cfg.environment.environment.spark.executorInstances != null) {
                    builder.config("spark.executor.instances",
                            cfg.environment.environment.spark.executorInstances.toString());
                }
                if (cfg.environment.environment.spark.executorCores != null) {
                    builder.config("spark.executor.cores",
                            cfg.environment.environment.spark.executorCores.toString());
                }
                if (cfg.environment.environment.spark.executorMemoryGib != null) {
                    builder.config("spark.executor.memory",
                            cfg.environment.environment.spark.executorMemoryGib + "g");
                }
            }
            if (cfg.environment.environment.yarn != null
                    && cfg.environment.environment.yarn.queue != null) {
                builder.config("spark.yarn.queue", cfg.environment.environment.yarn.queue);
            }
        }
        if (cfg != null && cfg.benchmark != null && cfg.benchmark.executionBaselines != null
                && cfg.benchmark.executionBaselines.spark != null) {
            for (Map.Entry<String, String> e : cfg.benchmark.executionBaselines.spark.entrySet()) {
                builder.config(e.getKey(), e.getValue());
            }
        }
        return builder.getOrCreate();
    }

    public SparkSession session() {
        return spark;
    }

    @Override
    public void applyOverrides(Map<String, String> overrides) {
        if (overrides == null) {
            return;
        }
        for (Map.Entry<String, String> e : overrides.entrySet()) {
            spark.conf().set(e.getKey(), e.getValue());
        }
    }

    @Override
    public void registerLayout(String tableName, String layoutPath) throws Exception {
        spark.read().format("orc").load(layoutPath).createOrReplaceTempView(tableName);
    }

    @Override
    public void registerRefs(String refsRoot) {
        if (refsRoot == null || refsRoot.isEmpty()) {
            return;
        }
        tryRegister(refsRoot + "/module_dictionary", "module_dictionary");
        tryRegister(refsRoot + "/epk_dictionary", "epk_dictionary");
        tryRegister(refsRoot + "/partition_dates", "partition_dates");
    }

    private void tryRegister(String path, String view) {
        try {
            spark.read().format("orc").load(path).createOrReplaceTempView(view);
        } catch (Exception ignored) {
            // refs may be absent for some layouts
        }
    }

    @Override
    public Long estimateOrcBytes(String layoutPath) {
        return HdfsPaths.sumFileBytes(layoutPath, spark.sparkContext().hadoopConfiguration());
    }

    /**
     * Count and fingerprint on the executors. {@code collect()} of Q07
     * ({@code SELECT *} / narrow projection of LOGON) materializes every
     * matching row on the driver and exhausts the default 1g heap
     * ({@code TaskResultLost} / {@code Java heap space}).
     */
    @Override
    public QueryExecResult execute(String sql) throws Exception {
        Dataset<Row> df = spark.sql(sql);
        // One accumulator per statement so concurrent slots do not add each other's bytes.
        LongAccumulator bytes = spark.sparkContext().longAccumulator("orcbench.hdfsBytesRead");
        JavaRDD<ResultDigest> partials = df.javaRDD().mapPartitions(new FoldPartition(bytes));
        ResultDigest total = partials.fold(new ResultDigest(), new MergeDigests());
        return new QueryExecResult(new String[0], Long.valueOf(bytes.value()), total.count(), total.hashHex());
    }

    /** One digest per partition. Static so the closure does not capture the backend. */
    private static final class FoldPartition
            implements FlatMapFunction<Iterator<Row>, ResultDigest>, Serializable {
        private static final long serialVersionUID = 1L;
        private final LongAccumulator hdfsBytes;

        FoldPartition(LongAccumulator hdfsBytes) {
            this.hdfsBytes = hdfsBytes;
        }

        @Override
        public Iterator<ResultDigest> call(Iterator<Row> rows) {
            TaskContext ctx = TaskContext.get();
            if (ctx != null && hdfsBytes != null) {
                ctx.addTaskCompletionListener(new TaskCompletionListener() {
                    @Override
                    public void onTaskCompletion(TaskContext context) {
                        if (context != null
                                && context.taskMetrics() != null
                                && context.taskMetrics().inputMetrics() != null) {
                            hdfsBytes.add(context.taskMetrics().inputMetrics().bytesRead());
                        }
                    }
                });
            }
            ResultDigest digest = new ResultDigest();
            while (rows.hasNext()) {
                digest.add(canonicalRow(rows.next()));
            }
            return Collections.singleton(digest).iterator();
        }
    }

    private static final class MergeDigests
            implements Function2<ResultDigest, ResultDigest, ResultDigest>, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public ResultDigest call(ResultDigest left, ResultDigest right) {
            ResultDigest out = left == null ? new ResultDigest() : left.copy();
            out.merge(right);
            return out;
        }
    }

    /** Same canonical row as Hive JDBC: {@code null} → {@code \N}, columns joined by {@code |}. */
    static String canonicalRow(Row row) {
        StringBuilder sb = new StringBuilder();
        int n = row == null ? 0 : row.length();
        for (int i = 0; i < n; i++) {
            if (i > 0) {
                sb.append('|');
            }
            if (row.isNullAt(i)) {
                sb.append("\\N");
            } else {
                sb.append(String.valueOf(row.get(i)));
            }
        }
        return sb.toString();
    }

    @Override
    public String explain(String sql) {
        try {
            Row[] rows = (Row[]) spark.sql("EXPLAIN EXTENDED " + sql).collect();
            StringBuilder sb = new StringBuilder();
            for (Row r : rows) {
                sb.append(r.getString(0)).append('\n');
            }
            return sb.toString();
        } catch (Exception ex) {
            return null;
        }
    }

    @Override
    public void close() {
        if (ownsSession && spark != null) {
            try {
                spark.stop();
            } catch (Exception ignored) {
                // best-effort
            }
        }
    }
}
