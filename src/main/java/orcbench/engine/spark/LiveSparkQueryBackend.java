package orcbench.engine.spark;

import orcbench.generate.SparkSessionSupport;
import orcbench.model.Models.LoadedConfig;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

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
        try {
            Path p = new Path(layoutPath);
            FileSystem fs = p.getFileSystem(spark.sparkContext().hadoopConfiguration());
            long sum = 0L;
            RemoteIterator<LocatedFileStatus> it = fs.listFiles(p, true);
            while (it.hasNext()) {
                sum += it.next().getLen();
            }
            return sum;
        } catch (Exception ex) {
            return null;
        }
    }

    @Override
    public QueryExecResult execute(String sql) throws Exception {
        Dataset<Row> df = spark.sql(sql);
        Row[] rows = (Row[]) df.collect();
        String[] parts = new String[rows.length];
        for (int i = 0; i < rows.length; i++) {
            parts[i] = rows[i].mkString("|");
        }
        return new QueryExecResult(parts, null);
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
