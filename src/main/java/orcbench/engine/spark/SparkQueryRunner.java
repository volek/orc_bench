package orcbench.engine.spark;

import orcbench.engine.QueryTemplate;
import orcbench.engine.QueryTemplate.BoundQuery;
import orcbench.model.Models.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public final class SparkQueryRunner {
    private SparkQueryRunner() {}

    public static SparkSession createSession(LoadedConfig cfg, String appName) {
        EnvironmentSection env = cfg.environment.environment;
        SparkSession.Builder builder = SparkSession.builder().appName(appName == null ? "orc-bench" : appName);
        String master = System.getProperty("spark.master", env.spark.master);
        builder.master(master);
        if (env.spark.enableHiveSupport == null || env.spark.enableHiveSupport) {
            builder.enableHiveSupport();
        }
        builder.config("spark.yarn.queue", env.yarn.queue);
        if (env.spark.executorInstances != null) {
            builder.config("spark.executor.instances", env.spark.executorInstances.toString());
        }
        if (env.spark.executorCores != null) {
            builder.config("spark.executor.cores", env.spark.executorCores.toString());
        }
        if (env.spark.executorMemoryGib != null) {
            builder.config("spark.executor.memory", env.spark.executorMemoryGib + "g");
        }
        for (Map.Entry<String, String> e : cfg.benchmark.executionBaselines.spark.entrySet()) {
            builder.config(e.getKey(), e.getValue());
        }
        return builder.getOrCreate();
    }

    public static List<RunResult> runExperiment(
            SparkSession spark, LoadedConfig cfg, ExperimentInstance inst,
            QuerySampleManifest samples, String runId, Integer concurrencyOverride) {

        List<RunResult> results = new ArrayList<>();
        if (inst.skipReason != null) {
            results.add(skipped(runId, inst, inst.skipReason));
            return results;
        }

        for (Map.Entry<String, String> e : inst.sessionOverrides.entrySet()) spark.conf().set(e.getKey(), e.getValue());
        for (Map.Entry<String, String> e : inst.readerOverrides.entrySet()) spark.conf().set(e.getKey(), e.getValue());

        String layout = inst.layoutPath;
        String table = "events_" + inst.experimentId + "_" + Math.abs(inst.variantLabel.hashCode());
        spark.read().format("orc").load(layout).createOrReplaceTempView(table);

        String refsRoot = layout.replaceAll("/[^/]+$", "/refs");
        try { spark.read().format("orc").load(refsRoot + "/module_dictionary").createOrReplaceTempView("module_dictionary"); } catch (Exception ignored) {}
        try { spark.read().format("orc").load(refsRoot + "/epk_dictionary").createOrReplaceTempView("epk_dictionary"); } catch (Exception ignored) {}
        try { spark.read().format("orc").load(refsRoot + "/partition_dates").createOrReplaceTempView("partition_dates"); } catch (Exception ignored) {}

        Long orcBytesTmp = null;
        try { orcBytesTmp = dirSize(spark, layout); } catch (Exception ignored) {}
        final Long orcBytes = orcBytesTmp;

        List<BoundQuery> queries = QueryTemplate.queriesForExperiment(
                cfg.queries, inst.queries, "spark_3_2", table, samples, inst.representation);

        int warmup = cfg.benchmark.runs.warmupPerQuery;
        int measured = cfg.benchmark.runs.measuredPerQuery;
        List<Integer> concLevels = concurrencyOverride != null
                ? Arrays.asList(concurrencyOverride) : Arrays.asList(1);

        for (final int conc : concLevels) {
            for (final BoundQuery bq : queries) {
                for (int i = 0; i < warmup; i++) {
                    results.add(executeOne(spark, cfg, inst, runId, bq, i, true, conc, orcBytes));
                }
                if (conc <= 1) {
                    for (int i = 0; i < measured; i++) {
                        results.add(executeOne(spark, cfg, inst, runId, bq, i, false, conc, orcBytes));
                    }
                } else {
                    ExecutorService pool = Executors.newFixedThreadPool(conc);
                    try {
                        for (int batch = 0; batch < measured; batch++) {
                            List<Future<RunResult>> futures = new ArrayList<>();
                            for (int c = 0; c < conc; c++) {
                                final int rep = batch * conc + c;
                                futures.add(pool.submit(() ->
                                        executeOne(spark, cfg, inst, runId, bq, rep, false, conc, orcBytes)));
                            }
                            for (Future<RunResult> f : futures) results.add(f.get());
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        pool.shutdownNow();
                    }
                }
            }
        }
        return results;
    }

    private static RunResult executeOne(
            SparkSession spark, LoadedConfig cfg, ExperimentInstance inst, String runId,
            BoundQuery bq, int repetition, boolean warmup, int concurrency, Long orcBytes) {
        String plan = null;
        if (cfg.benchmark.runs.recordExplainPlans) {
            try {
                Row[] rows = (Row[]) spark.sql("EXPLAIN EXTENDED " + bq.sql).collect();
                StringBuilder sb = new StringBuilder();
                for (Row r : rows) sb.append(r.getString(0)).append('\n');
                plan = sb.toString();
            } catch (Exception ignored) {}
        }

        long t0 = System.nanoTime();
        try {
            Dataset<Row> df = spark.sql(bq.sql);
            Row[] rows = (Row[]) df.collect();
            String[] parts = new String[rows.length];
            for (int i = 0; i < rows.length; i++) parts[i] = rows[i].mkString("|");
            String hash = hashRows(parts);
            long latency = (System.nanoTime() - t0) / 1_000_000L;
            return ok(runId, inst, bq.queryId, repetition, warmup, concurrency, latency, hash, rows.length, orcBytes, plan);
        } catch (Exception ex) {
            long latency = (System.nanoTime() - t0) / 1_000_000L;
            RunResult r = ok(runId, inst, bq.queryId, repetition, warmup, concurrency, latency, "", 0, orcBytes, plan);
            r.status = "error";
            r.errorMessage = ex.getMessage();
            return r;
        }
    }

    public static String hashRows(String[] parts) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            String[] sorted = parts.clone();
            Arrays.sort(sorted);
            for (String p : sorted) md.update(p.getBytes(StandardCharsets.UTF_8));
            byte[] dig = md.digest();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < Math.min(8, dig.length); i++) sb.append(String.format("%02x", dig[i]));
            return sb.toString();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static long dirSize(SparkSession spark, String path) throws Exception {
        Path p = new Path(path);
        FileSystem fs = p.getFileSystem(spark.sparkContext().hadoopConfiguration());
        long sum = 0L;
        org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus> it = fs.listFiles(p, true);
        while (it.hasNext()) sum += it.next().getLen();
        return sum;
    }

    private static RunResult ok(String runId, ExperimentInstance inst, String queryId, int rep,
                                boolean warmup, int conc, long latency, String hash, long rows,
                                Long orcBytes, String plan) {
        RunResult r = new RunResult();
        r.runId = runId;
        r.experimentId = inst.experimentId;
        r.variantLabel = inst.variantLabel;
        r.engine = "spark";
        r.queryId = queryId;
        r.repetition = rep;
        r.warmup = warmup;
        r.concurrency = conc;
        r.cacheState = inst.cacheState;
        r.latencyMs = latency;
        r.status = "ok";
        r.resultHash = hash;
        r.logicalRows = rows;
        r.orcFileBytes = orcBytes;
        r.enginePlan = plan;
        return r;
    }

    private static RunResult skipped(String runId, ExperimentInstance inst, String reason) {
        RunResult r = new RunResult();
        r.runId = runId;
        r.experimentId = inst.experimentId;
        r.variantLabel = inst.variantLabel;
        r.engine = "spark";
        r.queryId = "*";
        r.status = "skipped";
        r.resultHash = "";
        r.errorMessage = reason;
        r.cacheState = inst.cacheState;
        r.concurrency = 1;
        return r;
    }
}
