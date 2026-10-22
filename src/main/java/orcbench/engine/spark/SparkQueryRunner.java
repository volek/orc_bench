package orcbench.engine.spark;

import orcbench.engine.QueryTemplate;
import orcbench.engine.QueryTemplate.BoundQuery;
import orcbench.engine.ResultDigest;
import orcbench.io.ResultStore;
import orcbench.engine.spark.SparkQueryBackend.QueryExecResult;
import orcbench.generate.SparkSessionSupport;
import orcbench.metrics.MetricsStore;
import orcbench.model.Models.ExperimentInstance;
import orcbench.model.Models.ExperimentPlan;
import orcbench.model.Models.LoadedConfig;
import orcbench.model.Models.QuerySampleManifest;
import orcbench.model.Models.RunPhaseResult;
import orcbench.model.Models.RunResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Checkpoint 6: Spark physical + engine OVAT query runs with session/profile
 * overrides, latency / result-hash metrics, and optional EXPLAIN plans.
 * <p>
 * Local acceptance: {@code --dry-run} / {@code --mock-run}. Live Spark SQL:
 * cluster node via {@link LiveSparkQueryBackend} only.
 */
public final class SparkQueryRunner {

    /** Spark §1 levers expected in engine_experiments / session overrides. */
    public static final List<String> SPARK_SECTION1_LEVERS = Collections.unmodifiableList(Arrays.asList(
            "spark.sql.orc.enableVectorizedReader",
            "spark.sql.adaptive.enabled",
            "spark.sql.optimizer.dynamicPartitionPruning.enabled",
            "spark.sql.cbo.enabled",
            "spark.sql.autoBroadcastJoinThreshold",
            "spark.sql.orc.impl",
            "spark.sql.orc.enableNestedColumnVectorizedReader",
            "spark.sql.orc.filterPushdown"
    ));

    private SparkQueryRunner() {}

    /**
     * High-level entry used by {@link orcbench.Main} for {@code --phase run} (Spark only).
     *
     * @param dryRun  plan only — no SQL execution
     * @param useMock when true (and not dry-run), use {@link FakeSparkQueryBackend}
     * @param backend optional override for unit tests
     */
    public static RunPhaseResult run(
            LoadedConfig cfg,
            ExperimentPlan plan,
            QuerySampleManifest samples,
            String scale,
            String runId,
            String resultsDir,
            boolean dryRun,
            boolean useMock,
            SparkQueryBackend backend) throws Exception {
        if (cfg == null) {
            throw new IllegalArgumentException("cfg is required");
        }
        if (plan == null) {
            throw new IllegalArgumentException("plan is required");
        }

        RunPhaseResult result = new RunPhaseResult();
        result.runId = runId;
        result.scale = scale == null ? "S" : scale.toUpperCase(Locale.ROOT);
        result.dryRun = dryRun;
        result.mocked = useMock && !dryRun;

        QuerySampleManifest effectiveSamples = samples != null ? samples : stubSamples(cfg, result.scale);
        List<ExperimentInstance> sparkCells = selectSparkCells(plan);
        result.experimentsPlanned = sparkCells.size();

        System.out.println("[run] spark cells=" + sparkCells.size()
                + " dryRun=" + dryRun
                + " mock=" + result.mocked
                + " concurrencySelected=" + plan.concurrencySelected);

        if (dryRun) {
            for (ExperimentInstance inst : sparkCells) {
                if (inst.skipReason != null) {
                    result.experimentsSkipped++;
                    result.notes.add("skip " + inst.experimentId + "/" + inst.variantLabel
                            + ": " + inst.skipReason);
                    continue;
                }
                List<BoundQuery> queries = QueryTemplate.queriesForExperiment(
                        cfg.queries,
                        effectiveQueries(inst),
                        "spark_3_2",
                        "events_dry",
                        effectiveSamples,
                        inst.representation);
                result.notes.add("dry-run would run " + inst.experimentId + "/" + inst.variantLabel
                        + " queries=" + queryIds(queries)
                        + " sessionOverrides=" + mergedOverrides(inst).keySet());
            }
            result.notes.add("dry-run: skip Spark SQL / HDFS / YARN");
            System.out.println("[run] dry-run: planned " + result.experimentsPlanned
                    + " spark cells (skipped=" + result.experimentsSkipped + ")");
            return result;
        }

        SparkQueryBackend sink = backend;
        boolean closeSink = false;
        if (sink == null) {
            if (useMock) {
                sink = new FakeSparkQueryBackend();
                result.notes.add("mock backend: deterministic FakeSparkQueryBackend, no HDFS");
                closeSink = true;
            } else {
                if (!SparkSessionSupport.sparkAvailable()) {
                    throw new IllegalStateException(
                            "Spark is not on the classpath. Live --phase run requires "
                                    + "spark-submit on a Hadoop cluster node, or use --dry-run / --mock-run.");
                }
                sink = LiveSparkQueryBackend.create(cfg, "orc-bench-run");
                result.notes.add("live Spark SQL backend (cluster)");
                closeSink = true;
            }
        }

        try {
            // Default concurrency 1 for all cells; extra levels only for SPARK_BASE.
            List<Integer> extraConc = new ArrayList<Integer>();
            if (plan.concurrencySelected != null) {
                for (Integer c : plan.concurrencySelected) {
                    if (c != null && c > 1) {
                        extraConc.add(c);
                    }
                }
            }
            String jsonl = null;
            if (resultsDir != null && !resultsDir.isEmpty()) {
                jsonl = ResultStore.child(resultsDir, runId, "metrics.jsonl");
                result.metricsJsonlPath = jsonl;
            }

            for (ExperimentInstance inst : sparkCells) {
                System.out.println("[run] spark " + inst.experimentId + "/" + inst.variantLabel);
                List<RunResult> one = runExperiment(
                        sink, cfg, inst, effectiveSamples, runId, null);
                result.results.addAll(one);
                appendMetrics(jsonl, one);
                if (inst.skipReason != null
                        || (!one.isEmpty() && "skipped".equals(one.get(0).status))) {
                    result.experimentsSkipped++;
                } else {
                    result.experimentsRun++;
                }

                if ("SPARK_BASE".equals(inst.experimentId) && !extraConc.isEmpty()) {
                    for (Integer conc : extraConc) {
                        System.out.println("[run] concurrency spark=" + conc);
                        List<RunResult> extra = runExperiment(
                                sink, cfg, inst, effectiveSamples, runId, conc);
                        result.results.addAll(extra);
                        appendMetrics(jsonl, extra);
                    }
                }
            }
            result.attempts = result.results.size();
            if (jsonl != null) {
                result.notes.add("wrote metrics.jsonl (" + result.attempts + " attempts) " + jsonl);
            }
        } finally {
            if (closeSink && sink != null) {
                sink.close();
            }
        }

        System.out.println("[run] complete attempts=" + result.attempts
                + " run=" + result.experimentsRun
                + " skipped=" + result.experimentsSkipped);
        return result;
    }

    /**
     * Execute one experiment cell (warmup + measured, optional concurrency override).
     */
    public static List<RunResult> runExperiment(
            SparkQueryBackend backend,
            LoadedConfig cfg,
            ExperimentInstance inst,
            QuerySampleManifest samples,
            String runId,
            Integer concurrencyOverride) throws Exception {
        List<RunResult> results = new ArrayList<RunResult>();
        if (inst == null) {
            return results;
        }
        if (inst.skipReason != null) {
            results.add(skipped(runId, inst, inst.skipReason));
            return results;
        }
        if (backend == null) {
            results.add(skipped(runId, inst, "no SparkQueryBackend"));
            return results;
        }

        Map<String, String> overrides = mergedOverrides(inst);
        backend.applyOverrides(overrides);

        String table = tableName(inst);
        String layout = inst.layoutPath;
        if (layout == null || layout.isEmpty()) {
            results.add(skipped(runId, inst, "missing layoutPath"));
            return results;
        }
        backend.registerLayout(table, layout);
        backend.registerRefs(refsRoot(layout));

        Long orcBytes = backend.estimateOrcBytes(layout);

        List<BoundQuery> queries = QueryTemplate.queriesForExperiment(
                cfg.queries,
                effectiveQueries(inst),
                "spark_3_2",
                table,
                samples == null ? new QuerySampleManifest() : samples,
                inst.representation);
        if (queries.isEmpty()) {
            results.add(skipped(runId, inst, "no bound queries for dialect spark_3_2"));
            return results;
        }

        int warmup = cfg.benchmark.runs.warmupPerQuery;
        int measured = cfg.benchmark.runs.measuredPerQuery;
        // Local/mock: keep attempt counts tiny so unit tests stay fast.
        if (backend instanceof FakeSparkQueryBackend) {
            warmup = Math.min(warmup, 1);
            measured = Math.min(measured, 2);
        }
        List<Integer> concLevels = concurrencyOverride != null
                ? Collections.singletonList(concurrencyOverride)
                : Collections.singletonList(1);

        for (final int conc : concLevels) {
            for (final BoundQuery bq : queries) {
                for (int i = 0; i < warmup; i++) {
                    results.add(executeOne(
                            backend, cfg, inst, runId, bq, i, true, conc, orcBytes));
                }
                if (conc <= 1) {
                    for (int i = 0; i < measured; i++) {
                        results.add(executeOne(
                                backend, cfg, inst, runId, bq, i, false, conc, orcBytes));
                    }
                } else {
                    ExecutorService pool = Executors.newFixedThreadPool(conc);
                    try {
                        for (int batch = 0; batch < measured; batch++) {
                            List<Future<RunResult>> futures = new ArrayList<Future<RunResult>>();
                            for (int c = 0; c < conc; c++) {
                                final int rep = batch * conc + c;
                                futures.add(pool.submit(new java.util.concurrent.Callable<RunResult>() {
                                    @Override
                                    public RunResult call() {
                                        return executeOne(
                                                backend, cfg, inst, runId, bq, rep, false, conc, orcBytes);
                                    }
                                }));
                            }
                            for (Future<RunResult> f : futures) {
                                results.add(f.get());
                            }
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

    private static void appendMetrics(String jsonl, List<RunResult> rows) throws Exception {
        if (jsonl == null || rows == null || rows.isEmpty()) {
            return;
        }
        MetricsStore.appendJsonl(jsonl, rows);
        System.out.println("[run] appended " + rows.size() + " rows to " + jsonl);
    }

    /**
     * Order-independent fingerprint of canonical row strings; first 16 hex chars.
     * Same sum as {@link ResultDigest}, so a live executor-side fold matches
     * {@code hashRows} of the same rows.
     */
    public static String hashRows(String[] parts) {
        return ResultDigest.hashRows(parts);
    }

    /**
     * Physical + engine cells that run on Spark for Checkpoint 6.
     * Hive Tez/LLAP cells are deferred to Checkpoint 7.
     */
    public static List<ExperimentInstance> selectSparkCells(ExperimentPlan plan) {
        List<ExperimentInstance> out = new ArrayList<ExperimentInstance>();
        if (plan == null) {
            return out;
        }
        for (ExperimentInstance i : plan.physical) {
            if (isSparkEngine(i)) {
                out.add(i);
            }
        }
        for (ExperimentInstance i : plan.engine) {
            if (isSparkEngine(i)) {
                out.add(i);
            }
        }
        return out;
    }

    public static boolean isSparkEngine(ExperimentInstance i) {
        if (i == null) {
            return false;
        }
        String e = i.engine == null ? "spark" : i.engine.toLowerCase(Locale.ROOT);
        return "spark".equals(e);
    }

    /** Merge reader + session overrides (session wins on key clash). */
    public static Map<String, String> mergedOverrides(ExperimentInstance inst) {
        Map<String, String> m = new LinkedHashMap<String, String>();
        if (inst.readerOverrides != null) {
            m.putAll(inst.readerOverrides);
        }
        if (inst.sessionOverrides != null) {
            m.putAll(inst.sessionOverrides);
        }
        return m;
    }

    /**
     * Document which §1 Spark levers appear in the expanded plan (for tests / dry-run notes).
     */
    public static Map<String, Boolean> section1Coverage(ExperimentPlan plan) {
        Map<String, Boolean> covered = new LinkedHashMap<String, Boolean>();
        for (String lever : SPARK_SECTION1_LEVERS) {
            covered.put(lever, Boolean.FALSE);
        }
        for (ExperimentInstance i : selectSparkCells(plan)) {
            for (Map.Entry<String, String> e : mergedOverrides(i).entrySet()) {
                if (covered.containsKey(e.getKey())) {
                    covered.put(e.getKey(), Boolean.TRUE);
                }
            }
        }
        return covered;
    }

    private static RunResult executeOne(
            SparkQueryBackend backend,
            LoadedConfig cfg,
            ExperimentInstance inst,
            String runId,
            BoundQuery bq,
            int repetition,
            boolean warmup,
            int concurrency,
            Long orcBytes) {
        String plan = null;
        if (cfg.benchmark.runs.recordExplainPlans && !warmup) {
            try {
                plan = backend.explain(bq.sql);
            } catch (Exception ignored) {
                // optional
            }
        }

        long t0 = System.nanoTime();
        try {
            QueryExecResult exec = backend.execute(bq.sql);
            String hash = exec.resultHash != null ? exec.resultHash : hashRows(exec.rowStrings);
            long nrows = exec.logicalRows >= 0 ? exec.logicalRows : exec.rowStrings.length;
            long latency = (System.nanoTime() - t0) / 1_000_000L;
            RunResult r = ok(runId, inst, bq.queryId, repetition, warmup, concurrency,
                    latency, hash, nrows, orcBytes, plan);
            r.hdfsBytesRead = exec.hdfsBytesRead;
            return r;
        } catch (Exception ex) {
            long latency = (System.nanoTime() - t0) / 1_000_000L;
            RunResult r = ok(runId, inst, bq.queryId, repetition, warmup, concurrency,
                    latency, "", 0, orcBytes, plan);
            r.status = "error";
            r.errorMessage = ex.getMessage();
            return r;
        }
    }

    private static RunResult ok(
            String runId, ExperimentInstance inst, String queryId, int rep,
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

    private static List<String> effectiveQueries(ExperimentInstance inst) {
        if (inst.queries != null && !inst.queries.isEmpty()) {
            return inst.queries;
        }
        // Baseline / rewrite cells without explicit query list: use full suite
        return Collections.emptyList();
    }

    private static String tableName(ExperimentInstance inst) {
        String id = inst.experimentId == null ? "x" : inst.experimentId;
        int h = inst.variantLabel == null ? 0 : Math.abs(inst.variantLabel.hashCode());
        return ("events_" + id + "_" + h).replaceAll("[^A-Za-z0-9_]", "_");
    }

    private static String refsRoot(String layout) {
        if (layout == null) {
            return null;
        }
        int slash = layout.lastIndexOf('/');
        if (slash <= 0) {
            return layout + "/../refs";
        }
        return layout.substring(0, slash) + "/refs";
    }

    private static String queryIds(List<BoundQuery> queries) {
        List<String> ids = new ArrayList<String>();
        for (BoundQuery q : queries) {
            ids.add(q.queryId);
        }
        return ids.toString();
    }

    private static QuerySampleManifest stubSamples(LoadedConfig cfg, String scale) {
        QuerySampleManifest m = new QuerySampleManifest();
        m.basePath = cfg.benchmark.benchmark.hdfsRoot + "/" + scale
                + "/" + cfg.benchmark.physicalBaseline.id;
        m.refsPath = cfg.benchmark.benchmark.hdfsRoot + "/" + scale + "/refs";
        m.manifestPath = cfg.benchmark.benchmark.hdfsRoot + "/" + scale + "/manifest";
        m.samples.put("userLogin", Collections.singletonMap("present",
                Arrays.asList("userLogin0", "userLogin1", "userLogin2")));
        m.samples.put("session", Collections.singletonMap("present",
                Collections.singletonList("copy2_0")));
        m.samples.put("SESSION_ID", Collections.singletonMap("present",
                Collections.singletonList("copy2_0")));
        m.samples.put("EPK_ID", Collections.singletonMap("present",
                Collections.singletonList("epk_0")));
        m.samples.put("IP_ADDRESS", Collections.singletonMap("present",
                Collections.singletonList("203.0.113.1")));
        m.samples.put("DeviceSystemName", Collections.singletonMap("present",
                Collections.singletonList("iOS")));
        m.partitionDates.add("2026-01-15");
        return m;
    }
}
