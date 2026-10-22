package orcbench.engine.hive;

import orcbench.engine.QueryTemplate;
import orcbench.engine.QueryTemplate.BoundQuery;
import orcbench.io.ResultStore;
import orcbench.engine.hive.HiveJdbcBackend.QueryExecResult;
import orcbench.engine.spark.SparkQueryRunner;
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
 * Checkpoint 7: Hive JDBC (Tez + LLAP) query runs with session/profile
 * overrides, DDL registration, latency / result-hash metrics (same
 * equivalence rules as Spark), and skip-aware MV / ZSTD / LLAP handling.
 * <p>
 * Local acceptance: {@code --dry-run} / {@code --mock-run}. Live JDBC:
 * cluster node via {@link LiveHiveJdbcBackend} only.
 */
public final class HiveJdbcRunner {

    /** Hive §1 levers expected in engine_experiments / session overrides. */
    public static final List<String> HIVE_SECTION1_LEVERS = Collections.unmodifiableList(Arrays.asList(
            "hive.vectorized.execution.enabled",
            "hive.cbo.enable",
            "hive.llap.execution.mode",
            "hive.execution.engine"
    ));

    /** Dialect id used in query-suite.yaml for Hive Tez / LLAP SQL. */
    public static final String HIVE_DIALECT = "hive_tez";

    private HiveJdbcRunner() {}

    /**
     * High-level entry used by {@link orcbench.Main} for {@code --phase run}
     * when {@code --engines} includes {@code hive_tez} / {@code hive_llap}.
     *
     * @param dryRun  plan only — no JDBC
     * @param useMock when true (and not dry-run), use {@link FakeHiveJdbcBackend}
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
            HiveJdbcBackend backend) throws Exception {
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
        List<ExperimentInstance> hiveCells = selectHiveCells(plan);
        result.experimentsPlanned = hiveCells.size();

        System.out.println("[run] hive cells=" + hiveCells.size()
                + " dryRun=" + dryRun
                + " mock=" + result.mocked
                + " concurrencySelected=" + plan.concurrencySelected);

        if (dryRun) {
            for (ExperimentInstance inst : hiveCells) {
                applyLocalSkipRules(inst);
                if (inst.skipReason != null) {
                    result.experimentsSkipped++;
                    result.notes.add("skip " + inst.experimentId + "/" + inst.variantLabel
                            + ": " + inst.skipReason);
                    continue;
                }
                List<BoundQuery> queries = QueryTemplate.queriesForExperiment(
                        cfg.queries,
                        effectiveQueries(inst),
                        HIVE_DIALECT,
                        "events_dry",
                        effectiveSamples,
                        inst.representation);
                String note = "dry-run would run " + inst.experimentId + "/" + inst.variantLabel
                        + " engine=" + inst.engine
                        + " queries=" + queryIds(queries)
                        + " sessionOverrides=" + mergedOverrides(inst).keySet();
                if (inst.llap) {
                    note += " (LLAP observational until cluster probe)";
                }
                result.notes.add(note);
            }
            result.notes.add("dry-run: skip Hive JDBC / HS2 / Tez / LLAP");
            result.notes.add("Hive ZSTD deferred (ORC 1.5.6 codec gap); Spark-only ZSTD layouts not required for Hive");
            System.out.println("[run] dry-run: planned " + result.experimentsPlanned
                    + " hive cells (skipped=" + result.experimentsSkipped + ")");
            return result;
        }

        HiveJdbcBackend sink = backend;
        boolean closeSink = false;
        if (sink == null) {
            if (useMock) {
                sink = new FakeHiveJdbcBackend();
                result.notes.add("mock backend: deterministic FakeHiveJdbcBackend, no HS2");
                closeSink = true;
            } else {
                String url = LiveHiveJdbcBackend.resolveJdbcUrl(cfg);
                if (url == null || url.isEmpty()) {
                    throw new IllegalStateException(
                            "Hive JDBC URL is null. Live --phase run with hive_tez/hive_llap "
                                    + "requires a cluster node (kinit + HS2), or use --dry-run / --mock-run.");
                }
                sink = LiveHiveJdbcBackend.connect(cfg);
                result.notes.add("live Hive JDBC backend (cluster HS2)");
                closeSink = true;
            }
        }

        try {
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

            for (ExperimentInstance inst : hiveCells) {
                System.out.println("[run] hive " + inst.experimentId + "/" + inst.variantLabel
                        + " engine=" + inst.engine
                        + (inst.llap ? " llap" : ""));
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

                if ("HIVE_TEZ_BASE".equals(inst.experimentId) && !extraConc.isEmpty()) {
                    for (Integer conc : extraConc) {
                        System.out.println("[run] concurrency hive_tez=" + conc);
                        List<RunResult> extra = runExperiment(
                                sink, cfg, inst, effectiveSamples, runId, conc);
                        result.results.addAll(extra);
                        appendMetrics(jsonl, extra);
                    }
                }
            }
            result.attempts = result.results.size();
            if (jsonl != null) {
                result.notes.add("wrote metrics.jsonl (" + result.attempts + " hive attempts) " + jsonl);
            }
        } finally {
            if (closeSink && sink != null) {
                sink.close();
            }
        }

        System.out.println("[run] hive complete attempts=" + result.attempts
                + " run=" + result.experimentsRun
                + " skipped=" + result.experimentsSkipped);
        return result;
    }

    private static void appendMetrics(String jsonl, List<RunResult> rows) throws Exception {
        if (jsonl == null || rows == null || rows.isEmpty()) {
            return;
        }
        MetricsStore.appendJsonl(jsonl, rows);
        System.out.println("[run] appended " + rows.size() + " hive rows to " + jsonl);
    }

    /**
     * Execute one Hive experiment cell (warmup + measured, optional concurrency).
     */
    public static List<RunResult> runExperiment(
            HiveJdbcBackend backend,
            LoadedConfig cfg,
            ExperimentInstance inst,
            QuerySampleManifest samples,
            String runId,
            Integer concurrencyOverride) throws Exception {
        List<RunResult> results = new ArrayList<RunResult>();
        if (inst == null) {
            return results;
        }
        applyLocalSkipRules(inst);
        if (inst.skipReason != null) {
            results.add(skipped(runId, inst, inst.skipReason));
            return results;
        }
        if (backend == null) {
            results.add(skipped(runId, inst, "no HiveJdbcBackend"));
            return results;
        }

        Map<String, String> overrides = mergedOverrides(inst);
        backend.applySessionOverrides(overrides);

        String table = tableName(inst);
        String layout = inst.layoutPath;
        if (layout == null || layout.isEmpty()) {
            results.add(skipped(runId, inst, "missing layoutPath"));
            return results;
        }

        String schemaText = cfg.schemaText;
        String ddl = SchemaDdl.createExternalTableSql(
                table,
                schemaText,
                layout,
                inst.partitionColumns,
                SchemaDdl.representationExtras(inst.representation));
        boolean repair = inst.partitionColumns != null && !inst.partitionColumns.isEmpty();
        backend.ensureExternalTable(table, ddl, repair);
        backend.ensureRefTables(refTableLocations(layout));

        Long orcBytes = backend.estimateOrcBytes(layout);

        List<BoundQuery> queries = QueryTemplate.queriesForExperiment(
                cfg.queries,
                effectiveQueries(inst),
                HIVE_DIALECT,
                table,
                samples == null ? new QuerySampleManifest() : samples,
                inst.representation);
        if (queries.isEmpty()) {
            results.add(skipped(runId, inst, "no bound queries for dialect " + HIVE_DIALECT));
            return results;
        }

        // Warm LLAP cache: one priming execute before measured loop (observational).
        if ("warm".equals(inst.cacheState) && !queries.isEmpty()) {
            try {
                backend.execute(queries.get(0).sql);
            } catch (Exception ignored) {
                // priming is best-effort
            }
        }

        int warmup = cfg.benchmark.runs.warmupPerQuery;
        int measured = cfg.benchmark.runs.measuredPerQuery;
        if (backend instanceof FakeHiveJdbcBackend) {
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

    /**
     * Engine cells that run on Hive Tez / LLAP for Checkpoint 7.
     * Physical OVAT cells stay on Spark (CP6); Hive reads baseline layout.
     */
    public static List<ExperimentInstance> selectHiveCells(ExperimentPlan plan) {
        List<ExperimentInstance> out = new ArrayList<ExperimentInstance>();
        if (plan == null) {
            return out;
        }
        for (ExperimentInstance i : plan.engine) {
            if (isHiveEngine(i)) {
                out.add(i);
            }
        }
        // Final comparison (BEST_ORC × engines) is Checkpoint 8 — not selected here.
        return out;
    }

    public static boolean isHiveEngine(ExperimentInstance i) {
        if (i == null) {
            return false;
        }
        String e = i.engine == null ? "" : i.engine.toLowerCase(Locale.ROOT);
        return "hive_tez".equals(e) || "hive_llap".equals(e);
    }

    /** Merge session overrides (Hive path ignores Spark readerOverrides). */
    public static Map<String, String> mergedOverrides(ExperimentInstance inst) {
        Map<String, String> m = new LinkedHashMap<String, String>();
        if (inst.sessionOverrides != null) {
            m.putAll(inst.sessionOverrides);
        }
        return m;
    }

    /**
     * Document which §1 Hive levers appear in the expanded plan.
     */
    public static Map<String, Boolean> section1Coverage(ExperimentPlan plan) {
        Map<String, Boolean> covered = new LinkedHashMap<String, Boolean>();
        for (String lever : HIVE_SECTION1_LEVERS) {
            covered.put(lever, Boolean.FALSE);
        }
        for (ExperimentInstance i : selectHiveCells(plan)) {
            for (Map.Entry<String, String> e : mergedOverrides(i).entrySet()) {
                if (covered.containsKey(e.getKey())) {
                    covered.put(e.getKey(), Boolean.TRUE);
                }
            }
        }
        return covered;
    }

    /**
     * True when a layout/compression candidate must not be used for Hive
     * (ZSTD deferred — ORC 1.5.6 codec gap; Spark-only).
     */
    public static boolean isHiveZstdExcluded(ExperimentInstance inst) {
        if (inst == null) {
            return false;
        }
        if (inst.orcSettings != null && inst.orcSettings.compression != null) {
            if ("ZSTD".equalsIgnoreCase(inst.orcSettings.compression)) {
                return true;
            }
        }
        if (inst.variantLabel != null
                && inst.variantLabel.toUpperCase(Locale.ROOT).contains("ZSTD")) {
            return true;
        }
        return "COMPRESSION".equals(inst.experimentId)
                && inst.variantLabel != null
                && inst.variantLabel.toUpperCase(Locale.ROOT).contains("ZSTD");
    }

    /**
     * Same SHA-256 sorted-row hash as Spark ({@code result_equivalence.scope}).
     */
    public static String hashRows(String[] parts) {
        return SparkQueryRunner.hashRows(parts);
    }

    /**
     * Local skip rules that do not require a live capability probe
     * (MV confirmed unsupported; Hive ZSTD deferred).
     */
    public static void applyLocalSkipRules(ExperimentInstance inst) {
        if (inst == null || inst.skipReason != null) {
            return;
        }
        if ("HIVE_MATERIALIZED_VIEW".equals(inst.experimentId)
                || (inst.requires != null && inst.requires.contains("materialized_views_supported"))) {
            inst.skipReason = "materialized_views_supported=false (expected skip)";
            return;
        }
        if (isHiveZstdExcluded(inst)) {
            inst.skipReason = "Hive ZSTD deferred (ORC 1.5.6 codec gap; Spark-only)";
        }
    }

    private static RunResult executeOne(
            HiveJdbcBackend backend,
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
            String hash = hashRows(exec.rowStrings);
            long latency = (System.nanoTime() - t0) / 1_000_000L;
            RunResult r = ok(runId, inst, bq.queryId, repetition, warmup, concurrency,
                    latency, hash, exec.rowStrings.length, orcBytes, plan);
            r.hdfsBytesRead = exec.hdfsBytesRead;
            if (inst.llap) {
                // LLAP cold/warm remain observational until cluster probe confirms readiness.
                r.cacheState = inst.cacheState == null ? "llap_observational" : inst.cacheState;
            }
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
        r.engine = inst.engine == null ? "hive_tez" : inst.engine;
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
        r.engine = inst.engine == null ? "hive_tez" : inst.engine;
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
        return Collections.emptyList();
    }

    private static String tableName(ExperimentInstance inst) {
        String id = inst.experimentId == null ? "x" : inst.experimentId;
        int h = inst.variantLabel == null ? 0 : Math.abs(inst.variantLabel.hashCode());
        return ("events_" + id + "_" + h).replaceAll("[^A-Za-z0-9_]", "_");
    }

    private static Map<String, String> refTableLocations(String layout) {
        Map<String, String> m = new LinkedHashMap<String, String>();
        String refs = refsRoot(layout);
        if (refs == null) {
            return m;
        }
        m.put("module_dictionary", refs + "/module_dictionary");
        m.put("epk_dictionary", refs + "/epk_dictionary");
        m.put("partition_dates", refs + "/partition_dates");
        return m;
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
