package orcbench.engine.hive;

import orcbench.engine.QueryTemplate;
import orcbench.engine.QueryTemplate.BoundQuery;
import orcbench.engine.spark.SparkQueryRunner;
import orcbench.model.Models.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public final class HiveJdbcRunner {
    private HiveJdbcRunner() {}

    public static Connection connect(LoadedConfig cfg) throws Exception {
        HiveEnv env = cfg.environment.environment.hive;
        String url = env.connectionJdbcUrl != null ? env.connectionJdbcUrl : env.jdbcUrl;
        if (url == null) {
            throw new IllegalStateException("Hive JDBC URL is null; set via --env-override");
        }
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        return DriverManager.getConnection(url);
    }

    public static List<RunResult> runExperiment(
            LoadedConfig cfg, ExperimentInstance inst, QuerySampleManifest samples,
            String runId, Integer concurrencyOverride, boolean dryRun) throws Exception {

        List<RunResult> results = new ArrayList<>();
        if (inst.skipReason != null) {
            results.add(skipped(runId, inst, inst.skipReason));
            return results;
        }
        if (dryRun) {
            RunResult r = skipped(runId, inst, "dry_run");
            r.status = "dry_run";
            r.errorMessage = null;
            results.add(r);
            return results;
        }

        List<BoundQuery> queries = QueryTemplate.queriesForExperiment(
                cfg.queries, inst.queries, "hive_tez", "events", samples, inst.representation);
        int warmup = cfg.benchmark.runs.warmupPerQuery;
        int measured = cfg.benchmark.runs.measuredPerQuery;
        List<Integer> concLevels = concurrencyOverride != null
                ? Arrays.asList(concurrencyOverride) : Arrays.asList(1);

        if ("warm".equals(inst.cacheState) && !queries.isEmpty()) {
            try (Connection conn = connect(cfg)) {
                applySession(conn, inst.sessionOverrides);
                ensureExternalTable(conn, cfg, "events", inst);
                try (Statement st = conn.createStatement()) {
                    st.execute(queries.get(0).sql);
                }
            }
        }

        for (int conc : concLevels) {
            for (BoundQuery bq : queries) {
                for (int i = 0; i < warmup; i++) {
                    results.add(executeOne(cfg, inst, runId, bq, i, true, conc));
                }
                if (conc <= 1) {
                    for (int i = 0; i < measured; i++) {
                        results.add(executeOne(cfg, inst, runId, bq, i, false, conc));
                    }
                } else {
                    ExecutorService pool = Executors.newFixedThreadPool(conc);
                    try {
                        for (int batch = 0; batch < measured; batch++) {
                            List<Future<RunResult>> futures = new ArrayList<>();
                            for (int c = 0; c < conc; c++) {
                                final int rep = batch * conc + c;
                                futures.add(pool.submit(() -> executeOne(cfg, inst, runId, bq, rep, false, conc)));
                            }
                            for (Future<RunResult> f : futures) results.add(f.get());
                        }
                    } finally {
                        pool.shutdownNow();
                    }
                }
            }
        }
        return results;
    }

    private static RunResult executeOne(
            LoadedConfig cfg, ExperimentInstance inst, String runId,
            BoundQuery bq, int repetition, boolean warmup, int concurrency) {
        long t0 = System.nanoTime();
        try (Connection conn = connect(cfg)) {
            applySession(conn, inst.sessionOverrides);
            ensureExternalTable(conn, cfg, "events", inst);
            ensureRefTables(conn, inst.layoutPath);

            String plan = null;
            if (cfg.benchmark.runs.recordExplainPlans) {
                try (Statement st = conn.createStatement();
                     ResultSet rs = st.executeQuery("EXPLAIN " + bq.sql)) {
                    plan = String.join("\n", readAll(rs));
                }
            }
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(bq.sql)) {
                List<String> rows = readAll(rs);
                String hash = SparkQueryRunner.hashRows(rows.toArray(new String[0]));
                long latency = (System.nanoTime() - t0) / 1_000_000L;
                return ok(runId, inst, bq.queryId, repetition, warmup, concurrency, latency, hash, rows.size(), plan);
            }
        } catch (Exception ex) {
            long latency = (System.nanoTime() - t0) / 1_000_000L;
            RunResult r = ok(runId, inst, bq.queryId, repetition, warmup, concurrency, latency, "", 0, null);
            r.status = "error";
            r.errorMessage = ex.getMessage();
            return r;
        }
    }

    private static void applySession(Connection conn, Map<String, String> overrides) throws Exception {
        try (Statement st = conn.createStatement()) {
            for (Map.Entry<String, String> e : overrides.entrySet()) {
                st.execute("SET " + e.getKey() + "=" + e.getValue());
            }
        }
    }

    private static void ensureExternalTable(Connection conn, LoadedConfig cfg, String table, ExperimentInstance inst) throws Exception {
        List<String[]> extras = new ArrayList<>();
        if ("FLAT".equals(inst.representation) || "HYBRID".equals(inst.representation)) {
            String[] names = {"epk_id", "session_id", "operation_uid", "channel_type", "vsp_code",
                    "osb_code", "tb_code", "ip_address", "device_system_name"};
            for (String n : names) extras.add(new String[]{n, "STRING"});
        } else if ("MAP".equals(inst.representation)) {
            extras.add(new String[]{"params", "MAP<STRING,STRING>"});
        } else if ("PAIRS".equals(inst.representation)) {
            extras.add(new String[]{"params", "ARRAY<STRUCT<name:STRING,value:STRING>>"});
        }
        String ddl = SchemaDdl.createExternalTableSql(table, cfg.schemaText, inst.layoutPath, inst.partitionColumns, extras);
        try (Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS " + table);
            st.execute(ddl);
            if (!inst.partitionColumns.isEmpty()) st.execute("MSCK REPAIR TABLE " + table);
        }
    }

    private static void ensureRefTables(Connection conn, String layoutPath) {
        String refs = layoutPath.replaceAll("/[^/]+$", "/refs");
        String[][] tables = {
                {"module_dictionary", refs + "/module_dictionary"},
                {"epk_dictionary", refs + "/epk_dictionary"},
                {"partition_dates", refs + "/partition_dates"}
        };
        try (Statement st = conn.createStatement()) {
            for (String[] t : tables) {
                try {
                    st.execute("DROP TABLE IF EXISTS " + t[0]);
                    st.execute("CREATE EXTERNAL TABLE " + t[0] + " STORED AS ORC LOCATION '" + t[1] + "'");
                } catch (Exception ignored) {}
            }
        } catch (Exception ignored) {}
    }

    private static List<String> readAll(ResultSet rs) throws Exception {
        ResultSetMetaData meta = rs.getMetaData();
        int cols = meta.getColumnCount();
        List<String> out = new ArrayList<>();
        while (rs.next()) {
            StringBuilder sb = new StringBuilder();
            for (int i = 1; i <= cols; i++) {
                if (i > 1) sb.append('|');
                Object v = rs.getObject(i);
                sb.append(v == null ? "null" : v.toString());
            }
            out.add(sb.toString());
        }
        return out;
    }

    private static RunResult ok(String runId, ExperimentInstance inst, String queryId, int rep,
                                boolean warmup, int conc, long latency, String hash, long rows, String plan) {
        RunResult r = new RunResult();
        r.runId = runId;
        r.experimentId = inst.experimentId;
        r.variantLabel = inst.variantLabel;
        r.engine = inst.engine;
        r.queryId = queryId;
        r.repetition = rep;
        r.warmup = warmup;
        r.concurrency = conc;
        r.cacheState = inst.cacheState;
        r.latencyMs = latency;
        r.status = "ok";
        r.resultHash = hash;
        r.logicalRows = rows;
        r.enginePlan = plan;
        return r;
    }

    private static RunResult skipped(String runId, ExperimentInstance inst, String reason) {
        RunResult r = new RunResult();
        r.runId = runId;
        r.experimentId = inst.experimentId;
        r.variantLabel = inst.variantLabel;
        r.engine = inst.engine;
        r.queryId = "*";
        r.status = "skipped";
        r.resultHash = "";
        r.errorMessage = reason;
        r.cacheState = inst.cacheState;
        r.concurrency = 1;
        return r;
    }
}
