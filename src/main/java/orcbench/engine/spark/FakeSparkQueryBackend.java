package orcbench.engine.spark;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Deterministic in-memory Spark query backend for unit tests and
 * {@code --mock-run}. Never touches HDFS / YARN / a real SparkSession.
 */
public final class FakeSparkQueryBackend implements SparkQueryBackend {

    public final Map<String, String> appliedOverrides = new LinkedHashMap<String, String>();
    public final Map<String, String> registeredTables = new LinkedHashMap<String, String>();
    public final List<String> executedSql = new ArrayList<String>();
    public final List<String> explainedSql = new ArrayList<String>();
    public Long fixedOrcBytes = 1_048_576L;
    public Long fixedHdfsBytesRead = 262_144L;
    public int executeCalls;
    public int closeCalls;

    @Override
    public void applyOverrides(Map<String, String> overrides) {
        if (overrides != null) {
            appliedOverrides.putAll(overrides);
        }
    }

    @Override
    public void registerLayout(String tableName, String layoutPath) {
        registeredTables.put(tableName, layoutPath);
    }

    @Override
    public void registerRefs(String refsRoot) {
        if (refsRoot != null) {
            registeredTables.put("module_dictionary", refsRoot + "/module_dictionary");
            registeredTables.put("epk_dictionary", refsRoot + "/epk_dictionary");
            registeredTables.put("partition_dates", refsRoot + "/partition_dates");
        }
    }

    @Override
    public Long estimateOrcBytes(String layoutPath) {
        return fixedOrcBytes;
    }

    @Override
    public QueryExecResult execute(String sql) {
        executeCalls++;
        executedSql.add(sql);
        // Deterministic synthetic row set from SQL fingerprint (order-independent hash tested separately).
        String fingerprint = shortHash(sql == null ? "" : sql);
        String[] rows = new String[] {
                "count|" + fingerprint.substring(0, 4),
                "sample|" + fingerprint.substring(4, 8)
        };
        return new QueryExecResult(rows, fixedHdfsBytesRead);
    }

    @Override
    public String explain(String sql) {
        explainedSql.add(sql);
        return "== Physical Plan ==\n* FakeScan " + (sql == null ? "" : sql.substring(0, Math.min(40, sql.length())));
    }

    @Override
    public void close() {
        closeCalls++;
    }

    /** True when a Spark §1 lever override was applied (vectorization / AQE / DPP / CBO / …). */
    public boolean hasOverride(String key) {
        return appliedOverrides.containsKey(key);
    }

    public String overrideValue(String key) {
        return appliedOverrides.get(key);
    }

    private static String shortHash(String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] dig = md.digest(s.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 8; i++) {
                sb.append(String.format(Locale.ROOT, "%02x", dig[i]));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
