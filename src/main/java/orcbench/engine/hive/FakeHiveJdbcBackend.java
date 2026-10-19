package orcbench.engine.hive;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Deterministic in-memory Hive JDBC backend for unit tests and
 * {@code --mock-run}. Never opens a real HiveServer2 connection.
 */
public final class FakeHiveJdbcBackend implements HiveJdbcBackend {

    public final Map<String, String> appliedOverrides = new LinkedHashMap<String, String>();
    public final Map<String, String> registeredTables = new LinkedHashMap<String, String>();
    public final List<String> executedDdl = new ArrayList<String>();
    public final List<String> executedSql = new ArrayList<String>();
    public final List<String> explainedSql = new ArrayList<String>();
    public Long fixedOrcBytes = 1_048_576L;
    public Long fixedHdfsBytesRead = 262_144L;
    public int executeCalls;
    public int closeCalls;

    @Override
    public void applySessionOverrides(Map<String, String> overrides) {
        if (overrides != null) {
            appliedOverrides.putAll(overrides);
        }
    }

    @Override
    public void ensureExternalTable(String tableName, String ddl, boolean repairPartitions) {
        registeredTables.put(tableName, ddl == null ? "" : ddl);
        if (ddl != null) {
            executedDdl.add(ddl);
        }
        if (repairPartitions) {
            executedDdl.add("MSCK REPAIR TABLE " + tableName);
        }
    }

    @Override
    public void ensureRefTables(Map<String, String> nameToLocation) {
        if (nameToLocation == null) {
            return;
        }
        for (Map.Entry<String, String> e : nameToLocation.entrySet()) {
            registeredTables.put(e.getKey(), e.getValue());
            executedDdl.add("CREATE EXTERNAL TABLE " + e.getKey()
                    + " STORED AS ORC LOCATION '" + e.getValue() + "'");
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
        String fingerprint = shortHash(sql == null ? "" : sql);
        // Nulls encoded as \N to match result_equivalence.null_encoding.
        String[] rows = new String[] {
                "count|" + fingerprint.substring(0, 4),
                "sample|" + fingerprint.substring(4, 8),
                "nullrow|\\N"
        };
        return new QueryExecResult(rows, fixedHdfsBytesRead);
    }

    @Override
    public String explain(String sql) {
        explainedSql.add(sql);
        String head = sql == null ? "" : sql.substring(0, Math.min(40, sql.length()));
        return "STAGE DEPENDENCIES:\n  FakeHive stage for " + head;
    }

    @Override
    public void close() {
        closeCalls++;
    }

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
