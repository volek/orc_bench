package orcbench.engine.hive;

import java.util.Map;

/**
 * Injectable Hive JDBC execution backend for Checkpoint 7.
 * Live path uses HiveServer2 on a cluster node; local/CI uses
 * {@link FakeHiveJdbcBackend}.
 */
public interface HiveJdbcBackend extends AutoCloseable {

    /** Apply Hive session settings ({@code SET key=value}). */
    void applySessionOverrides(Map<String, String> overrides) throws Exception;

    /**
     * Drop+create an external ORC table (and optional {@code MSCK REPAIR}).
     *
     * @param ddl full {@code CREATE EXTERNAL TABLE …} statement
     */
    void ensureExternalTable(String tableName, String ddl, boolean repairPartitions)
            throws Exception;

    /** Best-effort register of reference dictionaries (name → HDFS location). */
    void ensureRefTables(Map<String, String> nameToLocation) throws Exception;

    /** Total bytes under layout path, or null if unknown. */
    Long estimateOrcBytes(String layoutPath);

    /**
     * Execute SQL and return canonical row strings ({@code col|col|…}) plus
     * optional HDFS bytes read if observable.
     */
    QueryExecResult execute(String sql) throws Exception;

    /** Optional {@code EXPLAIN} text; may return null. */
    String explain(String sql);

    /** Result of one SQL execution. */
    final class QueryExecResult {
        public final String[] rowStrings;
        public final Long hdfsBytesRead;

        public QueryExecResult(String[] rowStrings, Long hdfsBytesRead) {
            this.rowStrings = rowStrings == null ? new String[0] : rowStrings;
            this.hdfsBytesRead = hdfsBytesRead;
        }
    }
}
