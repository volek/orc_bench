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

    /** Register reference dictionaries (name → HDFS location) with an explicit schema. */
    void ensureRefTables(Map<String, String> nameToLocation) throws Exception;

    /**
     * Independent session for one concurrency slot. HiveServer2 connections
     * are single-threaded; sharing one connection across a pool kills Tez DAGs.
     * The caller closes the returned backend when it is not {@code this}.
     */
    HiveJdbcBackend openSession() throws Exception;

    /** Total bytes under layout path, or null if unknown. */
    Long estimateOrcBytes(String layoutPath);

    /**
     * Execute SQL. Fake backends return canonical row strings. The live backend
     * streams the JDBC result and returns only {@code logicalRows} and
     * {@code resultHash}, so a wide result is not buffered on the driver.
     */
    QueryExecResult execute(String sql) throws Exception;

    /** Optional {@code EXPLAIN} text; may return null. */
    String explain(String sql);

    /** Result of one SQL execution. */
    final class QueryExecResult {
        public final String[] rowStrings;
        public final Long hdfsBytesRead;
        /** Row count of the SQL result. {@code -1} means {@code rowStrings.length}. */
        public final long logicalRows;
        /** Precomputed fingerprint. {@code null} means hash {@code rowStrings}. */
        public final String resultHash;

        public QueryExecResult(String[] rowStrings, Long hdfsBytesRead) {
            this(rowStrings, hdfsBytesRead, -1L, null);
        }

        public QueryExecResult(String[] rowStrings, Long hdfsBytesRead,
                                long logicalRows, String resultHash) {
            this.rowStrings = rowStrings == null ? new String[0] : rowStrings;
            this.hdfsBytesRead = hdfsBytesRead;
            this.logicalRows = logicalRows;
            this.resultHash = resultHash;
        }
    }
}
