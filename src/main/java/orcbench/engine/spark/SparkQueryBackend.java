package orcbench.engine.spark;

import java.util.Map;

/**
 * Injectable Spark SQL execution backend for Checkpoint 6.
 * Live path uses a SparkSession on a cluster node; local/CI uses
 * {@link FakeSparkQueryBackend}.
 */
public interface SparkQueryBackend extends AutoCloseable {

    /** Apply session / reader overrides (Spark conf or no-op for fake). */
    void applyOverrides(Map<String, String> overrides);

    /** Register ORC layout as a temp view. */
    void registerLayout(String tableName, String layoutPath) throws Exception;

    /** Best-effort register of reference dictionaries under refsRoot. */
    void registerRefs(String refsRoot);

    /** Total bytes under layout path (ORC files), or null if unknown. */
    Long estimateOrcBytes(String layoutPath);

    /**
     * Execute SQL. Fake backends return canonical row strings. The live backend
     * returns only {@code logicalRows} and {@code resultHash} computed on the
     * executors, so a wide result is not collected on the driver.
     */
    QueryExecResult execute(String sql) throws Exception;

    /** Optional EXPLAIN EXTENDED text; may return null. */
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
