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
     * Execute SQL and return canonical row strings ({@code col|col|…}) plus
     * optional HDFS bytes read if the backend can observe them.
     */
    QueryExecResult execute(String sql) throws Exception;

    /** Optional EXPLAIN EXTENDED text; may return null. */
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
