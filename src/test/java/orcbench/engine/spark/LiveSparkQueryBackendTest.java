package orcbench.engine.spark;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

import orcbench.engine.ResultDigest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class LiveSparkQueryBackendTest {

    @Test
    void executeFoldsRowsOnExecutors() throws Exception {
        SparkSession spark = SparkSession.builder()
                .master("local[2]")
                .appName("orc-bench-digest")
                .config("spark.ui.enabled", "false")
                .getOrCreate();
        try {
            LiveSparkQueryBackend backend = new LiveSparkQueryBackend(spark, false);
            SparkQueryBackend.QueryExecResult result = backend.execute(
                    "SELECT * FROM VALUES (1, 'a'), (2, CAST(NULL AS STRING)), (1, 'a') t(x, y)");
            String[] canonical = new String[]{"1|a", "2|\\N", "1|a"};
            assertEquals(3L, result.logicalRows);
            assertEquals(ResultDigest.hashRows(canonical), result.resultHash);
            assertEquals(0, result.rowStrings.length);
            assertNotNull(result.hdfsBytesRead);
        } finally {
            spark.stop();
        }
    }
}
