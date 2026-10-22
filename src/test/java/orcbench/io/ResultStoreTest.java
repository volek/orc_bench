package orcbench.io;

import orcbench.config.ConfigLoader;
import orcbench.model.Models.LoadedConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ResultStoreTest {

    @Test
    void childKeepsHdfsScheme() {
        assertEquals(
                "hdfs://sdp-dzo-lt-omlt/orc_test/results/run1/metrics.jsonl",
                ResultStore.child("hdfs://sdp-dzo-lt-omlt/orc_test/results", "run1", "metrics.jsonl"));
    }

    @Test
    void localRoundTrip(@TempDir Path tmp) throws Exception {
        String path = ResultStore.child(tmp.toString(), "run1", "metrics.jsonl");
        assertFalse(ResultStore.onHdfs(path));
        ResultStore.appendUtf8(path, "a\n");
        ResultStore.appendUtf8(path, "b\n");
        List<String> lines = ResultStore.readLines(path);
        assertEquals(2, lines.size());
        assertEquals("a", lines.get(0));
        assertEquals("b", lines.get(1));
        assertTrue(Files.isRegularFile(tmp.resolve("run1").resolve("metrics.jsonl")));
    }

    @Test
    void yarnDefaultResultsDirIsHdfs() throws Exception {
        LoadedConfig cfg = ConfigLoader.load("conf", null);
        assertEquals("results", ResultStore.resolveResultsDir("results", cfg));
        assertEquals("results", ResultStore.resolveResultsDir(null, cfg));
        System.setProperty("spark.yarn.app.id", "application_test_0001");
        try {
            String dir = ResultStore.resolveResultsDir("results", cfg);
            assertEquals("hdfs://sdp-dzo-lt-omlt/orc_test/results/sdp-dzo-lt-omlt", dir);
            assertEquals("hdfs://sdp-dzo-lt-omlt/orc_test/logs/sdp-dzo-lt-omlt", ResultStore.hdfsLogsRoot(cfg));
            assertTrue(ResultStore.onHdfs(dir));
            assertEquals(
                    "hdfs://sdp-dzo-lt-omlt/orc_test/results/sdp-dzo-lt-omlt",
                    ResultStore.resolveResultsDir(
                            "hdfs://orc_test/results/sdp-dzo-lt-omlt", cfg));
            assertEquals(
                    "hdfs://sdp-dzo-lt-omlt/orc_test/custom",
                    ResultStore.resolveResultsDir("/orc_test/custom", cfg));
        } finally {
            System.clearProperty("spark.yarn.app.id");
        }
    }
}
