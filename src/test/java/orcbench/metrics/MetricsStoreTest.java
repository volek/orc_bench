package orcbench.metrics;

import orcbench.model.Models.AggregateStats;
import orcbench.model.Models.RunResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MetricsStoreTest {

    private static RunResult sample(long lat, String exp) {
        RunResult r = new RunResult();
        r.runId = "r1";
        r.experimentId = exp;
        r.variantLabel = "v";
        r.engine = "spark";
        r.queryId = "Q02";
        r.warmup = false;
        r.concurrency = 1;
        r.latencyMs = lat;
        r.status = "ok";
        r.resultHash = "abc";
        r.logicalRows = 1;
        r.orcFileBytes = 1000L;
        r.hdfsBytesRead = 100L;
        return r;
    }

    @Test
    void aggregatesPercentilesAndSla() {
        List<RunResult> rows = Arrays.asList(
                sample(1000, "E1"), sample(2000, "E1"), sample(2500, "E1"),
                sample(4000, "E1"), sample(5000, "E1"));
        List<AggregateStats> agg = MetricsStore.aggregate(rows, 3000L);
        assertEquals(1, agg.size());
        AggregateStats a = agg.get(0);
        assertEquals(5, a.n);
        assertEquals(2900.0, a.avgMs, 1e-9);
        assertEquals(2500.0, a.p50Ms, 1e-9);
        assertEquals(0.6, a.slaSuccessRate, 1e-9);
        assertEquals(0.1, a.scanRatio, 1e-9);
    }

    @Test
    void ranksBestOrc() {
        RunResult c = sample(2000, "C");
        c.hdfsBytesRead = 50L;
        List<AggregateStats> agg = MetricsStore.aggregate(
                Arrays.asList(sample(5000, "A"), sample(2000, "B"), c), 3000L);
        String best = MetricsStore.rankBestOrc(agg, new HashSet<>(Arrays.asList("A", "B", "C")));
        assertEquals("C", best);
    }

    @Test
    void appendJsonlCreatesFile(@TempDir Path tmp) throws Exception {
        Path out = tmp.resolve("metrics.jsonl");
        MetricsStore.appendJsonl(out.toString(), Arrays.asList(sample(100, "E1")));
        assertTrue(Files.exists(out));
        String body = new String(Files.readAllBytes(out), "UTF-8");
        assertTrue(body.contains("\"experimentId\":\"E1\""));
        assertTrue(body.contains("\"resultHash\":\"abc\""));
    }

    @Test
    void readJsonlRoundTrip(@TempDir Path tmp) throws Exception {
        Path out = tmp.resolve("metrics.jsonl");
        RunResult original = sample(123, "E1");
        original.warmup = true;
        original.cacheState = "warm";
        MetricsStore.appendJsonl(out.toString(), Arrays.asList(original));
        List<RunResult> back = MetricsStore.readJsonl(out.toString());
        assertEquals(1, back.size());
        assertEquals("E1", back.get(0).experimentId);
        assertEquals(123L, back.get(0).latencyMs);
        assertTrue(back.get(0).warmup);
        assertEquals("warm", back.get(0).cacheState);
        assertEquals("abc", back.get(0).resultHash);
    }

    @Test
    void readFixtureMetrics() throws Exception {
        List<RunResult> rows = MetricsStore.readJsonl(
                "src/test/resources/fixtures/cp8/metrics.jsonl");
        assertTrue(rows.size() >= 12);
        assertTrue(rows.stream().anyMatch(r -> "skipped".equals(r.status)));
        assertTrue(rows.stream().anyMatch(r ->
                r.variantLabel != null && r.variantLabel.contains("ZSTD")));
    }
}
