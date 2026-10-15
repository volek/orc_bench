package orcbench.metrics;

import orcbench.model.Models.AggregateStats;
import orcbench.model.Models.RunResult;
import org.junit.jupiter.api.Test;

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
}
