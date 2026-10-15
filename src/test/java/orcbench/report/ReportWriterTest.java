package orcbench.report;

import orcbench.metrics.MetricsStore;
import orcbench.model.Models.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ReportWriterTest {

    @TempDir
    Path tmp;

    @Test
    void writeRunCreatesArtifacts() throws Exception {
        RunResult r = new RunResult();
        r.runId = "run1";
        r.experimentId = "E1";
        r.variantLabel = "v";
        r.engine = "spark";
        r.queryId = "Q01";
        r.latencyMs = 100;
        r.status = "ok";
        r.resultHash = "abc";
        r.logicalRows = 1;
        r.concurrency = 1;

        List<RunResult> results = Collections.singletonList(r);
        List<AggregateStats> agg = MetricsStore.aggregate(results, 3000);
        CapabilityReport cap = new CapabilityReport();
        cap.capabilities.put("hive_jdbc", true);
        cap.notes.add("note1");

        String dir = ReportWriter.writeRun(
                tmp.toString(),
                "run1",
                results,
                agg,
                Collections.singletonList(new String[]{"E2", "skipped"}),
                cap,
                "/best");

        Path out = java.nio.file.Paths.get(dir);
        assertTrue(Files.exists(out.resolve("metrics.jsonl")));
        assertTrue(Files.exists(out.resolve("summary.csv")));
        assertTrue(Files.exists(out.resolve("skipped.tsv")));
        assertTrue(Files.exists(out.resolve("SUMMARY.txt")));
        String summary = new String(Files.readAllBytes(out.resolve("SUMMARY.txt")), java.nio.charset.StandardCharsets.UTF_8);
        assertTrue(summary.contains("bestOrc=/best"));
        assertTrue(summary.contains("hive_jdbc=true"));
    }
}
