package orcbench.report;

import orcbench.config.ConfigLoader;
import orcbench.layout.ExperimentExpander;
import orcbench.metrics.MetricsStore;
import orcbench.model.Models.AggregateStats;
import orcbench.model.Models.BestOrcChoice;
import orcbench.model.Models.CapabilityReport;
import orcbench.model.Models.ExperimentPlan;
import orcbench.model.Models.LoadedConfig;
import orcbench.model.Models.ReportPhaseResult;
import orcbench.model.Models.RunResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ReportWriterTest {

    @TempDir
    Path tmp;

    @Test
    void writeRunCreatesAllArtifacts() throws Exception {
        List<RunResult> results = MetricsStore.readJsonl(
                Paths.get("src/test/resources/fixtures/cp8/metrics.jsonl").toString());
        List<AggregateStats> agg = MetricsStore.aggregate(results, 3000L);
        BestOrcChoice best = BestOrcSelector.select(agg, new orcbench.model.Models.BestOrcRule(), null);

        CapabilityReport cap = new CapabilityReport();
        cap.capabilities.put("hive_jdbc", true);
        cap.notes.add("fixture");

        String dir = ReportWriter.writeRun(
                tmp.toString(),
                "run1",
                "S",
                results,
                agg,
                Collections.singletonList(new String[]{"E2", "skipped"}),
                cap,
                best,
                ExperimentExpander.finalProfiles(
                        ConfigLoader.load("conf", null).benchmark,
                        best.layoutPath == null ? "/orc_test/S/BEST" : best.layoutPath),
                Collections.singletonList("unit-note"),
                3000L,
                true);

        assertTrue(Files.exists(Paths.get(dir, "metrics.jsonl")));
        assertTrue(Files.exists(Paths.get(dir, "summary.csv")));
        assertTrue(Files.exists(Paths.get(dir, "skipped.tsv")));
        assertTrue(Files.exists(Paths.get(dir, "SUMMARY.txt")));
        assertTrue(Files.exists(Paths.get(dir, "final_comparison.tsv")));

        String summary = new String(Files.readAllBytes(Paths.get(dir, "SUMMARY.txt")), StandardCharsets.UTF_8);
        assertTrue(summary.contains("runId=run1"));
        assertTrue(summary.contains("scale=S"));
        assertTrue(summary.contains("bestOrc.experimentId="));
        assertFalse(summary.contains("compression=ZSTD")
                || (best.variantLabel != null && best.variantLabel.contains("ZSTD")));
        assertTrue(summary.contains("hive_jdbc=true"));
        assertTrue(summary.contains("finalComparisonCells="));

        String csv = new String(Files.readAllBytes(Paths.get(dir, "summary.csv")), StandardCharsets.UTF_8);
        assertTrue(csv.contains("avgMs"));
        assertTrue(csv.contains("slaSuccessRate"));
        assertTrue(csv.contains("slaPass"));

        String skipped = new String(Files.readAllBytes(Paths.get(dir, "skipped.tsv")), StandardCharsets.UTF_8);
        assertTrue(skipped.contains("E2"));
    }

    @Test
    void reportPhaseFromFixtureMetricsJsonl() throws Exception {
        LoadedConfig cfg = ConfigLoader.load("conf", null);
        ExperimentPlan plan = ExperimentExpander.buildPlan(
                cfg, "S", Arrays.asList("spark", "hive_tez"), null);

        Path fixture = Paths.get("src/test/resources/fixtures/cp8/metrics.jsonl");
        ReportPhaseResult result = ReportPhase.report(
                cfg,
                plan,
                "S",
                tmp.toString(),
                "fixture-cp8",
                fixture.toString(),
                null,
                null,
                null,
                false,
                true);

        assertEquals("final", result.phase);
        assertTrue(result.resultRows > 0);
        assertTrue(result.aggregateRows > 0);
        assertNotNull(result.bestOrc);
        assertNotNull(result.bestOrc.experimentId);
        assertFalse(result.bestOrc.variantLabel != null
                && result.bestOrc.variantLabel.toUpperCase().contains("ZSTD"));
        assertTrue(result.finalComparisonCells >= 4);
        assertNotNull(result.outputDir);
        assertTrue(Files.exists(Paths.get(result.outputDir).resolve("SUMMARY.txt")));
        assertTrue(Files.exists(Paths.get(result.outputDir).resolve("final_comparison.tsv")));
    }

    @Test
    void reportPhaseDryRunWithoutMetrics() throws Exception {
        LoadedConfig cfg = ConfigLoader.load("conf", null);
        ExperimentPlan plan = ExperimentExpander.buildPlan(cfg, "S", Arrays.asList("spark"), null);

        ReportPhaseResult result = ReportPhase.report(
                cfg, plan, "S", tmp.toString(), "dry-report", null, null, null, null, true, false);

        assertEquals("report", result.phase);
        assertTrue(result.dryRun);
        assertTrue(Files.exists(Paths.get(result.outputDir).resolve("SUMMARY.txt")));
        assertTrue(Files.exists(Paths.get(result.outputDir).resolve("summary.csv")));
        assertTrue(Files.exists(Paths.get(result.outputDir).resolve("skipped.tsv")));
    }
}
