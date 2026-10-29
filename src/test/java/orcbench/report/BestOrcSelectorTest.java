package orcbench.report;

import orcbench.config.ConfigLoader;
import orcbench.layout.ExperimentExpander;
import orcbench.metrics.MetricsStore;
import orcbench.model.Models.AggregateStats;
import orcbench.model.Models.BestOrcChoice;
import orcbench.model.Models.BestOrcRule;
import orcbench.model.Models.ExperimentInstance;
import orcbench.model.Models.ExperimentPlan;
import orcbench.model.Models.LoadedConfig;
import orcbench.model.Models.OrcWriteSettings;
import orcbench.model.Models.RunResult;
import org.junit.jupiter.api.Test;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BestOrcSelectorTest {

    @Test
    void excludesZstdEvenWhenFastest() throws Exception {
        List<RunResult> rows = MetricsStore.readJsonl(
                Paths.get("src/test/resources/fixtures/cp8/metrics.jsonl").toString());
        assertFalse(rows.isEmpty());
        List<AggregateStats> agg = MetricsStore.aggregate(rows, 3000L);

        BestOrcRule rule = new BestOrcRule();
        rule.objectiveQueries = Arrays.asList("Q02", "Q03", "Q07_NARROW");
        BestOrcChoice choice = BestOrcSelector.select(agg, rule, null);

        assertNotNull(choice.experimentId);
        assertFalse(choice.experimentId.contains("ZSTD")
                || (choice.variantLabel != null && choice.variantLabel.contains("ZSTD")),
                "BEST_ORC must not recommend ZSTD for Hive-shared final_comparison");
        // MINMAX_SORT has lowest non-ZSTD mean p50 in the fixture
        assertEquals("MINMAX_SORT", choice.experimentId);
        assertTrue(choice.slaPassed);
    }

    @Test
    void tieBreakPrefersPartitionedOverSortedWhenP50Equal() {
        BestOrcRule rule = new BestOrcRule();
        rule.objectiveQueries = Arrays.asList("Q02", "Q03", "Q07_NARROW");
        List<AggregateStats> agg = new ArrayList<AggregateStats>();
        agg.addAll(objectiveRows("MINMAX_SORT", "sorting=[userLogin]", 1000));
        agg.addAll(objectiveRows("PARTITIONING", "partition_columns=[event_date]", 1000));

        ExperimentInstance sorted = layout("MINMAX_SORT", "sorting=[userLogin]",
                Collections.<String>emptyList(), Arrays.asList("userLogin"), "SNAPPY");
        ExperimentInstance part = layout("PARTITIONING", "partition_columns=[event_date]",
                Arrays.asList("event_date"), Collections.<String>emptyList(), "SNAPPY");

        BestOrcChoice choice = BestOrcSelector.select(agg, rule, Arrays.asList(sorted, part));
        assertEquals("PARTITIONING", choice.experimentId);
        assertEquals("partitioned", choice.tieBreakClass);
    }

    @Test
    void emptySortListIsFlatNotSorted() {
        BestOrcRule rule = new BestOrcRule();
        rule.objectiveQueries = Arrays.asList("Q_SESSION_EQ");
        AggregateStats a = new AggregateStats();
        a.experimentId = "MINMAX_SORT";
        a.variantLabel = "sorting=[]";
        a.queryId = "Q_SESSION_EQ";
        a.concurrency = 1;
        a.n = 30;
        a.p50Ms = 180;
        a.slaSuccessRate = 1.0;
        ExperimentInstance layout = layout("MINMAX_SORT", "sorting=[]",
                Collections.<String>emptyList(), Collections.<String>emptyList(), "ZLIB");
        BestOrcChoice choice = BestOrcSelector.select(
                Collections.singletonList(a), rule, Collections.singletonList(layout));
        assertEquals("flat", choice.tieBreakClass);
    }

    @Test
    void usesPhysicalLayoutsFromConfPlan() throws Exception {
        LoadedConfig cfg = ConfigLoader.load("conf", null);
        ExperimentPlan plan = ExperimentExpander.buildPlan(
                cfg, "S", Arrays.asList("spark"),
                Arrays.asList("RAW_ARRAY_BASE", "MINMAX_SORT", "PARTITIONING", "COMPRESSION"));

        List<RunResult> rows = MetricsStore.readJsonl(
                Paths.get("src/test/resources/fixtures/cp8/metrics.jsonl").toString());
        // Align fixture variant labels with expander labels where possible; selector
        // falls back when layout index misses — still excludes ZSTD via aggregate labels.
        List<AggregateStats> agg = MetricsStore.aggregate(rows, 3000L);
        BestOrcChoice choice = BestOrcSelector.select(agg, cfg.benchmark.bestOrcRule, plan.physical);
        assertNotNull(choice);
        if (choice.experimentId != null) {
            assertFalse(choice.variantLabel != null && choice.variantLabel.toUpperCase().contains("ZSTD"));
        }
    }

    private static List<AggregateStats> objectiveRows(String exp, String variant, long p50) {
        List<AggregateStats> out = new ArrayList<AggregateStats>();
        for (String q : Arrays.asList("Q02", "Q03", "Q07_NARROW")) {
            AggregateStats a = new AggregateStats();
            a.experimentId = exp;
            a.variantLabel = variant;
            a.engine = "spark";
            a.queryId = q;
            a.concurrency = 1;
            a.n = 5;
            a.avgMs = p50;
            a.p50Ms = p50;
            a.p95Ms = p50;
            a.p99Ms = p50;
            a.slaSuccessRate = 1.0;
            out.add(a);
        }
        return out;
    }

    private static ExperimentInstance layout(
            String id, String variant, List<String> parts, List<String> sort, String compression) {
        ExperimentInstance i = new ExperimentInstance();
        i.experimentId = id;
        i.variantLabel = variant;
        i.kind = "rewrite";
        i.layoutPath = "/orc_test/S/" + id;
        i.partitionColumns = new ArrayList<String>(parts);
        i.sorting = new ArrayList<String>(sort);
        i.orcSettings = new OrcWriteSettings();
        i.orcSettings.compression = compression;
        return i;
    }
}
