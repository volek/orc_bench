package orcbench.layout;

import orcbench.config.ConfigLoader;
import orcbench.layout.ExperimentExpander.ConcurrencySelection;
import orcbench.model.Models.ExperimentInstance;
import orcbench.model.Models.ExperimentPlan;
import orcbench.model.Models.LoadedConfig;
import orcbench.model.Models.OrcWriteSettings;
import orcbench.model.Models.SparkEnv;
import orcbench.model.Models.YarnEnv;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class ExperimentExpanderTest {

    private static LoadedConfig cfg;

    @BeforeAll
    static void setUp() throws Exception {
        cfg = ConfigLoader.load("conf", null);
    }

    @Test
    void baselineSettingsCopiedFromPhysicalBaseline() {
        OrcWriteSettings s = ExperimentExpander.baselineSettings(cfg.benchmark.physicalBaseline);
        assertEquals("ZLIB", s.compression);
        assertEquals(10000, s.rowIndexStride);
        assertNotSame(cfg.benchmark.physicalBaseline.orc.bloomColumns, s.bloomColumns);
    }

    @Test
    void expandPhysicalIncludesBaselineAndFilter() {
        List<ExperimentInstance> all = ExperimentExpander.expandPhysical(
                cfg.benchmark, "S", Collections.<String>emptyList());
        assertTrue(all.stream().anyMatch(i -> "RAW_ARRAY_BASE".equals(i.experimentId)));
        assertTrue(all.size() > 20);
        assertTrue(all.size() < 200);

        List<ExperimentInstance> filtered = ExperimentExpander.expandPhysical(
                cfg.benchmark, "S", Collections.singletonList("BLOOM_SESSION"));
        Set<String> ids = filtered.stream().map(i -> i.experimentId).collect(Collectors.toSet());
        assertTrue(ids.contains("BLOOM_SESSION"));
        assertFalse(ids.contains("COMPRESSION"));
        assertFalse(ids.contains("RAW_ARRAY_BASE"));
    }

    @Test
    void expandPhysicalCreatesDistinctLayoutPathsForRewrite() {
        List<ExperimentInstance> compression = ExperimentExpander.expandPhysical(
                cfg.benchmark, "M", Collections.singletonList("COMPRESSION"));
        assertEquals(3, compression.size());
        Set<String> paths = compression.stream().map(i -> i.layoutPath).collect(Collectors.toSet());
        assertEquals(3, paths.size());
        assertTrue(paths.stream().allMatch(p -> p.contains("/M/")));
        assertTrue(paths.stream().allMatch(p -> p.startsWith("hdfs://sdp-dzo-lt-omlt/orc_test/")));
        assertTrue(compression.stream().anyMatch(i -> "ZSTD".equals(i.orcSettings.compression)));
    }

    @Test
    void expandEngineAndFinalProfiles() {
        List<ExperimentInstance> engines = ExperimentExpander.expandEngine(
                cfg.benchmark, "S", "/orc_test/S/RAW_ARRAY_BASE",
                Arrays.asList("spark", "hive_tez", "hive_llap"), Collections.<String>emptyList());
        assertTrue(engines.stream().anyMatch(i -> "SPARK_BASE".equals(i.experimentId)));
        assertTrue(engines.stream().anyMatch(i -> "SPARK_AQE".equals(i.experimentId)));
        assertTrue(engines.stream().anyMatch(i -> "HIVE_TEZ_BASE".equals(i.experimentId)));
        assertTrue(engines.stream().anyMatch(i -> i.llap));
        // HIVE_VECTORIZATION / HIVE_CBO expand property values (false/true)
        assertTrue(engines.stream().anyMatch(i ->
                "HIVE_VECTORIZATION".equals(i.experimentId)
                        && "true".equals(i.sessionOverrides.get("hive.vectorized.execution.enabled"))));
        assertTrue(engines.stream().noneMatch(i -> "HIVE_CBO".equals(i.experimentId)));

        List<ExperimentInstance> asked = ExperimentExpander.expandEngine(
                cfg.benchmark, "S", "/orc_test/S/RAW_ARRAY_BASE",
                Collections.singletonList("hive_tez"),
                Collections.singletonList("HIVE_CBO"));
        assertEquals(1, asked.size());
        assertEquals("HIVE_CBO", asked.get(0).experimentId);
        assertNotNull(asked.get(0).skipReason);
        assertTrue(asked.get(0).skipReason.contains("excluded"), asked.get(0).skipReason);

        List<ExperimentInstance> bucketing = ExperimentExpander.expandPhysical(
                cfg.benchmark, "S", Collections.singletonList("BUCKETING"));
        assertEquals(1, bucketing.size());
        assertNotNull(bucketing.get(0).skipReason);
        assertTrue(bucketing.get(0).skipReason.contains("excluded"));

        List<ExperimentInstance> finals = ExperimentExpander.finalProfiles(cfg.benchmark, "/orc_test/S/BEST");
        assertEquals(cfg.benchmark.finalComparison.engines.size(), finals.size());
        assertTrue(finals.stream().anyMatch(i ->
                "SPARK_COMMON".equals(i.experimentId) && "spark".equals(i.engine)));
    }

    @Test
    void readerSettingsVariants() {
        List<ExperimentInstance> push = ExperimentExpander.expandPhysical(
                cfg.benchmark, "S", Collections.singletonList("ORC_PUSHDOWN"));
        assertEquals(2, push.size());
        assertTrue(push.stream().anyMatch(i ->
                "false".equals(i.readerOverrides.get("spark.sql.orc.filterPushdown"))));
        assertTrue(push.stream().anyMatch(i ->
                "true".equals(i.readerOverrides.get("spark.sql.orc.filterPushdown"))));
    }

    @Test
    void capacityAwareConcurrencyForClusterDefaults() {
        YarnEnv yarn = cfg.environment.environment.yarn;
        SparkEnv spark = cfg.environment.environment.spark;
        List<Integer> candidates = cfg.benchmark.runs.concurrencyLevels;

        ConcurrencySelection s = ExperimentExpander.selectConcurrency(candidates, "S", yarn, spark);
        assertEquals(Arrays.asList(1, 5, 9), s.levels);

        ConcurrencySelection m = ExperimentExpander.selectConcurrency(candidates, "M", yarn, spark);
        assertEquals(Arrays.asList(1, 5, 9), m.levels);

        ConcurrencySelection l = ExperimentExpander.selectConcurrency(candidates, "L", yarn, spark);
        assertEquals(Arrays.asList(1, 5), l.levels);
    }

    @Test
    void capacityAwareConcurrencyAllowsHigherWhenHeadroom() {
        YarnEnv yarn = new YarnEnv();
        yarn.availableMemoryGib = 500L;
        yarn.availableVcores = 200;
        SparkEnv spark = new SparkEnv();
        spark.executorInstances = 10;
        spark.executorCores = 4;
        spark.executorMemoryGib = 16;

        ConcurrencySelection s = ExperimentExpander.selectConcurrency(
                Arrays.asList(1, 5, 10, 25, 50), "S", yarn, spark);
        assertTrue(s.levels.contains(10) || s.levels.contains(25));
        assertFalse(s.levels.contains(50));
    }

    @Test
    void buildPlanAndPrintDryRun() throws Exception {
        ExperimentPlan plan = ExperimentExpander.buildPlan(
                cfg, "S", Arrays.asList("spark", "hive_tez"), Collections.<String>emptyList());
        assertFalse(plan.physical.isEmpty());
        assertFalse(plan.engine.isEmpty());
        assertFalse(plan.finalComparison.isEmpty());
        assertEquals(Arrays.asList(1, 5, 9), plan.concurrencySelected);
        assertTrue(plan.engine.stream().noneMatch(i -> i.llap),
                "hive_llap not requested → no LLAP cells");

        StringWriter sw = new StringWriter();
        ExperimentExpander.printPlan(plan, sw);
        String text = sw.toString();
        assertTrue(text.contains("[plan] scale=S"));
        assertTrue(text.contains("RAW_ARRAY_BASE"));
        assertTrue(text.contains("selected=[1, 5]") || text.contains("selected=" + plan.concurrencySelected));
    }
}
