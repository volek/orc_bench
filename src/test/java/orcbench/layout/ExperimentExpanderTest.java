package orcbench.layout;

import orcbench.config.ConfigLoader;
import orcbench.model.Models.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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
        assertEquals("SNAPPY", s.compression);
        assertEquals(10000, s.rowIndexStride);
        assertNotSame(cfg.benchmark.physicalBaseline.orc.bloomColumns, s.bloomColumns);
    }

    @Test
    void expandPhysicalIncludesBaselineAndFilter() {
        List<ExperimentInstance> all = ExperimentExpander.expandPhysical(cfg.benchmark, "S", Collections.emptyList());
        assertTrue(all.stream().anyMatch(i -> "RAW_ARRAY_BASE".equals(i.experimentId)));

        List<ExperimentInstance> filtered = ExperimentExpander.expandPhysical(
                cfg.benchmark, "S", Collections.singletonList("BLOOM_USER"));
        Set<String> ids = filtered.stream().map(i -> i.experimentId).collect(Collectors.toSet());
        assertTrue(ids.contains("BLOOM_USER"));
        assertFalse(ids.contains("COMPRESSION"));
    }

    @Test
    void expandPhysicalCreatesDistinctLayoutPathsForRewrite() {
        List<ExperimentInstance> compression = ExperimentExpander.expandPhysical(
                cfg.benchmark, "M", Collections.singletonList("COMPRESSION"));
        assertEquals(3, compression.size());
        Set<String> paths = compression.stream().map(i -> i.layoutPath).collect(Collectors.toSet());
        assertEquals(3, paths.size());
        assertTrue(paths.stream().allMatch(p -> p.contains("/M/")));
    }

    @Test
    void expandEngineAndFinalProfiles() {
        List<ExperimentInstance> engines = ExperimentExpander.expandEngine(
                cfg.benchmark, "S", "/benchmarks/audit_orc/S/RAW_ARRAY_BASE",
                Arrays.asList("spark", "hive_tez", "hive_llap"), Collections.emptyList());
        assertTrue(engines.stream().anyMatch(i -> "SPARK_BASE".equals(i.experimentId)));
        assertTrue(engines.stream().anyMatch(i -> i.llap));

        List<ExperimentInstance> finals = ExperimentExpander.finalProfiles(cfg.benchmark, "/best");
        assertEquals(cfg.benchmark.finalComparison.engines.size(), finals.size());
        assertTrue(finals.stream().anyMatch(i -> "HIVE_LLAP_WARM".equals(i.experimentId) && "warm".equals(i.cacheState)));
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
}
