package orcbench.config;

import orcbench.layout.ExperimentExpander;
import orcbench.model.Models.*;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class ConfigLoaderTest {

    @Test
    void loadsYamlContract() throws Exception {
        LoadedConfig cfg = ConfigLoader.load("conf", null);
        assertEquals("audit_orc_hdfs_spark_hive", cfg.benchmark.benchmark.id);
        assertEquals(100L, cfg.dataset.scales.get("S").targetLogicalGib);
        assertEquals(1024L, cfg.dataset.scales.get("M").targetLogicalGib);
        assertEquals(5120L, cfg.dataset.scales.get("L").targetLogicalGib);
        assertFalse(cfg.queries.queries.isEmpty());
        assertTrue(cfg.schemaText.contains("userLogin"));
        assertEquals("benchmark", cfg.environment.environment.yarn.queue);
    }

    @Test
    void expandsOvatPhysicalExperiments() throws Exception {
        LoadedConfig cfg = ConfigLoader.load("conf", null);
        List<ExperimentInstance> instances = ExperimentExpander.expandPhysical(cfg.benchmark, "S", Collections.emptyList());
        Set<String> ids = new HashSet<>();
        for (ExperimentInstance i : instances) ids.add(i.experimentId);
        assertTrue(ids.contains("RAW_ARRAY_BASE"));
        assertTrue(ids.contains("BLOOM_USER"));
        assertTrue(ids.contains("PARTITIONING"));
        assertTrue(ids.contains("NESTED_REPRESENTATIONS"));
        assertTrue(instances.size() < 200);
        assertTrue(instances.size() > 20);
    }

    @Test
    void expandsEngineAndFinalProfiles() throws Exception {
        LoadedConfig cfg = ConfigLoader.load("conf", null);
        List<ExperimentInstance> engines = ExperimentExpander.expandEngine(
                cfg.benchmark, "S", "/benchmarks/audit_orc/S/RAW_ARRAY_BASE",
                java.util.Arrays.asList("spark", "hive_tez", "hive_llap"), Collections.emptyList());
        Set<String> ids = new HashSet<>();
        boolean llap = false;
        for (ExperimentInstance i : engines) {
            ids.add(i.experimentId);
            if (i.llap) llap = true;
        }
        assertTrue(ids.contains("SPARK_BASE"));
        assertTrue(ids.contains("SPARK_AQE"));
        assertTrue(ids.contains("HIVE_TEZ_BASE"));
        assertTrue(llap);

        List<ExperimentInstance> finals = ExperimentExpander.finalProfiles(cfg.benchmark, "/benchmarks/audit_orc/S/BEST");
        Set<String> finalIds = new HashSet<>();
        for (ExperimentInstance i : finals) finalIds.add(i.experimentId);
        assertEquals(new HashSet<>(cfg.benchmark.finalComparison.engines), finalIds);
    }

    @Test
    void cliParsesFlags() {
        CliArgs parsed = CliOptions.parse(new String[]{
                "--config", "conf", "--scale", "S", "--phase", "preflight", "--dry-run"
        });
        assertNotNull(parsed);
        assertEquals("S", parsed.scale);
        assertTrue(parsed.dryRun);
    }
}
