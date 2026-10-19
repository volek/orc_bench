package orcbench.config;

import orcbench.config.ConfigLoader.RawConfig;
import orcbench.model.Models.LoadedConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Local unit tests: load YAML contract without a cluster (CP1 raw + CP2 typed).
 */
class ConfigLoaderTest {

    @TempDir
    Path tmp;

    @Test
    void loadsRepoConfRawWithoutCluster() throws Exception {
        Path conf = Paths.get("conf");
        assertTrue(Files.isDirectory(conf), "expected conf/ (copy of configs_final) at repo root");

        RawConfig cfg = ConfigLoader.loadRaw(conf.toString(), null);

        assertTrue(cfg.environment.containsKey("environment"), "environment.yaml root key");
        assertTrue(cfg.benchmark.containsKey("benchmark"), "benchmark.yaml root key");
        assertTrue(cfg.dataset.containsKey("dataset"), "dataset.yaml root key");
        assertTrue(cfg.querySuite.containsKey("query_suite"), "query-suite.yaml root key");
        assertNotNull(cfg.schemaOrc);
        assertTrue(cfg.schemaOrc.trim().startsWith("struct<"), "schema.orc.txt");
    }

    @Test
    void loadsTypedContractFromConf() throws Exception {
        LoadedConfig cfg = ConfigLoader.load("conf", null);

        assertEquals("audit_orc_hdfs_spark_hive", cfg.benchmark.benchmark.id);
        assertEquals(2, cfg.benchmark.benchmark.specVersion);
        assertEquals("/orc_test", cfg.benchmark.benchmark.hdfsRoot);
        assertEquals("RAW_ARRAY_BASE", cfg.benchmark.physicalBaseline.id);
        assertEquals("SNAPPY", cfg.benchmark.physicalBaseline.orc.compression);
        assertEquals(10000, cfg.benchmark.physicalBaseline.orc.rowIndexStride);
        assertFalse(cfg.benchmark.physicalExperiments.isEmpty());
        assertTrue(cfg.benchmark.physicalExperiments.stream().anyMatch(e -> "BLOOM_USER".equals(e.id)));
        assertTrue(cfg.benchmark.physicalExperiments.stream().anyMatch(e -> "COMPRESSION".equals(e.id)));
        assertFalse(cfg.benchmark.engineExperiments.spark.isEmpty());
        assertFalse(cfg.benchmark.engineExperiments.hive.isEmpty());
        assertEquals(5, cfg.benchmark.runs.concurrencyLevels.size());
        assertEquals("per_scale_capacity_aware", cfg.benchmark.runs.concurrencyPolicy);
        assertEquals(Arrays.asList("Q02", "Q03", "Q07_NARROW"),
                cfg.benchmark.bestOrcRule.objectiveQueries);
        assertEquals(0.95, cfg.benchmark.bestOrcRule.slaSuccessRateMin, 1e-9);
        assertTrue(cfg.benchmark.bestOrcRule.excludeZstd);
        assertFalse(cfg.benchmark.finalComparison.engines.isEmpty());
        assertEquals("BEST_ORC", cfg.benchmark.finalComparison.physicalDataset);

        assertEquals(100L, cfg.dataset.scales.get("S").targetLogicalGib);
        assertEquals(500L, cfg.dataset.scales.get("M").targetLogicalGib);
        assertEquals(1024L, cfg.dataset.scales.get("L").targetLogicalGib);
        assertEquals(123456789L, cfg.dataset.generation.seed);
        assertFalse(cfg.dataset.columns.isEmpty());

        assertEquals("default", cfg.environment.environment.yarn.queue);
        assertEquals(Long.valueOf(216L), cfg.environment.environment.yarn.availableMemoryGib);
        assertEquals(Integer.valueOf(67), cfg.environment.environment.yarn.availableVcores);
        assertEquals("cluster", cfg.environment.environment.spark.deployMode);
        assertEquals(Integer.valueOf(10), cfg.environment.environment.spark.executorInstances);
        assertEquals(Boolean.FALSE, cfg.environment.environment.hive.materializedViewsSupported);

        assertFalse(cfg.queries.queries.isEmpty());
        assertTrue(cfg.queries.queries.stream().anyMatch(q -> "Q01".equals(q.id)));
        assertTrue(cfg.schemaText.contains("userLogin"));
    }

    @Test
    void appliesEnvOverrideMerge() throws Exception {
        Path conf = tmp.resolve("conf");
        Files.createDirectories(conf);
        Files.write(conf.resolve("environment.yaml"),
                ("environment:\n  spark:\n    deploy_mode: cluster\n    executor_instances: 10\n"
                        + "  yarn:\n    queue: default\n    available_memory_gib: 216\n")
                        .getBytes(StandardCharsets.UTF_8));
        Files.write(conf.resolve("benchmark.yaml"),
                "benchmark:\n  id: t\n  physical_baseline:\n    id: RAW_ARRAY_BASE\n    orc:\n      compression: SNAPPY\n"
                        .getBytes(StandardCharsets.UTF_8));
        Files.write(conf.resolve("dataset.yaml"),
                "dataset:\n  id: d\n  scales:\n    S:\n      target_logical_gib: 100\n"
                        .getBytes(StandardCharsets.UTF_8));
        Files.write(conf.resolve("query-suite.yaml"),
                "query_suite:\n  id: q\n  queries: []\n".getBytes(StandardCharsets.UTF_8));
        Files.write(conf.resolve("schema.orc.txt"), "struct<>\n".getBytes(StandardCharsets.UTF_8));

        Path override = tmp.resolve("environment.local.yaml");
        Files.write(override,
                "environment:\n  spark:\n    deploy_mode: client\n".getBytes(StandardCharsets.UTF_8));

        RawConfig raw = ConfigLoader.loadRaw(conf.toString(), override.toString());
        @SuppressWarnings("unchecked")
        Map<String, Object> env = (Map<String, Object>) raw.environment.get("environment");
        @SuppressWarnings("unchecked")
        Map<String, Object> spark = (Map<String, Object>) env.get("spark");
        assertEquals("client", String.valueOf(spark.get("deploy_mode")));
        assertEquals("10", String.valueOf(spark.get("executor_instances")));

        LoadedConfig typed = ConfigLoader.load(conf.toString(), override.toString());
        assertEquals("client", typed.environment.environment.spark.deployMode);
        assertEquals(Integer.valueOf(10), typed.environment.environment.spark.executorInstances);
        assertEquals("t", typed.benchmark.benchmark.id);
        assertEquals(100L, typed.dataset.scales.get("S").targetLogicalGib);
    }
}
