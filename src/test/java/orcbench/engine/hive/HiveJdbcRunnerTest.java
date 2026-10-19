package orcbench.engine.hive;

import orcbench.config.ConfigLoader;
import orcbench.layout.ExperimentExpander;
import orcbench.model.Models.ExperimentInstance;
import orcbench.model.Models.ExperimentPlan;
import orcbench.model.Models.LoadedConfig;
import orcbench.model.Models.OrcWriteSettings;
import orcbench.model.Models.QuerySampleManifest;
import orcbench.model.Models.RunPhaseResult;
import orcbench.model.Models.RunResult;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class HiveJdbcRunnerTest {

    private static LoadedConfig cfg;

    @BeforeAll
    static void setUp() throws Exception {
        cfg = ConfigLoader.load("conf", null);
    }

    @Test
    void dryRunPhaseProducesNotesWithoutJdbc() throws Exception {
        ExperimentPlan plan = ExperimentExpander.buildPlan(
                cfg, "S", Arrays.asList("hive_tez", "hive_llap"),
                Arrays.asList("HIVE_TEZ_BASE", "HIVE_VECTORIZATION", "HIVE_MATERIALIZED_VIEW"));
        RunPhaseResult result = HiveJdbcRunner.run(
                cfg, plan, stubSamples(), "S", "dry-hive", null, true, false, null);
        assertTrue(result.dryRun);
        assertEquals(0, result.attempts);
        assertFalse(result.notes.isEmpty());
        assertTrue(result.experimentsPlanned > 0);
        assertTrue(result.experimentsSkipped >= 1, "MV expected skip");
        boolean sawMv = false;
        for (String n : result.notes) {
            if (n.contains("HIVE_MATERIALIZED_VIEW") && n.contains("skip")) {
                sawMv = true;
            }
        }
        assertTrue(sawMv);
        boolean sawZstdNote = false;
        for (String n : result.notes) {
            if (n.contains("ZSTD")) {
                sawZstdNote = true;
            }
        }
        assertTrue(sawZstdNote);
    }

    @Test
    void skippedExperimentReturnsSkippedStatus() throws Exception {
        ExperimentInstance inst = new ExperimentInstance();
        inst.experimentId = "HIVE_LLAP";
        inst.variantLabel = "cold";
        inst.engine = "hive_llap";
        inst.llap = true;
        inst.skipReason = "missing llap";
        inst.layoutPath = "/orc_test/S/RAW_ARRAY_BASE";
        List<RunResult> results = HiveJdbcRunner.runExperiment(
                new FakeHiveJdbcBackend(), cfg, inst, new QuerySampleManifest(), "run", null);
        assertEquals(1, results.size());
        assertEquals("skipped", results.get(0).status);
        assertEquals("missing llap", results.get(0).errorMessage);
    }

    @Test
    void materializedViewIsAlwaysSkippedLocally() throws Exception {
        ExperimentInstance inst = new ExperimentInstance();
        inst.experimentId = "HIVE_MATERIALIZED_VIEW";
        inst.variantLabel = "v";
        inst.engine = "hive_tez";
        inst.layoutPath = "/orc_test/S/RAW_ARRAY_BASE";
        inst.requires = Collections.singletonList("materialized_views_supported");
        List<RunResult> results = HiveJdbcRunner.runExperiment(
                new FakeHiveJdbcBackend(), cfg, inst, stubSamples(), "run", null);
        assertEquals("skipped", results.get(0).status);
        assertTrue(results.get(0).errorMessage.contains("materialized_views"));
    }

    @Test
    void hiveZstdLayoutIsSkipped() {
        ExperimentInstance inst = new ExperimentInstance();
        inst.experimentId = "COMPRESSION";
        inst.variantLabel = "ZSTD";
        inst.engine = "hive_tez";
        inst.orcSettings = new OrcWriteSettings();
        inst.orcSettings.compression = "ZSTD";
        assertTrue(HiveJdbcRunner.isHiveZstdExcluded(inst));
        HiveJdbcRunner.applyLocalSkipRules(inst);
        assertNotNull(inst.skipReason);
        assertTrue(inst.skipReason.contains("ZSTD"));
    }

    @Test
    void fakeBackendAppliesTezSessionOverridesAndProducesHashes() throws Exception {
        ExperimentPlan plan = ExperimentExpander.buildPlan(
                cfg, "S", Collections.singletonList("hive_tez"),
                Arrays.asList("HIVE_VECTORIZATION", "HIVE_TEZ_BASE"));
        FakeHiveJdbcBackend fake = new FakeHiveJdbcBackend();
        ExperimentInstance vec = null;
        for (ExperimentInstance i : plan.engine) {
            if ("HIVE_VECTORIZATION".equals(i.experimentId)
                    && i.variantLabel != null
                    && i.variantLabel.contains("true")) {
                vec = i;
                break;
            }
        }
        assertNotNull(vec, "HIVE_VECTORIZATION=true cell expected after property expansion");

        List<RunResult> results = HiveJdbcRunner.runExperiment(
                fake, cfg, vec, stubSamples(), "run1", null);
        assertFalse(results.isEmpty());
        assertTrue(fake.hasOverride("hive.vectorized.execution.enabled"));
        assertEquals("true", fake.overrideValue("hive.vectorized.execution.enabled"));
        assertTrue(fake.executeCalls > 0);
        assertFalse(fake.executedDdl.isEmpty());

        boolean anyOk = false;
        for (RunResult r : results) {
            if ("ok".equals(r.status)) {
                anyOk = true;
                assertNotNull(r.resultHash);
                assertEquals(16, r.resultHash.length());
                assertEquals("hive_tez", r.engine);
            }
        }
        assertTrue(anyOk);
    }

    @Test
    void llapWarmUsesFakeBackendObservationally() throws Exception {
        ExperimentPlan plan = ExperimentExpander.buildPlan(
                cfg, "S", Collections.singletonList("hive_llap"),
                Collections.singletonList("HIVE_LLAP"));
        FakeHiveJdbcBackend fake = new FakeHiveJdbcBackend();
        ExperimentInstance warm = null;
        for (ExperimentInstance i : plan.engine) {
            if ("HIVE_LLAP".equals(i.experimentId) && "warm".equals(i.cacheState)) {
                warm = i;
                break;
            }
        }
        assertNotNull(warm);
        assertTrue(warm.llap);
        assertEquals("only", warm.sessionOverrides.get("hive.llap.execution.mode"));

        List<RunResult> results = HiveJdbcRunner.runExperiment(
                fake, cfg, warm, stubSamples(), "llap1", null);
        assertFalse(results.isEmpty());
        assertTrue(fake.hasOverride("hive.llap.execution.mode"));
        assertTrue(fake.executeCalls > 0);
    }

    @Test
    void section1LeversCoveredInExpandedPlan() {
        ExperimentPlan plan = ExperimentExpander.buildPlan(
                cfg, "S", Arrays.asList("hive_tez", "hive_llap"),
                Collections.<String>emptyList());
        Map<String, Boolean> coverage = HiveJdbcRunner.section1Coverage(plan);
        assertTrue(coverage.get("hive.vectorized.execution.enabled"), "vectorization");
        assertTrue(coverage.get("hive.cbo.enable"), "CBO");
        assertTrue(coverage.get("hive.llap.execution.mode"), "LLAP mode");
    }

    @Test
    void mockRunWritesMetricsJsonl(@TempDir Path tmp) throws Exception {
        ExperimentPlan plan = ExperimentExpander.buildPlan(
                cfg, "S", Collections.singletonList("hive_tez"),
                Collections.singletonList("HIVE_TEZ_BASE"));
        FakeHiveJdbcBackend fake = new FakeHiveJdbcBackend();
        RunPhaseResult result = HiveJdbcRunner.run(
                cfg, plan, stubSamples(), "S", "mock-hive-run", tmp.toString(),
                false, true, fake);
        assertTrue(result.attempts > 0);
        assertNotNull(result.metricsJsonlPath);
        assertTrue(Files.exists(tmp.resolve("mock-hive-run").resolve("metrics.jsonl")));
        assertTrue(fake.executeCalls > 0);
        assertEquals(0, fake.closeCalls); // injected backend not closed by runner
    }

    @Test
    void selectHiveCellsExcludesSpark() {
        ExperimentPlan plan = ExperimentExpander.buildPlan(
                cfg, "S", Arrays.asList("spark", "hive_tez", "hive_llap"),
                Arrays.asList("SPARK_AQE", "HIVE_VECTORIZATION", "HIVE_TEZ_BASE"));
        List<ExperimentInstance> hive = HiveJdbcRunner.selectHiveCells(plan);
        assertFalse(hive.isEmpty());
        for (ExperimentInstance i : hive) {
            assertTrue(HiveJdbcRunner.isHiveEngine(i));
            assertFalse(i.experimentId.startsWith("SPARK_"));
        }
    }

    @Test
    void hashRowsMatchesSparkEquivalence() {
        String[] rows = new String[]{"a|1", "b|\\N"};
        assertEquals(
                orcbench.engine.spark.SparkQueryRunner.hashRows(rows),
                HiveJdbcRunner.hashRows(rows));
    }

    @Test
    void resolveJdbcUrlNullFailsConnectHelper() throws Exception {
        LoadedConfig local = ConfigLoader.load("conf", null);
        local.environment.environment.hive.connectionJdbcUrl = null;
        local.environment.environment.hive.jdbcUrl = null;
        assertNull(LiveHiveJdbcBackend.resolveJdbcUrl(local));
        assertThrows(IllegalStateException.class, () -> LiveHiveJdbcBackend.openConnection(local));
    }

    private static QuerySampleManifest stubSamples() {
        QuerySampleManifest m = new QuerySampleManifest();
        m.samples.put("userLogin", Collections.singletonMap("present",
                Arrays.asList("userLogin0", "userLogin1", "userLogin2")));
        m.samples.put("session", Collections.singletonMap("present",
                Collections.singletonList("copy2_0")));
        m.samples.put("SESSION_ID", Collections.singletonMap("present",
                Collections.singletonList("copy2_0")));
        m.samples.put("EPK_ID", Collections.singletonMap("present",
                Collections.singletonList("epk_0")));
        m.samples.put("IP_ADDRESS", Collections.singletonMap("present",
                Collections.singletonList("203.0.113.1")));
        m.samples.put("DeviceSystemName", Collections.singletonMap("present",
                Collections.singletonList("iOS")));
        return m;
    }
}
