package orcbench.engine.spark;

import orcbench.config.ConfigLoader;
import orcbench.layout.ExperimentExpander;
import orcbench.model.Models.ExperimentInstance;
import orcbench.model.Models.ExperimentPlan;
import orcbench.model.Models.LoadedConfig;
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

class SparkQueryRunnerTest {

    private static LoadedConfig cfg;

    @BeforeAll
    static void setUp() throws Exception {
        cfg = ConfigLoader.load("conf", null);
    }

    @Test
    void hashRowsIsOrderIndependent() {
        String h1 = SparkQueryRunner.hashRows(new String[]{"a|1", "b|2"});
        String h2 = SparkQueryRunner.hashRows(new String[]{"b|2", "a|1"});
        assertEquals(h1, h2);
        assertEquals(16, h1.length());
    }

    @Test
    void skippedExperimentReturnsSkippedStatus() throws Exception {
        ExperimentInstance inst = new ExperimentInstance();
        inst.experimentId = "X";
        inst.variantLabel = "v";
        inst.skipReason = "nope";
        inst.layoutPath = "/orc_test/S/RAW_ARRAY_BASE";
        List<RunResult> results = SparkQueryRunner.runExperiment(
                new FakeSparkQueryBackend(), cfg, inst, new QuerySampleManifest(), "run", null);
        assertEquals(1, results.size());
        assertEquals("skipped", results.get(0).status);
        assertEquals("nope", results.get(0).errorMessage);
    }

    @Test
    void fakeBackendAppliesSessionOverridesAndProducesHashes(@TempDir Path tmp) throws Exception {
        ExperimentPlan plan = ExperimentExpander.buildPlan(
                cfg, "S", Collections.singletonList("spark"),
                Arrays.asList("SPARK_VECTORIZATION", "SPARK_AQE", "SPARK_ORC_READER"));
        FakeSparkQueryBackend fake = new FakeSparkQueryBackend();
        ExperimentInstance vec = null;
        for (ExperimentInstance i : plan.engine) {
            if ("SPARK_VECTORIZATION".equals(i.experimentId)
                    && i.variantLabel != null
                    && i.variantLabel.contains("true")) {
                vec = i;
                break;
            }
        }
        assertNotNull(vec);

        List<RunResult> results = SparkQueryRunner.runExperiment(
                fake, cfg, vec, stubSamples(), "run1", null);
        assertFalse(results.isEmpty());
        assertTrue(fake.hasOverride("spark.sql.orc.enableVectorizedReader"));
        assertEquals("true", fake.overrideValue("spark.sql.orc.enableVectorizedReader"));
        assertTrue(fake.executeCalls > 0);

        boolean anyOk = false;
        for (RunResult r : results) {
            if ("ok".equals(r.status)) {
                anyOk = true;
                assertNotNull(r.resultHash);
                assertEquals(16, r.resultHash.length());
                assertNotNull(r.orcFileBytes);
            }
        }
        assertTrue(anyOk);
    }

    @Test
    void section1LeversCoveredInExpandedPlan() {
        ExperimentPlan plan = ExperimentExpander.buildPlan(
                cfg, "S", Collections.singletonList("spark"), Collections.<String>emptyList());
        // Include physical ORC_PUSHDOWN for filterPushdown lever
        Map<String, Boolean> coverage = SparkQueryRunner.section1Coverage(plan);
        assertTrue(coverage.get("spark.sql.orc.enableVectorizedReader"), "vectorization");
        assertTrue(coverage.get("spark.sql.adaptive.enabled"), "AQE");
        assertTrue(coverage.get("spark.sql.optimizer.dynamicPartitionPruning.enabled"), "DPP stays on the Spark baseline");
        assertFalse(coverage.get("spark.sql.cbo.enabled"), "CBO experiment is outside the SM_UKO mix");
        assertFalse(coverage.get("spark.sql.autoBroadcastJoinThreshold"), "broadcast experiment is outside the SM_UKO mix");
        assertTrue(coverage.get("spark.sql.orc.impl"), "native/hive ORC");
        assertTrue(coverage.get("spark.sql.orc.enableNestedColumnVectorizedReader"), "nested vectorization");
        assertTrue(coverage.get("spark.sql.orc.filterPushdown"), "pushdown (physical)");
    }

    @Test
    void dryRunPhaseProducesNotesWithoutExecuting() throws Exception {
        ExperimentPlan plan = ExperimentExpander.buildPlan(
                cfg, "S", Collections.singletonList("spark"),
                Arrays.asList("SPARK_VECTORIZATION", "ORC_COLUMN_PRUNING"));
        RunPhaseResult result = SparkQueryRunner.run(
                cfg, plan, stubSamples(), "S", "dry-run-id", null, true, false, null);
        assertTrue(result.dryRun);
        assertEquals(0, result.attempts);
        assertFalse(result.notes.isEmpty());
        assertTrue(result.experimentsPlanned > 0);
    }

    @Test
    void mockRunWritesMetricsJsonl(@TempDir Path tmp) throws Exception {
        ExperimentPlan plan = ExperimentExpander.buildPlan(
                cfg, "S", Collections.singletonList("spark"),
                Collections.singletonList("SPARK_BASE"));
        FakeSparkQueryBackend fake = new FakeSparkQueryBackend();
        RunPhaseResult result = SparkQueryRunner.run(
                cfg, plan, stubSamples(), "S", "mock-run-id", tmp.toString(),
                false, true, fake);
        assertTrue(result.mocked || result.experimentsRun >= 1);
        assertTrue(result.attempts > 0);
        assertNotNull(result.metricsJsonlPath);
        assertTrue(Files.exists(tmp.resolve("mock-run-id").resolve("metrics.jsonl")));
        assertTrue(fake.executeCalls > 0);
        assertTrue(fake.closeCalls == 0); // backend injected → runner must not close
    }

    @Test
    void selectSparkCellsExcludesHive() {
        ExperimentPlan plan = ExperimentExpander.buildPlan(
                cfg, "S", Arrays.asList("spark", "hive_tez", "hive_llap"),
                Arrays.asList("SPARK_AQE", "HIVE_VECTORIZATION", "HIVE_TEZ_BASE"));
        List<ExperimentInstance> spark = SparkQueryRunner.selectSparkCells(plan);
        for (ExperimentInstance i : spark) {
            assertEquals("spark", i.engine);
            assertFalse(i.experimentId.startsWith("HIVE_"));
        }
        assertFalse(spark.isEmpty());
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
