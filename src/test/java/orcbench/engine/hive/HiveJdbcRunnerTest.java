package orcbench.engine.hive;

import orcbench.config.ConfigLoader;
import orcbench.model.Models.*;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class HiveJdbcRunnerTest {

    @Test
    void dryRunReturnsDryRunStatus() throws Exception {
        LoadedConfig cfg = ConfigLoader.load("conf", null);
        ExperimentInstance inst = new ExperimentInstance();
        inst.experimentId = "HIVE_TEZ_BASE";
        inst.variantLabel = "v";
        inst.engine = "hive_tez";
        inst.layoutPath = "/benchmarks/audit_orc/S/RAW_ARRAY_BASE";
        inst.representation = "RAW_ARRAY";

        List<RunResult> results = HiveJdbcRunner.runExperiment(
                cfg, inst, new QuerySampleManifest(), "run", null, true);
        assertEquals(1, results.size());
        assertEquals("dry_run", results.get(0).status);
    }

    @Test
    void skippedExperimentReturnsSkipped() throws Exception {
        LoadedConfig cfg = ConfigLoader.load("conf", null);
        ExperimentInstance inst = new ExperimentInstance();
        inst.experimentId = "HIVE_LLAP";
        inst.variantLabel = "cold";
        inst.engine = "hive_llap";
        inst.skipReason = "missing llap";
        List<RunResult> results = HiveJdbcRunner.runExperiment(
                cfg, inst, new QuerySampleManifest(), "run", null, false);
        assertEquals("skipped", results.get(0).status);
        assertEquals("missing llap", results.get(0).errorMessage);
    }

    @Test
    void connectFailsWhenJdbcUrlNull() throws Exception {
        LoadedConfig cfg = ConfigLoader.load("conf", null);
        cfg.environment.environment.hive.connectionJdbcUrl = null;
        cfg.environment.environment.hive.jdbcUrl = null;
        assertThrows(IllegalStateException.class, () -> HiveJdbcRunner.connect(cfg));
    }
}
