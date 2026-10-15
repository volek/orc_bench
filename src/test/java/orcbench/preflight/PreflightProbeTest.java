package orcbench.preflight;

import orcbench.config.ConfigLoader;
import orcbench.model.Models.*;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PreflightProbeTest {

    @Test
    void dryRunProducesCapabilityReport() throws Exception {
        LoadedConfig cfg = ConfigLoader.load("conf", null);
        CapabilityReport report = PreflightProbe.run(cfg, "S", true);
        assertTrue(report.capabilities.containsKey("hdfs_writable"));
        assertTrue(report.capabilities.containsKey("hive_jdbc"));
        assertTrue(report.capabilities.get("nested_columns"));
        assertFalse(report.notes.isEmpty());
        PreflightProbe.printReport(report);
    }

    @Test
    void applySkipMarksMissingCapabilities() throws Exception {
        LoadedConfig cfg = ConfigLoader.load("conf", null);
        CapabilityReport report = PreflightProbe.run(cfg, "S", true);
        report.capabilities.put("llap_daemons_running", false);

        ExperimentInstance inst = new ExperimentInstance();
        inst.experimentId = "HIVE_LLAP";
        inst.variantLabel = "cold";
        inst.requires = new ArrayList<>(Arrays.asList("llap_daemons_running"));

        List<ExperimentInstance> updated = PreflightProbe.applySkip(
                Collections.singletonList(inst), report, true);
        assertNotNull(updated.get(0).skipReason);
        assertTrue(updated.get(0).skipReason.contains("llap_daemons_running"));
        assertFalse(report.skippedExperiments.isEmpty());
    }

    @Test
    void applySkipDisabledLeavesRequiresUntouched() {
        CapabilityReport report = new CapabilityReport();
        ExperimentInstance inst = new ExperimentInstance();
        inst.experimentId = "X";
        inst.requires = Arrays.asList("missing_cap");
        List<ExperimentInstance> updated = PreflightProbe.applySkip(
                Collections.singletonList(inst), report, false);
        assertNull(updated.get(0).skipReason);
    }

    @Test
    void existingSkipReasonIsPreserved() {
        CapabilityReport report = new CapabilityReport();
        ExperimentInstance inst = new ExperimentInstance();
        inst.experimentId = "Y";
        inst.skipReason = "already";
        List<ExperimentInstance> updated = PreflightProbe.applySkip(
                Collections.singletonList(inst), report, true);
        assertEquals("already", updated.get(0).skipReason);
        assertEquals(1, report.skippedExperiments.size());
    }
}
