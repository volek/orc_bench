package orcbench.preflight;

import orcbench.config.ConfigLoader;
import orcbench.layout.ExperimentExpander;
import orcbench.model.Models.CapabilityReport;
import orcbench.model.Models.ExperimentInstance;
import orcbench.model.Models.ExperimentPlan;
import orcbench.model.Models.LoadedConfig;
import orcbench.preflight.PreflightBackend.HdfsProbeResult;
import orcbench.preflight.PreflightBackend.JdbcProbeResult;
import orcbench.preflight.PreflightBackend.KerberosProbeResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Checkpoint 3 unit tests use fakes — no live HDFS / YARN / Hive.
 */
class PreflightProbeTest {

    @Test
    void dryRunConfigDerivedCapabilities() throws Exception {
        LoadedConfig cfg = ConfigLoader.load("conf", null);
        CapabilityReport report = PreflightProbe.run(cfg, "S", true, new RecordingBackend());
        assertTrue(report.capabilities.containsKey("hdfs_writable"));
        assertTrue(report.capabilities.containsKey("hive_jdbc"));
        assertTrue(report.capabilities.containsKey("kerberos_ticket"));
        assertTrue(Boolean.TRUE.equals(report.capabilities.get("nested_columns")));
        assertFalse(Boolean.TRUE.equals(report.capabilities.get("materialized_views_supported")));
        assertFalse(Boolean.TRUE.equals(report.capabilities.get("centralized_cache_supported")));
        assertFalse(Boolean.TRUE.equals(report.capabilities.get("llap_daemons_running")));
        assertTrue(report.notes.stream().anyMatch(n -> n.contains("dry-run")));
        assertTrue(report.notes.stream().anyMatch(n -> n.contains("HIVE_MATERIALIZED_VIEW")));
        assertTrue(report.notes.stream().anyMatch(n -> n.contains("HDFS_CACHE.centralized")));
        PreflightProbe.printReport(report);
    }

    @Test
    void liveModeUsesInjectedFakeBackend() throws Exception {
        LoadedConfig cfg = ConfigLoader.load("conf", null);
        RecordingBackend fake = new RecordingBackend();
        fake.hdfs = new HdfsProbeResult(true, 20_000L, "fake hdfs ok");
        fake.jdbc = new JdbcProbeResult(true, "fake jdbc ok");
        fake.kerberos = new KerberosProbeResult(true, "fake kerberos ok");

        CapabilityReport report = PreflightProbe.run(cfg, "S", false, fake);
        assertEquals(1, fake.hdfsCalls);
        assertEquals(1, fake.jdbcCalls);
        assertEquals(1, fake.kerberosCalls);
        assertTrue(Boolean.TRUE.equals(report.capabilities.get("hdfs_writable")));
        assertTrue(Boolean.TRUE.equals(report.capabilities.get("hive_jdbc")));
        assertTrue(Boolean.TRUE.equals(report.capabilities.get("kerberos_ticket")));
        assertEquals("20000", report.versions.get("hdfs.free_gib_observed"));
    }

    @Test
    void liveHdfsFailureMarksNotWritable() throws Exception {
        LoadedConfig cfg = ConfigLoader.load("conf", null);
        RecordingBackend fake = new RecordingBackend();
        fake.hdfs = new HdfsProbeResult(false, -1L, "boom");
        fake.jdbc = new JdbcProbeResult(false, "no jdbc");
        fake.kerberos = new KerberosProbeResult(false, "no ticket");

        CapabilityReport report = PreflightProbe.run(cfg, "S", false, fake);
        assertFalse(Boolean.TRUE.equals(report.capabilities.get("hdfs_writable")));
        // jdbc falls back to jdbc_connection_verified from conf when live fails
        assertTrue(Boolean.TRUE.equals(report.capabilities.get("hive_jdbc")));
        // kerberos falls back to kinit_verified from conf
        assertTrue(Boolean.TRUE.equals(report.capabilities.get("kerberos_ticket")));
    }

    @Test
    void skipMatrixMarksExpectedSkips() throws Exception {
        LoadedConfig cfg = ConfigLoader.load("conf", null);
        CapabilityReport report = PreflightProbe.run(cfg, "S", true, new RecordingBackend());
        ExperimentPlan plan = ExperimentExpander.buildPlan(
                cfg, "S", Arrays.asList("spark", "hive_tez", "hive_llap"), Collections.<String>emptyList());
        PreflightProbe.applySkipToPlan(plan, report, true);

        assertTrue(hasSkip(report, "HIVE_MATERIALIZED_VIEW"));
        assertTrue(hasSkip(report, "HDFS_CACHE") && report.skippedExperiments.stream()
                .anyMatch(r -> r[0].contains("centralized")));
        assertTrue(hasSkip(report, "HIVE_LLAP"));
    }

    @Test
    void applySkipMarksMissingCapabilities() {
        CapabilityReport report = new CapabilityReport();
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

    @Test
    void enforceFailPolicyThrowsWhenConfigured() {
        CapabilityReport report = new CapabilityReport();
        report.skippedExperiments.add(new String[]{"A", "missing"});
        orcbench.model.Models.PreflightEnv pf = new orcbench.model.Models.PreflightEnv();
        pf.failIfRequiredCapabilityMissing = true;
        assertThrows(IllegalStateException.class, () -> PreflightProbe.enforceFailPolicy(report, pf));
    }

    @Test
    void reportWriterCreatesArtefacts(@TempDir Path tmp) throws Exception {
        CapabilityReport report = new CapabilityReport();
        report.capabilities.put("hive_jdbc", true);
        report.notes.add("n1");
        report.skippedExperiments.add(new String[]{"HIVE_MATERIALIZED_VIEW/n/a", "missing capabilities: materialized_views_supported"});
        report.versions.put("java.runtime", "1.8.0_test");

        String dir = PreflightReportWriter.write(tmp.toString(), "run1", report, "S");
        assertTrue(Files.exists(Paths.get(dir, "SUMMARY.txt")));
        assertTrue(Files.exists(Paths.get(dir, "capabilities.tsv")));
        assertTrue(Files.exists(Paths.get(dir, "skipped.tsv")));
        assertTrue(Files.exists(Paths.get(dir, "preflight-notes.txt")));
        String skipped = new String(Files.readAllBytes(Paths.get(dir, "skipped.tsv")), "UTF-8");
        assertTrue(skipped.contains("HIVE_MATERIALIZED_VIEW"));
    }

    private static boolean hasSkip(CapabilityReport report, String experimentId) {
        for (String[] row : report.skippedExperiments) {
            if (row[0] != null && row[0].startsWith(experimentId)) {
                return true;
            }
        }
        return false;
    }

    /** Fake backend — never touches HDFS/Hive. */
    static final class RecordingBackend implements PreflightBackend {
        int hdfsCalls;
        int jdbcCalls;
        int kerberosCalls;
        HdfsProbeResult hdfs = new HdfsProbeResult(true, 1000L, "unused");
        JdbcProbeResult jdbc = new JdbcProbeResult(true, "unused");
        KerberosProbeResult kerberos = new KerberosProbeResult(true, "unused");

        @Override
        public String javaRuntimeVersion() {
            return "1.8.0_292";
        }

        @Override
        public HdfsProbeResult probeHdfs(String namenodeUri, String workDir) {
            hdfsCalls++;
            return hdfs;
        }

        @Override
        public JdbcProbeResult probeHiveJdbc(String jdbcUrl) {
            jdbcCalls++;
            return jdbc;
        }

        @Override
        public KerberosProbeResult probeKerberos(String principal, String keytabPath) {
            kerberosCalls++;
            return kerberos;
        }
    }
}
