package orcbench.preflight;

import orcbench.model.Models.CapabilityReport;
import orcbench.model.Models.EnvironmentSection;
import orcbench.model.Models.ExperimentInstance;
import orcbench.model.Models.ExperimentPlan;
import orcbench.model.Models.LoadedConfig;
import orcbench.model.Models.PreflightEnv;
import orcbench.model.Models.ScaleSpec;
import orcbench.preflight.PreflightBackend.HdfsProbeResult;
import orcbench.preflight.PreflightBackend.JdbcProbeResult;
import orcbench.preflight.PreflightBackend.KerberosProbeResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Checkpoint 3: version / HDFS / Kerberos / Hive JDBC / capability probes and
 * skip-matrix application. Live probes are for Hadoop cluster nodes only;
 * local acceptance uses injected fakes and {@code dryRun} config-derived mode.
 */
public final class PreflightProbe {
    private PreflightProbe() {}

    public static CapabilityReport run(LoadedConfig cfg, String scale, boolean dryRun) {
        return run(cfg, scale, dryRun, new LiveClusterBackend());
    }

    public static CapabilityReport run(
            LoadedConfig cfg, String scale, boolean dryRun, PreflightBackend backend) {
        if (cfg == null) {
            throw new IllegalArgumentException("cfg is required");
        }
        if (backend == null) {
            throw new IllegalArgumentException("backend is required");
        }
        EnvironmentSection env = cfg.environment.environment;
        PreflightEnv pf = env.preflight;
        CapabilityReport report = new CapabilityReport();
        String scaleKey = scale == null ? "S" : scale.toUpperCase(Locale.ROOT);

        String javaVer = backend.javaRuntimeVersion();
        report.versions.put("java.runtime", javaVer);
        if (pf.requireJava8RuntimeCheck) {
            boolean ok = isJava8(javaVer);
            report.capabilities.put("java8", ok);
            if (!ok) {
                report.notes.add("Java runtime is " + javaVer + "; cluster expects 1.8");
            }
        }

        for (Map.Entry<String, Optional<String>> e : env.versions.entrySet()) {
            if (e.getValue() != null && e.getValue().isPresent()) {
                report.versions.put(e.getKey(), e.getValue().get());
            } else {
                report.notes.add("version." + e.getKey() + " is null (fill via --env-override)");
            }
        }

        if (pf.requireExactVersions) {
            checkExactVersion(report, "java", env.versions.get("java"), javaVer);
        }

        // Config-derived feature flags (always available; live probes may override connectivity).
        putCapability(report, "hdfs_short_circuit_supported",
                Boolean.TRUE.equals(env.hdfs.shortCircuitSupported));
        putCapability(report, "client_datanode_colocation",
                Boolean.TRUE.equals(env.hdfs.dataLocalityVerifiable));
        putCapability(report, "centralized_cache_supported",
                Boolean.TRUE.equals(env.hdfs.centralizedCacheSupported));
        putCapability(report, "derived_event_date_partition", true);
        putCapability(report, "partition_column_event_date", true);
        putCapability(report, "date_partitioned_table", true);
        putCapability(report, "partition_dimension_join", true);
        putCapability(report, "native_orc_reader", true);
        putCapability(report, "nested_columns", true);
        putCapability(report, "nested_sql_dialect_variant", true);
        putCapability(report, "hive_session_allows_mode_override", true);

        Optional<String> tez = env.versions.get("tez");
        putCapability(report, "tez_version_verified",
                (tez != null && tez.isPresent()) || env.hive.tezAvailable);
        putCapability(report, "llap_daemons_available",
                Boolean.TRUE.equals(env.hive.llapAvailable) || env.hive.llapConfigured);
        boolean llapRunning = Boolean.TRUE.equals(env.hive.llapDaemonsRunningVerified);
        putCapability(report, "llap_daemons_running", llapRunning);
        putCapability(report, "llap_task_execution_verified", llapRunning);
        putCapability(report, "materialized_views_supported",
                Boolean.TRUE.equals(env.hive.materializedViewsSupported));

        if (!env.operations.permissionToChangeClusterConfig) {
            report.notes.add("cluster config changes forbidden; SHORT_CIRCUIT / HDFS_CACHE.centralized may skip");
        }
        if (!env.operations.permissionToResetCaches) {
            report.notes.add("cache reset forbidden; warm/cold relies on natural state");
        }
        if (!env.hdfs.shortCircuitMutableByRunner) {
            report.notes.add("SHORT_CIRCUIT is observational (mutable_by_runner=false)");
        }
        if (!Boolean.TRUE.equals(env.hive.materializedViewsSupported)) {
            report.notes.add("expected skip: HIVE_MATERIALIZED_VIEW (materialized_views_supported=false)");
        }
        if (!Boolean.TRUE.equals(env.hdfs.centralizedCacheSupported)) {
            report.notes.add("expected skip: HDFS_CACHE.centralized (centralized_cache.supported not true)");
        }
        if (!llapRunning) {
            report.notes.add("LLAP running not verified (llap_daemons_running_verified); HIVE_LLAP* may skip until cluster probe");
        }

        String jdbcUrl = env.hive.connectionJdbcUrl != null
                ? env.hive.connectionJdbcUrl : env.hive.jdbcUrl;

        if (dryRun) {
            report.notes.add("dry-run: live HDFS/JDBC/Kerberos probes skipped (config-derived capabilities only)");
            boolean hdfsOk = Boolean.TRUE.equals(env.hdfs.writeVerified)
                    || (env.hdfs.freeSpaceGib != null && env.hdfs.freeSpaceGib > 0L);
            if (pf.requireHdfsCapacityCheck) {
                long target = scaleTargetGib(cfg, scaleKey);
                long free = env.hdfs.freeSpaceGib == null ? 0L : env.hdfs.freeSpaceGib;
                report.versions.put("hdfs.free_gib_configured", Long.toString(free));
                if (target > 0L && free < target) {
                    report.notes.add("HDFS free " + free + " GiB < scale " + scaleKey + " target " + target + " GiB");
                    hdfsOk = false;
                }
            }
            putCapability(report, "hdfs_writable", hdfsOk);
            putCapability(report, "hive_jdbc",
                    env.hive.jdbcConnectionVerified || jdbcUrl != null);
            boolean kerberosOk = !env.security.enabled
                    || Boolean.TRUE.equals(env.security.kinitVerified);
            putCapability(report, "kerberos_ticket", kerberosOk);
            if (jdbcUrl != null) {
                report.notes.add("dry-run: would probe JDBC " + jdbcUrl);
            }
        } else {
            report.notes.add("live probes: cluster-node execution");
            HdfsProbeResult hdfs = backend.probeHdfs(env.hdfs.namenodeUri, env.hdfs.workDir);
            if (hdfs.freeSpaceGib >= 0L) {
                report.versions.put("hdfs.free_gib_observed", Long.toString(hdfs.freeSpaceGib));
            }
            if (hdfs.detail != null) {
                report.notes.add(hdfs.detail);
            }
            boolean hdfsOk = hdfs.writable;
            if (pf.requireHdfsCapacityCheck && hdfs.freeSpaceGib >= 0L) {
                long target = scaleTargetGib(cfg, scaleKey);
                if (target > 0L && hdfs.freeSpaceGib < target) {
                    report.notes.add("HDFS free " + hdfs.freeSpaceGib + " GiB < scale "
                            + scaleKey + " target " + target + " GiB");
                    hdfsOk = false;
                }
            }
            putCapability(report, "hdfs_writable", hdfsOk);

            JdbcProbeResult jdbc = backend.probeHiveJdbc(jdbcUrl);
            if (jdbc.detail != null) {
                report.notes.add(jdbc.detail);
            }
            putCapability(report, "hive_jdbc", jdbc.ok || env.hive.jdbcConnectionVerified);

            if (env.security.enabled) {
                KerberosProbeResult kb = backend.probeKerberos(
                        env.security.runnerPrincipal, env.security.keytabPath);
                if (kb.detail != null) {
                    report.notes.add(kb.detail);
                }
                putCapability(report, "kerberos_ticket",
                        kb.ok || Boolean.TRUE.equals(env.security.kinitVerified));
            } else {
                putCapability(report, "kerberos_ticket", true);
                report.notes.add("security.enabled=false; Kerberos probe skipped");
            }
        }

        if (pf.requireFeatureProbe) {
            report.notes.add("feature probe required: capability matrix populated from env + live/config probes");
        }
        return report;
    }

    public static List<ExperimentInstance> applySkip(
            List<ExperimentInstance> instances, CapabilityReport report, boolean markSkipped) {
        if (instances == null) {
            return new ArrayList<>();
        }
        if (!markSkipped) {
            return new ArrayList<>(instances);
        }
        List<ExperimentInstance> updated = new ArrayList<>();
        for (ExperimentInstance inst : instances) {
            if (inst.skipReason != null) {
                report.skippedExperiments.add(new String[]{
                        cellKey(inst), inst.skipReason});
                updated.add(inst);
                continue;
            }
            List<String> missing = new ArrayList<>();
            if (inst.requires != null) {
                for (String r : inst.requires) {
                    if (!Boolean.TRUE.equals(report.capabilities.get(r))) {
                        missing.add(r);
                    }
                }
            }
            if (!missing.isEmpty()) {
                String reason = "missing capabilities: " + join(missing);
                inst.skipReason = reason;
                report.skippedExperiments.add(new String[]{cellKey(inst), reason});
            }
            updated.add(inst);
        }
        return updated;
    }

    /** Apply skip matrix to all cells in the plan; records skips on {@code report}. */
    public static void applySkipToPlan(
            ExperimentPlan plan, CapabilityReport report, boolean markSkipped) {
        if (plan == null) {
            return;
        }
        plan.physical = applySkip(plan.physical, report, markSkipped);
        plan.engine = applySkip(plan.engine, report, markSkipped);
        plan.finalComparison = applySkip(plan.finalComparison, report, markSkipped);
    }

    /**
     * When {@code failIfRequiredCapabilityMissing} is set, throw if any skip was
     * recorded for a missing capability. Default configs keep this false.
     */
    public static void enforceFailPolicy(CapabilityReport report, PreflightEnv pf) {
        if (pf == null || !pf.failIfRequiredCapabilityMissing) {
            return;
        }
        if (report != null && !report.skippedExperiments.isEmpty()) {
            throw new IllegalStateException(
                    "required capability missing; skipped=" + report.skippedExperiments.size()
                            + " (fail_if_required_capability_missing=true)");
        }
    }

    public static void printReport(CapabilityReport report, Appendable out) throws IOException {
        out.append("=== Preflight ===\n");
        for (Map.Entry<String, String> e : new TreeMap<>(report.versions).entrySet()) {
            out.append("  version.").append(e.getKey()).append(" = ").append(e.getValue()).append('\n');
        }
        for (Map.Entry<String, Boolean> e : new TreeMap<>(report.capabilities).entrySet()) {
            out.append("  capability.").append(e.getKey()).append(" = ")
                    .append(String.valueOf(e.getValue())).append('\n');
        }
        for (String n : report.notes) {
            out.append("  note: ").append(n).append('\n');
        }
        if (!report.skippedExperiments.isEmpty()) {
            out.append("  skipped (").append(Integer.toString(report.skippedExperiments.size())).append("):\n");
            for (String[] s : report.skippedExperiments) {
                out.append("    - ").append(s[0]).append(": ").append(s[1]).append('\n');
            }
        }
    }

    public static void printReport(CapabilityReport report) {
        try {
            printReport(report, System.out);
        } catch (IOException ignored) {
            // System.out does not throw
        }
    }

    private static void putCapability(CapabilityReport report, String key, boolean value) {
        report.capabilities.put(key, value);
    }

    private static boolean isJava8(String javaVer) {
        if (javaVer == null) {
            return false;
        }
        return javaVer.startsWith("1.8") || javaVer.startsWith("8");
    }

    private static void checkExactVersion(
            CapabilityReport report, String key, Optional<String> expected, String observed) {
        if (expected == null || !expected.isPresent() || observed == null) {
            return;
        }
        String exp = expected.get();
        // Major.minor match for Java (1.8.x); prefix match for vendor builds.
        boolean ok;
        if ("java".equals(key)) {
            ok = isJava8(observed) && (exp.startsWith("1.8") || exp.startsWith("8"));
        } else {
            ok = observed.startsWith(exp) || exp.startsWith(observed) || observed.contains(exp);
        }
        report.capabilities.put("version_exact_" + key, ok);
        if (!ok) {
            report.notes.add("version mismatch " + key + ": expected~" + exp + " observed=" + observed);
        }
    }

    private static long scaleTargetGib(LoadedConfig cfg, String scaleKey) {
        if (cfg.dataset == null || cfg.dataset.scales == null) {
            return 0L;
        }
        ScaleSpec s = cfg.dataset.scales.get(scaleKey);
        return s == null ? 0L : s.targetLogicalGib;
    }

    private static String cellKey(ExperimentInstance inst) {
        String v = inst.variantLabel == null ? "-" : inst.variantLabel;
        return inst.experimentId + "/" + v;
    }

    private static String join(List<String> parts) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(parts.get(i));
        }
        return sb.toString();
    }
}
