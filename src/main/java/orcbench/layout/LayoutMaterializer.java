package orcbench.layout;

import orcbench.generate.SparkSessionSupport;
import orcbench.model.Models.ExperimentInstance;
import orcbench.model.Models.LayoutWriteRequest;
import orcbench.model.Models.LoadedConfig;
import orcbench.model.Models.MaterializeResult;
import orcbench.model.Models.OrcWriteSettings;
import orcbench.model.Models.RepresentationSpec;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Checkpoint 5: OVAT physical layout materialization from {@code RAW_ARRAY_BASE}.
 * Local acceptance: {@code --dry-run} / {@code --mock-materialize} (+ unit tests with
 * {@link FakeMaterializeBackend}). Live HDFS writes: cluster node only via
 * {@link SparkMaterializeBackend}.
 */
public final class LayoutMaterializer {

    static final Set<String> REWRITE_KINDS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            "rewrite", "hive_compatible_write")));

    private static final List<String> DEFAULT_FLAT_COLS = Collections.unmodifiableList(Arrays.asList(
            "epk_id", "session_id", "operation_uid", "channel_type",
            "vsp_code", "osb_code", "tb_code", "ip_address", "device_system_name"));

    private LayoutMaterializer() {}

    public static boolean isRewriteKind(String kind) {
        return kind != null && REWRITE_KINDS.contains(kind);
    }

    /**
     * True when the instance needs a physical rewrite distinct from the baseline path.
     */
    public static boolean needsMaterialize(ExperimentInstance i, String basePath) {
        if (i == null || i.skipReason != null) {
            return false;
        }
        if (i.layoutPath == null || i.layoutPath.isEmpty()) {
            return false;
        }
        if (!isRewriteKind(i.kind)) {
            return false;
        }
        return !i.layoutPath.equals(basePath);
    }

    /**
     * Idempotency marker under {@code {hdfs_root}/state/…}.
     * Resume never overwrites an existing marker (execution_policy).
     */
    public static String markerPath(String hdfsRoot, String scale, ExperimentInstance i) {
        String root = hdfsRoot == null ? "/orc_test" : hdfsRoot;
        String sc = scale == null ? "S" : scale.toUpperCase(Locale.ROOT);
        String exp = i == null || i.experimentId == null ? "unknown" : i.experimentId;
        String variant = i == null || i.variantLabel == null ? "v" : sanitize(i.variantLabel);
        return root + "/state/" + sc + "__" + exp + "__" + variant + ".done";
    }

    public static String sanitize(String label) {
        if (label == null || label.isEmpty()) {
            return "v";
        }
        return label.replaceAll("[^A-Za-z0-9._-]+", "_");
    }

    /**
     * Human-readable OVAT factor summary for dry-run / logs (unit-testable).
     */
    public static String describeRewrite(ExperimentInstance i) {
        if (i == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder();
        sb.append(i.experimentId).append('/').append(i.variantLabel);
        sb.append(" kind=").append(i.kind);
        sb.append(" rep=").append(i.representation);
        OrcWriteSettings orc = i.orcSettings;
        if (orc != null) {
            sb.append(" compression=").append(orc.compression);
            sb.append(" stripeMiB=").append(orc.stripeSizeMib);
            sb.append(" stride=").append(orc.rowIndexStride);
            sb.append(" bloom=").append(orc.bloomColumns);
            sb.append(" fpp=").append(orc.bloomFpp);
        }
        sb.append(" sort=").append(i.sorting);
        sb.append(" partitions=").append(i.partitionColumns);
        sb.append(" fileMiB=").append(i.targetFileSizeMib);
        if (i.bucketCount > 0) {
            sb.append(" buckets=").append(i.bucketCount).append('@').append(i.bucketKey);
        }
        return sb.toString();
    }

    /**
     * Decide which scale-directory children to delete under cleanup policy:
     * always keep baseline, refs, manifest, state; keep names matching retain
     * (experiment ids or layout directory names); delete other rewrite dirs.
     *
     * @return directory names (not full paths) selected for deletion
     */
    public static List<String> selectCleanupTargets(
            List<String> children,
            String baselineId,
            List<ExperimentInstance> instances,
            List<String> retain) {
        Set<String> retainSet = new LinkedHashSet<>();
        if (baselineId != null) {
            retainSet.add(baselineId);
        }
        retainSet.add("refs");
        retainSet.add("manifest");
        retainSet.add("state");
        if (retain != null) {
            for (String r : retain) {
                if (r != null && !r.trim().isEmpty()) {
                    retainSet.add(r.trim());
                }
            }
        }
        if (instances != null) {
            for (ExperimentInstance i : instances) {
                if (i == null || i.layoutPath == null) {
                    continue;
                }
                boolean keepExp = retain != null && retainContains(retain, i.experimentId);
                if (keepExp) {
                    retainSet.add(dirName(i.layoutPath));
                }
            }
        }

        List<String> delete = new ArrayList<>();
        if (children == null) {
            return delete;
        }
        for (String name : children) {
            if (name == null || name.isEmpty()) {
                continue;
            }
            if (retainSet.contains(name)) {
                continue;
            }
            // Keep if any retain token matches as prefix of layout dir (experiment id)
            boolean matched = false;
            for (String r : retainSet) {
                if (name.startsWith(r + "_") || name.equals(r)) {
                    matched = true;
                    break;
                }
            }
            if (!matched) {
                delete.add(name);
            }
        }
        return delete;
    }

    private static boolean retainContains(List<String> retain, String experimentId) {
        if (retain == null || experimentId == null) {
            return false;
        }
        for (String r : retain) {
            if (experimentId.equals(r)) {
                return true;
            }
        }
        return false;
    }

    static String dirName(String layoutPath) {
        if (layoutPath == null) {
            return "";
        }
        String p = layoutPath;
        while (p.endsWith("/")) {
            p = p.substring(0, p.length() - 1);
        }
        int slash = p.lastIndexOf('/');
        return slash < 0 ? p : p.substring(slash + 1);
    }

    public static List<String> flatColumns(LoadedConfig cfg) {
        if (cfg != null && cfg.benchmark != null && cfg.benchmark.representations != null) {
            RepresentationSpec flat = cfg.benchmark.representations.get("FLAT");
            if (flat != null && flat.columns != null && !flat.columns.isEmpty()) {
                return new ArrayList<>(flat.columns);
            }
            RepresentationSpec hybrid = cfg.benchmark.representations.get("HYBRID");
            if (hybrid != null && hybrid.columns != null && !hybrid.columns.isEmpty()) {
                return new ArrayList<>(hybrid.columns);
            }
        }
        return new ArrayList<>(DEFAULT_FLAT_COLS);
    }

    public static LayoutWriteRequest toRequest(
            ExperimentInstance i,
            String basePath,
            List<String> flatCols,
            long sourceBytes,
            String marker) {
        LayoutWriteRequest req = new LayoutWriteRequest();
        req.experimentId = i.experimentId;
        req.variantLabel = i.variantLabel;
        req.kind = i.kind;
        req.sourceBasePath = basePath;
        req.layoutPath = i.layoutPath;
        req.representation = i.representation;
        req.orcSettings = i.orcSettings == null ? new OrcWriteSettings() : i.orcSettings.copy();
        req.partitionColumns = new ArrayList<>(i.partitionColumns);
        req.sorting = new ArrayList<>(i.sorting);
        req.flatColumns = flatCols == null ? new ArrayList<>(DEFAULT_FLAT_COLS) : new ArrayList<>(flatCols);
        req.bucketCount = i.bucketCount;
        req.bucketKey = i.bucketKey;
        req.targetFileSizeMib = i.targetFileSizeMib <= 0 ? 256 : i.targetFileSizeMib;
        req.sourceBytesEstimate = Math.max(1L, sourceBytes);
        req.markerPath = marker;
        return req;
    }

    public static String generateDdl(ExperimentInstance inst, String path) {
        String table = "bench.events__"
                + "s__"
                + (inst.experimentId == null ? "x" : inst.experimentId.toLowerCase(Locale.ROOT))
                + "__"
                + "v" + Integer.toHexString(inst.variantLabel == null ? 0 : inst.variantLabel.hashCode());
        StringBuilder partitions = new StringBuilder();
        if (inst.partitionColumns != null && !inst.partitionColumns.isEmpty()) {
            List<String> parts = new ArrayList<>();
            for (String c : inst.partitionColumns) {
                parts.add(c + " STRING");
            }
            partitions.append("PARTITIONED BY (").append(String.join(", ", parts)).append(")\n");
        }
        String buckets = "";
        if (inst.bucketCount > 0 && inst.bucketKey != null) {
            buckets = "CLUSTERED BY (" + inst.bucketKey + ") INTO " + inst.bucketCount + " BUCKETS\n";
        }
        return "CREATE EXTERNAL TABLE IF NOT EXISTS " + table
                + "\nSTORED AS ORC\nLOCATION '" + path + "'\n"
                + partitions
                + buckets
                + ";\n";
    }

    /**
     * High-level entry used by {@link orcbench.Main}.
     *
     * @param dryRun        plan only — no backend writes
     * @param useMock       when true (and not dry-run), use {@link FakeMaterializeBackend}
     * @param cleanup       when true, delete non-retained rewrite layouts after writes
     * @param keepLayouts   experiment ids / dir names retained during cleanup
     * @param backend       optional override (unit tests); if null, mock or Spark is chosen
     */
    public static MaterializeResult materialize(
            LoadedConfig cfg,
            String scale,
            List<ExperimentInstance> instances,
            boolean dryRun,
            boolean useMock,
            boolean cleanup,
            List<String> keepLayouts,
            MaterializeBackend backend) throws Exception {
        if (cfg == null) {
            throw new IllegalArgumentException("cfg is required");
        }
        String scaleKey = scale == null ? "S" : scale.toUpperCase(Locale.ROOT);
        String hdfsRoot = cfg.benchmark.benchmark.hdfsRoot;
        String baselineId = cfg.benchmark.physicalBaseline.id;
        String basePath = hdfsRoot + "/" + scaleKey + "/" + baselineId;
        List<String> flatCols = flatColumns(cfg);

        MaterializeResult result = new MaterializeResult();
        result.scale = scaleKey;
        result.basePath = basePath;
        result.dryRun = dryRun;
        result.mocked = useMock && !dryRun;

        List<ExperimentInstance> toWrite = new ArrayList<>();
        for (ExperimentInstance i : instances == null
                ? Collections.<ExperimentInstance>emptyList() : instances) {
            if (needsMaterialize(i, basePath)) {
                toWrite.add(i);
            } else if (i != null && i.skipReason != null) {
                result.skippedOther++;
            }
        }
        result.plannedWrites = toWrite.size();

        System.out.println("[materialize] scale=" + scaleKey
                + " base=" + basePath
                + " rewriteLayouts=" + toWrite.size()
                + " dryRun=" + dryRun
                + " mock=" + result.mocked
                + " cleanup=" + cleanup);

        if (dryRun) {
            for (ExperimentInstance i : toWrite) {
                String marker = markerPath(hdfsRoot, scaleKey, i);
                result.layoutPaths.add(i.layoutPath);
                result.markerPaths.add(marker);
                result.notes.add("dry-run: would write " + describeRewrite(i)
                        + " -> " + i.layoutPath + " marker=" + marker);
                System.out.println("  dry-run would write " + i.layoutPath
                        + " (" + describeRewrite(i) + ")");
            }
            result.notes.add("dry-run: skip ORC rewrite / markers / cleanup");
            return result;
        }

        MaterializeBackend sink = backend;
        if (sink == null) {
            if (useMock) {
                FakeMaterializeBackend fake = new FakeMaterializeBackend();
                fake.seedBaseline(basePath, 64L * 1024L * 1024L);
                sink = fake;
                result.notes.add("mock backend: in-memory FS, no HDFS");
            } else {
                if (!SparkSessionSupport.sparkAvailable()) {
                    throw new IllegalStateException(
                            "Spark is not on the classpath. Live --phase materialize requires "
                                    + "spark-submit on a Hadoop cluster node, or use --dry-run / --mock-materialize.");
                }
                sink = new SparkMaterializeBackend(
                        SparkSessionSupport.create(cfg, "orc-bench-materialize"));
                result.notes.add("live Spark materialize backend (cluster)");
            }
        }

        if (!sink.pathExists(basePath)) {
            String msg = "RAW_ARRAY_BASE missing at " + basePath
                    + " — run --phase generate first (on cluster for live HDFS)";
            result.notes.add(msg);
            throw new IllegalStateException(msg);
        }

        long sourceBytes = sink.estimateBytes(basePath);
        List<String> ddlBuf = new ArrayList<>();

        for (ExperimentInstance inst : toWrite) {
            String marker = markerPath(hdfsRoot, scaleKey, inst);
            if (sink.markerExists(marker)) {
                result.skippedExistingMarker++;
                result.notes.add("skip existing marker " + marker);
                System.out.println("[materialize] skip (marker) " + inst.experimentId
                        + "/" + inst.variantLabel);
                continue;
            }

            LayoutWriteRequest req = toRequest(inst, basePath, flatCols, sourceBytes, marker);
            System.out.println("[materialize] " + describeRewrite(inst) + " -> " + inst.layoutPath);
            sink.writeLayout(req);

            String markerBody = "experiment=" + inst.experimentId
                    + "\nvariant=" + inst.variantLabel
                    + "\nlayout=" + inst.layoutPath
                    + "\n";
            sink.writeMarker(marker, markerBody);

            String ddl = generateDdl(inst, inst.layoutPath);
            ddlBuf.add(ddl);
            result.ddlStatements.add(ddl);
            result.layoutPaths.add(inst.layoutPath);
            result.markerPaths.add(marker);
            result.written++;
        }

        String ddlPath = cfg.benchmark.benchmark.generatedHiveDdl;
        if (ddlPath != null && !ddlBuf.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append("-- generated by orc-bench LayoutMaterializer (Checkpoint 5)\n");
            for (String d : ddlBuf) {
                sb.append(d).append('\n');
            }
            sink.writeLocalText(ddlPath, sb.toString());
            result.notes.add("wrote DDL stub " + ddlPath + " (" + ddlBuf.size() + " tables)");
        }

        if (cleanup) {
            result.cleaned = cleanupLayouts(sink, hdfsRoot, scaleKey, baselineId,
                    instances, keepLayouts, result.notes);
        }

        System.out.println("[materialize] OK written=" + result.written
                + " skippedMarkers=" + result.skippedExistingMarker
                + " cleaned=" + result.cleaned);
        return result;
    }

    /** Convenience overload without injected backend. */
    public static MaterializeResult materialize(
            LoadedConfig cfg,
            String scale,
            List<ExperimentInstance> instances,
            boolean dryRun,
            boolean useMock,
            boolean cleanup,
            List<String> keepLayouts) throws Exception {
        return materialize(cfg, scale, instances, dryRun, useMock, cleanup, keepLayouts, null);
    }

    static int cleanupLayouts(
            MaterializeBackend sink,
            String hdfsRoot,
            String scale,
            String baselineId,
            List<ExperimentInstance> instances,
            List<String> retain,
            List<String> notes) throws Exception {
        String scaleRoot = hdfsRoot + "/" + scale;
        List<String> children = sink.listChildren(scaleRoot);
        List<String> targets = selectCleanupTargets(children, baselineId, instances, retain);
        int deleted = 0;
        for (String name : targets) {
            String path = scaleRoot + "/" + name;
            System.out.println("[cleanup] deleting " + path);
            sink.deleteRecursive(path);
            deleted++;
            if (notes != null) {
                notes.add("cleaned " + path);
            }
        }
        return deleted;
    }
}
