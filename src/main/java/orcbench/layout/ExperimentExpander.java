package orcbench.layout;

import orcbench.model.Models.BenchmarkContract;
import orcbench.model.Models.EngineExperiment;
import orcbench.model.Models.EngineProfile;
import orcbench.model.Models.EnvironmentSection;
import orcbench.model.Models.ExperimentInstance;
import orcbench.model.Models.ExperimentPlan;
import orcbench.model.Models.LoadedConfig;
import orcbench.model.Models.OrcWriteSettings;
import orcbench.model.Models.PhysicalBaseline;
import orcbench.model.Models.PhysicalExperiment;
import orcbench.model.Models.SparkEnv;
import orcbench.model.Models.YarnEnv;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Expands physical / engine YAML matrices into runnable cells and selects
 * capacity-aware concurrency levels per dataset scale (Checkpoint 2).
 */
public final class ExperimentExpander {
    private ExperimentExpander() {}

    public static OrcWriteSettings baselineSettings(PhysicalBaseline b) {
        OrcWriteSettings s = new OrcWriteSettings();
        s.compression = b.orc.compression;
        s.stripeSizeMib = b.orc.stripeSizeMib;
        s.createIndex = b.orc.createIndex;
        s.rowIndexStride = b.orc.rowIndexStride;
        s.bloomColumns = new ArrayList<>(b.orc.bloomColumns);
        s.bloomFpp = b.orc.bloomFpp;
        return s;
    }

    /**
     * Build a dry-runnable experiment plan: physical cells, engine cells,
     * final-comparison stubs, and capacity-aware concurrency for {@code scale}.
     */
    public static ExperimentPlan buildPlan(
            LoadedConfig cfg, String scale, List<String> engines, List<String> experimentFilter) {
        ExperimentPlan plan = new ExperimentPlan();
        plan.scale = scale == null ? "S" : scale.toUpperCase(Locale.ROOT);
        String baselinePath = cfg.benchmark.benchmark.hdfsRoot + "/" + plan.scale + "/"
                + cfg.benchmark.physicalBaseline.id;

        plan.physical = expandPhysical(cfg.benchmark, plan.scale, experimentFilter);
        plan.engine = expandEngine(cfg.benchmark, plan.scale, baselinePath, engines, experimentFilter);
        plan.finalComparison = finalProfiles(cfg.benchmark, baselinePath);
        plan.concurrencyCandidates = new ArrayList<>(cfg.benchmark.runs.concurrencyLevels);

        EnvironmentSection env = cfg.environment.environment;
        ConcurrencySelection sel = selectConcurrency(
                plan.concurrencyCandidates,
                plan.scale,
                env.yarn,
                env.spark);
        plan.concurrencySelected = sel.levels;
        plan.concurrencyNote = sel.note;
        return plan;
    }

    public static List<ExperimentInstance> expandPhysical(
            BenchmarkContract contract, String scale, List<String> filterIds) {
        PhysicalBaseline base = contract.physicalBaseline;
        OrcWriteSettings baseOrc = baselineSettings(base);
        String hdfsRoot = contract.benchmark.hdfsRoot;
        Set<String> want = new HashSet<>(filterIds == null
                ? Collections.<String>emptyList() : filterIds);
        List<ExperimentInstance> out = new ArrayList<>();

        if (want.isEmpty() || want.contains(base.id)) {
            out.add(baselineInstance(base, baseOrc, hdfsRoot, scale));
        }

        for (PhysicalExperiment exp : contract.physicalExperiments) {
            if (!want.isEmpty() && !want.contains(exp.id)) {
                continue;
            }
            String kind = exp.kind == null ? "" : exp.kind;
            switch (kind) {
                case "query_only":
                    out.add(queryOnly(exp, base, baseOrc, hdfsRoot, scale));
                    break;
                case "reader_settings":
                    for (Object v : exp.variants) {
                        boolean enabled = Boolean.parseBoolean(String.valueOf(v));
                        ExperimentInstance i = baseCopy(exp, base, baseOrc, hdfsRoot, scale, "pushdown=" + enabled);
                        i.kind = exp.kind;
                        i.layoutPath = hdfsRoot + "/" + scale + "/" + base.id;
                        i.readerOverrides.put("spark.sql.orc.filterPushdown", Boolean.toString(enabled));
                        i.queries = new ArrayList<>(exp.queries);
                        out.add(i);
                    }
                    break;
                case "cluster_read":
                    List<String> variants = new ArrayList<>();
                    if (exp.variants.isEmpty()) {
                        variants.add("default");
                    } else {
                        for (Object v : exp.variants) {
                            variants.add(String.valueOf(v));
                        }
                    }
                    for (String v : variants) {
                        ExperimentInstance i = baseCopy(exp, base, baseOrc, hdfsRoot, scale, v);
                        i.kind = exp.kind;
                        i.layoutPath = hdfsRoot + "/" + scale + "/" + base.id;
                        boolean centralized = "HDFS_CACHE".equals(exp.id) && "centralized".equals(v);
                        i.requires = new ArrayList<>(
                                centralized ? exp.requiresForCentralized : exp.requires);
                        if ("HDFS_CACHE".equals(exp.id)) {
                            i.cacheState = v;
                        }
                        out.add(i);
                    }
                    break;
                case "derived_layout_query":
                    ExperimentInstance d = baseCopy(exp, base, baseOrc, hdfsRoot, scale, "derived_partition");
                    d.kind = exp.kind;
                    d.layoutPath = hdfsRoot + "/" + scale + "/PARTITIONING_event_date";
                    d.partitionColumns = Arrays.asList("event_date");
                    d.queries = new ArrayList<>(exp.queries);
                    d.requires = new ArrayList<>(exp.requires);
                    out.add(d);
                    break;
                case "rewrite":
                case "hive_compatible_write":
                    out.addAll(expandRewrite(exp, base, baseOrc, hdfsRoot, scale));
                    break;
                default:
                    ExperimentInstance u = baseCopy(exp, base, baseOrc, hdfsRoot, scale, "unsupported");
                    u.kind = exp.kind;
                    u.requires = new ArrayList<>(exp.requires);
                    u.requires.add("unsupported_kind_" + exp.kind);
                    u.skipReason = "unsupported kind: " + exp.kind;
                    out.add(u);
            }
        }
        return out;
    }

    private static List<ExperimentInstance> expandRewrite(
            PhysicalExperiment exp, PhysicalBaseline base, OrcWriteSettings baseOrc,
            String hdfsRoot, String scale) {
        List<ExperimentInstance> out = new ArrayList<>();
        String factor = exp.factor == null ? "unknown" : exp.factor;
        String representation = exp.representation != null ? exp.representation : base.representation;
        for (Object value : exp.values) {
            OrcWriteSettings orc = baseOrc.copy();
            List<String> sorting = new ArrayList<>(base.sorting);
            List<String> partitions = new ArrayList<>(base.partitioning);
            int fileSize = base.targetFileSizeMib;
            int buckets = 0;
            String rep = representation;
            String label = labelOf(factor, value);

            switch (factor) {
                case "sorting":
                    sorting = asStringList(value);
                    break;
                case "bloom_columns":
                    orc.bloomColumns = asStringList(value);
                    break;
                case "bloom_fpp":
                    orc.bloomFpp = Double.parseDouble(value.toString());
                    if (!exp.requiresBloomColumns.isEmpty()) {
                        orc.bloomColumns = new ArrayList<>(exp.requiresBloomColumns);
                    } else if (orc.bloomColumns.isEmpty()) {
                        orc.bloomColumns = Arrays.asList("userLogin");
                    }
                    break;
                case "row_index_stride":
                    orc.rowIndexStride = Integer.parseInt(value.toString());
                    break;
                case "stripe_size_mib":
                    orc.stripeSizeMib = Integer.parseInt(value.toString());
                    break;
                case "compression":
                    orc.compression = value.toString();
                    break;
                case "partition_columns":
                    partitions = asStringList(value);
                    break;
                case "bucket_count":
                    buckets = Integer.parseInt(value.toString());
                    rep = exp.representation != null ? exp.representation : "HYBRID";
                    break;
                case "target_file_size_mib":
                    fileSize = Integer.parseInt(value.toString());
                    break;
                case "representation":
                    rep = value.toString();
                    break;
                default:
                    break;
            }

            ExperimentInstance i = baseCopy(exp, base, orc, hdfsRoot, scale, label);
            i.kind = exp.kind;
            i.layoutPath = hdfsRoot + "/" + scale + "/" + exp.id + "_" + sanitize(label);
            i.representation = rep;
            i.partitionColumns = partitions;
            i.sorting = sorting;
            i.bucketCount = buckets;
            i.bucketKey = exp.bucketKey;
            i.targetFileSizeMib = fileSize;
            i.queries = new ArrayList<>(exp.queries);
            i.requires = new ArrayList<>(exp.requires);
            out.add(i);
        }
        return out;
    }

    public static List<ExperimentInstance> expandEngine(
            BenchmarkContract contract, String scale, String layoutPath,
            List<String> engines, List<String> filterIds) {
        PhysicalBaseline base = contract.physicalBaseline;
        OrcWriteSettings baseOrc = baselineSettings(base);
        Set<String> want = new HashSet<>(filterIds == null
                ? Collections.<String>emptyList() : filterIds);
        List<String> eng = engines == null ? Collections.<String>emptyList() : engines;
        List<ExperimentInstance> out = new ArrayList<>();

        if (eng.contains("spark")) {
            ExperimentInstance sparkBase = new ExperimentInstance();
            sparkBase.experimentId = "SPARK_BASE";
            sparkBase.kind = "engine_baseline";
            sparkBase.variantLabel = "baseline";
            sparkBase.layoutPath = layoutPath;
            sparkBase.representation = base.representation;
            sparkBase.orcSettings = baseOrc;
            sparkBase.targetFileSizeMib = base.targetFileSizeMib;
            sparkBase.sessionOverrides = new LinkedHashMap<>(contract.executionBaselines.spark);
            sparkBase.engine = "spark";
            if (want.isEmpty() || want.contains("SPARK_BASE")) {
                out.add(sparkBase);
            }

            for (EngineExperiment exp : contract.engineExperiments.spark) {
                if (!want.isEmpty() && !want.contains(exp.id)) {
                    continue;
                }
                if (exp.property != null && !exp.values.isEmpty()) {
                    for (Object v : exp.values) {
                        ExperimentInstance i = engineBase(exp, base, baseOrc, layoutPath, exp.property + "=" + v);
                        i.sessionOverrides = new LinkedHashMap<>(contract.executionBaselines.spark);
                        i.sessionOverrides.put(exp.property, v.toString());
                        i.queries = new ArrayList<>(exp.queries);
                        i.requires = new ArrayList<>(exp.requires);
                        i.engine = "spark";
                        out.add(i);
                    }
                } else {
                    ExperimentInstance i = engineBase(exp, base, baseOrc, layoutPath, exp.id);
                    i.sessionOverrides = new LinkedHashMap<>(contract.executionBaselines.spark);
                    i.sessionOverrides.putAll(exp.sessionOverrides);
                    i.queries = new ArrayList<>(exp.queries);
                    i.requires = new ArrayList<>(exp.requires);
                    i.engine = "spark";
                    out.add(i);
                }
            }
        }

        if (eng.contains("hive_tez") || eng.contains("hive_llap")) {
            for (EngineExperiment exp : contract.engineExperiments.hive) {
                if (!want.isEmpty() && !want.contains(exp.id)) {
                    continue;
                }
                boolean isLlap = Boolean.TRUE.equals(exp.llap);
                if (isLlap && !eng.contains("hive_llap")) {
                    continue;
                }
                if (!isLlap && !eng.contains("hive_tez")) {
                    continue;
                }

                List<String> cacheStates = new ArrayList<>();
                if (!exp.cacheStates.isEmpty()) {
                    cacheStates.addAll(exp.cacheStates);
                } else if (!exp.workingSetFractions.isEmpty()) {
                    for (Double f : exp.workingSetFractions) {
                        cacheStates.add("ws=" + f);
                    }
                } else {
                    cacheStates.add("n/a");
                }

                if (exp.property != null && !exp.values.isEmpty()) {
                    // HIVE_VECTORIZATION / HIVE_CBO — OVAT on property values (Tez dialect).
                    for (Object v : exp.values) {
                        for (String cs : cacheStates) {
                            String label = exp.property + "=" + v;
                            if (!"n/a".equals(cs)) {
                                label = label + ":" + cs;
                            }
                            ExperimentInstance i = engineBase(exp, base, baseOrc, layoutPath, label);
                            i.sessionOverrides = new LinkedHashMap<>(contract.executionBaselines.hive);
                            i.sessionOverrides.putAll(exp.sessionOverrides);
                            i.sessionOverrides.put(exp.property, v.toString());
                            i.queries = new ArrayList<>(exp.queries);
                            i.requires = new ArrayList<>(exp.requires);
                            i.engine = isLlap ? "hive_llap" : "hive_tez";
                            i.llap = isLlap;
                            if (!"n/a".equals(cs)) {
                                i.cacheState = cs;
                            }
                            out.add(i);
                        }
                    }
                } else {
                    for (String cs : cacheStates) {
                        ExperimentInstance i = engineBase(exp, base, baseOrc, layoutPath,
                                "n/a".equals(cs) ? exp.id : exp.id + ":" + cs);
                        i.sessionOverrides = new LinkedHashMap<>(contract.executionBaselines.hive);
                        i.sessionOverrides.putAll(exp.sessionOverrides);
                        i.queries = new ArrayList<>(exp.queries);
                        i.requires = new ArrayList<>(exp.requires);
                        i.engine = isLlap ? "hive_llap" : "hive_tez";
                        i.llap = isLlap;
                        if (!"n/a".equals(cs)) {
                            i.cacheState = cs;
                        }
                        out.add(i);
                    }
                }
            }
        }
        return out;
    }

    public static List<ExperimentInstance> finalProfiles(BenchmarkContract contract, String bestOrcPath) {
        PhysicalBaseline base = contract.physicalBaseline;
        OrcWriteSettings baseOrc = baselineSettings(base);
        List<ExperimentInstance> out = new ArrayList<>();
        for (String name : contract.finalComparison.engines) {
            ExperimentInstance i = new ExperimentInstance();
            i.experimentId = name;
            i.kind = "final_comparison";
            i.variantLabel = name;
            i.layoutPath = bestOrcPath;
            i.representation = base.representation;
            i.orcSettings = baseOrc;
            i.partitionColumns = new ArrayList<>(base.partitioning);
            i.sorting = new ArrayList<>(base.sorting);
            i.targetFileSizeMib = base.targetFileSizeMib;
            i.queries = new ArrayList<>(contract.finalComparison.queries);

            EngineProfile profile = contract.engineProfiles.get(name);
            switch (name) {
                case "SPARK_BASE":
                    i.engine = "spark";
                    i.sessionOverrides = new LinkedHashMap<>(contract.executionBaselines.spark);
                    applyProfileSet(i, profile);
                    break;
                case "SPARK_OPTIMIZED":
                    i.engine = "spark";
                    i.sessionOverrides = new LinkedHashMap<>(contract.executionBaselines.spark);
                    if (profile != null && !profile.set.isEmpty()) {
                        applyProfileSet(i, profile);
                    } else {
                        i.sessionOverrides.put("spark.sql.cbo.enabled", "true");
                        i.sessionOverrides.put("spark.sql.autoBroadcastJoinThreshold", "10485760");
                        i.sessionOverrides.put("spark.sql.orc.enableNestedColumnVectorizedReader", "true");
                    }
                    break;
                case "HIVE_TEZ_BASE":
                    i.engine = "hive_tez";
                    i.sessionOverrides = new LinkedHashMap<>(contract.executionBaselines.hive);
                    if (profile != null && !profile.set.isEmpty()) {
                        applyProfileSet(i, profile);
                    } else {
                        i.sessionOverrides.put("hive.llap.execution.mode", "none");
                    }
                    break;
                case "HIVE_LLAP_COLD":
                    i.engine = "hive_llap";
                    i.llap = true;
                    i.cacheState = "cold";
                    i.sessionOverrides = new LinkedHashMap<>(contract.executionBaselines.hive);
                    if (profile != null && !profile.set.isEmpty()) {
                        applyProfileSet(i, profile);
                    } else {
                        i.sessionOverrides.put("hive.llap.execution.mode", "only");
                    }
                    i.requires = Arrays.asList("llap_daemons_running", "llap_task_execution_verified");
                    break;
                case "HIVE_LLAP_WARM":
                    i.engine = "hive_llap";
                    i.llap = true;
                    i.cacheState = "warm";
                    i.sessionOverrides = new LinkedHashMap<>(contract.executionBaselines.hive);
                    if (profile != null && !profile.set.isEmpty()) {
                        applyProfileSet(i, profile);
                    } else {
                        i.sessionOverrides.put("hive.llap.execution.mode", "only");
                    }
                    i.requires = Arrays.asList("llap_daemons_running", "llap_task_execution_verified");
                    break;
                default:
                    i.engine = name.toLowerCase(Locale.ROOT).contains("hive") ? "hive_tez" : "spark";
                    if (profile != null) {
                        if (profile.extendsBaseline != null && profile.extendsBaseline.contains("hive")) {
                            i.sessionOverrides = new LinkedHashMap<>(contract.executionBaselines.hive);
                            i.engine = "hive_tez";
                        } else {
                            i.sessionOverrides = new LinkedHashMap<>(contract.executionBaselines.spark);
                        }
                        applyProfileSet(i, profile);
                    }
            }
            out.add(i);
        }
        return out;
    }

    private static void applyProfileSet(ExperimentInstance i, EngineProfile profile) {
        if (profile != null && profile.set != null) {
            i.sessionOverrides.putAll(profile.set);
        }
    }

    /**
     * Capacity- and scale-aware filter of YAML concurrency candidates (§8.2).
     * Candidates are a ceiling; heavier levels require spare YARN capacity.
     */
    public static ConcurrencySelection selectConcurrency(
            List<Integer> candidates, String scale, YarnEnv yarn, SparkEnv spark) {
        List<Integer> cand = candidates == null ? Collections.<Integer>emptyList() : candidates;
        long memGib = yarn != null && yarn.availableMemoryGib != null ? yarn.availableMemoryGib : 0L;
        int vcores = yarn != null && yarn.availableVcores != null ? yarn.availableVcores : 0;
        int execInst = spark != null && spark.executorInstances != null ? spark.executorInstances : 10;
        int execCores = spark != null && spark.executorCores != null ? spark.executorCores : 4;
        int execMem = spark != null && spark.executorMemoryGib != null ? spark.executorMemoryGib : 16;

        double reservedMem = (double) execInst * execMem;
        double reservedCores = (double) execInst * execCores;
        double memUtil = memGib <= 0L ? 1.0 : reservedMem / (double) memGib;
        double coreUtil = vcores <= 0 ? 1.0 : reservedCores / (double) vcores;
        double util = Math.max(memUtil, coreUtil);

        // First-trial sizing (~160 GiB / 40 vCores of 216 / 67) lands near util≈0.74 → cap 5.
        int capacityCap;
        if (util >= 0.90) {
            capacityCap = 1;
        } else if (util >= 0.70) {
            capacityCap = 5;
        } else if (util >= 0.50) {
            capacityCap = 10;
        } else {
            capacityCap = 25;
        }

        String scaleKey = scale == null ? "S" : scale.toUpperCase(Locale.ROOT);
        int scaleCap;
        if ("L".equals(scaleKey)) {
            // Heavy scale: only concurrency 1 unless clear headroom (§8.2)
            scaleCap = util < 0.50 ? 5 : 1;
        } else if ("M".equals(scaleKey)) {
            scaleCap = util < 0.70 ? 10 : 5;
        } else {
            // S: start with low levels; allow 10/25 only with spare capacity
            scaleCap = util < 0.50 ? 25 : (util < 0.70 ? 10 : 5);
        }

        int max = Math.min(capacityCap, scaleCap);
        List<Integer> selected = new ArrayList<>();
        for (Integer c : cand) {
            if (c != null && c >= 1 && c <= max) {
                selected.add(c);
            }
        }
        if (selected.isEmpty()) {
            selected.add(1);
        }

        ConcurrencySelection sel = new ConcurrencySelection();
        sel.levels = selected;
        sel.maxAllowed = max;
        sel.utilization = util;
        sel.note = String.format(Locale.ROOT,
                "scale=%s util=%.2f (mem %.0f/%d GiB, cores %.0f/%d) capacityCap=%d scaleCap=%d → %s",
                scaleKey, util, reservedMem, memGib, reservedCores, vcores, capacityCap, scaleCap, selected);
        return sel;
    }

    /** Print a human-readable experiment plan (dry-run). */
    public static void printPlan(ExperimentPlan plan, Appendable out) throws java.io.IOException {
        out.append("[plan] scale=").append(plan.scale)
                .append(" physicalCells=").append(Integer.toString(plan.physical.size()))
                .append(" engineCells=").append(Integer.toString(plan.engine.size()))
                .append(" finalCells=").append(Integer.toString(plan.finalComparison.size()))
                .append('\n');
        out.append("[plan] concurrency candidates=").append(String.valueOf(plan.concurrencyCandidates))
                .append(" selected=").append(String.valueOf(plan.concurrencySelected))
                .append('\n');
        if (plan.concurrencyNote != null) {
            out.append("[plan] ").append(plan.concurrencyNote).append('\n');
        }
        out.append("[plan] --- physical ---\n");
        for (ExperimentInstance i : plan.physical) {
            appendCell(out, i);
        }
        out.append("[plan] --- engine ---\n");
        for (ExperimentInstance i : plan.engine) {
            appendCell(out, i);
        }
        out.append("[plan] --- final_comparison ---\n");
        for (ExperimentInstance i : plan.finalComparison) {
            appendCell(out, i);
        }
    }

    private static void appendCell(Appendable out, ExperimentInstance i) throws java.io.IOException {
        out.append("  ").append(i.experimentId)
                .append(" / ").append(i.variantLabel == null ? "-" : i.variantLabel)
                .append(" kind=").append(i.kind == null ? "-" : i.kind)
                .append(" engine=").append(i.engine == null ? "-" : i.engine)
                .append(" layout=").append(i.layoutPath == null ? "-" : i.layoutPath)
                .append(" queries=").append(Integer.toString(i.queries == null ? 0 : i.queries.size()));
        if (i.skipReason != null) {
            out.append(" SKIP=").append(i.skipReason);
        }
        out.append('\n');
    }

    public static final class ConcurrencySelection {
        public List<Integer> levels = new ArrayList<>();
        public int maxAllowed;
        public double utilization;
        public String note;
    }

    private static ExperimentInstance baselineInstance(
            PhysicalBaseline base, OrcWriteSettings orc, String hdfsRoot, String scale) {
        ExperimentInstance i = new ExperimentInstance();
        i.experimentId = base.id;
        i.kind = "baseline";
        i.variantLabel = "baseline";
        i.layoutPath = hdfsRoot + "/" + scale + "/" + base.id;
        i.representation = base.representation;
        i.orcSettings = orc;
        i.partitionColumns = new ArrayList<>(base.partitioning);
        i.sorting = new ArrayList<>(base.sorting);
        i.bucketCount = base.bucketing == null ? 0 : base.bucketing;
        i.targetFileSizeMib = base.targetFileSizeMib;
        i.engine = "spark";
        return i;
    }

    private static ExperimentInstance queryOnly(
            PhysicalExperiment exp, PhysicalBaseline base, OrcWriteSettings orc, String hdfsRoot, String scale) {
        ExperimentInstance i = baseCopy(exp, base, orc, hdfsRoot, scale, "query_only");
        i.kind = exp.kind;
        i.layoutPath = hdfsRoot + "/" + scale + "/" + base.id;
        i.queries = new ArrayList<>(exp.queries);
        i.requires = new ArrayList<>(exp.requires);
        return i;
    }

    private static ExperimentInstance baseCopy(
            PhysicalExperiment exp, PhysicalBaseline base, OrcWriteSettings orc,
            String hdfsRoot, String scale, String label) {
        ExperimentInstance i = new ExperimentInstance();
        i.experimentId = exp.id;
        i.variantLabel = label;
        i.representation = base.representation;
        i.orcSettings = orc;
        i.partitionColumns = new ArrayList<>(base.partitioning);
        i.sorting = new ArrayList<>(base.sorting);
        i.targetFileSizeMib = base.targetFileSizeMib;
        i.engine = "spark";
        return i;
    }

    private static ExperimentInstance engineBase(
            EngineExperiment exp, PhysicalBaseline base, OrcWriteSettings orc, String layoutPath, String label) {
        ExperimentInstance i = new ExperimentInstance();
        i.experimentId = exp.id;
        i.kind = "engine";
        i.variantLabel = label;
        i.layoutPath = layoutPath;
        i.representation = base.representation;
        i.orcSettings = orc;
        i.partitionColumns = new ArrayList<>(base.partitioning);
        i.targetFileSizeMib = base.targetFileSizeMib;
        return i;
    }

    @SuppressWarnings("unchecked")
    private static List<String> asStringList(Object v) {
        List<String> out = new ArrayList<>();
        if (v instanceof List) {
            for (Object o : (List<Object>) v) {
                if (o != null) {
                    out.add(o.toString());
                }
            }
        } else if (v != null) {
            out.add(v.toString());
        }
        return out;
    }

    private static String labelOf(String factor, Object value) {
        if (value instanceof List) {
            List<?> l = (List<?>) value;
            if (l.isEmpty()) {
                return factor + "=[]";
            }
            StringBuilder sb = new StringBuilder(factor).append("=[");
            for (int i = 0; i < l.size(); i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(l.get(i));
            }
            return sb.append(']').toString();
        }
        return factor + "=" + value;
    }

    private static String sanitize(String s) {
        String cleaned = s.replaceAll("[^A-Za-z0-9_.=\\-]+", "_");
        return cleaned.substring(0, Math.min(120, cleaned.length()));
    }
}
