package orcbench.layout;

import orcbench.model.Models.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    public static List<ExperimentInstance> expandPhysical(BenchmarkContract contract, String scale, List<String> filterIds) {
        PhysicalBaseline base = contract.physicalBaseline;
        OrcWriteSettings baseOrc = baselineSettings(base);
        String hdfsRoot = contract.benchmark.hdfsRoot;
        Set<String> want = new HashSet<>(filterIds);
        List<ExperimentInstance> out = new ArrayList<>();

        if (want.isEmpty() || want.contains(base.id)) {
            out.add(baselineInstance(base, baseOrc, hdfsRoot, scale));
        }

        for (PhysicalExperiment exp : contract.physicalExperiments) {
            if (!want.isEmpty() && !want.contains(exp.id)) continue;
            switch (exp.kind) {
                case "query_only":
                    out.add(queryOnly(exp, base, baseOrc, hdfsRoot, scale));
                    break;
                case "reader_settings":
                    for (Object v : exp.variants) {
                        boolean enabled = Boolean.parseBoolean(v.toString());
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
                    if (exp.variants.isEmpty()) variants.add("default");
                    else for (Object v : exp.variants) variants.add(v.toString());
                    for (String v : variants) {
                        ExperimentInstance i = baseCopy(exp, base, baseOrc, hdfsRoot, scale, v);
                        i.kind = exp.kind;
                        i.layoutPath = hdfsRoot + "/" + scale + "/" + base.id;
                        i.requires = new ArrayList<>(
                                "HDFS_CACHE".equals(exp.id) && "centralized".equals(v)
                                        ? exp.requiresForCentralized : exp.requires);
                        if ("HDFS_CACHE".equals(exp.id)) i.cacheState = v;
                        out.add(i);
                    }
                    break;
                case "derived_layout_query":
                    ExperimentInstance d = baseCopy(exp, base, baseOrc, hdfsRoot, scale, "derived_partition");
                    d.kind = exp.kind;
                    d.layoutPath = hdfsRoot + "/" + scale + "/PARTITIONING_event_date";
                    d.partitionColumns = java.util.Arrays.asList("event_date");
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
                    if (!exp.requiresBloomColumns.isEmpty()) orc.bloomColumns = new ArrayList<>(exp.requiresBloomColumns);
                    else if (orc.bloomColumns.isEmpty()) orc.bloomColumns = java.util.Arrays.asList("userLogin");
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
        Set<String> want = new HashSet<>(filterIds);
        List<ExperimentInstance> out = new ArrayList<>();

        if (engines.contains("spark")) {
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
            out.add(sparkBase);

            for (EngineExperiment exp : contract.engineExperiments.spark) {
                if (!want.isEmpty() && !want.contains(exp.id)) continue;
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

        if (engines.contains("hive_tez") || engines.contains("hive_llap")) {
            for (EngineExperiment exp : contract.engineExperiments.hive) {
                if (!want.isEmpty() && !want.contains(exp.id)) continue;
                boolean isLlap = Boolean.TRUE.equals(exp.llap);
                if (isLlap && !engines.contains("hive_llap")) continue;
                if (!isLlap && !engines.contains("hive_tez") && !engines.contains("hive_llap")) continue;

                List<String> cacheStates = new ArrayList<>();
                if (!exp.cacheStates.isEmpty()) cacheStates.addAll(exp.cacheStates);
                else if (!exp.workingSetFractions.isEmpty()) {
                    for (Double f : exp.workingSetFractions) cacheStates.add("ws=" + f);
                } else cacheStates.add("n/a");

                for (String cs : cacheStates) {
                    ExperimentInstance i = engineBase(exp, base, baseOrc, layoutPath,
                            "n/a".equals(cs) ? exp.id : exp.id + ":" + cs);
                    i.sessionOverrides = new LinkedHashMap<>(contract.executionBaselines.hive);
                    i.sessionOverrides.putAll(exp.sessionOverrides);
                    i.queries = new ArrayList<>(exp.queries);
                    i.requires = new ArrayList<>(exp.requires);
                    i.engine = isLlap ? "hive_llap" : "hive_tez";
                    i.llap = isLlap;
                    if (!"n/a".equals(cs)) i.cacheState = cs;
                    out.add(i);
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

            switch (name) {
                case "SPARK_BASE":
                    i.engine = "spark";
                    i.sessionOverrides = new LinkedHashMap<>(contract.executionBaselines.spark);
                    break;
                case "SPARK_OPTIMIZED":
                    i.engine = "spark";
                    i.sessionOverrides = new LinkedHashMap<>(contract.executionBaselines.spark);
                    i.sessionOverrides.put("spark.sql.orc.enableVectorizedReader", "true");
                    i.sessionOverrides.put("spark.sql.adaptive.enabled", "true");
                    i.sessionOverrides.put("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true");
                    i.sessionOverrides.put("spark.sql.cbo.enabled", "true");
                    break;
                case "HIVE_TEZ_BASE":
                    i.engine = "hive_tez";
                    i.sessionOverrides = new LinkedHashMap<>(contract.executionBaselines.hive);
                    i.sessionOverrides.put("hive.llap.execution.mode", "none");
                    break;
                case "HIVE_TEZ_OPTIMIZED":
                    i.engine = "hive_tez";
                    i.sessionOverrides = new LinkedHashMap<>(contract.executionBaselines.hive);
                    i.sessionOverrides.put("hive.llap.execution.mode", "none");
                    i.sessionOverrides.put("hive.vectorized.execution.enabled", "true");
                    i.sessionOverrides.put("hive.cbo.enable", "true");
                    break;
                case "HIVE_LLAP_COLD":
                    i.engine = "hive_llap";
                    i.llap = true;
                    i.cacheState = "cold";
                    i.sessionOverrides = new LinkedHashMap<>(contract.executionBaselines.hive);
                    i.sessionOverrides.put("hive.llap.execution.mode", "only");
                    i.requires = java.util.Arrays.asList("llap_daemons_running", "llap_task_execution_verified");
                    break;
                case "HIVE_LLAP_WARM":
                    i.engine = "hive_llap";
                    i.llap = true;
                    i.cacheState = "warm";
                    i.sessionOverrides = new LinkedHashMap<>(contract.executionBaselines.hive);
                    i.sessionOverrides.put("hive.llap.execution.mode", "only");
                    i.requires = java.util.Arrays.asList("llap_daemons_running", "llap_task_execution_verified");
                    break;
                default:
                    i.engine = "spark";
                    i.cacheState = name;
            }
            out.add(i);
        }
        return out;
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
                if (o != null) out.add(o.toString());
            }
        } else if (v != null) {
            out.add(v.toString());
        }
        return out;
    }

    private static String labelOf(String factor, Object value) {
        if (value instanceof List) {
            List<?> l = (List<?>) value;
            if (l.isEmpty()) return factor + "=[]";
            StringBuilder sb = new StringBuilder(factor).append("=[");
            for (int i = 0; i < l.size(); i++) {
                if (i > 0) sb.append(',');
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
