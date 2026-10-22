package orcbench.config;

import orcbench.model.Models.BenchmarkContract;
import orcbench.model.Models.BenchmarkSection;
import orcbench.model.Models.BestOrcRule;
import orcbench.model.Models.ColumnGenerator;
import orcbench.model.Models.ConfigurationValidation;
import orcbench.model.Models.DatasetContract;
import orcbench.model.Models.EngineExperiment;
import orcbench.model.Models.EngineProfile;
import orcbench.model.Models.EnvironmentContract;
import orcbench.model.Models.FinalComparison;
import orcbench.model.Models.InputsSection;
import orcbench.model.Models.LoadedConfig;
import orcbench.model.Models.PhysicalBaseline;
import orcbench.model.Models.PhysicalExperiment;
import orcbench.model.Models.QuerySpec;
import orcbench.model.Models.QuerySuiteContract;
import orcbench.model.Models.ReferenceDataset;
import orcbench.model.Models.RepresentationSpec;
import orcbench.model.Models.ScaleSpec;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static orcbench.config.YamlSupport.anyList;
import static orcbench.config.YamlSupport.bool;
import static orcbench.config.YamlSupport.deepMerge;
import static orcbench.config.YamlSupport.doubleVal;
import static orcbench.config.YamlSupport.intVal;
import static orcbench.config.YamlSupport.loadMap;
import static orcbench.config.YamlSupport.loadText;
import static orcbench.config.YamlSupport.longVal;
import static orcbench.config.YamlSupport.nestedMap;
import static orcbench.config.YamlSupport.optBool;
import static orcbench.config.YamlSupport.optDouble;
import static orcbench.config.YamlSupport.optInt;
import static orcbench.config.YamlSupport.optLong;
import static orcbench.config.YamlSupport.optStr;
import static orcbench.config.YamlSupport.resolvePath;
import static orcbench.config.YamlSupport.str;
import static orcbench.config.YamlSupport.stringList;
import static orcbench.config.YamlSupport.stringMap;

/**
 * Loads configs_final / conf/ YAML into typed models (Checkpoint 2).
 * Nested keys live under the single root document key (benchmark / dataset / …).
 */
public final class ConfigLoader {
    private ConfigLoader() {}

    /** Checkpoint 1 raw maps (kept for overlay / debug). */
    public static RawConfig loadRaw(String configDir, String envOverridePath) throws Exception {
        RawConfig cfg = new RawConfig();
        cfg.environment = loadMap(resolvePath(configDir, "environment.yaml"));
        cfg.benchmark = loadMap(resolvePath(configDir, "benchmark.yaml"));
        cfg.dataset = loadMap(resolvePath(configDir, "dataset.yaml"));
        cfg.querySuite = loadMap(resolvePath(configDir, "query-suite.yaml"));
        cfg.schemaOrc = loadText(resolvePath(configDir, "schema.orc.txt"));

        if (envOverridePath != null && !envOverridePath.trim().isEmpty()) {
            Map<String, Object> overlay = loadMap(envOverridePath);
            cfg.environment = deepMerge(cfg.environment, overlay);
        }
        return cfg;
    }

    public static LoadedConfig load(String configDir, String envOverride) throws Exception {
        RawConfig raw = loadRaw(configDir, envOverride);
        LoadedConfig cfg = new LoadedConfig();
        cfg.benchmark = parseBenchmark(raw.benchmark);
        cfg.dataset = parseDataset(raw.dataset);
        cfg.environment = parseEnvironment(raw.environment);
        cfg.queries = parseQueries(raw.querySuite);
        cfg.schemaText = raw.schemaOrc;
        cfg.configDir = configDir;
        return cfg;
    }

    @SuppressWarnings("unchecked")
    private static BenchmarkContract parseBenchmark(Map<String, Object> root) {
        BenchmarkContract c = new BenchmarkContract();
        // configs_final: everything under the single root key "benchmark"
        Map<String, Object> b = nestedMap(root, "benchmark");

        BenchmarkSection bs = new BenchmarkSection();
        bs.id = str(b, "id", "");
        bs.specVersion = intVal(b, "spec_version", 1);
        bs.slaMeanLatencyMs = longVal(b, "sla_mean_latency_ms", 3000L);
        bs.isolation = str(b, "isolation", "");
        bs.requireResultEquivalence = bool(b, "require_result_equivalence", true);
        bs.hdfsRoot = str(b, "hdfs_root", "hdfs://orc_test");
        bs.generatedHiveDdl = str(b, "generated_hive_ddl", "generated/hive-table.sql");
        bs.derivedPartitionColumns = stringList(b, "derived_partition_columns");
        c.benchmark = bs;

        Map<String, Object> inputs = nestedMap(b, "inputs");
        InputsSection is = new InputsSection();
        is.schema = str(inputs, "schema", "");
        is.dataset = str(inputs, "dataset", "");
        is.environment = str(inputs, "environment", "");
        is.queries = str(inputs, "queries", "");
        c.inputs = is;

        Map<String, Object> baseline = nestedMap(b, "physical_baseline");
        Map<String, Object> orcBase = nestedMap(baseline, "orc");
        Map<String, Object> bloom = nestedMap(orcBase, "bloom");
        PhysicalBaseline pb = new PhysicalBaseline();
        pb.id = str(baseline, "id", "RAW_ARRAY_BASE");
        pb.representation = str(baseline, "representation", "RAW_ARRAY");
        pb.partitioning = stringList(baseline, "partitioning");
        pb.bucketing = optInt(baseline, "bucketing");
        pb.sorting = stringList(baseline, "sorting");
        pb.targetFileSizeMib = intVal(baseline, "target_file_size_mib", 256);
        pb.orc.compression = str(orcBase, "compression", "SNAPPY");
        pb.orc.stripeSizeMib = intVal(orcBase, "stripe_size_mib", 64);
        pb.orc.createIndex = bool(orcBase, "create_index", true);
        pb.orc.rowIndexStride = intVal(orcBase, "row_index_stride", 10000);
        pb.orc.bloomColumns = stringList(bloom, "columns");
        pb.orc.bloomFpp = doubleVal(bloom, "fpp", 0.01);
        c.physicalBaseline = pb;

        for (Map.Entry<String, Object> e : nestedMap(b, "representations").entrySet()) {
            Map<String, Object> m = e.getValue() instanceof Map
                    ? (Map<String, Object>) e.getValue() : new LinkedHashMap<String, Object>();
            RepresentationSpec rs = new RepresentationSpec();
            rs.transform = str(m, "transform", e.getKey());
            rs.paramsType = optStr(m, "params_type");
            rs.columns = stringList(m, "columns");
            rs.keepOriginalParameters = optBool(m, "keep_original_parameters");
            rs.lossless = optBool(m, "lossless");
            rs.immutableSourceBaseline = optBool(m, "immutable_source_baseline");
            c.representations.put(e.getKey(), rs);
        }

        for (Object o : anyList(b, "physical_experiments")) {
            if (!(o instanceof Map)) {
                continue;
            }
            Map<String, Object> e = (Map<String, Object>) o;
            PhysicalExperiment pe = new PhysicalExperiment();
            pe.id = str(e, "id", "");
            pe.kind = str(e, "kind", "");
            pe.basedOn = optStr(e, "based_on");
            pe.factor = optStr(e, "factor");
            pe.values = anyList(e, "values");
            pe.variants = anyList(e, "variants");
            pe.queries = stringList(e, "queries");
            pe.requires = stringList(e, "requires");
            pe.requiresBloomColumns = stringList(e, "requires_bloom_columns");
            pe.requiresForCentralized = stringList(e, "requires_for_centralized");
            pe.representation = optStr(e, "representation");
            pe.bucketKey = optStr(e, "bucket_key");
            pe.engine = optStr(e, "engine");
            c.physicalExperiments.add(pe);
        }

        Map<String, Object> exec = nestedMap(b, "execution_baselines");
        c.executionBaselines.spark = stringMap(exec, "spark");
        Map<String, String> hiveBaseline = stringMap(exec, "hive");
        hiveBaseline.remove("comment");
        c.executionBaselines.hive = hiveBaseline;

        for (Map.Entry<String, Object> e : nestedMap(b, "engine_profiles").entrySet()) {
            Map<String, Object> m = e.getValue() instanceof Map
                    ? (Map<String, Object>) e.getValue() : new LinkedHashMap<String, Object>();
            EngineProfile ep = new EngineProfile();
            ep.extendsBaseline = optStr(m, "extends");
            ep.set = stringMap(m, "set");
            ep.prepare = optStr(m, "prepare");
            ep.cacheState = optStr(m, "cache_state");
            ep.status = optStr(m, "status");
            ep.reason = optStr(m, "reason");
            c.engineProfiles.put(e.getKey(), ep);
        }

        Map<String, Object> engineEx = nestedMap(b, "engine_experiments");
        c.engineExperiments.spark = parseEngineExps(engineEx, "spark");
        c.engineExperiments.hive = parseEngineExps(engineEx, "hive");

        Map<String, Object> finalCmp = nestedMap(b, "final_comparison");
        FinalComparison fc = new FinalComparison();
        fc.physicalDataset = str(finalCmp, "physical_dataset", "BEST_ORC");
        fc.engines = stringList(finalCmp, "engines");
        fc.requireIdenticalFileManifest = bool(finalCmp, "require_identical_file_manifest", true);
        fc.requireEqualQueryResults = bool(finalCmp, "require_equal_query_results", true);
        fc.queries = stringList(finalCmp, "queries");
        c.finalComparison = fc;

        Map<String, Object> bor = nestedMap(b, "best_orc_rule");
        c.bestOrcRule = parseBestOrcRule(bor);

        Map<String, Object> runs = nestedMap(b, "runs");
        c.runs.warmupPerQuery = intVal(runs, "warmup_per_query", 5);
        c.runs.measuredPerQuery = intVal(runs, "measured_per_query", 30);
        for (Object o : anyList(runs, "concurrency_levels")) {
            if (o instanceof Number) {
                c.runs.concurrencyLevels.add(((Number) o).intValue());
            } else if (o != null) {
                try {
                    c.runs.concurrencyLevels.add(Integer.parseInt(o.toString()));
                } catch (NumberFormatException ignored) {
                    // skip non-numeric candidate
                }
            }
        }
        c.runs.concurrencyPolicy = str(runs, "concurrency_policy", "per_scale_capacity_aware");
        c.runs.cacheStatesNotAggregated = bool(runs, "cache_states_not_aggregated", true);
        c.runs.recordExplainPlans = bool(runs, "record_explain_plans", true);
        c.runs.compareCompleteResults = bool(runs, "compare_complete_results", true);
        c.runs.timingScope = str(runs, "timing_scope", "");
        c.runs.maxFetchRows = longVal(runs, "max_fetch_rows", 10_000_000L);

        Map<String, Object> metrics = nestedMap(b, "metrics");
        c.metrics.required = stringList(metrics, "required");
        c.metrics.optional = stringList(metrics, "optional");
        c.metrics.slaSuccessThresholdMs = longVal(metrics, "sla_success_threshold_ms", 3000L);

        Map<String, Object> validation = nestedMap(b, "configuration_validation");
        ConfigurationValidation cv = new ConfigurationValidation();
        cv.rejectQueryReferencingMissingColumn =
                bool(validation, "reject_query_referencing_missing_column", true);
        cv.requireReaderProfileOverrideApplied =
                bool(validation, "require_reader_profile_override_applied", true);
        cv.requireIdenticalCanonicalRowHashAcrossVariants =
                bool(validation, "require_identical_canonical_row_hash_across_variants", true);
        cv.requirePhysicalManifestForFinalEngineComparison =
                bool(validation, "require_physical_manifest_for_final_engine_comparison", true);
        cv.requireNestedParameterAlignment =
                bool(validation, "require_nested_parameter_alignment", true);
        c.configurationValidation = cv;
        return c;
    }

    private static BestOrcRule parseBestOrcRule(Map<String, Object> bor) {
        BestOrcRule rule = new BestOrcRule();
        if (bor == null || bor.isEmpty()) {
            return rule;
        }
        rule.objectiveRaw = str(bor, "objective", rule.objectiveRaw);
        List<String> parsed = extractBraceTokens(rule.objectiveRaw);
        if (!parsed.isEmpty()) {
            rule.objectiveQueries = parsed;
        }
        if (rule.objectiveRaw.toLowerCase().contains("p50")) {
            rule.objectiveStat = "p50";
        } else if (rule.objectiveRaw.toLowerCase().contains("avg")
                || rule.objectiveRaw.toLowerCase().contains("mean")) {
            rule.objectiveStat = "avg";
        }
        rule.slaGateRaw = str(bor, "sla_gate", rule.slaGateRaw);
        Double sla = extractLeadingDouble(rule.slaGateRaw);
        if (sla != null) {
            rule.slaSuccessRateMin = sla.doubleValue();
        }
        rule.tieBreakRaw = str(bor, "tie_break", rule.tieBreakRaw);
        rule.excludedCandidatesRaw = str(bor, "excluded_candidates", rule.excludedCandidatesRaw);
        rule.excludeZstd = rule.excludedCandidatesRaw == null
                || rule.excludedCandidatesRaw.toUpperCase().contains("ZSTD");
        return rule;
    }

    private static List<String> extractBraceTokens(String raw) {
        List<String> out = new ArrayList<>();
        if (raw == null) {
            return out;
        }
        int open = raw.indexOf('{');
        int close = raw.indexOf('}');
        if (open < 0 || close <= open) {
            return out;
        }
        String body = raw.substring(open + 1, close);
        for (String p : body.split(",")) {
            String t = p.trim();
            if (!t.isEmpty()) {
                out.add(t);
            }
        }
        return out;
    }

    private static Double extractLeadingDouble(String raw) {
        if (raw == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        boolean seenDigit = false;
        boolean seenDot = false;
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            if (Character.isDigit(c)) {
                sb.append(c);
                seenDigit = true;
            } else if (c == '.' && !seenDot && seenDigit) {
                sb.append(c);
                seenDot = true;
            } else if (seenDigit) {
                break;
            }
        }
        if (!seenDigit) {
            return null;
        }
        try {
            return Double.parseDouble(sb.toString());
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private static List<EngineExperiment> parseEngineExps(Map<String, Object> engineEx, String key) {
        List<EngineExperiment> out = new ArrayList<>();
        for (Object o : anyList(engineEx, key)) {
            if (!(o instanceof Map)) {
                continue;
            }
            Map<String, Object> e = (Map<String, Object>) o;
            EngineExperiment ee = new EngineExperiment();
            ee.id = str(e, "id", "");
            ee.property = optStr(e, "property");
            ee.values = anyList(e, "values");
            ee.queries = stringList(e, "queries");
            ee.requires = stringList(e, "requires");
            ee.engine = optStr(e, "engine");
            ee.llap = optBool(e, "llap");
            ee.cacheStates = stringList(e, "cache_states");
            for (Object f : anyList(e, "working_set_fractions")) {
                if (f instanceof Number) {
                    ee.workingSetFractions.add(((Number) f).doubleValue());
                }
            }
            ee.sessionOverrides = stringMap(e, "session_overrides");
            ee.onMissing = optStr(e, "on_missing");
            out.add(ee);
        }
        return out;
    }

    private static ColumnGenerator parseColumnGen(Map<String, Object> m) {
        ColumnGenerator g = new ColumnGenerator();
        g.generator = str(m, "generator", "");
        g.prefix = optStr(m, "prefix");
        g.cardinality = optLong(m, "cardinality");
        g.values = stringList(m, "values");
        for (Object w : anyList(m, "weights")) {
            if (w instanceof Number) {
                g.weights.add(((Number) w).doubleValue());
            }
        }
        g.correlatedWith = optStr(m, "correlated_with");
        g.elementType = optStr(m, "element_type");
        String src = optStr(m, "source");
        if (src == null) {
            src = optStr(m, "column");
        }
        g.source = src;
        g.template = optStr(m, "template");
        for (Object o : anyList(m, "offset_seconds")) {
            if (o instanceof Number) {
                g.offsetSeconds.add(((Number) o).intValue());
            }
        }
        return g;
    }

    @SuppressWarnings("unchecked")
    private static DatasetContract parseDataset(Map<String, Object> root) {
        DatasetContract c = new DatasetContract();
        Map<String, Object> d = nestedMap(root, "dataset");
        Map<String, Object> gen = nestedMap(d, "generation");
        Map<String, Object> ts = nestedMap(gen, "timestamp_range");

        c.dataset.id = str(d, "id", "");
        c.dataset.specVersion = intVal(d, "spec_version", 1);
        c.dataset.canonicalSchemaFile = str(d, "canonical_schema_file", "");
        c.dataset.canonicalRepresentation = str(d, "canonical_representation", "RAW_ARRAY");
        c.dataset.prohibitedData = str(d, "prohibited_data", "");

        for (Map.Entry<String, Object> e : nestedMap(d, "scales").entrySet()) {
            Map<String, Object> m = e.getValue() instanceof Map
                    ? (Map<String, Object>) e.getValue() : new LinkedHashMap<String, Object>();
            ScaleSpec s = new ScaleSpec();
            s.targetLogicalGib = longVal(m, "target_logical_gib", 0L);
            c.scales.put(e.getKey(), s);
        }

        c.generation.seed = longVal(gen, "seed", 0L);
        c.generation.algorithmVersion = str(gen, "algorithm_version", "");
        c.generation.batchRows = intVal(gen, "batch_rows", 1024);
        c.generation.baseTimeZone = str(gen, "base_time_zone", "UTC");
        c.generation.timestampFrom = str(ts, "from", "");
        c.generation.timestampToExclusive = str(ts, "to_exclusive", "");
        c.generation.sourceMaterialization = str(gen, "source_materialization", "");
        c.generation.rowCount = optLong(gen, "row_count");
        c.generation.strictCardinality = bool(gen, "strict_cardinality", true);
        c.generation.nullProfile = str(gen, "null_profile", "");
        c.generation.parallelArrayAlignment = str(gen, "parallel_array_alignment", "");

        for (Map.Entry<String, Object> e : nestedMap(d, "columns").entrySet()) {
            if (e.getValue() instanceof Map) {
                c.columns.put(e.getKey(), parseColumnGen((Map<String, Object>) e.getValue()));
            }
        }

        Map<String, Object> params = nestedMap(d, "parameters");
        c.parameters.internalRepresentation = str(params, "internal_representation", "");
        c.parameters.defaultOrderProfile = str(params, "default_order_profile", "fixed");
        c.parameters.storageRepresentation = str(params, "storage_representation", "");
        c.parameters.rawPhysicalColumns = stringList(params, "raw_physical_columns");
        for (Map.Entry<String, Object> e : nestedMap(params, "catalog").entrySet()) {
            if (e.getValue() instanceof Map) {
                c.parameters.catalog.put(e.getKey(), parseColumnGen((Map<String, Object>) e.getValue()));
            }
        }

        for (Map.Entry<String, Object> e : nestedMap(d, "json_templates").entrySet()) {
            if (e.getValue() instanceof Map) {
                c.jsonTemplates.put(e.getKey(), (Map<String, Object>) e.getValue());
            }
        }

        for (Map.Entry<String, Object> e : nestedMap(d, "reference_datasets").entrySet()) {
            Map<String, Object> m = e.getValue() instanceof Map
                    ? (Map<String, Object>) e.getValue() : new LinkedHashMap<String, Object>();
            ReferenceDataset rd = new ReferenceDataset();
            rd.enabled = bool(m, "enabled", true);
            rd.joinKey = optStr(m, "join_key");
            rd.columns = stringList(m, "columns");
            rd.rowCount = optInt(m, "row_count");
            c.referenceDatasets.put(e.getKey(), rd);
        }

        Map<String, Object> qs = nestedMap(d, "query_samples");
        c.querySamples.keys = stringList(qs, "keys");
        c.querySamples.selection = str(qs, "selection", "");
        c.querySamples.cases = stringList(qs, "cases");
        c.querySamples.valuesPerCase = intVal(qs, "values_per_case", 50);
        c.querySamples.partitionColumnsDerived = stringMap(qs, "partition_columns_derived");

        Map<String, Object> validation = nestedMap(d, "validation");
        c.validation.checkRowCount = bool(validation, "check_row_count", true);
        c.validation.checkNameValueAlignment = bool(validation, "check_name_value_alignment", true);
        c.validation.checkDuplicateNames = bool(validation, "check_duplicate_names", true);
        c.validation.checkCanonicalRowHash = bool(validation, "check_canonical_row_hash", true);
        c.validation.checkJsonParseable = bool(validation, "check_json_parseable", true);
        c.validation.checkNullHandling = bool(validation, "check_null_handling", true);
        c.validation.failOnViolation = bool(validation, "fail_on_violation", true);
        c.validation.checkNestedTypes = bool(validation, "check_nested_types", true);
        return c;
    }

    private static EnvironmentContract parseEnvironment(Map<String, Object> root) {
        EnvironmentContract c = new EnvironmentContract();
        Map<String, Object> e = nestedMap(root, "environment");
        c.environment.id = str(e, "id", "");
        c.environment.specVersion = intVal(e, "spec_version", 1);
        c.environment.timeZone = str(e, "time_zone", "UTC");

        Map<String, Object> sec = nestedMap(e, "security");
        c.environment.security.enabled = bool(sec, "enabled", true);
        c.environment.security.mechanism = str(sec, "mechanism", "kerberos");
        c.environment.security.runnerPrincipal = optStr(sec, "runner_principal");
        c.environment.security.keytabPath = optStr(sec, "keytab_path");
        c.environment.security.kinitVerified = optBool(sec, "kinit_verified");
        c.environment.security.realm = optStr(sec, "realm");
        c.environment.security.hiveJdbcAuth = optStr(sec, "hive_jdbc_auth");

        for (Map.Entry<String, Object> v : nestedMap(e, "versions").entrySet()) {
            c.environment.versions.put(v.getKey(),
                    v.getValue() == null ? Optional.<String>empty() : Optional.of(v.getValue().toString()));
        }

        Map<String, Object> hdfs = nestedMap(e, "hdfs");
        Map<String, Object> sc = nestedMap(hdfs, "short_circuit_read");
        Map<String, Object> cc = nestedMap(hdfs, "centralized_cache");
        c.environment.hdfs.namenodeUri = optStr(hdfs, "namenode_uri");
        c.environment.hdfs.defaultReplication = optInt(hdfs, "default_replication");
        c.environment.hdfs.blockSizeMib = optInt(hdfs, "block_size_mib");
        c.environment.hdfs.freeSpaceGib = optLong(hdfs, "free_space_gib");
        c.environment.hdfs.workDir = str(hdfs, "work_dir", "hdfs://orc_test");
        c.environment.hdfs.writeVerified = optBool(hdfs, "write_verified");
        c.environment.hdfs.shortCircuitSupported = optBool(sc, "supported");
        c.environment.hdfs.shortCircuitEnabled = optBool(sc, "enabled");
        c.environment.hdfs.shortCircuitMutableByRunner = bool(sc, "mutable_by_runner", false);
        c.environment.hdfs.centralizedCacheSupported = optBool(cc, "supported");
        c.environment.hdfs.centralizedCacheMutableByRunner = bool(cc, "mutable_by_runner", false);
        c.environment.hdfs.dataLocalityVerifiable = optBool(hdfs, "data_locality_verifiable");

        Map<String, Object> yarn = nestedMap(e, "yarn");
        c.environment.yarn.resourceManager = optStr(yarn, "resource_manager");
        c.environment.yarn.resourceManagerStandby = optStr(yarn, "resource_manager_standby");
        c.environment.yarn.queue = str(yarn, "queue", "default");
        c.environment.yarn.availableMemoryGib = optLong(yarn, "available_memory_gib");
        c.environment.yarn.availableVcores = optInt(yarn, "available_vcores");

        Map<String, Object> spark = nestedMap(e, "spark");
        c.environment.spark.master = str(spark, "master", "yarn");
        c.environment.spark.deployMode = str(spark, "deploy_mode", "cluster");
        c.environment.spark.executorInstances = optInt(spark, "executor_instances");
        c.environment.spark.executorCores = optInt(spark, "executor_cores");
        c.environment.spark.executorMemoryGib = optInt(spark, "executor_memory_gib");
        c.environment.spark.enableHiveSupport = optBool(spark, "enable_hive_support");

        Map<String, Object> hive = nestedMap(e, "hive");
        Map<String, Object> conn = nestedMap(hive, "connection");
        c.environment.hive.jdbcUrl = optStr(hive, "jdbc_url");
        c.environment.hive.metastoreUri = optStr(hive, "metastore_uri");
        c.environment.hive.tezAvailable = bool(hive, "tez_available", true);
        c.environment.hive.llapAvailable = optBool(hive, "llap_available");
        Object llapDaemons = hive.get("llap_daemons");
        c.environment.hive.llapDaemons = llapDaemons == null ? null : llapDaemons.toString();
        c.environment.hive.llapCacheGib = optDouble(hive, "llap_cache_gib");
        c.environment.hive.materializedViewsSupported = optBool(hive, "materialized_views_supported");
        c.environment.hive.bucketVersion = optStr(hive, "bucket_version");
        c.environment.hive.llapAppName = str(hive, "llap_app_name", "llap0");
        c.environment.hive.llapExecutionMode = str(hive, "llap_execution_mode", "only");
        c.environment.hive.llapDaemonsRunningVerified = optBool(hive, "llap_daemons_running_verified");
        c.environment.hive.jdbcConnectionVerified = bool(hive, "jdbc_connection_verified", false);
        c.environment.hive.llapConfigured = bool(hive, "llap_configured", false);
        c.environment.hive.ddlExecutionVerified = bool(hive, "ddl_execution_verified", false);
        c.environment.hive.connectionJdbcUrl = optStr(conn, "jdbc_url");
        c.environment.hive.observedProperties = stringMap(hive, "observed_properties");

        Map<String, Object> ops = nestedMap(e, "operations");
        c.environment.operations.permissionToResetCaches = bool(ops, "permission_to_reset_caches", false);
        c.environment.operations.permissionToChangeClusterConfig =
                bool(ops, "permission_to_change_cluster_config", false);
        c.environment.operations.metricsExportAvailable = optBool(ops, "metrics_export_available");
        c.environment.operations.allowProductionClusterChanges =
                bool(ops, "allow_production_cluster_changes", false);

        Map<String, Object> pre = nestedMap(e, "preflight");
        c.environment.preflight.requireExactVersions = bool(pre, "require_exact_versions", true);
        c.environment.preflight.requireHdfsCapacityCheck = bool(pre, "require_hdfs_capacity_check", true);
        c.environment.preflight.requireFeatureProbe = bool(pre, "require_feature_probe", true);
        c.environment.preflight.failIfRequiredCapabilityMissing =
                bool(pre, "fail_if_required_capability_missing", false);
        c.environment.preflight.markUnavailableExperimentsAsSkipped =
                bool(pre, "mark_unavailable_experiments_as_skipped", true);
        c.environment.preflight.requireTezVersionProbe = bool(pre, "require_tez_version_probe", false);
        c.environment.preflight.requireJava8RuntimeCheck = bool(pre, "require_java8_runtime_check", true);
        return c;
    }

    @SuppressWarnings("unchecked")
    private static QuerySuiteContract parseQueries(Map<String, Object> root) {
        QuerySuiteContract c = new QuerySuiteContract();
        Map<String, Object> qs = nestedMap(root, "query_suite");
        c.querySuite.id = str(qs, "id", "");
        c.querySuite.specVersion = intVal(qs, "spec_version", 1);
        c.querySuite.defaults = stringMap(qs, "defaults");
        c.querySuite.parameterSource = str(qs, "parameter_source", "");
        c.querySuite.dialects = stringList(qs, "dialects");
        c.querySuite.baselineRepresentation = str(qs, "baseline_representation", "RAW_ARRAY");

        // queries live under query_suite in configs_final
        for (Object o : anyList(qs, "queries")) {
            if (!(o instanceof Map)) {
                continue;
            }
            Map<String, Object> e = (Map<String, Object>) o;
            QuerySpec q = new QuerySpec();
            q.id = str(e, "id", "");
            q.description = str(e, "description", "");
            q.representations = stringList(e, "representations");
            q.sql = optStr(e, "sql");
            q.dialects = stringList(e, "dialects");
            q.requires = stringList(e, "requires");
            c.queries.add(q);
        }
        return c;
    }

    /** Untyped snapshot of configs_final / conf/. */
    public static final class RawConfig {
        public Map<String, Object> environment = new LinkedHashMap<>();
        public Map<String, Object> benchmark = new LinkedHashMap<>();
        public Map<String, Object> dataset = new LinkedHashMap<>();
        public Map<String, Object> querySuite = new LinkedHashMap<>();
        public String schemaOrc;
    }
}
