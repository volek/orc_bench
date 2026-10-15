package orcbench.config;

import orcbench.model.Models.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static orcbench.config.YamlSupport.*;

public final class ConfigLoader {
    private ConfigLoader() {}

    public static LoadedConfig load(String configDir, String envOverride) throws Exception {
        Map<String, Object> benchRaw = loadMap(resolvePath(configDir, "benchmark.yaml"));
        Map<String, Object> datasetRaw = loadMap(resolvePath(configDir, "dataset.yaml"));
        Map<String, Object> envRaw = loadMap(resolvePath(configDir, "environment.yaml"));
        if (envOverride != null && !envOverride.isEmpty()) {
            envRaw = deepMerge(envRaw, loadMap(envOverride));
        }
        Map<String, Object> queryRaw = loadMap(resolvePath(configDir, "query-suite.yaml"));
        String schemaText = loadText(resolvePath(configDir, "schema.orc.txt"));

        LoadedConfig cfg = new LoadedConfig();
        cfg.benchmark = parseBenchmark(benchRaw);
        cfg.dataset = parseDataset(datasetRaw);
        cfg.environment = parseEnvironment(envRaw);
        cfg.queries = parseQueries(queryRaw);
        cfg.schemaText = schemaText;
        cfg.configDir = configDir;
        return cfg;
    }

    @SuppressWarnings("unchecked")
    private static BenchmarkContract parseBenchmark(Map<String, Object> root) {
        BenchmarkContract c = new BenchmarkContract();
        Map<String, Object> b = nestedMap(root, "benchmark");
        Map<String, Object> baseline = nestedMap(root, "physical_baseline");
        Map<String, Object> orcBase = nestedMap(baseline, "orc");
        Map<String, Object> bloom = nestedMap(orcBase, "bloom");
        Map<String, Object> runs = nestedMap(root, "runs");
        Map<String, Object> metrics = nestedMap(root, "metrics");
        Map<String, Object> validation = nestedMap(root, "configuration_validation");
        Map<String, Object> exec = nestedMap(root, "execution_baselines");
        Map<String, Object> engineEx = nestedMap(root, "engine_experiments");
        Map<String, Object> finalCmp = nestedMap(root, "final_comparison");
        Map<String, Object> inputs = nestedMap(root, "inputs");

        BenchmarkSection bs = new BenchmarkSection();
        bs.id = str(b, "id", "");
        bs.specVersion = intVal(b, "spec_version", 1);
        bs.slaMeanLatencyMs = longVal(b, "sla_mean_latency_ms", 3000L);
        bs.isolation = str(b, "isolation", "");
        bs.requireResultEquivalence = bool(b, "require_result_equivalence", true);
        bs.hdfsRoot = str(b, "hdfs_root", "/benchmarks/audit_orc");
        bs.generatedHiveDdl = str(b, "generated_hive_ddl", "generated/hive-table.sql");
        bs.derivedPartitionColumns = stringList(b, "derived_partition_columns");
        bs.llapRunningVerified = optBool(b, "llap_running_verified");
        bs.tezVersion = str(b, "tez_version", "");
        bs.llapConfiguredMode = str(b, "llap_configured_mode", "only");
        c.benchmark = bs;

        InputsSection is = new InputsSection();
        is.schema = str(inputs, "schema", "");
        is.dataset = str(inputs, "dataset", "");
        is.environment = str(inputs, "environment", "");
        is.queries = str(inputs, "queries", "");
        c.inputs = is;

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

        Map<String, Object> reps = nestedMap(root, "representations");
        for (Map.Entry<String, Object> e : reps.entrySet()) {
            Map<String, Object> m = e.getValue() instanceof Map ? (Map<String, Object>) e.getValue() : new LinkedHashMap<>();
            RepresentationSpec rs = new RepresentationSpec();
            rs.transform = str(m, "transform", e.getKey());
            rs.paramsType = optStr(m, "params_type");
            rs.columns = stringList(m, "columns");
            rs.keepOriginalParameters = optBool(m, "keep_original_parameters");
            rs.lossless = optBool(m, "lossless");
            rs.immutableSourceBaseline = optBool(m, "immutable_source_baseline");
            c.representations.put(e.getKey(), rs);
        }

        for (Object o : anyList(root, "physical_experiments")) {
            if (!(o instanceof Map)) continue;
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
            c.physicalExperiments.add(pe);
        }

        c.executionBaselines.spark = stringMap(exec, "spark");
        Map<String, String> hive = stringMap(exec, "hive");
        hive.remove("comment");
        c.executionBaselines.hive = hive;

        c.engineExperiments.spark = parseEngineExps(engineEx, "spark");
        c.engineExperiments.hive = parseEngineExps(engineEx, "hive");

        c.finalComparison.physicalDataset = str(finalCmp, "physical_dataset", "BEST_ORC");
        c.finalComparison.engines = stringList(finalCmp, "engines");
        c.finalComparison.requireIdenticalFileManifest = bool(finalCmp, "require_identical_file_manifest", true);
        c.finalComparison.requireEqualQueryResults = bool(finalCmp, "require_equal_query_results", true);

        c.runs.warmupPerQuery = intVal(runs, "warmup_per_query", 5);
        c.runs.measuredPerQuery = intVal(runs, "measured_per_query", 30);
        for (Object o : anyList(runs, "concurrency_levels")) {
            if (o instanceof Number) c.runs.concurrencyLevels.add(((Number) o).intValue());
        }
        c.runs.cacheStatesNotAggregated = bool(runs, "cache_states_not_aggregated", true);
        c.runs.recordExplainPlans = bool(runs, "record_explain_plans", true);
        c.runs.compareCompleteResults = bool(runs, "compare_complete_results", true);
        c.runs.timingScope = str(runs, "timing_scope", "");

        c.metrics.required = stringList(metrics, "required");
        c.metrics.optional = stringList(metrics, "optional");
        c.metrics.slaSuccessThresholdMs = longVal(metrics, "sla_success_threshold_ms", 3000L);

        c.configurationValidation.rejectQueryReferencingMissingColumn =
                bool(validation, "reject_query_referencing_missing_column", true);
        c.configurationValidation.requireReaderProfileOverrideApplied =
                bool(validation, "require_reader_profile_override_applied", true);
        c.configurationValidation.requireIdenticalCanonicalRowHashAcrossVariants =
                bool(validation, "require_identical_canonical_row_hash_across_variants", true);
        c.configurationValidation.requirePhysicalManifestForFinalEngineComparison =
                bool(validation, "require_physical_manifest_for_final_engine_comparison", true);
        c.configurationValidation.requireNestedParameterAlignment =
                bool(validation, "require_nested_parameter_alignment", true);
        return c;
    }

    @SuppressWarnings("unchecked")
    private static List<EngineExperiment> parseEngineExps(Map<String, Object> engineEx, String key) {
        List<EngineExperiment> out = new ArrayList<>();
        for (Object o : anyList(engineEx, key)) {
            if (!(o instanceof Map)) continue;
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
                if (f instanceof Number) ee.workingSetFractions.add(((Number) f).doubleValue());
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
            if (w instanceof Number) g.weights.add(((Number) w).doubleValue());
        }
        g.correlatedWith = optStr(m, "correlated_with");
        g.elementType = optStr(m, "element_type");
        String src = optStr(m, "source");
        if (src == null) src = optStr(m, "column");
        g.source = src;
        g.template = optStr(m, "template");
        for (Object o : anyList(m, "offset_seconds")) {
            if (o instanceof Number) g.offsetSeconds.add(((Number) o).intValue());
        }
        return g;
    }

    @SuppressWarnings("unchecked")
    private static DatasetContract parseDataset(Map<String, Object> root) {
        DatasetContract c = new DatasetContract();
        Map<String, Object> d = nestedMap(root, "dataset");
        Map<String, Object> gen = nestedMap(root, "generation");
        Map<String, Object> ts = nestedMap(gen, "timestamp_range");

        c.dataset.id = str(d, "id", "");
        c.dataset.canonicalSchemaFile = str(d, "canonical_schema_file", "");
        c.dataset.canonicalRepresentation = str(d, "canonical_representation", "RAW_ARRAY");
        c.dataset.prohibitedData = str(d, "prohibited_data", "");

        for (Map.Entry<String, Object> e : nestedMap(root, "scales").entrySet()) {
            Map<String, Object> m = e.getValue() instanceof Map ? (Map<String, Object>) e.getValue() : new LinkedHashMap<>();
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

        for (Map.Entry<String, Object> e : nestedMap(root, "columns").entrySet()) {
            if (e.getValue() instanceof Map) {
                c.columns.put(e.getKey(), parseColumnGen((Map<String, Object>) e.getValue()));
            }
        }

        Map<String, Object> params = nestedMap(root, "parameters");
        c.parameters.internalRepresentation = str(params, "internal_representation", "");
        c.parameters.defaultOrderProfile = str(params, "default_order_profile", "fixed");
        c.parameters.storageRepresentation = str(params, "storage_representation", "");
        c.parameters.rawPhysicalColumns = stringList(params, "raw_physical_columns");
        for (Map.Entry<String, Object> e : nestedMap(params, "catalog").entrySet()) {
            if (e.getValue() instanceof Map) {
                c.parameters.catalog.put(e.getKey(), parseColumnGen((Map<String, Object>) e.getValue()));
            }
        }

        for (Map.Entry<String, Object> e : nestedMap(root, "json_templates").entrySet()) {
            if (e.getValue() instanceof Map) {
                c.jsonTemplates.put(e.getKey(), (Map<String, Object>) e.getValue());
            }
        }

        for (Map.Entry<String, Object> e : nestedMap(root, "reference_datasets").entrySet()) {
            Map<String, Object> m = e.getValue() instanceof Map ? (Map<String, Object>) e.getValue() : new LinkedHashMap<>();
            ReferenceDataset rd = new ReferenceDataset();
            rd.enabled = bool(m, "enabled", true);
            rd.joinKey = optStr(m, "join_key");
            rd.columns = stringList(m, "columns");
            rd.rowCount = optInt(m, "row_count");
            c.referenceDatasets.put(e.getKey(), rd);
        }

        Map<String, Object> qs = nestedMap(root, "query_samples");
        c.querySamples.keys = stringList(qs, "keys");
        c.querySamples.selection = str(qs, "selection", "");
        c.querySamples.cases = stringList(qs, "cases");
        c.querySamples.valuesPerCase = intVal(qs, "values_per_case", 50);
        c.querySamples.partitionColumnsDerived = stringMap(qs, "partition_columns_derived");

        Map<String, Object> validation = nestedMap(root, "validation");
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
        c.environment.timeZone = str(e, "time_zone", "UTC");

        for (Map.Entry<String, Object> v : nestedMap(e, "versions").entrySet()) {
            c.environment.versions.put(v.getKey(),
                    v.getValue() == null ? Optional.empty() : Optional.of(v.getValue().toString()));
        }

        Map<String, Object> hdfs = nestedMap(e, "hdfs");
        Map<String, Object> sc = nestedMap(hdfs, "short_circuit_read");
        Map<String, Object> cc = nestedMap(hdfs, "centralized_cache");
        c.environment.hdfs.namenodeUri = optStr(hdfs, "namenode_uri");
        c.environment.hdfs.defaultReplication = optInt(hdfs, "default_replication");
        c.environment.hdfs.blockSizeMib = optInt(hdfs, "block_size_mib");
        c.environment.hdfs.freeSpaceGib = optLong(hdfs, "free_space_gib");
        c.environment.hdfs.shortCircuitSupported = optBool(sc, "supported");
        c.environment.hdfs.shortCircuitEnabled = optBool(sc, "enabled");
        c.environment.hdfs.shortCircuitMutableByRunner = bool(sc, "mutable_by_runner", false);
        c.environment.hdfs.centralizedCacheSupported = optBool(cc, "supported");
        c.environment.hdfs.centralizedCacheMutableByRunner = bool(cc, "mutable_by_runner", false);
        c.environment.hdfs.dataLocalityVerifiable = optBool(hdfs, "data_locality_verifiable");

        Map<String, Object> yarn = nestedMap(e, "yarn");
        c.environment.yarn.resourceManager = optStr(yarn, "resource_manager");
        c.environment.yarn.queue = str(yarn, "queue", "benchmark");
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
        c.environment.hive.tezAvailable = bool(hive, "tez_available", true);
        c.environment.hive.llapAvailable = optBool(hive, "llap_available");
        c.environment.hive.llapDaemons = optBool(hive, "llap_daemons");
        c.environment.hive.llapCacheGib = optDouble(hive, "llap_cache_gib");
        c.environment.hive.materializedViewsSupported = optBool(hive, "materialized_views_supported");
        c.environment.hive.bucketVersion = optStr(hive, "bucket_version");
        c.environment.hive.llapAppName = str(hive, "llap_app_name", "llap0");
        c.environment.hive.llapExecutionMode = str(hive, "llap_execution_mode", "only");
        c.environment.hive.llapDaemonsRunningVerified = optBool(hive, "llap_daemons_running_verified");
        c.environment.hive.jdbcConnectionVerified = bool(hive, "jdbc_connection_verified", false);
        c.environment.hive.llapConfigured = bool(hive, "llap_configured", false);
        c.environment.hive.connectionJdbcUrl = optStr(conn, "jdbc_url");
        c.environment.hive.observedProperties = stringMap(hive, "observed_properties");

        Map<String, Object> ops = nestedMap(e, "operations");
        c.environment.operations.permissionToResetCaches = bool(ops, "permission_to_reset_caches", false);
        c.environment.operations.permissionToChangeClusterConfig = bool(ops, "permission_to_change_cluster_config", false);
        c.environment.operations.metricsExportAvailable = optBool(ops, "metrics_export_available");
        c.environment.operations.allowProductionClusterChanges = bool(ops, "allow_production_cluster_changes", false);

        Map<String, Object> pre = nestedMap(e, "preflight");
        c.environment.preflight.requireExactVersions = bool(pre, "require_exact_versions", true);
        c.environment.preflight.requireHdfsCapacityCheck = bool(pre, "require_hdfs_capacity_check", true);
        c.environment.preflight.requireFeatureProbe = bool(pre, "require_feature_probe", true);
        c.environment.preflight.failIfRequiredCapabilityMissing = bool(pre, "fail_if_required_capability_missing", false);
        c.environment.preflight.markUnavailableExperimentsAsSkipped = bool(pre, "mark_unavailable_experiments_as_skipped", true);
        c.environment.preflight.requireTezVersionProbe = bool(pre, "require_tez_version_probe", false);
        c.environment.preflight.requireJava8RuntimeCheck = bool(pre, "require_java8_runtime_check", true);
        return c;
    }

    @SuppressWarnings("unchecked")
    private static QuerySuiteContract parseQueries(Map<String, Object> root) {
        QuerySuiteContract c = new QuerySuiteContract();
        Map<String, Object> qs = nestedMap(root, "query_suite");
        c.querySuite.id = str(qs, "id", "");
        c.querySuite.defaults = stringMap(qs, "defaults");
        c.querySuite.parameterSource = str(qs, "parameter_source", "");
        c.querySuite.dialects = stringList(qs, "dialects");
        c.querySuite.baselineRepresentation = str(qs, "baseline_representation", "RAW_ARRAY");

        for (Object o : anyList(root, "queries")) {
            if (!(o instanceof Map)) continue;
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
}
