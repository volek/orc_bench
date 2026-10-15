package orcbench.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Runtime models loaded from the YAML contract. */
public final class Models {
    private Models() {}

    public static final class CliArgs {
        public String configDir = "conf";
        public String scale = "S";
        public String phase = "all";
        public List<String> experiments = new ArrayList<>();
        public List<String> engines = new ArrayList<>(java.util.Arrays.asList("spark", "hive_tez", "hive_llap"));
        public String envOverride;
        public List<String> retain = new ArrayList<>();
        public boolean cleanup;
        public String resultsDir = "results";
        public boolean dryRun;
    }

    public static final class LoadedConfig {
        public BenchmarkContract benchmark;
        public DatasetContract dataset;
        public EnvironmentContract environment;
        public QuerySuiteContract queries;
        public String schemaText;
        public String configDir;
    }

    public static final class BenchmarkContract {
        public BenchmarkSection benchmark;
        public InputsSection inputs;
        public PhysicalBaseline physicalBaseline;
        public Map<String, RepresentationSpec> representations = new LinkedHashMap<>();
        public List<PhysicalExperiment> physicalExperiments = new ArrayList<>();
        public ExecutionBaselines executionBaselines = new ExecutionBaselines();
        public EngineExperiments engineExperiments = new EngineExperiments();
        public FinalComparison finalComparison = new FinalComparison();
        public RunsSection runs = new RunsSection();
        public MetricsSection metrics = new MetricsSection();
        public ConfigurationValidation configurationValidation = new ConfigurationValidation();
    }

    public static final class BenchmarkSection {
        public String id;
        public int specVersion;
        public long slaMeanLatencyMs = 3000L;
        public String isolation;
        public boolean requireResultEquivalence = true;
        public String hdfsRoot = "/benchmarks/audit_orc";
        public String generatedHiveDdl = "generated/hive-table.sql";
        public List<String> derivedPartitionColumns = new ArrayList<>();
        public Boolean llapRunningVerified;
        public String tezVersion;
        public String llapConfiguredMode = "only";
    }

    public static final class InputsSection {
        public String schema;
        public String dataset;
        public String environment;
        public String queries;
    }

    public static final class PhysicalBaseline {
        public String id = "RAW_ARRAY_BASE";
        public String representation = "RAW_ARRAY";
        public List<String> partitioning = new ArrayList<>();
        public Integer bucketing;
        public List<String> sorting = new ArrayList<>();
        public int targetFileSizeMib = 256;
        public OrcBaseline orc = new OrcBaseline();
    }

    public static final class OrcBaseline {
        public String compression = "SNAPPY";
        public int stripeSizeMib = 64;
        public boolean createIndex = true;
        public int rowIndexStride = 10000;
        public List<String> bloomColumns = new ArrayList<>();
        public double bloomFpp = 0.01;
    }

    public static final class RepresentationSpec {
        public String transform;
        public String paramsType;
        public List<String> columns = new ArrayList<>();
        public Boolean keepOriginalParameters;
        public Boolean lossless;
        public Boolean immutableSourceBaseline;
    }

    public static final class PhysicalExperiment {
        public String id;
        public String kind;
        public String basedOn;
        public String factor;
        public List<Object> values = new ArrayList<>();
        public List<Object> variants = new ArrayList<>();
        public List<String> queries = new ArrayList<>();
        public List<String> requires = new ArrayList<>();
        public List<String> requiresBloomColumns = new ArrayList<>();
        public List<String> requiresForCentralized = new ArrayList<>();
        public String representation;
        public String bucketKey;
    }

    public static final class ExecutionBaselines {
        public Map<String, String> spark = new LinkedHashMap<>();
        public Map<String, String> hive = new LinkedHashMap<>();
    }

    public static final class EngineExperiments {
        public List<EngineExperiment> spark = new ArrayList<>();
        public List<EngineExperiment> hive = new ArrayList<>();
    }

    public static final class EngineExperiment {
        public String id;
        public String property;
        public List<Object> values = new ArrayList<>();
        public List<String> queries = new ArrayList<>();
        public List<String> requires = new ArrayList<>();
        public String engine;
        public Boolean llap;
        public List<String> cacheStates = new ArrayList<>();
        public List<Double> workingSetFractions = new ArrayList<>();
        public Map<String, String> sessionOverrides = new LinkedHashMap<>();
        public String onMissing;
    }

    public static final class FinalComparison {
        public String physicalDataset = "BEST_ORC";
        public List<String> engines = new ArrayList<>();
        public boolean requireIdenticalFileManifest = true;
        public boolean requireEqualQueryResults = true;
    }

    public static final class RunsSection {
        public int warmupPerQuery = 5;
        public int measuredPerQuery = 30;
        public List<Integer> concurrencyLevels = new ArrayList<>();
        public boolean cacheStatesNotAggregated = true;
        public boolean recordExplainPlans = true;
        public boolean compareCompleteResults = true;
        public String timingScope;
    }

    public static final class MetricsSection {
        public List<String> required = new ArrayList<>();
        public List<String> optional = new ArrayList<>();
        public long slaSuccessThresholdMs = 3000L;
    }

    public static final class ConfigurationValidation {
        public boolean rejectQueryReferencingMissingColumn = true;
        public boolean requireReaderProfileOverrideApplied = true;
        public boolean requireIdenticalCanonicalRowHashAcrossVariants = true;
        public boolean requirePhysicalManifestForFinalEngineComparison = true;
        public boolean requireNestedParameterAlignment = true;
    }

    public static final class DatasetContract {
        public DatasetSection dataset = new DatasetSection();
        public Map<String, ScaleSpec> scales = new LinkedHashMap<>();
        public GenerationSection generation = new GenerationSection();
        public Map<String, ColumnGenerator> columns = new LinkedHashMap<>();
        public ParametersSection parameters = new ParametersSection();
        public Map<String, Map<String, Object>> jsonTemplates = new LinkedHashMap<>();
        public Map<String, ReferenceDataset> referenceDatasets = new LinkedHashMap<>();
        public QuerySamplesSection querySamples = new QuerySamplesSection();
        public ValidationSection validation = new ValidationSection();
    }

    public static final class DatasetSection {
        public String id;
        public String canonicalSchemaFile;
        public String canonicalRepresentation = "RAW_ARRAY";
        public String prohibitedData;
    }

    public static final class ScaleSpec {
        public long targetLogicalGib;
    }

    public static final class GenerationSection {
        public long seed;
        public String algorithmVersion;
        public int batchRows = 1024;
        public String baseTimeZone = "UTC";
        public String timestampFrom;
        public String timestampToExclusive;
        public String sourceMaterialization;
        public Long rowCount;
        public boolean strictCardinality = true;
        public String nullProfile;
        public String parallelArrayAlignment;
    }

    public static final class ColumnGenerator implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public String generator;
        public String prefix;
        public Long cardinality;
        public List<String> values = new ArrayList<>();
        public List<Double> weights = new ArrayList<>();
        public String correlatedWith;
        public String elementType;
        public String source;
        public String template;
        public List<Integer> offsetSeconds = new ArrayList<>();
    }

    public static final class ParametersSection {
        public String internalRepresentation;
        public String defaultOrderProfile = "fixed";
        public Map<String, ColumnGenerator> catalog = new LinkedHashMap<>();
        public String storageRepresentation;
        public List<String> rawPhysicalColumns = new ArrayList<>();
    }

    public static final class ReferenceDataset {
        public boolean enabled = true;
        public String joinKey;
        public List<String> columns = new ArrayList<>();
        public Integer rowCount;
    }

    public static final class QuerySamplesSection {
        public List<String> keys = new ArrayList<>();
        public String selection;
        public List<String> cases = new ArrayList<>();
        public int valuesPerCase = 50;
        public Map<String, String> partitionColumnsDerived = new LinkedHashMap<>();
    }

    public static final class ValidationSection {
        public boolean checkRowCount = true;
        public boolean checkNameValueAlignment = true;
        public boolean checkDuplicateNames = true;
        public boolean checkCanonicalRowHash = true;
        public boolean checkJsonParseable = true;
        public boolean checkNullHandling = true;
        public boolean failOnViolation = true;
        public boolean checkNestedTypes = true;
    }

    public static final class EnvironmentContract {
        public EnvironmentSection environment = new EnvironmentSection();
    }

    public static final class EnvironmentSection {
        public String id;
        public String timeZone = "UTC";
        public Map<String, Optional<String>> versions = new LinkedHashMap<>();
        public HdfsEnv hdfs = new HdfsEnv();
        public YarnEnv yarn = new YarnEnv();
        public SparkEnv spark = new SparkEnv();
        public HiveEnv hive = new HiveEnv();
        public OperationsEnv operations = new OperationsEnv();
        public PreflightEnv preflight = new PreflightEnv();
    }

    public static final class HdfsEnv {
        public String namenodeUri;
        public Integer defaultReplication;
        public Integer blockSizeMib;
        public Long freeSpaceGib;
        public Boolean shortCircuitSupported;
        public Boolean shortCircuitEnabled;
        public boolean shortCircuitMutableByRunner;
        public Boolean centralizedCacheSupported;
        public boolean centralizedCacheMutableByRunner;
        public Boolean dataLocalityVerifiable;
    }

    public static final class YarnEnv {
        public String resourceManager;
        public String queue = "benchmark";
        public Long availableMemoryGib;
        public Integer availableVcores;
    }

    public static final class SparkEnv {
        public String master = "yarn";
        public String deployMode = "cluster";
        public Integer executorInstances;
        public Integer executorCores;
        public Integer executorMemoryGib;
        public Boolean enableHiveSupport;
    }

    public static final class HiveEnv {
        public String jdbcUrl;
        public boolean tezAvailable = true;
        public Boolean llapAvailable;
        public Boolean llapDaemons;
        public Double llapCacheGib;
        public Boolean materializedViewsSupported;
        public String bucketVersion;
        public String llapAppName = "llap0";
        public String llapExecutionMode = "only";
        public Boolean llapDaemonsRunningVerified;
        public boolean jdbcConnectionVerified;
        public boolean llapConfigured;
        public String connectionJdbcUrl;
        public Map<String, String> observedProperties = new LinkedHashMap<>();
    }

    public static final class OperationsEnv {
        public boolean permissionToResetCaches;
        public boolean permissionToChangeClusterConfig;
        public Boolean metricsExportAvailable;
        public boolean allowProductionClusterChanges;
    }

    public static final class PreflightEnv {
        public boolean requireExactVersions = true;
        public boolean requireHdfsCapacityCheck = true;
        public boolean requireFeatureProbe = true;
        public boolean failIfRequiredCapabilityMissing;
        public boolean markUnavailableExperimentsAsSkipped = true;
        public boolean requireTezVersionProbe;
        public boolean requireJava8RuntimeCheck = true;
    }

    public static final class QuerySuiteContract {
        public QuerySuiteSection querySuite = new QuerySuiteSection();
        public List<QuerySpec> queries = new ArrayList<>();
    }

    public static final class QuerySuiteSection {
        public String id;
        public Map<String, String> defaults = new LinkedHashMap<>();
        public String parameterSource;
        public List<String> dialects = new ArrayList<>();
        public String baselineRepresentation = "RAW_ARRAY";
    }

    public static final class QuerySpec {
        public String id;
        public String description;
        public List<String> representations = new ArrayList<>();
        public String sql;
        public List<String> dialects = new ArrayList<>();
        public List<String> requires = new ArrayList<>();
    }

    public static final class OrcWriteSettings {
        public String compression;
        public int stripeSizeMib;
        public boolean createIndex;
        public int rowIndexStride;
        public List<String> bloomColumns = new ArrayList<>();
        public double bloomFpp;

        public OrcWriteSettings copy() {
            OrcWriteSettings c = new OrcWriteSettings();
            c.compression = compression;
            c.stripeSizeMib = stripeSizeMib;
            c.createIndex = createIndex;
            c.rowIndexStride = rowIndexStride;
            c.bloomColumns = new ArrayList<>(bloomColumns);
            c.bloomFpp = bloomFpp;
            return c;
        }
    }

    public static final class ExperimentInstance {
        public String experimentId;
        public String kind;
        public String variantLabel;
        public String layoutPath;
        public String representation;
        public OrcWriteSettings orcSettings = new OrcWriteSettings();
        public List<String> partitionColumns = new ArrayList<>();
        public List<String> sorting = new ArrayList<>();
        public int bucketCount;
        public String bucketKey;
        public int targetFileSizeMib;
        public Map<String, String> readerOverrides = new LinkedHashMap<>();
        public Map<String, String> sessionOverrides = new LinkedHashMap<>();
        public List<String> queries = new ArrayList<>();
        public List<String> requires = new ArrayList<>();
        public String engine = "spark";
        public boolean llap;
        public String cacheState;
        public String skipReason;
    }

    public static final class CapabilityReport {
        public Map<String, Boolean> capabilities = new LinkedHashMap<>();
        public Map<String, String> versions = new LinkedHashMap<>();
        public List<String> notes = new ArrayList<>();
        public List<String[]> skippedExperiments = new ArrayList<>();
    }

    public static final class QuerySampleManifest {
        public Map<String, Map<String, List<String>>> samples = new LinkedHashMap<>();
        public List<String> partitionDates = new ArrayList<>();
    }

    public static final class RunResult {
        public String runId;
        public String experimentId;
        public String variantLabel;
        public String engine;
        public String queryId;
        public int repetition;
        public boolean warmup;
        public int concurrency;
        public String cacheState;
        public long latencyMs;
        public String status;
        public String resultHash;
        public long logicalRows;
        public Long orcFileBytes;
        public Long hdfsBytesRead;
        public String enginePlan;
        public Long rowsRead;
        public Long filesOpened;
        public Long partitionsSelected;
        public Long cpuMs;
        public Long shuffleReadBytes;
        public Long shuffleWriteBytes;
        public Long spillBytes;
        public Double cacheHitRatio;
        public String errorMessage;
    }

    public static final class AggregateStats {
        public String experimentId;
        public String variantLabel;
        public String engine;
        public String queryId;
        public int concurrency;
        public String cacheState;
        public int n;
        public double avgMs;
        public double p50Ms;
        public double p95Ms;
        public double p99Ms;
        public double slaSuccessRate;
        public Double scanRatio;
        public String resultHash;
    }

    public static <T> List<T> listOrEmpty(List<T> in) {
        return in == null ? Collections.emptyList() : in;
    }
}
