package orcbench.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Typed runtime models for the YAML contract (Checkpoint 2). */
public final class Models {
    private Models() {}

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
        public Map<String, EngineProfile> engineProfiles = new LinkedHashMap<>();
        public EngineExperiments engineExperiments = new EngineExperiments();
        public FinalComparison finalComparison = new FinalComparison();
        public BestOrcRule bestOrcRule = new BestOrcRule();
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
        public String hdfsRoot = "/orc_test";
        public String generatedHiveDdl = "generated/hive-table.sql";
        public List<String> derivedPartitionColumns = new ArrayList<>();
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
        public String engine;
    }

    public static final class ExecutionBaselines {
        public Map<String, String> spark = new LinkedHashMap<>();
        public Map<String, String> hive = new LinkedHashMap<>();
    }

    public static final class EngineProfile {
        public String extendsBaseline;
        public Map<String, String> set = new LinkedHashMap<>();
        public String prepare;
        public String cacheState;
        public String status;
        public String reason;
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
        public List<String> queries = new ArrayList<>();
    }

    /**
     * Selection rule for {@code BEST_ORC} from {@code benchmark.best_orc_rule}
     * (Checkpoint 8). ZSTD is excluded from Hive-shared recommendations.
     */
    public static final class BestOrcRule {
        public String objectiveRaw =
                "min p50 over {Q02, Q03, Q07_NARROW}";
        public List<String> objectiveQueries =
                new ArrayList<>(java.util.Arrays.asList("Q02", "Q03", "Q07_NARROW"));
        public String objectiveStat = "p50";
        public String slaGateRaw = "success_rate >= 0.95";
        public double slaSuccessRateMin = 0.95;
        public String tieBreakRaw = "partitioned > sorted > baseline";
        public String excludedCandidatesRaw =
                "ZSTD (Hive side ORC 1.5.6 codec gap; Spark ZSTD kept; Hive ZSTD deferred until codec support)";
        /** Always true for BEST_ORC used by Hive engines in final_comparison. */
        public boolean excludeZstd = true;
    }

    /** Result of {@link orcbench.report.BestOrcSelector}. */
    public static final class BestOrcChoice {
        public String experimentId;
        public String variantLabel;
        public String layoutPath;
        public double meanP50Ms;
        public double meanSlaSuccessRate;
        public boolean slaPassed;
        public String tieBreakClass;
        public List<String> notes = new ArrayList<>();
    }

    public static final class RunsSection {
        public int warmupPerQuery = 5;
        public int measuredPerQuery = 30;
        /** Candidate ceiling from YAML; actual levels come from capacity-aware selection. */
        public List<Integer> concurrencyLevels = new ArrayList<>();
        public String concurrencyPolicy = "per_scale_capacity_aware";
        public boolean cacheStatesNotAggregated = true;
        public boolean recordExplainPlans = true;
        public boolean compareCompleteResults = true;
        public String timingScope;
        public long maxFetchRows = 10_000_000L;
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
        public int specVersion;
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
        public int specVersion;
        public String timeZone = "UTC";
        public SecurityEnv security = new SecurityEnv();
        public Map<String, Optional<String>> versions = new LinkedHashMap<>();
        public HdfsEnv hdfs = new HdfsEnv();
        public YarnEnv yarn = new YarnEnv();
        public SparkEnv spark = new SparkEnv();
        public HiveEnv hive = new HiveEnv();
        public OperationsEnv operations = new OperationsEnv();
        public PreflightEnv preflight = new PreflightEnv();
    }

    public static final class SecurityEnv {
        public boolean enabled = true;
        public String mechanism = "kerberos";
        public String runnerPrincipal;
        public String keytabPath;
        public Boolean kinitVerified;
        public String realm;
        public String hiveJdbcAuth;
    }

    public static final class HdfsEnv {
        public String namenodeUri;
        public Integer defaultReplication;
        public Integer blockSizeMib;
        public Long freeSpaceGib;
        public String workDir = "/orc_test";
        public Boolean writeVerified;
        public Boolean shortCircuitSupported;
        public Boolean shortCircuitEnabled;
        public boolean shortCircuitMutableByRunner;
        public Boolean centralizedCacheSupported;
        public boolean centralizedCacheMutableByRunner;
        public Boolean dataLocalityVerifiable;
    }

    public static final class YarnEnv {
        public String resourceManager;
        public String resourceManagerStandby;
        public String queue = "default";
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
        public String metastoreUri;
        public boolean tezAvailable = true;
        public Boolean llapAvailable;
        public String llapDaemons;
        public Double llapCacheGib;
        public Boolean materializedViewsSupported;
        public String bucketVersion;
        public String llapAppName = "llap0";
        public String llapExecutionMode = "only";
        public Boolean llapDaemonsRunningVerified;
        public boolean jdbcConnectionVerified;
        public boolean llapConfigured;
        public boolean ddlExecutionVerified;
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
        public int specVersion;
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

    /** One runnable cell of the OVAT / engine matrix. */
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

    public static final class ExperimentPlan {
        public String scale;
        public List<ExperimentInstance> physical = new ArrayList<>();
        public List<ExperimentInstance> engine = new ArrayList<>();
        public List<ExperimentInstance> finalComparison = new ArrayList<>();
        public List<Integer> concurrencyCandidates = new ArrayList<>();
        public List<Integer> concurrencySelected = new ArrayList<>();
        public String concurrencyNote;
    }

    /** Preflight capability / skip-matrix report (Checkpoint 3). */
    public static final class CapabilityReport {
        public Map<String, Boolean> capabilities = new LinkedHashMap<>();
        public Map<String, String> versions = new LinkedHashMap<>();
        public List<String> notes = new ArrayList<>();
        /** Rows: [experimentId/variant, reason]. */
        public List<String[]> skippedExperiments = new ArrayList<>();
    }

    /**
     * Deterministic query placeholder samples produced during generate
     * (Checkpoint 4). Used by later query runners.
     */
    public static final class QuerySampleManifest {
        public Map<String, Map<String, List<String>>> samples = new LinkedHashMap<>();
        public List<String> partitionDates = new ArrayList<>();
        public long estimatedRows;
        public String basePath;
        public String refsPath;
        public String manifestPath;
    }

    /** Summary of a generate-phase execution (Checkpoint 4). */
    public static final class GenerateResult {
        public String scale;
        public long targetLogicalGib;
        public long estimatedRows;
        public String basePath;
        public String refsPath;
        public String manifestPath;
        public boolean dryRun;
        public boolean mocked;
        public QuerySampleManifest samples = new QuerySampleManifest();
        public List<String> notes = new ArrayList<>();
    }

    /**
     * Summary of a materialize-phase execution (Checkpoint 5).
     * Layouts are rewritten from {@code RAW_ARRAY_BASE} with OVAT factors.
     */
    public static final class MaterializeResult {
        public String scale;
        public String basePath;
        public boolean dryRun;
        public boolean mocked;
        public int plannedWrites;
        public int written;
        public int skippedExistingMarker;
        public int skippedOther;
        public int cleaned;
        public List<String> layoutPaths = new ArrayList<>();
        public List<String> markerPaths = new ArrayList<>();
        public List<String> ddlStatements = new ArrayList<>();
        public List<String> notes = new ArrayList<>();
    }

    /**
     * Immutable write request handed to {@link orcbench.layout.MaterializeBackend}
     * for one OVAT layout variant.
     */
    public static final class LayoutWriteRequest {
        public String experimentId;
        public String variantLabel;
        public String kind;
        public String sourceBasePath;
        public String layoutPath;
        public String representation;
        public OrcWriteSettings orcSettings = new OrcWriteSettings();
        public List<String> partitionColumns = new ArrayList<>();
        public List<String> sorting = new ArrayList<>();
        public List<String> flatColumns = new ArrayList<>();
        public int bucketCount;
        public String bucketKey;
        public int targetFileSizeMib;
        public long sourceBytesEstimate;
        public String markerPath;
    }

    /** One query attempt (warmup or measured) from Spark/Hive runners (Checkpoint 6+). */
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

    /** Aggregated latency / SLA / scan stats over measured ok attempts. */
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

    /** Summary of a Spark {@code --phase run} execution (Checkpoint 6). */
    public static final class RunPhaseResult {
        public String runId;
        public String scale;
        public boolean dryRun;
        public boolean mocked;
        public int experimentsPlanned;
        public int experimentsRun;
        public int experimentsSkipped;
        public int attempts;
        public String metricsJsonlPath;
        public List<RunResult> results = new ArrayList<>();
        public List<String> notes = new ArrayList<>();
    }

    /** Summary of {@code --phase report} / {@code --phase final} (Checkpoint 8). */
    public static final class ReportPhaseResult {
        public String runId;
        public String scale;
        public boolean dryRun;
        public boolean mocked;
        public String phase;
        public String outputDir;
        public String metricsJsonlPath;
        public int resultRows;
        public int aggregateRows;
        public int skippedRows;
        public BestOrcChoice bestOrc;
        public int finalComparisonCells;
        public boolean slaGateOverall;
        public List<String> notes = new ArrayList<>();
    }

    public static <T> List<T> listOrEmpty(List<T> in) {
        return in == null ? Collections.<T>emptyList() : in;
    }
}
