package orcbench.generate;

import orcbench.generate.GenerateBackend.EventRowFactory;
import orcbench.model.Models.ColumnGenerator;
import orcbench.model.Models.DatasetContract;
import orcbench.model.Models.GenerateResult;
import orcbench.model.Models.LoadedConfig;
import orcbench.model.Models.OrcBaseline;
import orcbench.model.Models.OrcWriteSettings;
import orcbench.model.Models.QuerySampleManifest;
import orcbench.model.Models.ScaleSpec;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Checkpoint 4: synthetic RAW_ARRAY_BASE generation from {@code schema.orc.txt}
 * + {@code dataset.yaml}. Local acceptance uses dry-run / {@link FakeGenerateBackend};
 * live HDFS writes go through {@link SparkOrcGenerateBackend} on a cluster node only.
 */
public final class DatasetGenerator {

    /**
     * Uncompressed bytes per row for the SM_UKO profile (~1.9 KB JSON, ~7.5 KB ORC).
     * Scale targets in dataset.yaml are logical GiB; ZLIB is about 27×.
     */
    static final long APPROX_BYTES_PER_ROW = 7500L;
    static final int ZLIB_RATIO = 27;

    private DatasetGenerator() {}

    public static long estimateRowCount(long targetLogicalGib) {
        return Math.max(1L, (targetLogicalGib * 1024L * 1024L * 1024L) / APPROX_BYTES_PER_ROW);
    }

    /**
     * High-level entry used by {@link orcbench.Main}.
     *
     * @param dryRun   plan only — no backend writes
     * @param useMock  when true (and not dry-run), use {@link FakeGenerateBackend}
     *                 (local/CI mock path; never touches HDFS)
     * @param backend  optional override (unit tests); if null, mock or Spark is chosen
     */
    public static GenerateResult generate(
            LoadedConfig cfg,
            String scale,
            boolean dryRun,
            boolean useMock,
            GenerateBackend backend) throws Exception {
        if (cfg == null) {
            throw new IllegalArgumentException("cfg is required");
        }
        String scaleKey = scale == null ? "S" : scale.toUpperCase(Locale.ROOT);
        DatasetContract ds = cfg.dataset;
        ScaleSpec scaleSpec = ds.scales.get(scaleKey);
        if (scaleSpec == null) {
            throw new IllegalArgumentException("Unknown scale " + scaleKey);
        }

        long rowCount = ds.generation.rowCount != null
                ? ds.generation.rowCount
                : estimateRowCount(scaleSpec.targetLogicalGib);
        String hdfsRoot = cfg.benchmark.benchmark.hdfsRoot;
        String baselineId = cfg.benchmark.physicalBaseline.id;
        String basePath = hdfsRoot + "/" + scaleKey + "/" + baselineId;
        String refsPath = hdfsRoot + "/" + scaleKey + "/refs";
        String manifestPath = hdfsRoot + "/" + scaleKey + "/manifest";

        GenerateResult result = new GenerateResult();
        result.scale = scaleKey;
        result.targetLogicalGib = scaleSpec.targetLogicalGib;
        result.estimatedRows = rowCount;
        result.basePath = basePath;
        result.refsPath = refsPath;
        result.manifestPath = manifestPath;
        result.dryRun = dryRun;
        result.mocked = useMock && !dryRun;

        System.out.println("[generate] scale=" + scaleKey
                + " targetGiB=" + scaleSpec.targetLogicalGib
                + " rows≈" + rowCount
                + " path=" + basePath
                + " dryRun=" + dryRun
                + " mock=" + result.mocked);

        if (dryRun) {
            result.notes.add("dry-run: skip ORC / refs / manifest materialization");
            QuerySampleManifest stub = stubManifest(cfg, rowCount, basePath, refsPath, manifestPath);
            result.samples = stub;
            System.out.println("[generate] dry-run: would write RAW_ARRAY_BASE, refs/, manifest/");
            return result;
        }

        GenerateBackend sink = backend;
        if (sink == null) {
            if (useMock) {
                sink = new FakeGenerateBackend();
                result.notes.add("mock backend: in-memory cap, no HDFS");
            } else {
                if (!SparkSessionSupport.sparkAvailable()) {
                    throw new IllegalStateException(
                            "Spark is not on the classpath. Live --phase generate requires "
                                    + "spark-submit on a Hadoop cluster node, or use --dry-run / --mock-generate.");
                }
                sink = new SparkOrcGenerateBackend(
                        SparkSessionSupport.create(cfg, "orc-bench-generate"));
                result.notes.add("live Spark ORC backend (cluster)");
            }
        }

        final long seed = ds.generation.seed;
        final String algo = ds.generation.algorithmVersion;
        final long fromMs = Instant.parse(ds.generation.timestampFrom).toEpochMilli();
        final long toMs = Instant.parse(ds.generation.timestampToExclusive).toEpochMilli();
        final List<String> paramKeys = sortedParamKeys(ds);
        final Map<String, ColumnGenerator> catalog = ds.parameters.catalog;
        final Map<String, Map<String, Object>> jsonTemplates = ds.jsonTemplates;

        // Explicit serializable factory (not an anonymous class) for Spark executors.
        EventRowFactory factory = new SynthEventRowFactory(
                seed, algo, fromMs, toMs, paramKeys, catalog, jsonTemplates);

        OrcWriteSettings orc = toWriteSettings(cfg.benchmark.physicalBaseline.orc);
        int fileMib = Math.max(32, cfg.benchmark.physicalBaseline.targetFileSizeMib);
        long compressedBytes = Math.max(1L, (rowCount * APPROX_BYTES_PER_ROW) / ZLIB_RATIO);
        int parallelism = (int) Math.min(2000L,
                Math.max(1L, compressedBytes / (fileMib * 1024L * 1024L)));

        sink.writeRawArray(basePath, rowCount, parallelism, factory, orc);
        sink.writeReferences(refsPath, ds.referenceDatasets, seed, algo);

        QuerySampleManifest samples = buildQuerySamplesFromSynth(
                factory, ds, cfg, rowCount, basePath, refsPath, manifestPath);
        sink.writeManifest(manifestPath, samples);

        validateSample(factory, ds, Math.min(rowCount, 64L));
        if (ds.validation.checkRowCount && !(sink instanceof FakeGenerateBackend)) {
            long observed = sink.countRows(basePath);
            if (observed >= 0 && observed != rowCount) {
                String msg = "row count mismatch: expected=" + rowCount + " actual=" + observed;
                if (ds.validation.failOnViolation) {
                    throw new IllegalStateException(msg);
                }
                result.notes.add("WARN " + msg);
            }
        } else if (sink instanceof FakeGenerateBackend) {
            FakeGenerateBackend fake = (FakeGenerateBackend) sink;
            result.notes.add("mock wrote " + fake.events.size()
                    + " of requested " + fake.requestedRowCount + " rows");
        }

        result.samples = samples;
        System.out.println("[generate] OK scale=" + scaleKey + " rows≈" + rowCount);
        return result;
    }

    /** Convenience: dry-run or mock without injecting a backend. */
    public static GenerateResult generate(LoadedConfig cfg, String scale, boolean dryRun, boolean useMock)
            throws Exception {
        return generate(cfg, scale, dryRun, useMock, null);
    }

    /** Serializable row factory shipped to Spark executors (no outer DatasetGenerator capture). */
    static final class SynthEventRowFactory implements EventRowFactory {
        private static final long serialVersionUID = 1L;
        private final long seed;
        private final String algo;
        private final long fromMs;
        private final long toMs;
        private final List<String> paramKeys;
        private final Map<String, ColumnGenerator> catalog;
        private final Map<String, Map<String, Object>> jsonTemplates;

        SynthEventRowFactory(
                long seed,
                String algo,
                long fromMs,
                long toMs,
                List<String> paramKeys,
                Map<String, ColumnGenerator> catalog,
                Map<String, Map<String, Object>> jsonTemplates) {
            this.seed = seed;
            this.algo = algo;
            this.fromMs = fromMs;
            this.toMs = toMs;
            this.paramKeys = paramKeys;
            this.catalog = catalog;
            this.jsonTemplates = jsonTemplates;
        }

        @Override
        public EventRow create(long rowId) {
            return buildEventRow(rowId, seed, algo, fromMs, toMs, paramKeys, catalog, jsonTemplates);
        }
    }

    /** Placeholder parameter names beyond the modeled catalog. See dataset.yaml unnamed_fillers.count. */
    static final int UNNAMED_FILLER_LIMIT = 20;

    static final String NAME_FIND = "SYSTEM_EVENTS_FIND";
    static final String NAME_LOGON = "SYSTEM_EVENTS_LOGON";
    static final String NAME_CREATE = "SYSTEM_EVENTS_CREATE_SESSION_WITH_CONFIGURATION_RESPONSE";
    static final String NAME_MONITORING = "MONITORING_IP_UFS_SESSION_REGISTRY_CHANGE_IP";
    static final String MODULE = "CI00982781_SM_UKO";
    static final String SOURCE_SYSTEM = "CI02001608";

    public static EventRow buildEventRow(
            long rowId,
            long seed,
            String algo,
            long fromMs,
            long toMs,
            List<String> paramKeys,
            Map<String, ColumnGenerator> catalog,
            Map<String, Map<String, Object>> jsonTemplates) {
        long group = rowId / 3L;
        int slot = (int) (rowId % 3L);
        boolean residual = SynthGenerators.bounded(seed, group, "residual", algo, 1000L) < 65L;
        String name = residual
                ? rareName(seed, rowId, algo)
                : (slot == 0 ? NAME_FIND : slot == 1 ? NAME_LOGON : NAME_CREATE);
        String session = SynthGenerators.hexString(
                seed, residual ? rowId : group, "session", algo, 64);
        String userLogin;
        if (NAME_FIND.equals(name)) {
            userLogin = "tech_find";
        } else if (NAME_MONITORING.equals(name)) {
            userLogin = "";
        } else {
            userLogin = String.format(Locale.ROOT, "%016d",
                    SynthGenerators.bounded(seed, group, "userLogin", algo, 10_000_000_000_000_000L));
        }
        long createdAtMs = SynthGenerators.timestampUniform(seed, rowId, "createdAt", algo, fromMs, toMs);
        String userNode = SynthGenerators.documentationIpv4(seed, rowId, "userNode", algo);
        boolean metaA = SynthGenerators.bounded(seed, rowId, "metamodel", algo, 1000L) < 856L;
        String metamodelVersion = SynthGenerators.hexString(
                seed, metaA ? 0L : 1L, "metamodelVersion", algo, 64);
        String metamodelHash = SynthGenerators.hexString(
                seed, metaA ? 0L : 1L, "metamodelHash", algo, 64);
        String userName = NAME_MONITORING.equals(name) ? null : "";
        String nodeId = NAME_MONITORING.equals(name)
                ? SynthGenerators.prefixedId(seed, rowId, "nodeId", algo, "node_", 1181L)
                : null;

        List<String> tags = new ArrayList<>();
        tags.add("audit.CImodule");
        tags.add("ns_" + SynthGenerators.bounded(seed, rowId, "namespace", algo, 8L));
        tags.add("pod_" + SynthGenerators.bounded(seed, rowId, "pod", algo, 1250L));

        List<String> names = new ArrayList<>();
        List<String> values = new ArrayList<>();
        List<String> oldValues = new ArrayList<>();
        fillParams(names, values, oldValues, name, slot, seed, rowId, group, algo, session,
                catalog, jsonTemplates);

        String archiveId = SynthGenerators.uuidDeterministic(seed, rowId, "archiveInternalId", algo);
        String id = SynthGenerators.uuidDeterministic(seed, rowId, "id", algo);
        int offset = (int) SynthGenerators.bounded(seed, rowId, "archiveInsertTime", algo, 3L);
        long archiveInsertMs = createdAtMs + offset * 1000L;

        return new EventRow(
                userLogin, createdAtMs, session, userNode, MODULE, name, userName, metamodelVersion,
                tags, oldValues, names, values,
                archiveId, archiveInsertMs, createdAtMs,
                id, SOURCE_SYSTEM, metamodelHash, nodeId, null);
    }

    private static String rareName(long seed, long rowId, String algo) {
        return SynthGenerators.weightedPick(seed, rowId, "rareName", algo,
                java.util.Arrays.asList(
                        "SYSTEM_EVENTS_CREATE_SESSION_RESPONSE",
                        "SYSTEM_EVENTS_GET_EXTENDED_PERMISSIONS_MOBILE_RESPONSE",
                        "SYSTEM_EVENTS_FORCE_MS_NOTIFICATION_RESPONSE",
                        NAME_MONITORING,
                        "SYSTEM_EVENTS_SEND_OPERATION_DATA_SBERKIDS_GUEST",
                        "SYSTEM_EVENTS_GET_CONFIGURATION_WEBVIEW_RESPONSE"),
                java.util.Arrays.asList(811676.0, 371027.0, 27659.0, 22807.0, 1573.0, 394.0));
    }

    private static int paramTarget(String name, long seed, long rowId, String algo) {
        if (NAME_CREATE.equals(name)) {
            return 19 + (int) SynthGenerators.bounded(seed, rowId, "paramSpan", algo, 8L);
        }
        if (NAME_FIND.equals(name)) {
            return 28 + (int) SynthGenerators.bounded(seed, rowId, "paramSpan", algo, 6L);
        }
        if (NAME_LOGON.equals(name)) {
            return 32 + (int) SynthGenerators.bounded(seed, rowId, "paramSpan", algo, 9L);
        }
        if ("SYSTEM_EVENTS_CREATE_SESSION_RESPONSE".equals(name)) {
            return 19;
        }
        if ("SYSTEM_EVENTS_GET_EXTENDED_PERMISSIONS_MOBILE_RESPONSE".equals(name)
                || "SYSTEM_EVENTS_GET_CONFIGURATION_WEBVIEW_RESPONSE".equals(name)) {
            return 17;
        }
        if ("SYSTEM_EVENTS_FORCE_MS_NOTIFICATION_RESPONSE".equals(name)) {
            return 18;
        }
        if (NAME_MONITORING.equals(name)) {
            return 11;
        }
        if ("SYSTEM_EVENTS_SEND_OPERATION_DATA_SBERKIDS_GUEST".equals(name)) {
            return 21;
        }
        return 17;
    }

    private static void fillParams(
            List<String> names,
            List<String> values,
            List<String> oldValues,
            String eventName,
            int slot,
            long seed,
            long rowId,
            long group,
            String algo,
            String session,
            Map<String, ColumnGenerator> catalog,
            Map<String, Map<String, Object>> jsonTemplates) {
        boolean client = NAME_FIND.equals(eventName) || NAME_LOGON.equals(eventName);
        List<String> keys = new ArrayList<>();
        if (!NAME_MONITORING.equals(eventName)) {
            keys.add("ufs.auditContextUUID");
            keys.add("ufs.employeeLogin");
            keys.add("ufs.orgUid");
            keys.add("ufs.serverLocation");
            for (int i = 1; i <= 13; i++) {
                keys.add(String.format(Locale.ROOT, "ufs.synthetic_%02d", i));
            }
        }
        if (client) {
            keys.add("EPK_ID");
            keys.add("SESSION_ID");
            keys.add("OPERATION_UID");
            keys.add("CARD_NUMBER");
            keys.add("IP_ADDRESS");
            keys.add("PERSON_DOCUMENT_NUMBER");
            keys.add("DEVICE_ID");
            keys.add("DEVICE_INFO");
            keys.add("USER_APP_VERSION");
        }
        if (SynthGenerators.bounded(seed, rowId, "auth", algo, 1000L) < 357L) {
            keys.add("AUTH_TOKEN");
        }
        if (SynthGenerators.bounded(seed, rowId, "codes", algo, 1000L) < 307L) {
            keys.add("TB_CODE");
            keys.add("OSB_CODE");
            keys.add("VSP_CODE");
        }
        int target = paramTarget(eventName, seed, rowId, algo);
        int filler = 1;
        // 30 modeled names + 20 fillers ≈ observed approx_distinct(paramName)=50.
        // Do not treat that approximate distinct as an exact unique-count gate.
        while (keys.size() < target && filler <= UNNAMED_FILLER_LIMIT) {
            keys.add(String.format(Locale.ROOT, "UNNAMED_%02d", filler));
            filler++;
        }
        while (keys.size() > target) {
            String last = keys.get(keys.size() - 1);
            if (last.startsWith("UNNAMED_") || "AUTH_TOKEN".equals(last)
                    || "TB_CODE".equals(last) || "OSB_CODE".equals(last) || "VSP_CODE".equals(last)) {
                keys.remove(keys.size() - 1);
            } else {
                break;
            }
        }
        if (client) {
            keys.remove("EPK_ID");
            int at = Math.min(6 + (slot % 3), keys.size());
            keys.add(at, "EPK_ID");
        }
        for (String key : keys) {
            names.add(key);
            values.add(paramValue(key, catalog, seed, rowId, group, algo, eventName, session, jsonTemplates));
            oldValues.add(null);
        }
    }

    private static String paramValue(
            String key,
            Map<String, ColumnGenerator> catalog,
            long seed,
            long rowId,
            long group,
            String algo,
            String eventName,
            String session,
            Map<String, Map<String, Object>> jsonTemplates) {
        if (key.startsWith("ufs.synthetic_")) {
            return SynthGenerators.dictionary(seed, rowId, key, algo, "ufs_", 9L);
        }
        if (key.startsWith("UNNAMED_")) {
            return SynthGenerators.dictionary(seed, rowId, key, algo, "v_", 10L);
        }
        if ("EPK_ID".equals(key)) {
            long hi = SynthGenerators.bounded(seed, group, "epkHi", algo, 1_000_000_000L);
            long lo = SynthGenerators.bounded(seed, group, "epkLo", algo, 10_000_000_000L);
            return String.format(Locale.ROOT, "%09d%010d", hi, lo);
        }
        ColumnGenerator g = catalog == null ? null : catalog.get(key);
        return generateParam(g, seed, rowId, key, algo, eventName, session, jsonTemplates);
    }

    /**
     * Validate alignment / null semantics for a sample of rows (no HDFS).
     */
    public static void validateSample(EventRowFactory factory, DatasetContract ds, long sampleRows) {
        for (long i = 0; i < sampleRows; i++) {
            EventRow row = factory.create(i);
            List<String> errors = validateRow(row, ds);
            if (!errors.isEmpty()) {
                String msg = "row " + i + ": " + errors;
                if (ds.validation.failOnViolation) {
                    throw new IllegalStateException(msg);
                }
                System.out.println("[validate] WARN " + msg);
            }
        }
    }

    /** Returns human-readable violation messages (empty if OK). */
    public static List<String> validateRow(EventRow row, DatasetContract ds) {
        List<String> errors = new ArrayList<>();
        if (row == null) {
            errors.add("row is null");
            return errors;
        }
        if (ds.validation.checkNullHandling) {
            if (row.tags == null || row.tags.size() != 3) {
                errors.add("tags must be an array of 3 elements");
            }
            if (row.paramOldValue == null) {
                errors.add("paramOldValue must be an array (of nulls), not null column");
            } else {
                for (int i = 0; i < row.paramOldValue.size(); i++) {
                    if (row.paramOldValue.get(i) != null) {
                        errors.add("paramOldValue[" + i + "] must be null element");
                        break;
                    }
                }
            }
        }
        if (ds.validation.checkNameValueAlignment) {
            if (row.paramName.size() != row.paramValue.size()) {
                errors.add("paramName/paramValue length mismatch");
            }
            if (row.paramName.size() != row.paramOldValue.size()) {
                errors.add("paramName/paramOldValue length mismatch");
            }
        }
        if (ds.validation.checkDuplicateNames) {
            java.util.HashSet<String> seen = new java.util.HashSet<>();
            for (String n : row.paramName) {
                if (!seen.add(n)) {
                    errors.add("duplicate paramName: " + n);
                    break;
                }
            }
        }
        return errors;
    }

    /** Canonical SHA-256 over a stable textual projection of the row. */
    public static String rowHash(EventRow row) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            StringBuilder sb = new StringBuilder(512);
            sb.append(row.userLogin).append('|')
                    .append(row.createdAtMs).append('|')
                    .append(row.session).append('|')
                    .append(row.userNode).append('|')
                    .append(row.module).append('|')
                    .append(row.name).append('|')
                    .append(row.userName).append('|')
                    .append(row.metamodelVersion).append('|')
                    .append(row.tags.size()).append('|')
                    .append(row.paramName.size()).append('|');
            for (int i = 0; i < row.paramName.size(); i++) {
                sb.append(row.paramName.get(i)).append('=')
                        .append(row.paramValue.get(i)).append(';');
            }
            sb.append(row.archiveInternalId).append('|')
                    .append(row.archiveInsertTimeMs).append('|')
                    .append(row.timeMs).append('|')
                    .append(row.id).append('|')
                    .append(row.sourceSystem).append('|')
                    .append(row.metamodelHash).append('|')
                    .append(row.nodeId).append('|')
                    .append(row.requestId);
            byte[] dig = md.digest(sb.toString().getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(dig.length * 2);
            for (byte b : dig) {
                hex.append(String.format("%02x", b));
            }
            return hex.toString();
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }

    static List<String> sortedParamKeys(DatasetContract ds) {
        List<String> keys = new ArrayList<>(ds.parameters.catalog.keySet());
        Collections.sort(keys);
        return keys;
    }

    static OrcWriteSettings toWriteSettings(OrcBaseline orc) {
        OrcWriteSettings s = new OrcWriteSettings();
        if (orc == null) {
            s.compression = "SNAPPY";
            s.stripeSizeMib = 64;
            s.createIndex = true;
            s.rowIndexStride = 10000;
            s.bloomFpp = 0.01;
            return s;
        }
        s.compression = orc.compression;
        s.stripeSizeMib = orc.stripeSizeMib;
        s.createIndex = orc.createIndex;
        s.rowIndexStride = orc.rowIndexStride;
        s.bloomColumns = new ArrayList<>(orc.bloomColumns);
        s.bloomFpp = orc.bloomFpp;
        return s;
    }

    private static String generateParam(
            ColumnGenerator g,
            long seed,
            long rowId,
            String key,
            String algo,
            String eventName,
            String session,
            Map<String, Map<String, Object>> jsonTemplates) {
        if (g == null) {
            return key + "_missing";
        }
        String gen = g.generator == null ? "" : g.generator;
        switch (gen) {
            case "always_null":
                return null;
            case "numeric_string":
                return String.format(Locale.ROOT, "%019d",
                        SynthGenerators.bounded(seed, rowId, key, algo, 1_000_000_000_000_000_000L));
            case "uuid_deterministic":
                return SynthGenerators.uuidDeterministic(seed, rowId, key, algo);
            case "copy_column":
                if ("name".equals(g.source)) {
                    return eventName;
                }
                if ("session".equals(g.source)) {
                    return session;
                }
                return SynthGenerators.prefixedId(seed, rowId, key, algo, "copy_", 1000L);
            case "prefixed_id":
                return SynthGenerators.prefixedId(seed, rowId, key, algo,
                        g.prefix == null ? "" : g.prefix,
                        g.cardinality == null ? 1_000_000L : g.cardinality);
            case "dictionary":
                return SynthGenerators.dictionary(seed, rowId, key, algo,
                        g.prefix == null ? "" : g.prefix,
                        g.cardinality == null ? 100L : g.cardinality);
            case "documentation_ip_v4":
                return SynthGenerators.documentationIpv4(seed, rowId, key, algo);
            case "fake_token":
                return SynthGenerators.fakeToken(seed, rowId, key, algo,
                        g.prefix == null ? "token_" : g.prefix,
                        g.cardinality == null ? 1_000_000L : g.cardinality);
            case "json_template":
                String templateKey = g.template != null ? g.template : "mobile_sdk_v1";
                Map<String, Object> tpl = jsonTemplates == null
                        ? Collections.<String, Object>emptyMap()
                        : jsonTemplates.getOrDefault(templateKey, Collections.<String, Object>emptyMap());
                return SynthGenerators.jsonMobileSdk(seed, rowId, algo, tpl);
            default:
                return SynthGenerators.prefixedId(seed, rowId, key, algo, gen + "_", 1000L);
        }
    }

    private static QuerySampleManifest stubManifest(
            LoadedConfig cfg, long rowCount, String basePath, String refsPath, String manifestPath) {
        QuerySampleManifest m = new QuerySampleManifest();
        m.estimatedRows = rowCount;
        m.basePath = basePath;
        m.refsPath = refsPath;
        m.manifestPath = manifestPath;
        m.partitionDates = Collections.singletonList("2025-04-14--12");
        return m;
    }

    static QuerySampleManifest buildQuerySamplesFromSynth(
            EventRowFactory factory,
            DatasetContract ds,
            LoadedConfig cfg,
            long rowCount,
            String basePath,
            String refsPath,
            String manifestPath) {
        int sampleN = Math.max(1, ds.querySamples.valuesPerCase);
        int need = sampleN * 4;
        long limit = Math.min(rowCount, Math.max(need, 200L));

        List<String> userLogins = new ArrayList<>();
        List<String> sessions = new ArrayList<>();
        List<String> epks = new ArrayList<>();
        List<String> nodes = new ArrayList<>();
        List<String> ops = new ArrayList<>();
        java.util.LinkedHashSet<String> epkFile = new java.util.LinkedHashSet<>();
        long epkScan = Math.min(rowCount, 20000L);

        for (long i = 0; i < Math.max(limit, epkScan); i++) {
            EventRow row = factory.create(i);
            if (i < limit) {
                if (userLogins.size() < need) {
                    userLogins.add(row.userLogin);
                }
                if (sessions.size() < need) {
                    sessions.add(row.session);
                }
                if (nodes.size() < need && row.userNode != null) {
                    nodes.add(row.userNode);
                }
            }
            for (int p = 0; p < row.paramName.size(); p++) {
                String k = row.paramName.get(p);
                String v = row.paramValue.get(p);
                if (v == null) {
                    continue;
                }
                if ("EPK_ID".equals(k)) {
                    if (epks.size() < need) {
                        epks.add(v);
                    }
                    if (epkFile.size() < 10800) {
                        epkFile.add(v);
                    }
                } else if ("OPERATION_UID".equals(k) && ops.size() < need) {
                    ops.add(v);
                }
            }
        }

        QuerySampleManifest manifest = new QuerySampleManifest();
        manifest.estimatedRows = rowCount;
        manifest.basePath = basePath;
        manifest.refsPath = refsPath;
        manifest.manifestPath = manifestPath;
        List<String> cases = ds.querySamples.cases.isEmpty()
                ? java.util.Arrays.asList("present", "absent", "selective", "common")
                : ds.querySamples.cases;
        manifest.samples.put("userLogin", bucket(userLogins, cases, sampleN));
        manifest.samples.put("session", bucket(sessions, cases, sampleN));
        manifest.samples.put("EPK_ID", bucket(epks, cases, sampleN));
        manifest.samples.put("userNode", bucket(nodes, cases, sampleN));
        manifest.samples.put("OPERATION_UID", bucket(ops, cases, sampleN));
        manifest.samples.put("epk_id_list", Collections.singletonMap(
                "present", new ArrayList<>(epkFile)));
        manifest.samples.put("name", Collections.singletonMap(
                "present", java.util.Arrays.asList(NAME_LOGON, NAME_FIND)));

        manifest.partitionDates = Collections.singletonList("2025-04-14--12");
        return manifest;
    }

    private static Map<String, List<String>> bucket(List<String> values, List<String> cases, int sampleN) {
        Map<String, List<String>> out = new LinkedHashMap<>();
        List<String> present = values.subList(0, Math.min(sampleN, values.size()));
        List<String> selective = values.subList(Math.min(sampleN, values.size()),
                Math.min(sampleN * 2, values.size()));
        List<String> common = values.subList(0, Math.min(3, values.size()));
        List<String> absent = new ArrayList<>();
        for (int i = 0; i < sampleN; i++) {
            absent.add("ABSENT_" + i + "_NOT_IN_DATA");
        }
        for (String c : cases) {
            switch (c) {
                case "present":
                    out.put(c, new ArrayList<>(present));
                    break;
                case "absent":
                    out.put(c, absent);
                    break;
                case "selective":
                    out.put(c, selective.isEmpty()
                            ? new ArrayList<>(present.subList(0, Math.min(1, present.size())))
                            : new ArrayList<>(selective));
                    break;
                case "common":
                    out.put(c, new ArrayList<>(common));
                    break;
                default:
                    out.put(c, new ArrayList<>(present));
            }
        }
        return out;
    }

    private static String extractJsonField(String json, String field) {
        if (json == null) {
            return null;
        }
        String needle = "\"" + field + "\":\"";
        int i = json.indexOf(needle);
        if (i < 0) {
            return null;
        }
        int start = i + needle.length();
        int end = json.indexOf('"', start);
        if (end < 0) {
            return null;
        }
        return json.substring(start, end);
    }
}
