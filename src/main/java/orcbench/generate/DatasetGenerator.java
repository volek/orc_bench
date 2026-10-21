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

    /** Approximate uncompressed serialized row size for scale→rowCount estimation. */
    static final long APPROX_BYTES_PER_ROW = 1200L;

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
        int batch = Math.max(1, ds.generation.batchRows);
        int parallelism = Math.max(1, (int) Math.min(2000L, rowCount / Math.max(1L, batch * 64L)));

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

    public static EventRow buildEventRow(
            long rowId,
            long seed,
            String algo,
            long fromMs,
            long toMs,
            List<String> paramKeys,
            Map<String, ColumnGenerator> catalog,
            Map<String, Map<String, Object>> jsonTemplates) {
        String userLogin = SynthGenerators.prefixedId(seed, rowId, "userLogin", algo, "userLogin", 1_000_000L);
        long createdAtMs = SynthGenerators.timestampUniform(seed, rowId, "createdAt", algo, fromMs, toMs);
        String session = SynthGenerators.prefixedId(seed, rowId, "session", algo, "copy2_", 5_000_000L);
        String userNode = SynthGenerators.dictionary(seed, rowId, "userNode", algo, "userNode", 1000L);
        String module = SynthGenerators.dictionary(seed, rowId, "module", algo, "module_", 500L);
        String name = SynthGenerators.weightedPick(seed, rowId, "name", algo,
                java.util.Arrays.asList("LOGON", "LOGOFF", "ACTION"),
                java.util.Arrays.asList(0.6, 0.25, 0.15));
        String userName = SynthGenerators.prefixedId(seed, rowId, "userName", algo, "userName", 1_000_000L);
        String metamodelVersion = SynthGenerators.dictionary(
                seed, rowId, "metamodelVersion", algo, "metamodelVersion", 1000L);

        List<String> names = new ArrayList<>();
        List<String> values = new ArrayList<>();
        List<String> oldValues = new ArrayList<>();
        for (String key : paramKeys) {
            names.add(key);
            values.add(generateParam(catalog.get(key), seed, rowId, key, algo, name, session, jsonTemplates));
            // null_semantics.paramOldValue: array of NULL elements (aligned)
            oldValues.add(null);
        }

        String archiveId = SynthGenerators.uuidDeterministic(seed, rowId, "archiveInternalId", algo);
        int offset = SynthGenerators.bounded(seed, rowId, "archiveInsertTime", algo, 2L) == 0 ? 0 : 120;
        long archiveInsertMs = createdAtMs + offset * 1000L;

        return new EventRow(
                userLogin, createdAtMs, session, userNode, module, name, userName, metamodelVersion,
                Collections.<String>emptyList(), // tags: empty array, not null
                oldValues,
                names,
                values,
                archiveId, archiveInsertMs, createdAtMs);
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
            if (row.tags == null) {
                errors.add("tags must be empty array [], not null");
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
                    .append(row.timeMs);
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
        String dateValue = "2026-01-15";
        if (cfg.queries != null && cfg.queries.querySuite != null
                && cfg.queries.querySuite.defaults != null
                && cfg.queries.querySuite.defaults.get("date_value") != null) {
            dateValue = cfg.queries.querySuite.defaults.get("date_value");
        }
        m.partitionDates = Collections.singletonList(dateValue);
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
        List<String> ips = new ArrayList<>();
        List<String> channels = new ArrayList<>();
        List<String> devices = new ArrayList<>();

        for (long i = 0; i < limit; i++) {
            EventRow row = factory.create(i);
            if (userLogins.size() < need) {
                userLogins.add(row.userLogin);
            }
            if (sessions.size() < need) {
                sessions.add(row.session);
            }
            for (int p = 0; p < row.paramName.size(); p++) {
                String k = row.paramName.get(p);
                String v = row.paramValue.get(p);
                if ("EPK_ID".equals(k) && epks.size() < need) {
                    epks.add(v);
                } else if ("IP_ADDRESS".equals(k) && ips.size() < need) {
                    ips.add(v);
                } else if ("CHANNEL_TYPE".equals(k) && channels.size() < need) {
                    channels.add(v);
                } else if ("MOBILE_SDK_DATA".equals(k) && devices.size() < need) {
                    String os = extractJsonField(v, "DeviceSystemName");
                    if (os != null) {
                        devices.add(os);
                    }
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
        manifest.samples.put("SESSION_ID", bucket(sessions, cases, sampleN));
        manifest.samples.put("IP_ADDRESS", bucket(ips, cases, sampleN));
        manifest.samples.put("CHANNEL_TYPE", bucket(channels, cases, sampleN));

        Map<String, List<String>> device = new LinkedHashMap<>();
        device.put("present", java.util.Arrays.asList("iOS", "Android"));
        device.put("absent", Collections.singletonList("UNKNOWN_OS"));
        device.put("selective", Collections.singletonList("iOS"));
        device.put("common", Collections.singletonList("Android"));
        manifest.samples.put("DeviceSystemName", device);

        String dateValue = "2026-01-15";
        if (cfg.queries != null && cfg.queries.querySuite != null
                && cfg.queries.querySuite.defaults != null
                && cfg.queries.querySuite.defaults.get("date_value") != null) {
            dateValue = cfg.queries.querySuite.defaults.get("date_value");
        }
        manifest.partitionDates = Collections.singletonList(dateValue);
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
