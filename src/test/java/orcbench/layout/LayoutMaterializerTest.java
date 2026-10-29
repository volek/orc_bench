package orcbench.layout;

import orcbench.config.ConfigLoader;
import orcbench.model.Models.ExperimentInstance;
import orcbench.model.Models.LayoutWriteRequest;
import orcbench.model.Models.LoadedConfig;
import orcbench.model.Models.MaterializeResult;
import orcbench.model.Models.OrcWriteSettings;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Checkpoint 5 unit tests — no HDFS / YARN / live Spark.
 */
class LayoutMaterializerTest {

    @Test
    void dryRunDoesNotWriteLayouts() throws Exception {
        LoadedConfig cfg = ConfigLoader.load("conf", null);
        List<ExperimentInstance> instances = ExperimentExpander.expandPhysical(
                cfg.benchmark, "S", Collections.singletonList("COMPRESSION"));
        FakeMaterializeBackend fake = new FakeMaterializeBackend();
        fake.seedBaseline("hdfs://sdp-dzo-lt-omlt/orc_test/S/RAW_ARRAY_BASE", 1024);

        MaterializeResult out = LayoutMaterializer.materialize(
                cfg, "S", instances, true, false, false, Collections.emptyList(), fake);

        assertTrue(out.dryRun);
        assertEquals(0, fake.writeLayoutCalls);
        assertEquals(0, fake.writeMarkerCalls);
        assertTrue(out.plannedWrites >= 1);
        assertTrue(out.notes.stream().anyMatch(n -> n.contains("dry-run")));
    }

    @Test
    void mockMaterializeAppliesCompressionRewrite() throws Exception {
        LoadedConfig cfg = ConfigLoader.load("conf", null);
        List<ExperimentInstance> instances = ExperimentExpander.expandPhysical(
                cfg.benchmark, "S", Collections.singletonList("COMPRESSION"));
        FakeMaterializeBackend fake = new FakeMaterializeBackend();
        fake.seedBaseline("hdfs://sdp-dzo-lt-omlt/orc_test/S/RAW_ARRAY_BASE", 128L * 1024L * 1024L);

        MaterializeResult out = LayoutMaterializer.materialize(
                cfg, "S", instances, false, true, false, Collections.emptyList(), fake);

        assertTrue(out.mocked || out.written > 0);
        assertTrue(fake.writeLayoutCalls >= 1);
        assertEquals(fake.writeLayoutCalls, fake.writeMarkerCalls);
        assertTrue(out.written >= 1);

        boolean sawZstd = false;
        boolean sawSnappy = false;
        for (LayoutWriteRequest w : fake.writes) {
            assertNotNull(w.orcSettings);
            assertNotNull(w.orcSettings.compression);
            if ("ZSTD".equalsIgnoreCase(w.orcSettings.compression)) {
                sawZstd = true;
            }
            if ("SNAPPY".equalsIgnoreCase(w.orcSettings.compression)) {
                sawSnappy = true;
            }
            assertTrue(w.markerPath.contains("/orc_test/state/"));
            assertTrue(w.markerPath.endsWith(".done"));
        }
        assertTrue(sawZstd, "COMPRESSION matrix must include ZSTD for Spark");
        assertTrue(sawSnappy, "COMPRESSION matrix must include SNAPPY");
    }

    @Test
    void idempotencySkipsExistingMarkers() throws Exception {
        LoadedConfig cfg = ConfigLoader.load("conf", null);
        List<ExperimentInstance> instances = ExperimentExpander.expandPhysical(
                cfg.benchmark, "S", Collections.singletonList("STRIPE_SIZE"));
        FakeMaterializeBackend fake = new FakeMaterializeBackend();
        fake.seedBaseline("hdfs://sdp-dzo-lt-omlt/orc_test/S/RAW_ARRAY_BASE", 64L * 1024L * 1024L);

        // First pass writes markers
        MaterializeResult first = LayoutMaterializer.materialize(
                cfg, "S", instances, false, true, false, Collections.emptyList(), fake);
        int writtenFirst = first.written;
        assertTrue(writtenFirst >= 1);

        int writesAfterFirst = fake.writeLayoutCalls;

        // Second pass must skip all via markers
        MaterializeResult second = LayoutMaterializer.materialize(
                cfg, "S", instances, false, true, false, Collections.emptyList(), fake);
        assertEquals(0, second.written);
        assertEquals(writtenFirst, second.skippedExistingMarker);
        assertEquals(writesAfterFirst, fake.writeLayoutCalls, "no additional writes on resume");
    }

    @Test
    void cleanupPolicyRetainsBaselineRefsAndKeepList() {
        List<String> children = Arrays.asList(
                "RAW_ARRAY_BASE", "refs", "manifest",
                "COMPRESSION_ZSTD", "MINMAX_SORT_userLogin", "BLOOM_USER_userLogin");
        List<ExperimentInstance> instances = Arrays.asList(
                inst("COMPRESSION", "COMPRESSION_ZSTD"),
                inst("MINMAX_SORT", "MINMAX_SORT_userLogin"),
                inst("BLOOM_USER", "BLOOM_USER_userLogin"));

        List<String> delete = LayoutMaterializer.selectCleanupTargets(
                children, "RAW_ARRAY_BASE", instances, Collections.singletonList("MINMAX_SORT"));

        assertFalse(delete.contains("RAW_ARRAY_BASE"));
        assertFalse(delete.contains("refs"));
        assertFalse(delete.contains("manifest"));
        assertFalse(delete.contains("MINMAX_SORT_userLogin"));
        assertTrue(delete.contains("COMPRESSION_ZSTD"));
        assertTrue(delete.contains("BLOOM_USER_userLogin"));
    }

    @Test
    void cleanupViaBackendDeletesNonRetained() throws Exception {
        LoadedConfig cfg = ConfigLoader.load("conf", null);
        List<ExperimentInstance> instances = ExperimentExpander.expandPhysical(
                cfg.benchmark, "S", Arrays.asList("COMPRESSION", "ROW_INDEX"));
        FakeMaterializeBackend fake = new FakeMaterializeBackend();
        fake.seedBaseline("hdfs://sdp-dzo-lt-omlt/orc_test/S/RAW_ARRAY_BASE", 32L * 1024L * 1024L);

        LayoutMaterializer.materialize(
                cfg, "S", instances, false, true, false, Collections.emptyList(), fake);
        assertTrue(fake.writeLayoutCalls >= 2);

        MaterializeResult cleaned = LayoutMaterializer.materialize(
                cfg, "S", instances, false, true, true,
                Collections.singletonList("COMPRESSION"), fake);

        // Markers cause zero new writes; cleanup still runs
        assertEquals(0, cleaned.written);
        assertTrue(cleaned.cleaned >= 1);
        assertTrue(fake.pathExists("hdfs://sdp-dzo-lt-omlt/orc_test/S/RAW_ARRAY_BASE"));
        // At least one ROW_INDEX layout should have been targeted for cleanup
        assertTrue(cleaned.notes.stream().anyMatch(n -> n.contains("cleaned") && n.contains("ROW_INDEX"))
                || fake.deleted.stream().anyMatch(p -> p.contains("ROW_INDEX")));
    }

    @Test
    void rewriteFactorsCoveredInDescribe() throws Exception {
        LoadedConfig cfg = ConfigLoader.load("conf", null);
        List<ExperimentInstance> bloom = ExperimentExpander.expandPhysical(
                cfg.benchmark, "S", Collections.singletonList("BLOOM_FPP"));
        assertFalse(bloom.isEmpty());
        String desc = LayoutMaterializer.describeRewrite(bloom.get(0));
        assertTrue(desc.contains("BLOOM_FPP"));
        assertTrue(desc.contains("fpp="));
        assertTrue(desc.contains("bloom="));

        List<ExperimentInstance> nest = ExperimentExpander.expandPhysical(
                cfg.benchmark, "S", Collections.singletonList("NESTED_REPRESENTATIONS"));
        boolean hasHybrid = false;
        for (ExperimentInstance i : nest) {
            if ("HYBRID".equals(i.representation)) {
                hasHybrid = true;
                assertTrue(LayoutMaterializer.needsMaterialize(i, "/orc_test/S/RAW_ARRAY_BASE"));
            }
        }
        assertTrue(hasHybrid);

        List<ExperimentInstance> buckets = ExperimentExpander.expandPhysical(
                cfg.benchmark, "S", Collections.singletonList("BUCKETING"));
        assertEquals(1, buckets.size(), "an explicit excluded id is recorded, not dropped");
        assertNotNull(buckets.get(0).skipReason);
        assertTrue(buckets.get(0).skipReason.contains("excluded"));
        assertFalse(LayoutMaterializer.needsMaterialize(buckets.get(0), "/orc_test/S/RAW_ARRAY_BASE"));
    }

    @Test
    void missingBaselineThrowsOnMock() throws Exception {
        LoadedConfig cfg = ConfigLoader.load("conf", null);
        List<ExperimentInstance> instances = ExperimentExpander.expandPhysical(
                cfg.benchmark, "S", Collections.singletonList("FILE_SIZES"));
        FakeMaterializeBackend fake = new FakeMaterializeBackend();
        // no seedBaseline
        assertThrows(IllegalStateException.class, () ->
                LayoutMaterializer.materialize(
                        cfg, "S", instances, false, true, false, Collections.emptyList(), fake));
    }

    @Test
    void generateDdlIncludesPartitionsAndBuckets() {
        ExperimentInstance i = new ExperimentInstance();
        i.experimentId = "PARTITIONING";
        i.variantLabel = "event_date";
        i.partitionColumns = Collections.singletonList("event_date");
        i.bucketCount = 32;
        i.bucketKey = "epk_id";
        String ddl = LayoutMaterializer.generateDdl(i, "/orc_test/S/PARTITIONING_event_date");
        assertTrue(ddl.contains("PARTITIONED BY"));
        assertTrue(ddl.contains("event_date"));
        assertTrue(ddl.contains("CLUSTERED BY (epk_id) INTO 32 BUCKETS"));
        assertTrue(ddl.contains("/orc_test/S/PARTITIONING_event_date"));
    }

    @Test
    void queryOnlyDoesNotNeedMaterialize() throws Exception {
        LoadedConfig cfg = ConfigLoader.load("conf", null);
        List<ExperimentInstance> instances = ExperimentExpander.expandPhysical(
                cfg.benchmark, "S", Collections.singletonList("ORC_COLUMN_PRUNING"));
        String base = "/orc_test/S/RAW_ARRAY_BASE";
        for (ExperimentInstance i : instances) {
            assertFalse(LayoutMaterializer.needsMaterialize(i, base));
        }
    }

    @Test
    void toRequestCopiesOrcSettingsIndependently() {
        ExperimentInstance i = new ExperimentInstance();
        i.experimentId = "ROW_INDEX";
        i.variantLabel = "20000";
        i.kind = "rewrite";
        i.layoutPath = "/orc_test/S/ROW_INDEX_20000";
        i.representation = "RAW_ARRAY";
        i.orcSettings = new OrcWriteSettings();
        i.orcSettings.compression = "SNAPPY";
        i.orcSettings.rowIndexStride = 20000;
        i.orcSettings.stripeSizeMib = 64;
        i.targetFileSizeMib = 256;

        LayoutWriteRequest req = LayoutMaterializer.toRequest(
                i, "/orc_test/S/RAW_ARRAY_BASE",
                Collections.singletonList("epk_id"), 1000L, "/orc_test/state/x.done");
        req.orcSettings.rowIndexStride = 1;
        assertEquals(20000, i.orcSettings.rowIndexStride);
        assertEquals("/orc_test/state/x.done", req.markerPath);
    }

    private static ExperimentInstance inst(String expId, String dir) {
        ExperimentInstance i = new ExperimentInstance();
        i.experimentId = expId;
        i.layoutPath = "/orc_test/S/" + dir;
        i.kind = "rewrite";
        i.variantLabel = "v";
        return i;
    }
}
