package orcbench.generate;

import orcbench.config.ConfigLoader;
import orcbench.model.Models.GenerateResult;
import orcbench.model.Models.LoadedConfig;
import orcbench.model.Models.QuerySampleManifest;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Checkpoint 4 unit tests — no HDFS / YARN / live Spark.
 */
class DatasetGeneratorTest {

    @Test
    void estimateRowCountScalesWithGib() {
        long s = DatasetGenerator.estimateRowCount(100);
        long m = DatasetGenerator.estimateRowCount(1024);
        assertTrue(s > 0);
        assertTrue(m > s);
        // 100 GiB / ~7500 B ≈ 14M rows
        assertTrue(s > 10_000_000L);
        assertTrue(s < 20_000_000L);
    }

    @Test
    void dryRunGenerateReturnsManifest() throws Exception {
        LoadedConfig cfg = ConfigLoader.load("conf", null);
        GenerateResult result = DatasetGenerator.generate(cfg, "S", true, false, null);
        assertTrue(result.dryRun);
        assertFalse(result.mocked);
        assertNotNull(result.samples);
        assertFalse(result.samples.partitionDates.isEmpty());
        assertTrue(result.basePath.startsWith("hdfs://sdp-dzo-lt-omlt/orc_test/S/RAW_ARRAY_BASE"));
        assertTrue(result.notes.stream().anyMatch(n -> n.contains("dry-run")));
    }

    @Test
    void unknownScaleThrows() throws Exception {
        LoadedConfig cfg = ConfigLoader.load("conf", null);
        assertThrows(IllegalArgumentException.class, () ->
                DatasetGenerator.generate(cfg, "XX", true, false, null));
    }

    @Test
    void buildEventRowAlignedArraysAndNullSemantics() throws Exception {
        LoadedConfig cfg = ConfigLoader.load("conf", null);
        List<String> keys = DatasetGenerator.sortedParamKeys(cfg.dataset);
        EventRow row = DatasetGenerator.buildEventRow(
                0L,
                cfg.dataset.generation.seed,
                cfg.dataset.generation.algorithmVersion,
                Instant.parse(cfg.dataset.generation.timestampFrom).toEpochMilli(),
                Instant.parse(cfg.dataset.generation.timestampToExclusive).toEpochMilli(),
                keys,
                cfg.dataset.parameters.catalog,
                cfg.dataset.jsonTemplates);

        assertNotNull(row.userLogin);
        assertNotNull(row.tags);
        assertEquals(3, row.tags.size(), "tags are CImodule, namespace, pod");
        assertEquals(DatasetGenerator.MODULE, row.module);
        assertEquals(64, row.session.length());
        assertFalse(row.paramName.isEmpty());
        assertEquals(row.paramName.size(), row.paramValue.size());
        assertEquals(row.paramName.size(), row.paramOldValue.size());
        for (String old : row.paramOldValue) {
            assertNull(old, "paramOldValue elements must be null");
        }
        assertTrue(DatasetGenerator.validateRow(row, cfg.dataset).isEmpty());
    }

    @Test
    void rowHashStableAndDistinct() throws Exception {
        LoadedConfig cfg = ConfigLoader.load("conf", null);
        List<String> keys = DatasetGenerator.sortedParamKeys(cfg.dataset);
        long seed = cfg.dataset.generation.seed;
        String algo = cfg.dataset.generation.algorithmVersion;
        long from = Instant.parse(cfg.dataset.generation.timestampFrom).toEpochMilli();
        long to = Instant.parse(cfg.dataset.generation.timestampToExclusive).toEpochMilli();

        EventRow a = DatasetGenerator.buildEventRow(0L, seed, algo, from, to, keys,
                cfg.dataset.parameters.catalog, cfg.dataset.jsonTemplates);
        EventRow a2 = DatasetGenerator.buildEventRow(0L, seed, algo, from, to, keys,
                cfg.dataset.parameters.catalog, cfg.dataset.jsonTemplates);
        EventRow b = DatasetGenerator.buildEventRow(1L, seed, algo, from, to, keys,
                cfg.dataset.parameters.catalog, cfg.dataset.jsonTemplates);

        String ha = DatasetGenerator.rowHash(a);
        assertEquals(64, ha.length());
        assertEquals(ha, DatasetGenerator.rowHash(a2));
        assertNotEquals(ha, DatasetGenerator.rowHash(b));
    }

    @Test
    void mockGenerateUsesFakeBackend() throws Exception {
        LoadedConfig cfg = ConfigLoader.load("conf", null);
        // Override row count via injected fake with small cap; still pass full config
        FakeGenerateBackend fake = new FakeGenerateBackend(128);
        // Temporarily force a tiny row count by generating with a copy… use mock path
        // with injected backend that caps rows.
        GenerateResult result = DatasetGenerator.generate(cfg, "S", false, true, fake);
        assertTrue(result.mocked);
        assertEquals(1, fake.writeRawCalls);
        assertEquals(1, fake.writeRefsCalls);
        assertEquals(1, fake.writeManifestCalls);
        assertFalse(fake.events.isEmpty());
        assertTrue(fake.events.size() <= 128);
        assertTrue(fake.requestedRowCount > fake.events.size());
        assertNotNull(fake.lastManifest);
        assertTrue(fake.lastBasePath.endsWith("/RAW_ARRAY_BASE")
                || fake.lastBasePath.contains("RAW_ARRAY_BASE"));

        Set<String> hashes = new HashSet<>();
        for (EventRow e : fake.events) {
            assertTrue(DatasetGenerator.validateRow(e, cfg.dataset).isEmpty());
            hashes.add(DatasetGenerator.rowHash(e));
        }
        assertEquals(fake.events.size(), hashes.size());
    }

    @Test
    void querySampleManifestFromSynthHasCases() throws Exception {
        LoadedConfig cfg = ConfigLoader.load("conf", null);
        List<String> keys = DatasetGenerator.sortedParamKeys(cfg.dataset);
        long seed = cfg.dataset.generation.seed;
        String algo = cfg.dataset.generation.algorithmVersion;
        long from = Instant.parse(cfg.dataset.generation.timestampFrom).toEpochMilli();
        long to = Instant.parse(cfg.dataset.generation.timestampToExclusive).toEpochMilli();

        QuerySampleManifest m = DatasetGenerator.buildQuerySamplesFromSynth(
                rowId -> DatasetGenerator.buildEventRow(rowId, seed, algo, from, to, keys,
                        cfg.dataset.parameters.catalog, cfg.dataset.jsonTemplates),
                cfg.dataset,
                cfg,
                500L,
                "/orc_test/S/RAW_ARRAY_BASE",
                "/orc_test/S/refs",
                "/orc_test/S/manifest");
        assertTrue(m.samples.containsKey("userLogin"));
        assertTrue(m.samples.get("userLogin").containsKey("present"));
        assertTrue(m.samples.get("userLogin").containsKey("absent"));
        assertFalse(m.partitionDates.isEmpty());
    }
}
