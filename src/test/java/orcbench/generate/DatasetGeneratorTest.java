package orcbench.generate;

import orcbench.config.ConfigLoader;
import orcbench.model.Models.*;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DatasetGeneratorTest {

    @Test
    void estimateRowCountScalesWithGib() {
        long s = DatasetGenerator.estimateRowCount(100);
        long m = DatasetGenerator.estimateRowCount(1024);
        assertTrue(s > 0);
        assertTrue(m > s);
    }

    @Test
    void dryRunGenerateReturnsManifest() throws Exception {
        LoadedConfig cfg = ConfigLoader.load("conf", null);
        QuerySampleManifest manifest = DatasetGenerator.generate(null, cfg, "S", true);
        assertNotNull(manifest);
        assertFalse(manifest.partitionDates.isEmpty());
    }

    @Test
    void unknownScaleThrows() throws Exception {
        LoadedConfig cfg = ConfigLoader.load("conf", null);
        assertThrows(IllegalArgumentException.class, () ->
                DatasetGenerator.generate(null, cfg, "XX", true));
    }

    @Test
    void buildEventRowProducesAlignedArrays() throws Exception {
        LoadedConfig cfg = ConfigLoader.load("conf", null);
        List<String> keys = new ArrayList<>(cfg.dataset.parameters.catalog.keySet());
        Collections.sort(keys);
        org.apache.spark.sql.Row row = DatasetGenerator.buildEventRow(
                0L, cfg.dataset.generation.seed, cfg.dataset.generation.algorithmVersion,
                Instant.parse(cfg.dataset.generation.timestampFrom).toEpochMilli(),
                Instant.parse(cfg.dataset.generation.timestampToExclusive).toEpochMilli(),
                keys, cfg.dataset.parameters.catalog, cfg.dataset.jsonTemplates);
        assertNotNull(row.getString(0));
        assertEquals(keys.size(), row.getList(10).size());
        assertEquals(keys.size(), row.getList(11).size());
    }
}
