package orcbench.engine.hive;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartitionLayoutTest {

    @Test
    void flatOrcFilesDropDeclaredPartitions() {
        List<String> children = Arrays.asList("_SUCCESS", "part-00000.orc", "part-00001.orc");
        assertFalse(PartitionLayout.hasHivePartitionDirs(children));
        assertTrue(PartitionLayout.effectivePartitions(
                Collections.singletonList("key"), children).isEmpty());
    }

    @Test
    void keyDirectoriesKeepDeclaredPartitions() {
        List<String> children = Arrays.asList("_SUCCESS", "key=2025-04-14--12");
        assertTrue(PartitionLayout.hasHivePartitionDirs(children));
        assertEquals(Collections.singletonList("key"),
                PartitionLayout.effectivePartitions(Collections.singletonList("key"), children));
    }

    @Test
    void failedListingKeepsDeclaredPartitions() {
        assertEquals(Collections.singletonList("key"),
                PartitionLayout.effectivePartitions(Collections.singletonList("key"), null));
    }
}
