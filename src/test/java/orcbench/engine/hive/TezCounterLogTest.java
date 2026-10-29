package orcbench.engine.hive;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TezCounterLogTest {

    @Test
    void sumsHdfsBytesReadFromSummary() {
        Long bytes = TezCounterLog.sumHdfsBytesRead(Arrays.asList(
                "INFO  : Map 1 HDFS_BYTES_READ=1000",
                "INFO  : Reducer 2 HDFS_BYTES_READ: 25"));
        assertEquals(Long.valueOf(1025L), bytes);
    }

    @Test
    void missingCounterIsNullNotZero() {
        assertNull(TezCounterLog.sumHdfsBytesRead(Arrays.asList("INFO : no counters")));
    }

    @Test
    void dagKillAndMissingSessionJarInvalidateLoad() {
        assertTrue(TezCounterLog.invalidatesLoadMeasurement(
                "Dag received [DAG_TERMINATE, DAG_KILL] in RUNNING state"));
        assertTrue(TezCounterLog.invalidatesLoadMeasurement(
                "FileNotFoundException: hdfs://x/tmp/hive/u/_tez_session_dir/abc/hive-hcatalog-core.jar"));
        assertFalse(TezCounterLog.invalidatesLoadMeasurement("Table not found 'module_dictionary'"));
    }
}
