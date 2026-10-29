package orcbench.generate;

import orcbench.generate.GenerateBackend.EventRowFactory;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class SparkOrcGenerateBackendTest {

    @Test
    void partitionMapperBuildsOneRowAtATime() {
        final int[] calls = new int[1];
        EventRowFactory factory = new EventRowFactory() {
            @Override
            public EventRow create(long rowId) {
                calls[0]++;
                return sample(rowId);
            }
        };
        Iterator<Long> ids = Arrays.asList(1L, 2L, 3L).iterator();
        Iterator<Row> rows = new SparkOrcGenerateBackend.RowPartitionMapper(factory).call(ids);

        assertEquals(0, calls[0]);
        assertEquals("1", rows.next().getString(1));
        assertEquals(1, calls[0]);
        assertEquals("2", rows.next().getString(1));
        assertEquals(2, calls[0]);
        rows.next();
        assertFalse(rows.hasNext());
        assertEquals(3, calls[0]);
    }

    private static EventRow sample(long rowId) {
        return new EventRow(
                "login",
                0L,
                Long.toString(rowId),
                "node",
                "module",
                "name",
                "",
                "ver",
                Collections.singletonList("tag"),
                Collections.<String>emptyList(),
                Collections.<String>emptyList(),
                Collections.<String>emptyList(),
                "arch",
                0L,
                0L,
                "id",
                "src",
                "hash",
                null,
                null);
    }
}
