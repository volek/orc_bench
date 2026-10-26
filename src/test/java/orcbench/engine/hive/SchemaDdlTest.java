package orcbench.engine.hive;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class SchemaDdlTest {

    @Test
    void hiveColumnsFromOrcSchema() {
        String schema = "struct<\n  userLogin:string,\n  createdAt:timestamp,\n  tags:array<string>\n>";
        String cols = SchemaDdl.hiveColumnsFromOrcSchema(schema);
        assertTrue(cols.contains("userLogin STRING"));
        assertTrue(cols.contains("createdAt TIMESTAMP"));
        assertTrue(cols.contains("tags ARRAY<STRING>"));
    }

    @Test
    void createExternalTableSqlWithPartitionsAndExtras() {
        String schema = "struct<userLogin:string,__time:timestamp>";
        String sql = SchemaDdl.createExternalTableSql(
                "events",
                schema,
                "/orc_test/S/RAW_ARRAY_BASE",
                Arrays.asList("event_date"),
                Collections.singletonList(new String[]{"epk_id", "STRING"}));
        assertTrue(sql.contains("CREATE EXTERNAL TABLE events"));
        assertTrue(sql.contains("`__time` TIMESTAMP"), sql);
        assertFalse(sql.contains("\n  __time TIMESTAMP"), sql);
        assertTrue(sql.contains("userLogin STRING"));
        assertTrue(sql.contains("epk_id STRING"));
        assertTrue(sql.contains("PARTITIONED BY (event_date STRING)"));
        assertTrue(sql.contains("LOCATION '/orc_test/S/RAW_ARRAY_BASE'"));
        assertTrue(sql.contains("STORED AS ORC"));
    }

    @Test
    void mapTypeConversion() {
        String cols = SchemaDdl.hiveColumnsFromOrcSchema("struct<params:map<string,string>>");
        assertTrue(cols.contains("params MAP<STRING,STRING>"));
    }

    @Test
    void nestedArrayStructPreservesCommas() {
        String cols = SchemaDdl.hiveColumnsFromOrcSchema(
                "struct<params:array<struct<name:string,value:string>>>");
        assertTrue(cols.contains("params ARRAY<STRUCT<"), cols);
        assertTrue(cols.contains("name:STRING"), cols);
        assertTrue(cols.contains("value:STRING"), cols);
        // Depth-aware split must not break the nested struct into two top-level columns.
        assertFalse(cols.contains("params ARRAY<STRUCT<name:STRING>,\n"), cols);
    }

    @Test
    void representationExtrasForFlatAndMap() {
        assertTrue(SchemaDdl.representationExtras("RAW_ARRAY").isEmpty());
        assertTrue(SchemaDdl.representationExtras("FLAT").size() >= 5);
        assertEquals(1, SchemaDdl.representationExtras("MAP").size());
        assertEquals("MAP<STRING,STRING>", SchemaDdl.representationExtras("MAP").get(0)[1]);
    }
}
