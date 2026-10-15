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
                "/benchmarks/audit_orc/S/RAW_ARRAY_BASE",
                Arrays.asList("event_date"),
                Collections.singletonList(new String[]{"epk_id", "STRING"}));
        assertTrue(sql.contains("CREATE EXTERNAL TABLE events"));
        assertTrue(sql.contains("epk_id STRING"));
        assertTrue(sql.contains("PARTITIONED BY (event_date STRING)"));
        assertTrue(sql.contains("LOCATION '/benchmarks/audit_orc/S/RAW_ARRAY_BASE'"));
        assertTrue(sql.contains("STORED AS ORC"));
    }

    @Test
    void mapTypeConversion() {
        String cols = SchemaDdl.hiveColumnsFromOrcSchema("struct<params:map<string,string>>");
        assertTrue(cols.contains("params MAP<STRING,STRING>"));
    }
}
