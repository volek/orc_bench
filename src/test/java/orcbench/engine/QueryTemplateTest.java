package orcbench.engine;

import orcbench.config.ConfigLoader;
import orcbench.model.Models.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class QueryTemplateTest {

    private static LoadedConfig cfg;
    private static QuerySampleManifest samples;

    @BeforeAll
    static void setUp() throws Exception {
        cfg = ConfigLoader.load("conf", null);
        samples = new QuerySampleManifest();
        samples.samples.put("userLogin", Collections.singletonMap("present", Arrays.asList("userLogin1", "userLogin2", "userLogin3")));
        samples.samples.put("session", Collections.singletonMap("present", Collections.singletonList("copy2_1")));
        samples.samples.put("EPK_ID", Collections.singletonMap("present", Collections.singletonList("epk_1")));
        samples.samples.put("SESSION_ID", Collections.singletonMap("present", Collections.singletonList("copy2_1")));
        samples.samples.put("IP_ADDRESS", Collections.singletonMap("present", Collections.singletonList("203.0.113.9")));
        samples.samples.put("DeviceSystemName", Collections.singletonMap("present", Collections.singletonList("iOS")));
    }

    @Test
    void resolvesSparkQueryWithSubstitution() {
        QuerySpec q = cfg.queries.queries.stream().filter(x -> "Q02".equals(x.id)).findFirst().get();
        QueryTemplate.BoundQuery b = QueryTemplate.resolveSql(q, cfg.queries, "spark_3_2", "events", samples, "RAW_ARRAY");
        assertNotNull(b);
        assertEquals("Q02", b.queryId);
        assertTrue(b.sql.contains("userLogin1"));
        assertTrue(b.sql.contains("FROM events"));
    }

    @Test
    void filtersByRepresentationAndDialect() {
        QuerySpec q = cfg.queries.queries.stream().filter(x -> "NP_ARRAY_EPK_SPARK".equals(x.id)).findFirst().get();
        assertNull(QueryTemplate.resolveSql(q, cfg.queries, "hive_tez", "events", samples, "RAW_ARRAY"));
        assertNotNull(QueryTemplate.resolveSql(q, cfg.queries, "spark_3_2", "events", samples, "RAW_ARRAY"));
    }

    @Test
    void npRawArrayFallsBackToDialectTemplate() {
        QuerySpec q = cfg.queries.queries.stream().filter(x -> "NP_RAW_ARRAY".equals(x.id)).findFirst().get();
        QueryTemplate.BoundQuery spark = QueryTemplate.resolveSql(q, cfg.queries, "spark_3_2", "events", samples, "RAW_ARRAY");
        assertNotNull(spark);
        assertTrue(spark.sql.toLowerCase().contains("array_position") || spark.sql.contains("element_at"));
    }

    @Test
    void queriesForExperimentFiltersIds() {
        List<QueryTemplate.BoundQuery> qs = QueryTemplate.queriesForExperiment(
                cfg.queries, Arrays.asList("Q01", "Q08"), "spark_3_2", "events", samples, "RAW_ARRAY");
        assertEquals(2, qs.size());
    }

    @Test
    void escapesQuotesInParams() {
        QuerySampleManifest evil = new QuerySampleManifest();
        evil.samples = new HashMap<>();
        evil.samples.put("userLogin", Collections.singletonMap("present", Collections.singletonList("O'Brien")));
        QuerySpec q = cfg.queries.queries.stream().filter(x -> "Q02".equals(x.id)).findFirst().get();
        QueryTemplate.BoundQuery b = QueryTemplate.resolveSql(q, cfg.queries, "spark_3_2", "t", evil, "RAW_ARRAY");
        assertTrue(b.sql.contains("O''Brien"));
    }
}
