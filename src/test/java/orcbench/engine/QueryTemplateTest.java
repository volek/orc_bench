package orcbench.engine;

import orcbench.config.ConfigLoader;
import orcbench.model.Models.LoadedConfig;
import orcbench.model.Models.QuerySampleManifest;
import orcbench.model.Models.QuerySpec;
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
        samples.samples.put("userLogin", Collections.singletonMap("present",
                Arrays.asList("userLogin1", "userLogin2", "userLogin3")));
        samples.samples.put("session", Collections.singletonMap("present",
                Collections.singletonList("copy2_1")));
        samples.samples.put("EPK_ID", Collections.singletonMap("present",
                Collections.singletonList("epk_1")));
        samples.samples.put("SESSION_ID", Collections.singletonMap("present",
                Collections.singletonList("copy2_1")));
        samples.samples.put("IP_ADDRESS", Collections.singletonMap("present",
                Collections.singletonList("203.0.113.9")));
        samples.samples.put("DeviceSystemName", Collections.singletonMap("present",
                Collections.singletonList("iOS")));
    }

    @Test
    void resolvesSparkQueryWithSubstitution() {
        QuerySpec q = cfg.queries.queries.stream().filter(x -> "Q_USERLOGIN".equals(x.id)).findFirst().get();
        QueryTemplate.BoundQuery b = QueryTemplate.resolveSql(
                q, cfg.queries, "spark_3_2", "events", samples, "RAW_ARRAY");
        assertNotNull(b);
        assertEquals("Q_USERLOGIN", b.queryId);
        assertTrue(b.sql.contains("userLogin1"));
        assertTrue(b.sql.contains("FROM events"));
    }

    @Test
    void filtersByRepresentationAndDialect() {
        QuerySpec q = cfg.queries.queries.stream()
                .filter(x -> "Q_EPK_SPARK".equals(x.id)).findFirst().get();
        assertNull(QueryTemplate.resolveSql(q, cfg.queries, "hive_tez", "events", samples, "RAW_ARRAY"));
        assertNotNull(QueryTemplate.resolveSql(q, cfg.queries, "spark_3_2", "events", samples, "RAW_ARRAY"));
    }

    @Test
    void queriesForExperimentFiltersIds() {
        List<QueryTemplate.BoundQuery> qs = QueryTemplate.queriesForExperiment(
                cfg.queries, Arrays.asList("Q_SESSION_ILIKE", "Q_UNFILTERED"), "spark_3_2", "events", samples, "RAW_ARRAY");
        assertEquals(2, qs.size());
    }

    @Test
    void hiveDialectQuotesMixedCaseColumns() {
        QuerySpec q = cfg.queries.queries.stream().filter(x -> "Q_USERLOGIN".equals(x.id)).findFirst().get();
        QueryTemplate.BoundQuery hive = QueryTemplate.resolveSql(
                q, cfg.queries, "hive_tez", "events", samples, "RAW_ARRAY");
        QueryTemplate.BoundQuery spark = QueryTemplate.resolveSql(
                q, cfg.queries, "spark_3_2", "events", samples, "RAW_ARRAY");
        assertTrue(hive.sql.contains("`userLogin`"), hive.sql);
        assertTrue(hive.sql.contains("userLogin1"), hive.sql);
        assertFalse(spark.sql.contains("`userLogin`"), spark.sql);
    }

    @Test
    void unboundQueryIsReportedInsteadOfDropped() {
        List<QueryTemplate.RejectedQuery> rejected = QueryTemplate.unboundQueries(
                cfg.queries, Arrays.asList("Q_EPK", "Q_MISSING"), "spark_3_2", "RAW_ARRAY");
        assertEquals(2, rejected.size());
        QueryTemplate.RejectedQuery epk = null;
        QueryTemplate.RejectedQuery missing = null;
        for (QueryTemplate.RejectedQuery rq : rejected) {
            if ("Q_EPK".equals(rq.queryId)) {
                epk = rq;
            }
            if ("Q_MISSING".equals(rq.queryId)) {
                missing = rq;
            }
        }
        assertNotNull(epk);
        assertTrue(epk.reason.contains("RAW_ARRAY"), epk.reason);
        assertNotNull(missing);
        assertTrue(missing.reason.contains("not in the suite"));
    }

    @Test
    void eventDateOnFlatLayoutIsRejected() {
        assertNotNull(QueryTemplate.layoutRejectReason(
                "SELECT COUNT(*) FROM t WHERE event_date = DATE '2026-01-15'",
                Collections.<String>emptyList()));
        assertNull(QueryTemplate.layoutRejectReason(
                "SELECT COUNT(*) FROM t WHERE event_date = DATE '2026-01-15'",
                Collections.singletonList("event_date")));
        assertNull(QueryTemplate.layoutRejectReason(
                "SELECT COUNT(*) FROM t WHERE name = 'event_date'",
                Collections.<String>emptyList()));
    }

    @Test
    void escapesQuotesInParams() {
        QuerySampleManifest evil = new QuerySampleManifest();
        evil.samples = new HashMap<>();
        evil.samples.put("userLogin", Collections.singletonMap("present",
                Collections.singletonList("O'Brien")));
        QuerySpec q = cfg.queries.queries.stream().filter(x -> "Q_USERLOGIN".equals(x.id)).findFirst().get();
        QueryTemplate.BoundQuery b = QueryTemplate.resolveSql(
                q, cfg.queries, "spark_3_2", "t", evil, "RAW_ARRAY");
        assertTrue(b.sql.contains("O''Brien"));
    }
}
