package orcbench.generate;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SynthGeneratorsTest {

    @Test
    void mixIsDeterministic() {
        long a = SynthGenerators.mix(1, 2, "col", "algo");
        long b = SynthGenerators.mix(1, 2, "col", "algo");
        assertEquals(a, b);
        assertNotEquals(a, SynthGenerators.mix(1, 3, "col", "algo"));
    }

    @Test
    void boundedRespectsCardinality() {
        for (long i = 0; i < 100; i++) {
            long v = SynthGenerators.bounded(42, i, "c", "a", 10);
            assertTrue(v >= 0 && v < 10);
        }
    }

    @Test
    void prefixedIdAndDictionary() {
        assertTrue(SynthGenerators.prefixedId(1, 0, "c", "a", "user", 100).startsWith("user"));
        assertTrue(SynthGenerators.dictionary(1, 0, "c", "a", "mod_", 50).startsWith("mod_"));
    }

    @Test
    void weightedPickUsesValues() {
        String v = SynthGenerators.weightedPick(1, 0, "name", "a",
                Arrays.asList("LOGON", "LOGOFF"), Arrays.asList(1.0, 0.0));
        assertEquals("LOGON", v);
    }

    @Test
    void uuidAndHexAndIp() {
        assertEquals(36, SynthGenerators.uuidDeterministic(1, 2, "c", "a").length());
        assertEquals(32, SynthGenerators.hexString(1, 2, "c", "a", 32).length());
        assertTrue(SynthGenerators.documentationIpv4(1, 2, "c", "a").startsWith("203.0.113."));
    }

    @Test
    void timestampUniformInRange() {
        long from = 1_000_000L;
        long to = 2_000_000L;
        long ts = SynthGenerators.timestampUniform(1, 5, "c", "a", from, to);
        assertTrue(ts >= from && ts < to);
    }

    @Test
    void jsonMobileSdkProducesJson() {
        Map<String, Object> tpl = new HashMap<>();
        Map<String, Object> sys = new HashMap<>();
        sys.put("values", Arrays.asList("iOS", "Android"));
        tpl.put("DeviceSystemName", sys);
        String json = SynthGenerators.jsonMobileSdk(1, 2, "a", tpl);
        assertTrue(json.contains("DeviceSystemName"));
        assertTrue(json.contains("HardwareID"));
    }

    @Test
    void fakeToken() {
        assertTrue(SynthGenerators.fakeToken(1, 2, "c", "a", "tok_", 10).startsWith("tok_"));
    }

    @Test
    void weightedPickRejectsEmpty() {
        assertThrows(IllegalArgumentException.class, () ->
                SynthGenerators.weightedPick(1, 1, "c", "a", Collections.emptyList(), null));
    }
}
