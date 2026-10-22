package orcbench.engine;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class ResultDigestTest {

    @Test
    void orderDoesNotChangeHashAndDuplicatesDoNotCancel() {
        String ab = ResultDigest.hashRows(new String[]{"a|1", "b|2"});
        String ba = ResultDigest.hashRows(new String[]{"b|2", "a|1"});
        assertEquals(ab, ba);
        assertEquals(16, ab.length());

        String twice = ResultDigest.hashRows(new String[]{"a|1", "a|1"});
        String once = ResultDigest.hashRows(new String[]{"a|1"});
        String empty = ResultDigest.hashRows(new String[0]);
        assertNotEquals(twice, once);
        assertNotEquals(twice, empty);
        assertEquals("0000000000000000", empty);
    }

    @Test
    void partitionSumsMatchFullHash() {
        ResultDigest left = new ResultDigest();
        left.add("a|1");
        left.add("b|2");
        ResultDigest right = new ResultDigest();
        right.add("a|1");
        ResultDigest merged = left.copy();
        merged.merge(right);

        assertEquals(3L, merged.count());
        assertEquals(ResultDigest.hashRows(new String[]{"b|2", "a|1", "a|1"}), merged.hashHex());
    }
}
