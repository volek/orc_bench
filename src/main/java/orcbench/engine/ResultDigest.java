package orcbench.engine;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Locale;

/**
 * Order-independent fingerprint of a SQL result.
 * Each canonical row ({@code col|col}, null as {@code \N}) contributes
 * SHA-256; contributions are added modulo 2^256, so duplicates do not cancel
 * and partial sums from partitions merge without sorting or shipping rows.
 * The published hash is the first 16 hex characters of that sum.
 */
public final class ResultDigest implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final int WIDTH = 32;

    private long count;
    private final byte[] sum = new byte[WIDTH];

    public void add(String canonicalRow) {
        count++;
        add256(sum, sha256(canonicalRow == null ? "" : canonicalRow));
    }

    public void merge(ResultDigest other) {
        if (other == null || other.count == 0) {
            return;
        }
        count += other.count;
        add256(sum, other.sum);
    }

    public ResultDigest copy() {
        ResultDigest out = new ResultDigest();
        out.count = count;
        System.arraycopy(sum, 0, out.sum, 0, WIDTH);
        return out;
    }

    public long count() {
        return count;
    }

    /** First 16 hex chars of the 256-bit sum. */
    public String hashHex() {
        StringBuilder sb = new StringBuilder(16);
        for (int i = 0; i < 8; i++) {
            sb.append(String.format(Locale.ROOT, "%02x", sum[i]));
        }
        return sb.toString();
    }

    public static String hashRows(String[] parts) {
        ResultDigest digest = new ResultDigest();
        if (parts != null) {
            for (String part : parts) {
                digest.add(part);
            }
        }
        return digest.hashHex();
    }

    private static byte[] sha256(String text) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            return md.digest(text.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** Little-endian-free 256-bit add: index 0 is the most significant byte. */
    private static void add256(byte[] acc, byte[] value) {
        int carry = 0;
        for (int i = WIDTH - 1; i >= 0; i--) {
            int s = (acc[i] & 0xff) + (value[i] & 0xff) + carry;
            acc[i] = (byte) s;
            carry = s >>> 8;
        }
    }
}
