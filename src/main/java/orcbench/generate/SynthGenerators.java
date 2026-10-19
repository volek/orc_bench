package orcbench.generate;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Deterministic column value generators for {@code audit_synth_v1}
 * ({@code dataset.yaml} generation.algorithm_version).
 */
public final class SynthGenerators {
    private SynthGenerators() {}

    public static long mix(long seed, long rowId, String column, String algo) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] dig = md.digest((seed + "|" + rowId + "|" + column + "|" + algo)
                    .getBytes(StandardCharsets.UTF_8));
            long v = 0L;
            for (int i = 0; i < 8; i++) {
                v = (v << 8) | (dig[i] & 0xffL);
            }
            return v;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static long bounded(long seed, long rowId, String column, String algo, long cardinality) {
        long c = Math.max(1L, cardinality);
        long m = mix(seed, rowId, column, algo);
        long abs = m == Long.MIN_VALUE ? 0L : Math.abs(m);
        return abs % c;
    }

    public static String prefixedId(long seed, long rowId, String column, String algo,
                                    String prefix, long cardinality) {
        return (prefix == null ? "" : prefix) + bounded(seed, rowId, column, algo, cardinality);
    }

    public static String dictionary(long seed, long rowId, String column, String algo,
                                    String prefix, long cardinality) {
        return prefixedId(seed, rowId, column, algo, prefix, cardinality);
    }

    public static String weightedPick(long seed, long rowId, String column, String algo,
                                      List<String> values, List<Double> weights) {
        if (values == null || values.isEmpty()) {
            throw new IllegalArgumentException("empty values");
        }
        double total = 0;
        for (int i = 0; i < values.size(); i++) {
            total += (weights != null && weights.size() == values.size()) ? weights.get(i) : 1.0;
        }
        double r = (bounded(seed, rowId, column, algo, 1_000_000L) / 1_000_000.0) * total;
        double acc = 0;
        for (int i = 0; i < values.size(); i++) {
            acc += (weights != null && weights.size() == values.size()) ? weights.get(i) : 1.0;
            if (r <= acc) {
                return values.get(i);
            }
        }
        return values.get(values.size() - 1);
    }

    public static String uuidDeterministic(long seed, long rowId, String column, String algo) {
        return new UUID(mix(seed, rowId, column, algo), mix(seed, rowId, column + "#2", algo)).toString();
    }

    public static String hexString(long seed, long rowId, String column, String algo, int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(Integer.toHexString((int) bounded(seed, rowId, column + "#" + i, algo, 16)));
        }
        return sb.substring(0, length);
    }

    public static String documentationIpv4(long seed, long rowId, String column, String algo) {
        return "203.0.113." + bounded(seed, rowId, column, algo, 256);
    }

    public static long timestampUniform(long seed, long rowId, String column, String algo,
                                        long fromMs, long toExclusiveMs) {
        long span = Math.max(1L, toExclusiveMs - fromMs);
        return fromMs + bounded(seed, rowId, column, algo, span);
    }

    public static String fakeToken(long seed, long rowId, String column, String algo,
                                   String prefix, long cardinality) {
        return prefixedId(seed, rowId, column, algo, prefix, cardinality);
    }

    @SuppressWarnings("unchecked")
    public static String jsonMobileSdk(long seed, long rowId, String algo, Map<String, Object> template) {
        String system = enumVal(template, "DeviceSystemName", seed, rowId, algo, "iOS", "Android");
        String version = enumVal(template, "DeviceSystemVersion", seed, rowId, algo, "18", "19", "20");
        String lang = enumVal(template, "Languages", seed, rowId, algo, "ru_RU", "en_US");
        String hardware = fakeToken(seed, rowId, "HardwareID", algo, "hardware_", 1_000_000L);
        String md5 = hexString(seed, rowId, "ApplicationMD5", algo, 32);
        String boot = uuidDeterministic(seed, rowId, "BootId", algo);
        return "{\"DeviceSystemName\":\"" + system + "\",\"DeviceSystemVersion\":\"" + version
                + "\",\"Languages\":\"" + lang + "\",\"HardwareID\":\"" + hardware
                + "\",\"ApplicationMD5\":\"" + md5 + "\",\"BootId\":\"" + boot + "\"}";
    }

    @SuppressWarnings("unchecked")
    private static String enumVal(Map<String, Object> template, String key, long seed, long rowId,
                                  String algo, String... fallback) {
        Object node = template == null ? null : template.get(key);
        if (node instanceof Map) {
            Object values = ((Map<String, Object>) node).get("values");
            if (values instanceof List && !((List<?>) values).isEmpty()) {
                List<?> list = (List<?>) values;
                return list.get((int) bounded(seed, rowId, key, algo, list.size())).toString();
            }
        }
        return fallback[(int) bounded(seed, rowId, key, algo, fallback.length)];
    }
}
