package orcbench.io;

import orcbench.generate.SparkSessionSupport;
import orcbench.model.Models.LoadedConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

/**
 * Text artefacts (metrics, reports, driver log) on the local disk or on HDFS.
 * Local paths stay on {@code java.nio} so unit tests do not need a cluster.
 * {@code hdfs://} / {@code viewfs://} paths, and absolute paths while the
 * process is a YARN cluster driver, go through Hadoop {@link FileSystem}.
 */
public final class ResultStore {
    private ResultStore() {}

    public static String newRunId() {
        return Instant.now().toString().replace(':', '-')
                + "_" + UUID.randomUUID().toString().substring(0, 8);
    }

    /**
     * {@code hdfs://orc_test/...} нельзя открыть: в хосте есть {@code _},
     * {@code URI.getHost()} возвращает null, HDFS бросает
     * {@code Incomplete HDFS URI, no host}. Каталог тот же, authority — nameservice
     * из конфига: {@code hdfs://sdp-dzo-lt-omlt/orc_test/...}.
     */
    public static String qualifyOrcTestUri(String path, String namenodeUri) {
        if (path == null) {
            return null;
        }
        String p = path.trim();
        String nn = namenodeUri == null ? "" : stripTrailingSlash(namenodeUri.trim());
        if (nn.isEmpty()) {
            return p;
        }
        final String broken = "hdfs://orc_test";
        if (p.equals(broken) || p.startsWith(broken + "/")) {
            return nn + "/orc_test" + p.substring(broken.length());
        }
        if ("/orc_test".equals(p) || p.startsWith("/orc_test/")) {
            return nn + p;
        }
        return p;
    }

    /**
     * Directory for this run. On a YARN cluster driver the implicit
     * {@code results} directory becomes
     * {@code hdfs://<namenode>/orc_test/results/<namenode-host>}.
     * {@code hdfs://orc_test/...} is rewritten onto the real nameservice.
     * Local relative paths are kept for dev hosts and unit tests.
     */
    public static String resolveResultsDir(String requested, LoadedConfig cfg) {
        String dir = requested == null ? "" : requested.trim();
        if (isDistributedScheme(dir)) {
            return stripTrailingSlash(qualifyOrcTestUri(dir, namenode(cfg)));
        }
        boolean implicit = dir.isEmpty() || "results".equals(dir);
        if (SparkSessionSupport.runningInsideYarnCluster()) {
            if (implicit) {
                return hdfsResultsRoot(cfg);
            }
            if (dir.startsWith("/")) {
                return stripTrailingSlash(qualifyOrcTestUri(dir, namenode(cfg)));
            }
        }
        return dir.isEmpty() ? "results" : dir;
    }

    /** {@code hdfs://<namenode>/orc_test/results/<host>} — metrics, SUMMARY, CSV, TSV. */
    public static String hdfsResultsRoot(LoadedConfig cfg) {
        return benchRoot(cfg) + "/results/" + namenodeHost(namenode(cfg));
    }

    /** {@code hdfs://<namenode>/orc_test/logs/<host>} — driver and submit-script logs. */
    public static String hdfsLogsRoot(LoadedConfig cfg) {
        return benchRoot(cfg) + "/logs/" + namenodeHost(namenode(cfg));
    }

    private static String benchRoot(LoadedConfig cfg) {
        String nn = namenode(cfg);
        if (nn == null || nn.isEmpty()) {
            return "hdfs://orc_test";
        }
        return stripTrailingSlash(nn) + "/orc_test";
    }

    public static String child(String dir, String... parts) {
        StringBuilder sb = new StringBuilder(dir == null || dir.isEmpty() ? "results" : dir);
        if (parts == null) {
            return sb.toString();
        }
        for (String part : parts) {
            if (part == null || part.isEmpty()) {
                continue;
            }
            if (sb.length() > 0 && sb.charAt(sb.length() - 1) != '/') {
                sb.append('/');
            }
            sb.append(part);
        }
        return sb.toString();
    }

    public static boolean onHdfs(String path) {
        if (path == null) {
            return false;
        }
        String p = path.trim();
        if (isDistributedScheme(p)) {
            return true;
        }
        return p.startsWith("/")
                && SparkSessionSupport.runningInsideYarnCluster()
                && defaultFsIsHdfs();
    }

    public static void mkdirs(String dir) throws IOException {
        if (dir == null || dir.isEmpty()) {
            return;
        }
        if (!onHdfs(dir)) {
            Files.createDirectories(Paths.get(dir));
            return;
        }
        Path p = new Path(dir);
        p.getFileSystem(new Configuration()).mkdirs(p);
    }

    public static void writeUtf8(String path, String content) throws IOException {
        byte[] bytes = (content == null ? "" : content).getBytes(StandardCharsets.UTF_8);
        if (!onHdfs(path)) {
            java.nio.file.Path p = Paths.get(path);
            if (p.getParent() != null) {
                Files.createDirectories(p.getParent());
            }
            Files.write(p, bytes);
            return;
        }
        Path p = new Path(path);
        FileSystem fs = p.getFileSystem(new Configuration());
        if (p.getParent() != null) {
            fs.mkdirs(p.getParent());
        }
        FSDataOutputStream out = fs.create(p, true);
        try {
            out.write(bytes);
            out.hflush();
        } finally {
            out.close();
        }
    }

    /**
     * Append UTF-8 text. On HDFS, {@code append} is used when the filesystem
     * allows it; otherwise the existing file is read back and rewritten.
     */
    public static void appendUtf8(String path, String content) throws IOException {
        if (content == null || content.isEmpty()) {
            return;
        }
        if (!onHdfs(path)) {
            java.nio.file.Path p = Paths.get(path);
            if (p.getParent() != null) {
                Files.createDirectories(p.getParent());
            }
            Files.write(p, content.getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            return;
        }
        Path p = new Path(path);
        FileSystem fs = p.getFileSystem(new Configuration());
        if (p.getParent() != null) {
            fs.mkdirs(p.getParent());
        }
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        if (!fs.exists(p)) {
            FSDataOutputStream created = fs.create(p, false);
            try {
                created.write(bytes);
                created.hflush();
            } finally {
                created.close();
            }
            return;
        }
        try {
            FSDataOutputStream appended = fs.append(p);
            try {
                appended.write(bytes);
                appended.hflush();
            } finally {
                appended.close();
            }
        } catch (UnsupportedOperationException ex) {
            byte[] prev = readAllBytes(fs, p);
            byte[] merged = new byte[prev.length + bytes.length];
            System.arraycopy(prev, 0, merged, 0, prev.length);
            System.arraycopy(bytes, 0, merged, prev.length, bytes.length);
            FSDataOutputStream rewritten = fs.create(p, true);
            try {
                rewritten.write(merged);
                rewritten.hflush();
            } finally {
                rewritten.close();
            }
        }
    }

    public static List<String> readLines(String path) throws IOException {
        if (path == null || path.isEmpty()) {
            return Collections.emptyList();
        }
        if (!onHdfs(path)) {
            java.nio.file.Path p = Paths.get(path);
            if (!Files.isRegularFile(p)) {
                return Collections.emptyList();
            }
            return Files.readAllLines(p, StandardCharsets.UTF_8);
        }
        Path p = new Path(path);
        FileSystem fs = p.getFileSystem(new Configuration());
        if (!isHdfsFile(fs, p)) {
            return Collections.emptyList();
        }
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(fs.open(p), StandardCharsets.UTF_8));
        try {
            List<String> lines = new ArrayList<String>();
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
            return lines;
        } finally {
            reader.close();
        }
    }

    public static boolean isFile(String path) {
        try {
            if (path == null || path.isEmpty()) {
                return false;
            }
            if (!onHdfs(path)) {
                return Files.isRegularFile(Paths.get(path));
            }
            Path p = new Path(path);
            FileSystem fs = p.getFileSystem(new Configuration());
            return isHdfsFile(fs, p);
        } catch (IOException ex) {
            return false;
        }
    }

    public static void deleteIfExists(String path) throws IOException {
        if (path == null || path.isEmpty()) {
            return;
        }
        if (!onHdfs(path)) {
            Files.deleteIfExists(Paths.get(path));
            return;
        }
        Path p = new Path(path);
        FileSystem fs = p.getFileSystem(new Configuration());
        if (fs.exists(p)) {
            fs.delete(p, false);
        }
    }

    /** Overwrite an HDFS file and return the still-open stream (driver log). */
    public static FSDataOutputStream create(String path) throws IOException {
        Path p = new Path(path);
        FileSystem fs = p.getFileSystem(new Configuration());
        if (p.getParent() != null) {
            fs.mkdirs(p.getParent());
        }
        return fs.create(p, true);
    }

    private static boolean isHdfsFile(FileSystem fs, Path p) throws IOException {
        try {
            return fs.getFileStatus(p).isFile();
        } catch (FileNotFoundException ex) {
            return false;
        }
    }

    private static byte[] readAllBytes(FileSystem fs, Path p) throws IOException {
        java.io.InputStream in = fs.open(p);
        try {
            java.io.ByteArrayOutputStream buf = new java.io.ByteArrayOutputStream();
            byte[] tmp = new byte[8192];
            int n;
            while ((n = in.read(tmp)) >= 0) {
                buf.write(tmp, 0, n);
            }
            return buf.toByteArray();
        } finally {
            in.close();
        }
    }

    private static boolean isDistributedScheme(String path) {
        String lower = path.toLowerCase(Locale.ROOT);
        return lower.startsWith("hdfs://") || lower.startsWith("viewfs://");
    }

    private static boolean defaultFsIsHdfs() {
        try {
            Class.forName("org.apache.hadoop.conf.Configuration");
            Configuration conf = new Configuration();
            String fs = conf.get("fs.defaultFS", conf.get("fs.default.name", "file:///"));
            if (fs == null) {
                return false;
            }
            String lower = fs.toLowerCase(Locale.ROOT);
            return lower.startsWith("hdfs:") || lower.startsWith("viewfs:");
        } catch (Throwable ex) {
            return false;
        }
    }

    /** Authority of {@code hdfs://sdp-dzo-lt-omlt} → {@code sdp-dzo-lt-omlt}. */
    static String namenodeHost(String namenodeUri) {
        if (namenodeUri == null) {
            return "namenode";
        }
        String host = namenodeUri.trim();
        int scheme = host.indexOf("://");
        if (scheme >= 0) {
            host = host.substring(scheme + 3);
        }
        int slash = host.indexOf('/');
        if (slash >= 0) {
            host = host.substring(0, slash);
        }
        if (host.isEmpty()) {
            return "namenode";
        }
        return host;
    }

    private static String namenode(LoadedConfig cfg) {
        if (cfg == null || cfg.environment == null || cfg.environment.environment == null
                || cfg.environment.environment.hdfs == null
                || cfg.environment.environment.hdfs.namenodeUri == null) {
            return null;
        }
        String nn = cfg.environment.environment.hdfs.namenodeUri.trim();
        return nn.isEmpty() ? null : nn;
    }

    private static String stripTrailingSlash(String s) {
        if (s == null || s.length() <= 1) {
            return s;
        }
        if (s.endsWith("/")) {
            return s.substring(0, s.length() - 1);
        }
        return s;
    }
}
