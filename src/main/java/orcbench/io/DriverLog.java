package orcbench.io;

import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;

/**
 * Copies driver stdout and stderr to {@code <resultsDir>/<runId>/driver.log}
 * on HDFS. Each flush is {@code hflush}'d so {@code hdfs dfs -tail} shows lines
 * while the YARN application is still running. Local runs are left unchanged.
 */
public final class DriverLog {
    private static final Object LOCK = new Object();
    private static PrintStream originalOut;
    private static PrintStream originalErr;
    private static FSDataOutputStream remote;
    private static String path;
    private static boolean attached;

    private DriverLog() {}

    public static void attach(String resultsDir, String runId) {
        synchronized (LOCK) {
            if (attached || resultsDir == null || runId == null || runId.trim().isEmpty()) {
                return;
            }
            if (!ResultStore.onHdfs(resultsDir)) {
                return;
            }
            String logPath = ResultStore.child(resultsDir, runId, "driver.log");
            try {
                remote = ResultStore.create(logPath);
                path = logPath;
                originalOut = System.out;
                originalErr = System.err;
                System.setOut(tee(originalOut));
                System.setErr(tee(originalErr));
                attached = true;
            } catch (Exception ex) {
                System.err.println("[orc-bench] WARNING: driver log was not opened on HDFS ("
                        + logPath + "): " + ex.getMessage());
                closeQuietly(remote);
                remote = null;
                return;
            }
        }
        System.out.println("[orc-bench] driver log: " + path);
        System.out.println("[orc-bench]   hdfs dfs -tail " + path);
    }

    public static void close() {
        synchronized (LOCK) {
            if (!attached) {
                return;
            }
            PrintStream out = originalOut;
            PrintStream err = originalErr;
            FSDataOutputStream stream = remote;
            if (out != null) {
                System.setOut(out);
            }
            if (err != null) {
                System.setErr(err);
            }
            attached = false;
            remote = null;
            originalOut = null;
            originalErr = null;
            if (stream != null) {
                try {
                    stream.hflush();
                } catch (IOException ignored) {
                    // best-effort
                }
                closeQuietly(stream);
            }
        }
    }

    private static PrintStream tee(PrintStream primary) throws IOException {
        return new PrintStream(new Tee(primary, remote, LOCK), true, "UTF-8");
    }

    private static void closeQuietly(OutputStream stream) {
        if (stream == null) {
            return;
        }
        try {
            stream.close();
        } catch (IOException ignored) {
            // best-effort
        }
    }

    private static final class Tee extends OutputStream {
        private final OutputStream primary;
        private final OutputStream mirror;
        private final Object lock;
        private boolean mirrorBroken;

        Tee(OutputStream primary, OutputStream mirror, Object lock) {
            this.primary = primary;
            this.mirror = mirror;
            this.lock = lock;
        }

        @Override
        public void write(int b) throws IOException {
            synchronized (lock) {
                primary.write(b);
                mirrorWrite(new byte[] {(byte) b}, 0, 1);
            }
        }

        @Override
        public void write(byte[] buf, int off, int len) throws IOException {
            synchronized (lock) {
                primary.write(buf, off, len);
                mirrorWrite(buf, off, len);
            }
        }

        @Override
        public void flush() throws IOException {
            synchronized (lock) {
                primary.flush();
                if (!mirrorBroken && mirror instanceof FSDataOutputStream) {
                    try {
                        ((FSDataOutputStream) mirror).hflush();
                    } catch (IOException ex) {
                        mirrorBroken = true;
                    }
                }
            }
        }

        private void mirrorWrite(byte[] buf, int off, int len) {
            if (mirrorBroken) {
                return;
            }
            try {
                mirror.write(buf, off, len);
            } catch (IOException ex) {
                mirrorBroken = true;
            }
        }
    }
}
