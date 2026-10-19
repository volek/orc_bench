/**
 * Synthetic ORC dataset generation (Checkpoint 4).
 *
 * <p>Local acceptance: dry-run / {@link orcbench.generate.FakeGenerateBackend} unit tests.
 * Live HDFS writes: {@link orcbench.generate.SparkOrcGenerateBackend} via spark-submit on a
 * cluster node only (plan §7.0).
 */
package orcbench.generate;
