# ORC benchmark configuration package — measured-profile update (2026-10-29)

Includes **all previous configuration files** and original `spark_queries.sql`, plus `observed-orc-profile.yaml`, `column_profile_result.txt` and `orc_meta_result.txt`.

## Changes
- `schema.orc.txt`: corrected the exact 20-field ORC footer order (critical for positional readers).
- `dataset.yaml`: corrected observed row count to 19,159,061 and `paramName` approximate distinct count to 50; filler model 23 -> 20 assuming all modeled names are present; corrected `userLogin` monitoring NULL model (conditional relationship requires grouping); added measured row, array, NULL and cardinality references. Existing approximate generator targets retained where useful.
- `benchmark.yaml`: retained 912 MiB benchmark file-size target and 64 MiB writer stripe target, recorded measured file manifest, stride, stripe and compression information separately.
- `profiling-suite.yaml`, `query-suite.yaml`, `pilot-s.yaml`: added references to observed metrics and optional disabled checks.
- `environment.yaml`, `environment.local.yaml.example`, `spark_queries.sql`: unchanged from previous updated package.

## Limitations and implementation
- All observations refer to **one new-era hour** `key=2026-06-28--09`, not entire index and not old-era S pilot.
- `observed-orc-profile.yaml` declares observed values; newly added keys need runner/ConfigLoader support. Profiling and newly proposed pilot validation remain disabled until implemented. No production performance query SQL changed.
- Approximate distinct estimates can exceed row count; never enforce those as exact unique counts.
- Additional Spark SQL results needed for EPK position/cardinality, duplicate keys, exact time equalities, actual monotonic sorting and full catalog. The fact 22,807 NULL userLogin rows equals the MONITORING event count is not a proof of row-wise equivalence.
- The observed compressed stripe mean cannot prove the original writer stripe target size.
- Original `spark_queries.sql` setup and 42 SELECT profiling statements remain unchanged. No cluster execution was performed.
