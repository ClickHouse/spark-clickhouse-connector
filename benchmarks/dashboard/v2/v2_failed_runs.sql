--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     https://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--
-- Tab-1 (REGRESSION) panel: "Failed / excluded runs".
--
-- WHY THIS EXISTS: runs that FAIL our checks are captured and exported (the
-- evidence survives — plan §6.5 failed-run telemetry), but every headline/band/
-- ratio view filters headline_ok = 1 and v_pair_ratios drops FAIL-class runs
-- entirely, so a failed run is otherwise INVISIBLE on the dashboard — the only
-- signal is a red CI job. This panel is the at-a-glance forensic view of the two
-- FAIL classes (contract §3, distinct from the flagged-not-failed guards):
--   * integrity mismatch   headline_ok = 0  (integrity_ok = 0; count/uniqExact/
--                          content-checksum vs source did not all match)
--   * failed outcome       outcome = 'failed'  (the ingest step itself failed)
-- Integrity UNKNOWN (integrity_ok NULL, legacy rows) is headline_ok = 1 and is
-- NOT a failure — it does not appear here. Empty result = good.
--
-- DEPLOYMENT: rendered as a `table` (raw records) viz over the runs-enriched
-- Superset dataset, NOT a standalone view — the connectors' datasets already
-- carry headline_ok/outcome. This file is the versioned definition of that
-- panel's projection + filter so the dashboard cannot silently drift from the
-- repo. It backs the panel on BOTH connectors:
--   * Spark  dashboard #427 — dataset v_runs_enriched  (deployed v2_runs_enriched)
--   * Kafka  dashboard #432 — dataset v_kc_runs
-- (WHERE clause + column list are identical; only the dataset name differs.)
--
-- SOURCE: raw_connectors_load_testing.* (ClickPipe DWH mirror of perf.*).

SELECT
  run_started_at,
  run_id,
  pair_id,
  arm,
  tier,
  outcome,
  integrity_ok,
  rows_delivered,
  rows_expected,
  duplicate_rows,
  flag_reason
FROM raw_connectors_load_testing.v_runs_enriched   -- Kafka: v_kc_runs
WHERE headline_ok = 0            -- integrity mismatch (integrity_ok = 0)
   OR outcome = 'failed'         -- the ingest step itself failed
ORDER BY run_started_at DESC
LIMIT 100;
