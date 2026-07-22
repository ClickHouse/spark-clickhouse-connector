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
-- WHY IT FAILED (fail_reason): a derived column states the reason in words so the
-- panel answers "why", not just "which". It is deployed as a Superset CALCULATED
-- COLUMN (`fail_reason`) on the runs-enriched dataset (same multiIf as below);
-- this file is the source of truth for that expression. The integrity classes are
-- self-explanatory from the numbers; a hard failure (outcome='failed') can only
-- say "see run logs" until the pipeline captures an error class into runtime['fail_reason']
-- (scoped follow-up — the Tier-2 failure-mode split).
--
-- DEPLOYMENT: rendered as a `table` (raw records) viz over the runs-enriched
-- Superset dataset. Backs the panel on BOTH connectors (identical projection/
-- filter; only the dataset name differs):
--   * Spark  dashboard #427 — dataset v_runs_enriched  (deployed v2_runs_enriched)
--   * Kafka  dashboard #432 — dataset v_kc_runs
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
  -- Derived reason (deployed as the `fail_reason` calculated column):
  multiIf(
    outcome = 'failed',                                          'ingest step failed — see run logs',
    integrity_ok = 0 AND rows_delivered < rows_expected,         concat('rows LOST: ', toString(toInt64(rows_delivered)), ' of ', toString(toInt64(rows_expected)), ' delivered'),
    integrity_ok = 0 AND duplicate_rows > 0,                     concat('extra rows / duplicates: +', toString(toInt64(duplicate_rows))),
    integrity_ok = 0 AND unique_delivered != unique_expected,    concat('unique WatchID mismatch: ', toString(toInt64(unique_delivered)), ' vs ', toString(toInt64(unique_expected))),
    integrity_ok = 0,                                            'integrity mismatch (content checksum or other)',
    'excluded — see columns'
  ) AS fail_reason,
  rows_delivered,
  rows_expected,
  unique_delivered,
  unique_expected,
  duplicate_rows,
  flag_reason
FROM raw_connectors_load_testing.v_runs_enriched   -- Kafka: v_kc_runs
WHERE headline_ok = 0            -- integrity mismatch (integrity_ok = 0)
   OR outcome = 'failed'         -- the ingest step itself failed
ORDER BY run_started_at DESC
LIMIT 100;
