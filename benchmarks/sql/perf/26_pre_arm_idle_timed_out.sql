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
-- Parameters ({name:Type}) are bound by run_metrics_sql.py.
--
-- pre_arm_idle_timed_out flag. wait_for_idle.py proceeds silently when it hits
-- IDLE_TIMEOUT (default 600s) — the target never visibly quiesced before the arm
-- started, so the companion pre_arm_idle_seconds value (25) is right-censored (a
-- floor, not the true idle time) AND the arm did not start from a fully quiesced
-- state. This 1/0 flag makes that censoring an explicit fact so dashboards can
-- exclude/mark such runs. Like settle_timed_out (22) this is a flagged-not-failed
-- guard (the run is non-comparable, not a regression): it does not fail the run,
-- and it trips the `idle_timeout` flag token — the run record's runtime map
-- carries the same flag (see insert_run_record.py) for filtering.

INSERT INTO perf.metrics (run_id, metric_name, unit, value)
VALUES ({run_id:String}, 'pre_arm_idle_timed_out', 'bool', toFloat64({pre_arm_idle_timed_out:Float64}));
