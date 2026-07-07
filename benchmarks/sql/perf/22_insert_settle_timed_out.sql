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
-- settle_timed_out flag (plan §6.4). wait_for_settle.py proceeds silently when
-- it hits SETTLE_TIMEOUT (default 1800s), which right-censors the companion
-- settle_seconds value (14): the merges had NOT visibly settled, so the
-- recorded settle time is a floor, not the true value. This 1/0 flag makes that
-- censoring an explicit fact so dashboards can exclude/mark such runs. Per plan
-- §6.10 this is a flagged-not-failed guard (the run is non-comparable, not a
-- regression), so it does not fail the run — the run record's runtime map
-- carries the same flag (see insert_run_record.py) for filtering.

INSERT INTO perf.metrics (run_id, metric_name, unit, value)
VALUES ({run_id:String}, 'settle_timed_out', 'bool', toFloat64({settle_timed_out:Float64}));
