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
-- Snapshots every insert into perf.ch_inserts for distribution analysis
-- (batch-size histograms, duration percentiles), so it survives the target's
-- system.query_log retention.

INSERT INTO perf.ch_inserts
SELECT
  {run_id:String},
  event_time,
  query_duration_ms,
  written_rows,
  written_bytes,
  memory_usage,
  toUInt64(ProfileEvents['NetworkReceiveBytes']) AS network_bytes,
  toUInt64(ProfileEvents['OSCPUVirtualTimeMicroseconds']) AS cpu_microseconds,
  exception_code
FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
  AND type = 'QueryFinish'
  -- UNION of Insert + AsyncInsertFlush filtered to non-zero rows:
  -- in async mode the real per-batch rows live on AsyncInsertFlush, in sync
  -- mode on Insert. `written_rows > 0` keeps only the "real write" row in
  -- each mode (the async-mode receipt has written_rows = 0).
  AND query_kind IN ('Insert', 'AsyncInsertFlush')
  AND written_rows > 0
  AND has(tables, {table_qualified:String});
