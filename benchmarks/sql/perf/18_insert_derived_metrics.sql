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
-- Server-side derived metric (benchmark-v2-plan §5, Tier 0 wire-efficiency):
--
--   bytes_on_wire_per_row [bytes/row]
--       = server NetworkReceiveBytes (bytes the connector actually pushed over
--         the wire, post-compression) / rows written.
--       Wire efficiency of the connector's protocol: catches compression and
--         encoding regressions (e.g. a serializer that stops compressing, or
--         switches to a chattier format, moves this number even when rows/s and
--         server CPU are flat). Complements the client-side per-row cost metrics
--         in 10_insert_from_event_log.sql.
--
-- Both numerator and denominator are taken from the SAME target system.query_log
-- window and the SAME table scope (has(tables)) used everywhere else in this
-- suite, so this is self-consistent with ch_network_receive_bytes (file 11):
--   * NetworkReceiveBytes is summed over the 'Insert' RECEIPTS — that is the
--     socket-level ingress the connector generated (async flushes are internal
--     to the server and receive no bytes over the client wire).
--   * rows are the mode-agnostic "real write" rows (Insert when async_insert=0,
--     AsyncInsertFlush when async_insert=1), selected by written_rows > 0, matching
--     ch_avg_rows_per_insert etc.
-- remoteSecure()/9440 reaches the ClickHouse Cloud target (see file 11 header).

INSERT INTO perf.metrics (run_id, metric_name, unit, value)
SELECT {run_id:String}, 'bytes_on_wire_per_row' AS metric_name, 'bytes/row' AS unit,
       (SELECT toFloat64(sum(ProfileEvents['NetworkReceiveBytes']))
        FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
        WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
          AND type = 'QueryFinish' AND query_kind = 'Insert'
          AND has(tables, {table_qualified:String}))
       / nullIf(
         (SELECT toFloat64(sum(written_rows))
          FROM remoteSecure({target_addr:String}, system.query_log, {target_user:String}, {target_password:String})
          WHERE event_time BETWEEN parseDateTimeBestEffort({run_start:String}) AND parseDateTimeBestEffort({run_end:String})
            AND type = 'QueryFinish' AND query_kind IN ('Insert', 'AsyncInsertFlush')
            AND written_rows > 0
            AND has(tables, {table_qualified:String})),
         0);
