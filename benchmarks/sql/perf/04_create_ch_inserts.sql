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
-- Per-insert raw stats for distribution analysis (batch-size / duration
-- histograms) without re-querying CH after the fact.
CREATE TABLE IF NOT EXISTS perf.ch_inserts (
  run_id           String,
  event_time       DateTime,
  query_duration_ms UInt64,
  written_rows     UInt64,
  written_bytes    UInt64,
  memory_usage     UInt64,
  network_bytes    UInt64,
  cpu_microseconds UInt64,
  exception_code   Int32
) ENGINE = MergeTree
ORDER BY (run_id, event_time);
