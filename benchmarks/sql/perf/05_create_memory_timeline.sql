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
-- Per-run server memory timeline. Captures the target's whole-server RSS (the
-- limit that MEMORY_LIMIT_EXCEEDED / code 241 inserts hit) plus the merge and
-- query components over the run, so memory pressure is visible per run — the
-- per-query/per-merge peak metrics alone don't show the server total.
CREATE TABLE IF NOT EXISTS perf.ch_memory_timeline (
  run_id            String,
  ts                DateTime,
  total_rss_bytes   UInt64,   -- whole-server resident memory (vs the cgroup cap)
  tracked_bytes     UInt64,   -- ClickHouse-tracked total
  merge_bytes       UInt64,   -- background merges/mutations
  queries_bytes     UInt64,   -- in-flight queries (the connector's inserts)
  cgroup_used_bytes UInt64    -- cgroup memory used
) ENGINE = MergeTree
ORDER BY (run_id, ts);
