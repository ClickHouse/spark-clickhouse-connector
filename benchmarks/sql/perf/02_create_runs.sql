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
CREATE TABLE IF NOT EXISTS perf.runs (
  run_id            String,
  run_started_at    DateTime,
  run_ended_at      DateTime,
  git_sha           String,
  -- Which connector produced the run (e.g. 'spark'); lets us add other
  -- connectors later and filter by them.
  connector          String DEFAULT 'spark',
  -- Cluster sizing / tier label for the run (e.g. 'small', 'big'); lets us
  -- compare runs across machine sizes.
  run_profile        String DEFAULT '',
  connector_version  String,
  clickhouse_version String DEFAULT '',
  -- Connector/runtime-specific attributes, kept generic so new connectors
  -- (kafka, ...) need no schema change. Spark fills e.g. spark_version,
  -- scala_version, emr_release; another connector records its own keys.
  runtime            Map(String, String),
  notes              String DEFAULT ''
) ENGINE = MergeTree
ORDER BY (run_started_at, run_id);
