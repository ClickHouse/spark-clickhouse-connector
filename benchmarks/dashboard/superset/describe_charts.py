#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Set an explanatory description on each Superset chart built by build_superset.py."""
import json, time, requests
BASE="http://localhost:8088"
DESC={
 "runs_overview":"One row per benchmark run with the headline numbers (throughput, parts, batch size, connections/insert). First thing to check after a run.",
 "trend_throughput_rows_per_sec":"Rows ingested per second end-to-end (write_rows / e2e wall clock). Higher is better. Size-independent, so comparable across smoke (1 file) and full (100 files) runs.",
 "trend_e2e_duration":"Total wall-clock of the Spark ingest job (seconds). Lower is better. Size-dependent: a full 100M run takes longer than a 1-file smoke.",
 "trend_connections_per_insert":"HTTP connections opened / inserts sent. ~1.0 = the connector opens a fresh connection per insert (the connection-churn problem); much less than 1.0 = keep-alive reuse. The key metric the coalesced-write work aims to drive down.",
 "trend_batch_size":"Average rows per insert the connector sends. Bigger batches = fewer inserts = fewer parts and connections. Controlled by spark.clickhouse.write.batchSize.",
 "trend_insert_latency":"Client-perceived insert latency p50/p99 in SECONDS (source metric is ms): how long each INSERT call took from Spark's side. A rising p99 signals server backpressure.",
 "trend_parts_created":"Total MergeTree parts created during the run. Each insert makes at least one part; fewer = less merge work afterwards. Spikes track small batches.",
 "trend_merge_total_duration":"AGGREGATE merge time (seconds) = sum of every merge operation's duration. Merges run in parallel, so this sums far above wall-clock; the wall-clock merge tail is ch_settle_seconds. High = lots of merging caused by small parts.",
 "trend_merge_amplification":"Bytes merges had to read / bytes inserted. ~2x is healthy (each row merged about once); much over 5x means tiny parts forced repeated re-merging. Lower is better.",
 "trend_executor_cpu_time":"CPU-seconds SUMMED across all Spark tasks (aggregate, not wall-clock — expected to exceed e2e on a multi-core cluster). Tracks the connector's CPU cost per run.",
 "trend_jvm_gc_time":"JVM garbage-collection time SUMMED across all tasks (seconds, aggregate). Compare to executor CPU time: a rising ratio = memory pressure on the executors.",
 "trend_peak_jvm_memory":"Peak JVM heap and off-heap memory across executors (bytes). Watch for growth approaching executor limits. 0 on local runs (executor metrics are only captured on the cluster).",
 "trend_connector_time_split":"Connector task-time SUMMED across all tasks (seconds): serialization vs. the actual insert/write call. Aggregate, so it runs well above the job's wall clock (many tasks/inserts in parallel); the write call usually dominates.",
 "trend_tasks":"Spark task count vs. failed tasks per run. Any failed > 0 means retries happened - cross-check the throttle/error counters to see why.",
 "trend_throttling":"ClickHouse insert throttling/error counters (delayed, rejected, failed, TOO_MANY_PARTS). All zero = the merge pool kept up; any non-zero = the server fought the ingest.",
 "trend_parts_vs_threshold":"Peak active parts during the run vs. ClickHouse's throttle thresholds (delay ~1000, throw ~3000). As the parts line nears the delay line the server starts sleeping inserts; at the throw line it rejects them.",
 "trend_merge_pool_pct":"Peak background-merge pool utilisation (%). 100% = the merger is saturated and is the run's bottleneck.",
 "drill_metrics_for_run":"Every captured metric for the run picked in the Run filter - the raw values behind the trend charts.",
 "drill_batch_size_distribution":"Histogram of insert batch sizes (rows) for the selected run. A long left tail = many small inserts (e.g. the last partial batch per Spark task).",
 "drill_insert_latency_distribution":"Histogram of per-insert durations in SECONDS for the selected run. A long right tail = throttling or merge backpressure on some inserts.",
 "drill_throughput_over_time_within_run":"Rows/MB written per 10-second bucket within the selected run. Shows the warmup / steady-state / cooldown shape of the ingest.",
}
s=requests.Session()
tok=s.post(f"{BASE}/api/v1/security/login",json={"username":"admin","password":"admin","provider":"db","refresh":True}).json()["access_token"]
s.headers.update({"Authorization":f"Bearer {tok}"})
s.headers.update({"X-CSRFToken":s.get(f"{BASE}/api/v1/security/csrf_token/").json()["result"],"Referer":BASE})
def find(name):
    return s.get(f"{BASE}/api/v1/chart/",params={"q":json.dumps({"filters":[{"col":"slice_name","opr":"eq","value":name}]})}).json()
n=0
for name,desc in DESC.items():
    time.sleep(0.05)
    r=find(name)
    if not r["count"]: print("missing chart:",name); continue
    cid=r["result"][0]["id"]
    pr=s.put(f"{BASE}/api/v1/chart/{cid}",json={"description":desc})
    if pr.status_code==429: time.sleep(1.2); pr=s.put(f"{BASE}/api/v1/chart/{cid}",json={"description":desc})
    if pr.status_code>=400: print("ERR",name,pr.status_code,pr.text[:120])
    else: n+=1
print(f"set descriptions on {n}/{len(DESC)} charts")
