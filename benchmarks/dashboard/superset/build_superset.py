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
"""Build the ClickBench load-test dashboard in Apache Superset from queries.sql.

The target ClickHouse connection is read from env vars (no secrets in source):
  TARGET_CH_HOST, TARGET_CH_USER, TARGET_CH_PASSWORD  (HTTPS port 8443, secure).
Run inside the Superset container, e.g.:
  docker exec clickbench-superset python /tmp/build_superset.py
"""
import json, os, re, time
from urllib.parse import quote_plus
import requests

BASE="http://localhost:8088"
CH_HOST=os.environ.get("TARGET_CH_HOST","localhost")
CH_USER=os.environ.get("TARGET_CH_USER","default")
CH_PASSWORD=os.environ.get("TARGET_CH_PASSWORD","")
CH_URI=f"clickhousedb://{quote_plus(CH_USER)}:{quote_plus(CH_PASSWORD)}@{CH_HOST}:8443/perf?secure=true"

DRILL_SQL={
 "drill_metrics_for_run":
   "SELECT run_id, metric_name, argMax(value, recorded_at) AS value, any(unit) AS unit "
   "FROM perf.metrics GROUP BY run_id, metric_name ORDER BY metric_name",
 "drill_batch_size_distribution":
   "SELECT run_id, floor(written_rows/1000)*1000 AS batch_size_bucket, count() AS n_inserts "
   "FROM perf.ch_inserts WHERE written_rows>0 GROUP BY run_id, batch_size_bucket ORDER BY batch_size_bucket",
 "drill_insert_latency_distribution":
   "SELECT run_id, round(floor(query_duration_ms/50)*50/1000, 3) AS duration_bucket_s, count() AS n_inserts "
   "FROM perf.ch_inserts GROUP BY run_id, duration_bucket_s ORDER BY duration_bucket_s LIMIT 100",
 "drill_throughput_over_time_within_run":
   "SELECT run_id, toStartOfInterval(event_time, INTERVAL 10 SECOND) AS t, sum(written_rows) AS rows_written, "
   "sum(written_bytes)/1024/1024 AS mb_written FROM perf.ch_inserts GROUP BY run_id, t ORDER BY t",
}
DRILL_VIZ={"drill_batch_size_distribution":("bar","batch_size_bucket","n_inserts"),
           "drill_insert_latency_distribution":("bar","duration_bucket_s","n_inserts"),
           "drill_throughput_over_time_within_run":("line","t","rows_written, mb_written"),
           "drill_metrics_for_run":("table",None,None)}

s=requests.Session()
tok=s.post(f"{BASE}/api/v1/security/login",json={"username":"admin","password":"admin","provider":"db","refresh":True}).json()["access_token"]
s.headers.update({"Authorization":f"Bearer {tok}"})
s.headers.update({"X-CSRFToken":s.get(f"{BASE}/api/v1/security/csrf_token/").json()["result"],"Referer":BASE})

def R(m,p,**k):
    for attempt in range(8):
        time.sleep(0.05)
        r=s.request(m,f"{BASE}{p}",**k)
        if r.status_code==429: time.sleep(1.2); continue
        if r.status_code>=400: print("ERR",m,p,r.status_code,r.text[:160])
        return r
    return r

db=next((d for d in R("GET","/api/v1/database/").json()["result"] if d["database_name"]=="ClickBench perf"),None)
db_id=db["id"] if db else R("POST","/api/v1/database/",json={"database_name":"ClickBench perf","sqlalchemy_uri":CH_URI}).json()["id"]

queries=[]
for b in open("/tmp/queries.sql").read().split("\n-- @query: ")[1:]:
    name=b.split("\n",1)[0].strip()
    viz=(re.search(r"-- @viz:\s*(\S+)",b) or [None,None])[1]
    x=(re.search(r"-- @x:\s*(.+)",b) or [None,None])[1]
    y=(re.search(r"-- @y:\s*(.+)",b) or [None,None])[1]
    rest=b.split("\n",1)[1] if "\n" in b else ""
    body="\n".join(l for l in rest.split("\n") if not l.strip().startswith("--")).strip().rstrip(";")
    if name=="compare_two_runs": continue
    if name in DRILL_SQL: body=DRILL_SQL[name]; viz,x,y=DRILL_VIZ[name]
    queries.append(dict(name=name,viz=viz,x=x,y=y,body=body))

def find(path,col,val):
    return R("GET",path,params={"q":json.dumps({"filters":[{"col":col,"opr":"eq","value":val}]})}).json()

# Explicit y-axis unit label per chart (Superset shows a bare number otherwise).
UNITS={
 "trend_throughput_rows_per_sec":"rows / second",
 "trend_e2e_duration":"seconds (wall clock)",
 "trend_connections_per_insert":"connections / insert (ratio)",
 "trend_batch_size":"rows / insert",
 "trend_insert_latency":"seconds",
 "trend_parts_created":"parts",
 "trend_merge_total_duration":"seconds (aggregate, not wall clock)",
 "trend_merge_amplification":"ratio (bytes read / written)",
 "trend_executor_cpu_time":"CPU-seconds (aggregate)",
 "trend_jvm_gc_time":"seconds (aggregate)",
 "trend_peak_jvm_memory":"bytes",
 "trend_connector_time_split":"seconds (aggregate)",
 "trend_tasks":"tasks",
 "trend_throttling":"events (count)",
 "trend_parts_vs_threshold":"parts",
 "trend_merge_pool_pct":"percent",
 "drill_batch_size_distribution":"inserts (count)",
 "drill_insert_latency_distribution":"inserts (count)",
 "drill_throughput_over_time_within_run":"rows / MB per 10s",
}
made=[]; drill_ds=None
for q in queries:
    ex=find("/api/v1/dataset/","table_name",q["name"])
    if ex["count"]:
        ds_id=ex["result"][0]["id"]; R("PUT",f"/api/v1/dataset/{ds_id}",json={"sql":q["body"]})
    else:
        ds_id=R("POST","/api/v1/dataset/",json={"database":db_id,"schema":"perf","table_name":q["name"],"sql":q["body"]}).json()["id"]
    cols=[c["column_name"] for c in R("GET",f"/api/v1/dataset/{ds_id}").json()["result"]["columns"]]
    if q["name"]=="drill_metrics_for_run": drill_ds=ds_id
    ycols=[c.strip() for c in (q["y"] or "").split(",") if c.strip() in cols]
    if q["viz"]=="table":
        vt="table"; params={"datasource":f"{ds_id}__table","viz_type":"table","query_mode":"raw","all_columns":cols,"row_limit":2000,"adhoc_filters":[]}
    else:
        vt="echarts_timeseries_bar" if q["viz"]=="bar" else "echarts_timeseries_line"
        # BUGFIX: the metric label MUST differ from the column name. When
        # label==column, Superset emits `MAX(col) AS col ... ORDER BY col`,
        # which the ClickHouse new analyzer rejects with Code 215 ("not under
        # aggregate function and not in GROUP BY"). Use the default AGG(col)
        # form so the alias is distinct from the grouped/ordered column.
        mets=[{"expressionType":"SIMPLE","column":{"column_name":c},"aggregate":"MAX","label":f"MAX({c})"} for c in ycols]
        params={"datasource":f"{ds_id}__table","viz_type":vt,"x_axis":q["x"],"metrics":mets,"groupby":[],
                "adhoc_filters":[],"row_limit":10000,"x_axis_sort_asc":True,"time_grain_sqla":None,
                "y_axis_title":UNITS.get(q["name"],""),"y_axis_title_margin":30,
                "x_axis_title":("time" if q["x"]=="t" else q["x"]),"x_axis_title_margin":15}
    exc=find("/api/v1/chart/","slice_name",q["name"])
    if exc["count"]:
        ch=exc["result"][0]["id"]; R("PUT",f"/api/v1/chart/{ch}",json={"params":json.dumps(params),"viz_type":vt})
    else:
        ch=R("POST","/api/v1/chart/",json={"slice_name":q["name"],"viz_type":vt,"datasource_id":ds_id,"datasource_type":"table","params":json.dumps(params)}).json()["id"]
    made.append((ch,q["name"])); print("chart",ch,q["name"])

pos={"DASHBOARD_VERSION_KEY":"v2","ROOT_ID":{"type":"ROOT","id":"ROOT_ID","children":["GRID_ID"]},
     "GRID_ID":{"type":"GRID","id":"GRID_ID","children":[],"parents":["ROOT_ID"]}}
row=None
# BUGFIX: build each chart into the layout exactly once. `made` can carry the
# same chart id more than once (an idempotent re-run reuses an existing chart
# via find-by-slice_name), and appending every entry placed each chart twice in
# position_json. De-duplicate by chart id, preserving first-seen order.
_seen=set()
layout=[(ch,name) for (ch,name) in made if not (ch in _seen or _seen.add(ch))]
for i,(ch,name) in enumerate(layout):
    if i%2==0:
        rid=f"ROW-{i}"; pos[rid]={"type":"ROW","id":rid,"children":[],"meta":{"background":"BACKGROUND_TRANSPARENT"},"parents":["ROOT_ID","GRID_ID"]}
        pos["GRID_ID"]["children"].append(rid); row=rid
    cid=f"CHART-{ch}"
    pos[cid]={"type":"CHART","id":cid,"children":[],"parents":["ROOT_ID","GRID_ID",row],"meta":{"chartId":ch,"width":6,"height":50,"sliceName":name}}
    pos[row]["children"].append(cid)
native=[{"id":"NATIVE_FILTER-run_id","name":"Run","filterType":"filter_select","type":"NATIVE_FILTER",
         "targets":[{"datasetId":drill_ds,"column":{"name":"run_id"}}],
         "controlValues":{"multiSelect":False,"enableEmptyFilter":False,"defaultToFirstItem":True,"searchAllOptions":True},
         "scope":{"rootPath":["ROOT_ID"],"excluded":[]},"defaultDataMask":{"filterState":{}}}]
body={"dashboard_title":"ClickBench Load Test","published":True,
      "position_json":json.dumps(pos),"json_metadata":json.dumps({"native_filter_configuration":native})}
for d in find("/api/v1/dashboard/","dashboard_title","ClickBench Load Test")["result"]:
    R("DELETE",f"/api/v1/dashboard/{d['id']}")
did=R("POST","/api/v1/dashboard/",json=body).json()["id"]
for ch,_ in made:
    R("PUT",f"/api/v1/chart/{ch}",json={"dashboards":[did]})
print(f"\nDASHBOARD id={did}  charts={len(made)}  run_id filter on dataset {drill_ds}")
print(f"open: {BASE}/superset/dashboard/{did}/")
