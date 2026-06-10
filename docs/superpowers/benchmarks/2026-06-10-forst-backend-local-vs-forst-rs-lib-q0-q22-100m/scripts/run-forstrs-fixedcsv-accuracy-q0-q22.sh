#!/usr/bin/env bash
set -euo pipefail
BASE_BENCH=/home/users/lijunqing/code/stczwd/ForSt/docs/superpowers/benchmarks/2026-06-10-forst-backend-local-vs-forst-rs-lib-q0-q22-100m
BASE_WORK=/home/users/lijunqing/forst-nexmark-q0-q22-work-20260610
RUN_ROOT=/home/users/lijunqing/forstrs-fixedcsv-q0q22-20260611
RUN_LABEL=forstrs-backend-fixedcsv-1m-fix800-20260611
FLINK_HOME=/home/users/lijunqing/workenv/flink-2.2.1
JDK25=/home/users/lijunqing/workenv/jdk25.0.2-linux_x64_gcc12
TEMPLATE_SRC=/home/users/lijunqing/forstrs-q5-fixed-csv-20260611/templates
MEASURE=/tmp/measure-sql-csv-accuracy-gateway.sh
CSV_DIR=/home/users/lijunqing/forst-nexmark-q0-q22-work-20260610/csv/nexmark-1m
mkdir -p "$RUN_ROOT"/{logs,results/accuracy-output,templates-streaming,templates-batch,state,tmp}
cp "$TEMPLATE_SRC/config-forst-rs-local.yaml.tpl" "$RUN_ROOT/templates-streaming/config-forst-rs-local.yaml.tpl"
perl -0pi -e "s#/home/users/lijunqing/forstrs-q5-fixed-csv-20260611#$RUN_ROOT#g" "$RUN_ROOT/templates-streaming/config-forst-rs-local.yaml.tpl"
cp "$RUN_ROOT/templates-streaming/config-forst-rs-local.yaml.tpl" "$RUN_ROOT/templates-batch/config-forst-rs-local.yaml.tpl"
perl -0pi -e 's#\nexecution:#\nexecution.runtime-mode: BATCH\n\nexecution:#' "$RUN_ROOT/templates-batch/config-forst-rs-local.yaml.tpl"
MATRIX="$RUN_ROOT/results/${RUN_LABEL}.tsv"
: > "$MATRIX"
printf 'run_label\tvariant\tquery\tmode\twall_ms\tsrc_out\texpected_src\tout_rows\tstate\telapsed_s\tjid\tresult_log\toutput_dir\tnote\n' >> "$MATRIX"
queries=${QUERIES:-q0 q1 q2 q3 q4 q5 q6 q7 q8 q9 q10 q11 q12 q13 q14 q15 q16 q17 q18 q19 q20 q21 q22}
for q in $queries; do
  run_id="${RUN_LABEL}-forst-rs-backend-${q}"
  log="$RUN_ROOT/logs/${run_id}.log"
  out_dir="$RUN_ROOT/results/accuracy-output/${run_id}/${q}"
  echo "=== RUN $q $run_id ==="
  rm -rf "$RUN_ROOT/state/data" "$RUN_ROOT/state/cache" "$RUN_ROOT/state/checkpoints" "$RUN_ROOT/tmp" "$out_dir"
  mkdir -p "$RUN_ROOT/state/data" "$RUN_ROOT/state/cache" "$RUN_ROOT/state/checkpoints" "$RUN_ROOT/tmp" "$out_dir"
  templates="$RUN_ROOT/templates-streaming"
  maxsec=900
  poll=5
  min_plateau=10
  plateau_polls=2
  out_stable=2
  csv_monitor=""
  if [ "$q" = q4 ]; then maxsec=2400; min_plateau=30; plateau_polls=6; out_stable=3; fi
  if [ "$q" = q6 ] || [ "$q" = q9 ]; then templates="$RUN_ROOT/templates-batch"; maxsec=900; fi
  if [ "$q" = q12 ]; then csv_monitor="1 s"; maxsec=900; min_plateau=60; plateau_polls=6; out_stable=3; fi
  set +e
  env \
    FLINK_HOME="$FLINK_HOME" \
    NEXMARK_HOME=/home/users/lijunqing/code/stczwd/ForSt/nexmark/nexmark-flink/target/nexmark-flink-bin/nexmark-flink \
    JDK25="$JDK25" \
    JDK17="$JDK25" \
    TEMPLATES="$templates" \
    CSV_DIR="$CSV_DIR" \
    RUN_ID="$run_id" \
    QUERY="$q" \
    CONFIG=forst-rs-ffm-local \
    EVENTS_NUM=1000000 \
    EXPECTED_SRC= \
    TPS=10000000 \
    MAXSEC="$maxsec" \
    DONE_ON_SRC=0 \
    DONE_ON_PLATEAU=1 \
    MIN_PLATEAU_SEC="$min_plateau" \
    PLATEAU_POLLS="$plateau_polls" \
    OUT_STABLE_POLLS="$out_stable" \
    POLL_SEC="$poll" \
    CSV_SOURCE_MONITOR_INTERVAL="$csv_monitor" \
    ACCURACY_FILE_OUTPUT=1 \
    ACCURACY_OUTPUT_DIR="$out_dir" \
    ACCURACY_FLUSH_EVERY=1 \
    FLINK_SKIP_SECURITY_MANAGER_ALLOW=1 \
    "$MEASURE" 2>&1 | tee "$log"
  rc=${PIPESTATUS[0]}
  set -e
  result_line=$(grep -a '^RESULT_TSV' "$log" | tail -1 || true)
  if [ -n "$result_line" ]; then
    IFS=$'\t' read -r tag rq mode wall_ms src_out expected_src out_rows state elapsed_s jid note <<< "$result_line"
  else
    rq="$q"; mode="NO_RESULT"; wall_ms=0; src_out=0; expected_src=0; out_rows=0; state="NO_RESULT"; elapsed_s=0; jid="-"; note="missing_RESULT_TSV rc=$rc"
  fi
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$RUN_LABEL" "forst-rs-backend" "$q" "$mode" "$wall_ms" "$src_out" "$expected_src" "$out_rows" "$state" "$elapsed_s" "$jid" "$log" "$out_dir" "$note" >> "$MATRIX"
  "$FLINK_HOME/bin/sql-gateway.sh" stop >/dev/null 2>&1 || true
  "$FLINK_HOME/bin/stop-cluster.sh" >/dev/null 2>&1 || true
  if [ "$rc" -ne 0 ]; then
    echo "WARN: measure script rc=$rc for $q" >&2
  fi
done
python3 - "$BASE_WORK" "$BASE_BENCH/artifacts/consolidated-q0-q22-fixed-csv-accuracy-20260610.tsv" "$RUN_ROOT" "$RUN_LABEL" <<'PY'
import csv, hashlib, sys, re
from collections import Counter
from pathlib import Path
base_work = Path(sys.argv[1])
manifest = Path(sys.argv[2])
run_root = Path(sys.argv[3])
run_label = sys.argv[4]
out = run_root / 'results' / f'{run_label}.compare-forst-local.tsv'
KINDS={'+I','+U','-U','-D'}
def norm(line):
    line=line.rstrip('\n')
    if not line: return None
    if '\t' in line:
        kind,row=line.split('\t',1)
    else:
        kind,row='+I',line
    if len(row)>=3 and row[:2] in KINDS and row[2]=='[':
        row=row[2:]
    return kind,row
def read_dir(d):
    c=Counter(); raw=0; neg=0
    for p in sorted(Path(d).glob('*')):
        if not p.is_file(): continue
        for line in p.read_text(errors='replace').splitlines():
            r=norm(line)
            if not r: continue
            raw+=1; kind,row=r
            if kind in ('+I','+U'): c[row]+=1
            elif kind in ('-U','-D'): c[row]-=1
            else: c[row]+=1
            if c[row] < 0: neg += 1
    c=Counter({k:v for k,v in c.items() if v})
    return c,raw,neg
def sha(c):
    h=hashlib.sha256()
    for row in sorted(c):
        h.update(str(c[row]).encode()); h.update(b'\t'); h.update(row.encode()); h.update(b'\n')
    return h.hexdigest()
def diff(a,b):
    return sum(abs(a.get(k,0)-b.get(k,0)) for k in set(a)|set(b))
def q12_stats(c):
    rows=sum(c.values()); s=0; bidders=set(); malformed=0
    for row,mul in c.items():
        m=re.match(r'^\[([^,]+),\s*([^,]+),\s*([^,]+),\s*([^\]]+)\]$', row)
        if not m:
            malformed += mul; continue
        try: bid_count=int(m.group(2).strip())
        except ValueError:
            malformed += mul; continue
        s += bid_count * mul; bidders.add(m.group(1).strip())
    hd=hashlib.sha256('\n'.join(sorted(bidders)).encode()).hexdigest()
    return rows,s,len(bidders),hd,malformed
rows=[]
with manifest.open(newline='') as f:
    for r in csv.DictReader(f, delimiter='\t'):
        q=r['query']; source=r['source_file'].replace('.accuracy-compare.tsv','')
        baseline=base_work/'results'/'accuracy-output'/f'{source}-forst-local-{q}'/q
        current=run_root/'results'/'accuracy-output'/f'{run_label}-forst-rs-backend-{q}'/q
        bc, braw, bneg = read_dir(baseline)
        cc, craw, cneg = read_dir(current)
        d=diff(bc,cc)
        status='PASS' if d==0 and bneg==0 and cneg==0 else 'FAIL'
        note='materialized_hash'
        bq12=cq12=('', '', '', '', '')
        if q=='q12':
            bq12=q12_stats(bc); cq12=q12_stats(cc)
            status='PASS' if (bq12[1]==920000 and cq12[1]==920000 and bq12[2]==cq12[2] and bq12[3]==cq12[3] and bq12[4]==0 and cq12[4]==0 and cq12[0] > 0) else 'FAIL'
            note='q12_proctime_invariant_expected_sum=920000'
        rows.append({
            'query':q,'status':status,'diff_materialized_rows':d,
            'forst_local_raw_count':braw,'forstrs_backend_raw_count':craw,
            'forst_local_materialized_count':sum(bc.values()),
            'forstrs_backend_materialized_count':sum(cc.values()),
            'forst_local_sha256':sha(bc),'forstrs_backend_sha256':sha(cc),
            'forst_local_negative_events':bneg,'forstrs_backend_negative_events':cneg,
            'forst_local_q12_sum_bid_count':bq12[1] if q=='q12' else '',
            'forstrs_backend_q12_sum_bid_count':cq12[1] if q=='q12' else '',
            'forst_local_q12_unique_bidders':bq12[2] if q=='q12' else '',
            'forstrs_backend_q12_unique_bidders':cq12[2] if q=='q12' else '',
            'note':note,'forst_local_dir':str(baseline),'forstrs_backend_dir':str(current)})
fields=list(rows[0].keys())
with out.open('w', newline='') as f:
    w=csv.DictWriter(f, delimiter='\t', fieldnames=fields); w.writeheader(); w.writerows(rows)
print(out)
PY
