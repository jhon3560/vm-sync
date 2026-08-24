#!/bin/bash
# vm-sync 性能实测 v2（真实 VictoriaMetrics + 内嵌同步）
set -e
BASE=${BASE:-/tmp/vm-perf}
VM=$BASE/victoria-metrics
LOGS=$BASE/logs
mkdir -p $LOGS

cleanup() {
  kill %1 %2 %3 %4 2>/dev/null || true
  wait 2>/dev/null || true
}
trap cleanup EXIT

rm -rf $BASE/src-data $BASE/dst-data $BASE/wal-src
cat > $BASE/count.py <<'EOF'
import sys, json
n = 0
lines = 0
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        obj = json.loads(line)
        n += len(obj.get("values", []))
    except Exception:
        print("PARSE_ERROR:", line[:200], file=sys.stderr)
    lines += 1
print(f"{n} {lines}")
EOF
MATCH='%7B__name__%3D~%22perf_.%2B%22%7D'
count() { # $1=端口
  curl -s "http://127.0.0.1:$1/api/v1/export?match[]=$MATCH&start=0" | python3 $BASE/count.py 2>>$LOGS/count.err | awk '{print $1}'
}

# ---------- 起两个 VM（源暂不开发送端） ----------
$VM -storageDataPath=$BASE/dst-data -httpListenAddr=127.0.0.1:18429 \
    -syncIsolation.listen=127.0.0.1:28101 -syncIsolation.targetURL=http://127.0.0.1:18429 \
    -syncIsolation.metricsAddr=127.0.0.1:28481 -loggerLevel=ERROR > $LOGS/dst.log 2>&1 &
$VM -storageDataPath=$BASE/src-data -httpListenAddr=127.0.0.1:18428 \
    -loggerLevel=ERROR > $LOGS/src.log 2>&1 &
for i in $(seq 1 60); do
  curl -sf http://127.0.0.1:18428/health >/dev/null 2>&1 && curl -sf http://127.0.0.1:18429/health >/dev/null 2>&1 && break
  sleep 1
done
echo "== VM 就绪 =="

# ---------- 历史数据：20 series × 1/s × 14h = 1,008,000 样本 ----------
python3 - <<'EOF'
import json, time
now_ms = int(time.time()*1000)
end = now_ms - 30_000
start = end - 14*3600*1000
n = 0
with open('/tmp/vm-perf/hist.json', 'w') as f:
    for i in range(20):
        name = f'perf_{i}'
        for ts in range(start, end, 1000):
            f.write(json.dumps({"metric":{"__name__":name,"zone":"east","id":str(i)},
                               "values":[1.5+i*0.1],"timestamps":[ts]}) + "\n")
            n += 1
print(f"generated {n}")
EOF
JSON_BYTES=$(stat -c%s $BASE/hist.json)
TOTAL=$(wc -l < $BASE/hist.json)
code=$(curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/json" --data-binary @$BASE/hist.json http://127.0.0.1:18428/api/v1/import)
echo "== 导入 $TOTAL 样本（${JSON_BYTES}B JSON）到源，http=$code =="
sleep 3

# ---------- linkproxy + 开发送端 ----------
$BASE/linkproxy -listen 127.0.0.1:28100 -target 127.0.0.1:28101 -metrics 127.0.0.1:28091 > $LOGS/linkproxy.log 2>&1 &
kill %2 2>/dev/null; wait %2 2>/dev/null || true
sleep 1
$VM -storageDataPath=$BASE/src-data -httpListenAddr=127.0.0.1:18428 \
    -syncIsolation.sendTo=127.0.0.1:28100 -syncIsolation.sourceURL=http://127.0.0.1:18428 \
    -syncIsolation.walPath=$BASE/wal-src -syncIsolation.metricsAddr=127.0.0.1:28480 \
    -loggerLevel=ERROR > $LOGS/src-sync.log 2>&1 &
for i in $(seq 1 60); do curl -sf http://127.0.0.1:18428/health >/dev/null 2>&1 && break; sleep 1; done
sleep 3

# ---------- 回填吞吐 ----------
T0=$(date +%s.%N)
for i in $(seq 1 900); do
  d=$(curl -s http://127.0.0.1:28480/metrics | grep '^sync_delay_seconds' | awk '{print $2}')
  if [ "$d" = "0" ]; then sleep 2; got=$(count 18429); [ "$got" -ge "$TOTAL" ] && break; fi
  sleep 1
done
T1=$(date +%s.%N)
BACKFILL_S=$(echo "$T1 - $T0" | bc)
GOT=$(count 18429)
echo "== 回填：${GOT}/${TOTAL} 样本 / ${BACKFILL_S}s =="
python3 - <<EOF
s=$TOTAL; t=$BACKFILL_S; h=14.0
print(f"吞吐 {s/t:,.0f} 样本/s | 数据速度 {h*3600/t:.1f} 小时数据/s | {h*24/t*60:.2f} 天/分钟")
EOF

# ---------- 零丢失 + 保真 ----------
curl -s "http://127.0.0.1:18428/api/v1/export?match[]=$MATCH&start=0" | sort > $BASE/src.export
curl -s "http://127.0.0.1:18429/api/v1/export?match[]=$MATCH&start=0" | sort > $BASE/dst.export
python3 $BASE/count.py < $BASE/src.export > $BASE/src.count 2>/dev/null
python3 $BASE/count.py < $BASE/dst.export > $BASE/dst.count 2>/dev/null
diff $BASE/src.export $BASE/dst.export > $BASE/diff.out || true
echo "== 保真：源 $(cat $BASE/src.count) | 目标 $(cat $BASE/dst.count) | diff 行数 $(wc -l < $BASE/diff.out) =="

# ---------- 链路压缩比 ----------
TX_BYTES=$(curl -s http://127.0.0.1:28091/metrics | grep linkproxy_tx_bytes_total | awk '{print $2}')
python3 - <<EOF
j=$JSON_BYTES; t=$TX_BYTES
print(f"== 带宽：链路正向 {t:,}B vs JSON {j:,}B → zstd 帧后压缩 {j/t:.1f}:1 ==")
EOF

# ---------- 实时 e2e（每 500ms 写 1 批，ts=now，持续 60s） ----------
rm -f $BASE/e2e_samples.txt
( for i in $(seq 1 80); do
    echo "$(date +%s.%N) $(curl -s http://127.0.0.1:28480/metrics | grep '^sync_e2e_delay_seconds' | awk '{print $2}')" >> $BASE/e2e_samples.txt
    sleep 0.75
  done ) &
SAMPLER=$!
python3 - <<'EOF'
import json, time, urllib.request
for batch in range(120):
    now_ms = int(time.time()*1000)
    buf = []
    for i in range(20):
        buf.append(json.dumps({"metric":{"__name__":f"perf_{i}","zone":"east","id":str(i)},
                              "values":[9.9],"timestamps":[now_ms]}) + "\n")
    req = urllib.request.Request("http://127.0.0.1:18428/api/v1/import",
                                 data="".join(buf).encode(),
                                 headers={"Content-Type": "application/json"})
    urllib.request.urlopen(req, timeout=5)
    time.sleep(0.5)
print("realtime 120 batches × 20 series written")
EOF
wait $SAMPLER
sleep 6
python3 - <<'EOF'
vals=[]
for line in open('/tmp/vm-perf/e2e_samples.txt'):
    p=line.split()
    if len(p)==2 and p[1] != '0':
        vals.append(float(p[1]))
if vals:
    vals.sort(); n=len(vals)
    pct=lambda p: vals[min(n-1,int(n*p))]
    print(f"== e2e 延迟：样本 {n} min={vals[0]:.1f}s p50={pct(.5):.1f}s p95={pct(.95):.1f}s max={vals[-1]:.1f}s ==")
else:
    print("e2e: 无有效样本（检查 dst.log 与 e2e_samples.txt）")
EOF
FINAL=$(count 18429)
echo "== 最终目标样本 $FINAL（源总 $((TOTAL+2400))）=="
echo DONE
