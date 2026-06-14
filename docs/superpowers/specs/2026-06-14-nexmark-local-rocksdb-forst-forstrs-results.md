# NexMark Local 三后端性能结论：RocksDB vs ForSt vs ForSt-RS

- 生成时间：2026-06-14 CST
- 测试场景：local NexMark，100M events
- Query 范围：`q0-q5 + q7-q22`，当前 runbook 不包含 `q6`
- 资源口径：8c32g TM 资源总预算，实际拓扑为 `TOPO=split`，2 个 TaskManager，每个 4c/16g；JobManager 和 SQL Client 不计入 TM 预算
- 单 query 超时：3600s
- RocksDB 刷新结果：`/ssd2/jackylee/frs-bench/logs/rocksdb-refresh-20260614/SUMMARY-ROCKSDB-REFRESH.md`
- ForSt-RS 数据来源：`/ssd2/jackylee/frs-bench/logs/refresh-20260614-latest/SUMMARY-FORST-RS-LATEST-7Q.md`，并结合上一轮完整表 `/ssd2/jackylee/frs-bench/logs/remote-refresh-20260613/SUMMARY-FORST-RS-LATEST.md`
- native ForSt 数据来源：`/ssd2/jackylee/frs-bench/logs/SUMMARY-INTEGRITY-FINAL.md`，并结合 `/ssd2/jackylee/frs-bench/logs/refresh-20260614-latest/SUMMARY-FORST-RS-LATEST-7Q.md` 中记录的 selected native values
- RocksDB 测试仓库：`/ssd2/jackylee/frs-bench/ForSt`
- RocksDB 仓库 HEAD：`c03dc5d2ad022fcd016251e70419d409a4ae0331`
- RocksDB 测试时 worktree 状态：dirty worktree，涉及 `scripts/run-8c32g.sh`、`config-forst-rs-local.yaml.tpl`、`config-rocksdb.yaml` 以及未跟踪文件

## 结论

在当前 local 目录、100M events、8c32g TM 资源口径下，ForSt-RS 是三后端里总耗时最优的 backend。包含 q4 时，ForSt-RS 总耗时为 11285.5s，比 RocksDB 快约 10.20%，比 native ForSt 快约 19.73%。排除 q4 后，排序不变，ForSt-RS 仍然最优，比 RocksDB 快约 7.46%，比 native ForSt 快约 10.75%。

从 by-query 胜出数看，ForSt-RS 胜出 10 个 query，RocksDB 胜出 7 个 query，native ForSt 胜出 5 个 query。ForSt-RS 的优势主要体现在 q1、q2、q4、q10、q13、q15、q16、q20、q21、q22；当前仍需要重点关注的落后项是 q7、q8、q9、q11、q12、q18、q19，其中 q7 是最大单项差距。

需要特别说明 q4：本轮 RocksDB q4 结果来自当前 dirty worktree，其中 RocksDB 配置存在 `table.exec.state.ttl: 1h`，而 ForSt 和 ForSt-RS local 模板使用 `ttl: 0 ms`。RocksDB q4 的 `out_rows=177628838`，ForSt-RS q4 约为 25.9M rows。因此 q4 wall time 可以作为当前 local dirty-worktree 的测试事实纳入总耗时，但若需要严格语义完全一致的 q4 对比，应补充 TTL 对齐后的重跑。

## 总耗时

| backend | total q0-q5+q7-q22 | human | vs best | excluding q4 | human ex-q4 | vs best ex-q4 |
|---|---:|---:|---:|---:|---:|---:|
| ForSt-RS | 11285.5s | 3h08m05.5s | best | 10414.2s | 2h53m34.2s | best |
| RocksDB | 12436.3s | 3h27m16.3s | 10.20% slower | 11191.4s | 3h06m31.4s | 7.46% slower |
| native ForSt | 13512.7s | 3h45m12.7s | 19.73% slower | 11533.8s | 3h12m13.8s | 10.75% slower |

排序：

1. 包含 q4：ForSt-RS 最优，RocksDB 第二，native ForSt 第三。
2. 排除 q4：排序不变，ForSt-RS 最优，RocksDB 第二，native ForSt 第三。

## By Query

| query | RocksDB | native ForSt | ForSt-RS | winner |
|---|---:|---:|---:|---|
| q0 | 117.7s | 119.8s | 120.3s | RocksDB |
| q1 | 114.5s | 110.0s | 109.4s | ForSt-RS |
| q2 | 108.6s | 107.5s | 101.3s | ForSt-RS |
| q3 | 124.3s | 146.5s | 144.2s | RocksDB |
| q4 | 1244.9s | 1978.9s | 871.3s | ForSt-RS |
| q5 | 590.8s | 800.6s | 592.9s | RocksDB |
| q7 | 1349.4s | 1336.6s | 1729.4s | native ForSt |
| q8 | 152.7s | 144.2s | 160.0s | native ForSt |
| q9 | 2240.4s | 1728.0s | 1752.9s | native ForSt |
| q10 | 247.7s | 244.6s | 214.9s | ForSt-RS |
| q11 | 439.6s | 307.9s | 342.6s | native ForSt |
| q12 | 155.0s | 134.4s | 161.7s | native ForSt |
| q13 | 190.1s | 186.2s | 177.2s | ForSt-RS |
| q14 | 109.2s | 112.5s | 109.6s | RocksDB |
| q15 | 1108.6s | 1168.8s | 713.1s | ForSt-RS |
| q16 | 965.3s | 1123.2s | 876.7s | ForSt-RS |
| q17 | 241.5s | 325.5s | 251.6s | RocksDB |
| q18 | 532.3s | 643.7s | 713.2s | RocksDB |
| q19 | 463.3s | 492.9s | 535.8s | RocksDB |
| q20 | 1534.8s | 1890.9s | 1237.5s | ForSt-RS |
| q21 | 243.8s | 244.0s | 218.7s | ForSt-RS |
| q22 | 161.7s | 166.0s | 151.2s | ForSt-RS |

胜出计数：

| backend | wins |
|---|---:|
| ForSt-RS | 10 |
| RocksDB | 7 |
| native ForSt | 5 |

## 数据口径说明

RocksDB 在本轮 refresh 中完整跑完 22/22 个 query，所有 query 均为 `FINISHED`。

ForSt-RS 不是 2026-06-14 单次从 q0 到 q22 的完整重跑结果，而是最新 projected full table：使用上一轮 latest full table，并用 2026-06-14 刷新的 q4、q7、q9、q11、q12、q17、q19 替换旧值。

native ForSt 总耗时由 by-query selected baseline values 重新汇总得到。这个值与 `SUMMARY-FORST-RS-LATEST-7Q.md` 中较早打印的 projected total 有差异，因为该文档的旧 total 看起来保留了早期 q9、q11、q12 数值，而同一文档的 by-query 表已经列出更新后的 selected native values。

## RocksDB 原始结果行

```text
RESULT-RDB: rdb14-q0 rc=0 RESULT: q0 FINISHED wall_ms=117698 (=117.7s) src_out=0 out_rows=100000000
RESULT-RDB: rdb14-q1 rc=0 RESULT: q1 FINISHED wall_ms=114523 (=114.5s) src_out=0 out_rows=100000000
RESULT-RDB: rdb14-q2 rc=0 RESULT: q2 FINISHED wall_ms=108624 (=108.6s) src_out=0 out_rows=100000000
RESULT-RDB: rdb14-q3 rc=0 RESULT: q3 FINISHED wall_ms=124313 (=124.3s) src_out=2199252 out_rows=2199252
RESULT-RDB: rdb14-q4 rc=0 RESULT: q4 FINISHED wall_ms=1244927 (=1244.9s) src_out=98000000 out_rows=177628838
RESULT-RDB: rdb14-q5 rc=0 RESULT: q5 FINISHED wall_ms=590764 (=590.8s) src_out=6001294 out_rows=29989148
RESULT-RDB: rdb14-q7 rc=0 RESULT: q7 FINISHED wall_ms=1349446 (=1349.4s) src_out=92000188 out_rows=92000002
RESULT-RDB: rdb14-q8 rc=0 RESULT: q8 FINISHED wall_ms=152711 (=152.7s) src_out=3066890 out_rows=3065518
RESULT-RDB: rdb14-q9 rc=0 RESULT: q9 FINISHED wall_ms=2240437 (=2240.4s) src_out=98000000 out_rows=91812891
RESULT-RDB: rdb14-q10 rc=0 RESULT: q10 FINISHED wall_ms=247738 (=247.7s) src_out=32 out_rows=100000000
RESULT-RDB: rdb14-q11 rc=0 RESULT: q11 FINISHED wall_ms=439586 (=439.6s) src_out=92000000 out_rows=92000000
RESULT-RDB: rdb14-q12 rc=0 RESULT: q12 FINISHED wall_ms=154980 (=155.0s) src_out=92000000 out_rows=92000000
RESULT-RDB: rdb14-q13 rc=0 RESULT: q13 FINISHED wall_ms=190123 (=190.1s) src_out=0 out_rows=100000000
RESULT-RDB: rdb14-q14 rc=0 RESULT: q14 FINISHED wall_ms=109155 (=109.2s) src_out=0 out_rows=100000000
RESULT-RDB: rdb14-q15 rc=0 RESULT: q15 FINISHED wall_ms=1108609 (=1108.6s) src_out=92000000 out_rows=92000000
RESULT-RDB: rdb14-q16 rc=0 RESULT: q16 FINISHED wall_ms=965347 (=965.3s) src_out=92000000 out_rows=92000000
RESULT-RDB: rdb14-q17 rc=0 RESULT: q17 FINISHED wall_ms=241531 (=241.5s) src_out=92000000 out_rows=92000000
RESULT-RDB: rdb14-q18 rc=0 RESULT: q18 FINISHED wall_ms=532308 (=532.3s) src_out=92000000 out_rows=92000000
RESULT-RDB: rdb14-q19 rc=0 RESULT: q19 FINISHED wall_ms=463252 (=463.3s) src_out=92000000 out_rows=92000000
RESULT-RDB: rdb14-q20 rc=0 RESULT: q20 FINISHED wall_ms=1534781 (=1534.8s) src_out=93198735 out_rows=93198735
RESULT-RDB: rdb14-q21 rc=0 RESULT: q21 FINISHED wall_ms=243759 (=243.8s) src_out=0 out_rows=100000000
RESULT-RDB: rdb14-q22 rc=0 RESULT: q22 FINISHED wall_ms=161692 (=161.7s) src_out=0 out_rows=100000000
```

## 后续建议

1. 对 q4 做 TTL 对齐重跑，消除 RocksDB dirty config 的语义口径差异。
2. 继续以 q7 为第一优先级做 ForSt-RS 性能攻关；当前 q7 的 ForSt-RS wall time 为 1729.4s，明显慢于 RocksDB 1349.4s 和 native ForSt 1336.6s。
3. 对 q9、q11、q12、q18、q19 做针对性 profiling，确认剩余差距是 backend read path、timer path、iterator/range scan，还是 SQL/operator 层调度与状态访问模式导致。
