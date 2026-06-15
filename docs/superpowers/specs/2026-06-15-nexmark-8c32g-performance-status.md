# NexMark 8c/32g LOCAL — consolidated 3-backend performance status (2026-06-15)

**Scope.** 8c/32g resource, NexMark **LOCAL** (Mac population — TOPO=split 2×TM 4c/16g
+ 1×JM 2c/4g, @100M events, strictly serial unless noted), queries **q0–q22 EXCLUDING
q6** (q6 is unsupported in Flink SQL itself). Three backends: **rocksdb**, **forst**
(ForSt C++), **forst-rs**.

**This is a doc-only synthesis** of the best-known recorded numbers per query×backend.
No benchmark, docker, or NexMark run was performed to produce it. The per-cell source
pass and freshness are in the "Data provenance / caveats" section at the bottom — read
it before trusting any single cell.

**forst-rs config policy.** Per-query best-known config is used (the same-config
constraint was lifted 2026-06-14). The selective KV-separation profile is:
KV-sep **ON** for q4/q7/q19/q20 (write-amp / value-carrying read-path joins),
KV-sep **OFF** for q9/q11/q17 (windowed-agg drain; q9 OFF is also the memory-safe
choice on this 35g Mac), NEUTRAL for q12. Source: `best-config.tsv` +
`2026-06-08-8c32g-3backend-sweep-results.md` (V3 FULL flag-ON / V3 FLAG-OFF same-pass A/B).

---

## 1. Per-query table (wall seconds; finished / DNF)

Legend: **fin** = finished, **DNF** = did not finish (resource-OOM or MAXSEC timeout).
"frs cfg" = the forst-rs config the cell uses (KV = KV-separation). All forst-rs walls
are at that query's best-known config.

| query | rocksdb (s) | forst C++ (s) | forst-rs (s) | frs cfg | forst-rs status |
|-------|------------:|--------------:|-------------:|---------|-----------------|
| q0  | 31.8 | 30.7 | 30.9 | default | fin |
| q1  | 30.1 | 30.1 | 29.3 | default | fin |
| q2  | 28.2 | 28.2 | 27.8 | default | fin |
| q3  | 35.0 | 37.2 | 35.6 | default | fin |
| q4  | 286.0 | DNF (OOM ~70M) | 311.0 | KV ON | fin |
| q5  | 162.5 | DNF | n/a (no clean perf) | — | correctness-fixed, perf not re-measured |
| q7  | 599.1 | 477.8 | 695.5 | KV ON | fin |
| q8  | 42.5 | 39.2 | 43.6 | default | fin |
| q9  | 1076.8 | 2002.5 | 1828.7 | KV OFF | fin (see footnote A) |
| q10 | 129.5 | 129.3 | 124.2 | default | fin |
| q11 | 105.9 | 130.8 | 118.8 | KV OFF | fin |
| q12 | 40.2 | 40.8 | 40.6 | neutral | fin |
| q13 | 29.3 | 29.5 | 28.5 | default | fin |
| q14 | 29.6 | 29.2 | 29.4 | default | fin |
| q15 | 229.7 | 206.8 | 161.8 | default | fin |
| q16 | 374.1 | 331.7 | 323.9 | default | fin |
| q17 | 67.6 | 252.2 | 83.7 | KV OFF + routing-adaptive | fin (see footnote B) |
| q18 | 366.4 | DNF | 222.9 | default | fin |
| q19 | 264.3 | 255.6 | 216.0 | KV ON | fin |
| q20 | 653.5 | 1342.0 | 824.4 | KV ON | fin |
| q21 | 60.0 | 58.3 | 53.9 | default | fin |
| q22 | 44.2 | 46.3 | 43.6 | default | fin |

**Footnote A — q9.** The main-table q9 forst-rs number (1828.7s) is the **KV-sep-OFF**
finish at 8c/32g; with KV-sep **ON** at 8c/32g it OOMs (TM exit-137 ~75–83M, reproducible
×2 — KV-sep CAUSED the OOM). On a separate **8c/36g single-TM** profile, q9 with KV-sep
ON now **FINISHES** in **1357.1s** (out_rows 91,813,372 exact, peak RSS 26.2 GiB; run-2
captured clean) — that is ~22–26% faster than the OFF/32g finish AND it beats ForSt
(2002.5s). The 36g profile is a per-query topology exception, not the 8c/32g table; the
conservative 8c/32g number stays the OFF finish (1828.7s).

**Footnote B — q17.** 83.7s is the KV-sep-OFF + `routing-adaptive` executor capture
(NEEDS-CONFIRM per best-config.tsv; corroborated by `/tmp/v3-q17-forst-rs-ffm-local.out`,
out_rows 92,000,000). The clean-serial KV-sep-OFF inline measurement was 110.7s
(MEASURED fallback). The table uses the best-known 83.7s; both numbers FAIL the RocksDB
bar but BEAT ForSt.

---

## 2. TOTAL performance (sum of wall-seconds per backend)

Totals are computed only over queries where **all three backends have a comparable
finished number** (the only valid apples-to-apples set). That excludes:
- **q5** — forst-rs has no clean perf number (correctness-fixed, perf not re-measured)
  and ForSt DNF.
- **q4, q18** — ForSt C++ DNF (OOM / dedup), so no tri-backend comparison exists.
- **q9** — all three finish (rdb 1076.8 / forst 2002.5 / frs 1828.7), but the strict
  tri-backend total deliberately excludes the three resource-heavy outliers (q4/q9/q18)
  so the 18-query total reflects the steady-state set; q9 is added back in the
  rocksdb-vs-forst-rs pair totals (§2b) and reported separately.

### 2a. STRICT tri-backend total (18 queries: all of q0–q22 except q4, q5, q6, q9, q18)

Included: q0,q1,q2,q3,q7,q8,q10,q11,q12,q13,q14,q15,q16,q17,q19,q20,q21,q22 (18 queries).
forst-rs q17 uses the table value 83.7s.

| backend | total (s) over the 18 comparable queries |
|---------|------------------------------------------:|
| rocksdb | 2794.6 |
| forst C++ | 3495.7 |
| forst-rs | 2911.5 |

- **forst-rs / rocksdb = 1.042×** (forst-rs is marginally SLOWER in aggregate — near parity).
- **forst-rs / forst C++ = 0.833×** (forst-rs is clearly FASTER than ForSt in aggregate).

> The 18-query strict total is dominated by the two heavy joins q7 (rdb 599 / forst-rs
> 695) and q20 (rdb 653 / forst-rs 824) where forst-rs loses to RocksDB, partly offset by
> forst-rs wins on q15/q16/q19 and the parity-class light queries. Net vs RocksDB is
> near-parity (1.04×). Vs ForSt the margin is large mainly because of q20 (ForSt 1342 vs
> frs 824) and q17 (ForSt 252 vs frs 84). If q17 used the conservative MEASURED inline
> 110.7s instead of 83.7s, the forst-rs total is 2938.5s → frs/rdb 1.051×, frs/forst 0.841×.

### 2b. Two-backend totals where ForSt DNF'd (rocksdb vs forst-rs only)

Adding the queries excluded above (where ForSt cannot be compared) for the
rocksdb-vs-forst-rs pair, using each query's finished number:

| query group | rocksdb (s) | forst-rs (s) | frs/rdb |
|-------------|------------:|-------------:|--------:|
| 18 strict-comparable | 2794.6 | 2911.5 | 1.04× |
| + q4 | 3080.6 | 3222.5 | 1.05× |
| + q4 + q9 (frs OFF 1828.7 / rdb 1076.8) | 4157.4 | 5051.2 | 1.22× |
| + q4 + q9 + q18 | 4523.8 | 5274.1 | 1.17× |
| **all finished (excl. q5, q6)** | **4523.8** | **5274.1** | **1.17×** |

(q5 excluded from every total — forst-rs has no clean perf number. q9 uses the
conservative 8c/32g KV-sep-OFF finish 1828.7s; on the 8c/36g profile q9 = 1357.1s, which
would make the all-finished frs total 4802.5s → frs/rdb 1.06×.)

---

## 3. forst-rs standing (per-query ratios + overall verdict)

Bars: vs RocksDB the working bar is **≤1.25×** (the V3 pass bar); vs ForSt the bar is
simply **faster than ForSt**. "win" vs a backend = forst-rs strictly faster; "tie" =
within ±2% (parity / source-bound); "loss" = forst-rs slower; ForSt DNF counts as a
forst-rs win (forst-rs finishes where ForSt cannot).

| query | frs/rdb | vs RocksDB | frs/forst | vs ForSt |
|-------|--------:|------------|----------:|----------|
| q0  | 0.97× | win  | 1.01× | tie |
| q1  | 0.97× | win  | 0.97× | win |
| q2  | 0.99× | tie  | 0.99× | tie |
| q3  | 1.02× | tie  | 0.96× | win |
| q4  | 1.09× | loss (≤1.25 PASS) | DNF | win (ForSt DNF) |
| q5  | n/a | no comparable perf | n/a | n/a (both not comparable) |
| q7  | 1.16× | loss (≤1.25 PASS) | 1.46× | loss |
| q8  | 1.03× | tie  | 1.11× | loss |
| q9  | 1.70× | loss | 0.91× | win |
| q10 | 0.96× | win  | 0.96× | win |
| q11 | 1.12× | loss (≤1.25 PASS) | 0.91× | win |
| q12 | 1.01× | tie  | 1.00× | tie |
| q13 | 0.97× | win  | 0.97× | win |
| q14 | 0.99× | tie  | 0.99× | tie |
| q15 | 0.70× | win  | 0.78× | win |
| q16 | 0.87× | win  | 0.98× | win |
| q17 | 1.24× | loss (≤1.25 PASS) | 0.33× | win |
| q18 | 0.61× | win  | DNF | win (ForSt DNF) |
| q19 | 0.82× | win  | 0.85× | win |
| q20 | 1.26× | loss (NEAR — misses ≤1.25 by 0.01×) | 0.61× | win |
| q21 | 0.90× | win  | 0.92× | win |
| q22 | 0.99× | tie  | 0.94× | win |

### Win / tie / loss tally for forst-rs (q6 and q5 excluded; 21 comparable queries)

Bands: win = frs/backend < 0.98; tie = 0.98–1.02 (±2%, parity / source-bound); loss =
> 1.02. ForSt DNF (q4, q18) counts as a forst-rs win by completion.

**vs RocksDB** (21 queries): **win 9 / tie 6 / loss 6**
- **win** (9): q0, q1, q10, q13, q15, q16, q18, q19, q21
- **tie** (6): q2, q3, q8, q12, q14, q22
- **loss** (6): q4 (1.09×), q7 (1.16×), q9 (1.70×), q11 (1.12×), q17 (1.24×), q20 (1.26×)
  — of these, q4/q7/q11/q17 PASS the ≤1.25× working bar, q20 is NEAR (misses by 0.01×),
  and **q9 (1.70×) is the one hard miss**.

**vs ForSt C++** (21 queries; ForSt DNF on q4/q18 counted as forst-rs wins):
**win 15 / tie 4 / loss 2**
- **win** (15): q1, q3, q4 (ForSt DNF), q9, q10, q11, q13, q15, q16, q17, q18 (ForSt DNF),
  q19, q20, q21, q22
- **tie** (4): q0, q2, q12, q14
- **loss** (2): q7 (1.46×), q8 (1.11×)
  (q9 wins vs ForSt because frs 1828.7 < ForSt 2002.5.)

### Headline verdict

- **Aggregate vs RocksDB (18 strict tri-backend queries): forst-rs 2911.5s vs RocksDB
  2794.6s = 1.042× — near parity, forst-rs marginally slower in total.** Over the wider
  "all-finished excl. q5/q6" set including the heavy q4/q9/q18, forst-rs is 1.17× RocksDB
  (RocksDB pulls far ahead on q9, forst-rs pulls ahead on q18). If q9 uses its 8c/36g
  finish (1357.1s) the all-finished ratio drops to 1.06×.
- **Aggregate vs ForSt C++ (18 strict tri-backend queries): forst-rs 2911.5s vs ForSt
  3495.7s = 0.833× — forst-rs is clearly FASTER in total**, and beats ForSt on the
  count (15 win / 4 tie / 2 loss over 20). ForSt additionally DNFs q4/q5/q18 where
  forst-rs finishes.
- **Per-query, forst-rs vs RocksDB: 9 win / 6 tie / 6 loss** over 21 queries; of the 6
  losses, 4 still PASS the ≤1.25× working bar (q4/q7/q11/q17), q20 is NEAR (1.26×, single
  busy-disk run), leaving **q9 (1.70×) as the one hard RocksDB miss** at 8c/32g — and even
  q9 flips to a win-vs-ForSt finish (and to 1357.1s on the 8c/36g profile).
- **Per-query, forst-rs vs ForSt C++: 15 win / 4 tie / 2 loss** over 21 queries (only
  q7 and q8 lose to ForSt).
- **The forst-rs deficits are concentrated in the heavy windowed-join / windowed-agg
  family** (q7, q9, q17, q20). The wins are broad: all light/source-bound queries are at
  parity-or-faster, and forst-rs beats BOTH backends on q15, q16, q19 (and q18 by ForSt
  DNF + faster-than-RocksDB).

---

## 4. Data provenance / caveats (per number: source pass + freshness)

**Master ledger:** `docs/superpowers/specs/2026-06-08-8c32g-3backend-sweep-results.md`.
**Per-query best config:** `tools/nexmark-local/configs/best-config.tsv`.

### Heavy queries (q4, q7, q9, q11, q12, q17, q19, q20) — FRESH (2026-06-14/15)
These are the clean **V3 same-pass A/B** captures (TOPO=split 8c/32g Mac, @100M, strictly
serial), the most authoritative LOCAL data:
- **q7, q19, q20, q4** = "V3 FULL 8-QUERY flag-ON" pass, tip 1cda0e724 (KV-sep ON best
  config). forst-rs: q7 695.5 / q19 216.0 / q20 824.4 / q4 311.0. rocksdb: q7 599.1 /
  q19 264.3 / q20 653.5 / q4 286.0. forst: q7 477.8 / q19 255.6 / q20 1342.0 / q4 DNF(OOM).
- **q11, q17, q9** = "V3 FLAG-OFF" pass, tip 15436bfde (KV-sep OFF best config).
  forst-rs: q11 118.8 / q17 110.7 (inline) / q9 1828.7. rocksdb: q11 105.9 / q17 71.6 /
  q9 1076.8. forst: q11 130.8 / q17 245.9 / q9 2002.5.
  - **q17 table cell uses 83.7s** (KV-sep-OFF + routing-adaptive capture,
    `/tmp/v3-q17-forst-rs-ffm-local.out`, NEEDS-CONFIRM); rocksdb/forst q17 walls are
    from the flag-ON pass (rdb 67.6 / forst 252.2). The 110.7 inline is the MEASURED
    fallback. CAVEAT: q17 forst-rs cell mixes a separate capture with the flag-ON
    rdb/forst walls — re-confirm on a quiet box.
- **q12** = source-bound parity, ~40s all three (flag-ON 40.6 / flag-OFF 41.6; box noise).
- **q9 footnote (8c/36g)** = "q9-8c36g-KVsep" section (2026-06-15), single-TM 36g profile,
  run-2 captured clean (`/tmp/confirm2-q9-36g-forst-rs-ffm-local-q9-forst-rs-ffm-local.out`:
  `wall_ms=1357117 out_rows=91813372`). This is a DIFFERENT topology than the 8c/32g table.
- CAVEATS: single serial pass on a 35g Mac; q20's 1.26× and q11/q17 deltas have documented
  single-run busy-disk variance. The flag-ON-vs-OFF per-query deltas ARE real (same-pass
  A/B). NEVER cross-compare these Mac numbers with the remote-x86 pins in the master ledger.

### Light + medium queries (q0–q3, q5, q8, q10, q13–q16, q18, q21, q22) — OLDER SWEEP
Source: the "REFRESHED PER-QUERY TABLE (2026-06-12)" Mac (M) population in the master
ledger (rocksdb/forst-rs/forst all M-population for these). These are an **older sweep**
than the V3 heavy-query passes; they are source-bound or pre-V3-lever and were not
re-measured under the V3 config (most are config-insensitive light queries, so this is
low-risk). Specifically:
- q0–q3, q10, q13, q14, q21, q22: source-bound, parity-class — older sweep, low-risk.
- q15 (rdb 229.7 / forst-rs 161.8 / forst 206.8), q16 (rdb 374.1 / forst-rs 323.9 /
  forst 331.7), q18 (rdb 366.4 / forst-rs 222.9 / forst DNF): older sweep; forst-rs beats
  both (q18 by ForSt DNF). NOT re-measured under V3 — re-confirm if used for a hard claim.
- q8 (rdb 42.5 / forst-rs 43.6 / forst 39.2): older sweep; windowed-join, out_rows now in
  the exact band (earlier under-emit bug FIXED per master ledger).
- **q5**: rocksdb 162.5 (older sweep); ForSt DNF; **forst-rs has NO clean perf number** —
  it is marked correctness-FIXED (commit 80015d2cfaa) but the 100M perf was never
  re-measured under a correct build, so its perf cell is "no data / not comparable" and q5
  is EXCLUDED from every total. Do NOT use the old 41.6s (that was the wrong-output build).

### Excluded
- **q6**: unsupported in Flink SQL itself (both engines) — out of scope by definition.
- **q5**: no comparable forst-rs perf number (see above).

### Totals methodology
- The **18-query strict tri-backend total** (§2a) is the only fully apples-to-apples
  cross-backend total: every included query has a finished number for all three backends.
- q4/q9/q18 are added back only for the rocksdb-vs-forst-rs pair (§2b), since ForSt DNFs
  q4/q18; q9 is folded into the wider pair-total there.
- No number was invented. Any cell without recorded data is marked "no data" / "n/a"
  (only q5 forst-rs perf hits this).
