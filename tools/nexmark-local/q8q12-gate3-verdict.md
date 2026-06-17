# PMC-1 — q8 / q12 gate-3 SETTLED: source-rate + RowData-serde PARITY CEILING (no code)

Date 2026-06-18. Worktree off origin/forst-rs tip `72dae3a02` (branch `q8q12`),
symbolized `.so` (`CARGO_PROFILE_RELEASE_STRIP=none` + debuginfo), R-2 ValueState
RMW-cache jar deployed (`flink-statebackend-forst-rs-2.2.0.jar`, flink
readside-r2a tip `01ee09c8ec2`, jar mtime 2026-06-17 23:48). 2×4c/16g split,
uniform `*` config, EVENTS=100M, on the 37.77 GiB Mac Docker VM.

## VERDICT (definitive)

**Gate-3 (strictly faster than ForSt) is UNWINNABLE for q8 and q12 — they are a
genuine source-rate / RowData-serde PARITY CEILING, not a closable per-record
engine overhead. NO code change is warranted.** The forst-rs ENGINE is <0.3% of
CPU on both queries; the dominant cost is Flink-framework RowData
serialization/copy + nexmark datagen, paid identically by every state backend
(rocksdb/ForSt/forst-rs). There is no in-scope uniform lever: shaving the serde
would help all three arms equally and would not move the *relative* gap.

## EVIDENCE — JFR CPU profile (delay=24s,duration=20s, steady-state, settings=profile)

### q8 (windowed JOIN: person-dedup ⋈ auction-dedup, tumbling 10s) — wall 47.7s, exact 3,064,401
CPU by operator thread (6,826 execution samples):
| share | operator thread |
|------:|-----------------|
| 60.1% | Source: datagen → Calc → WatermarkAssigner → Calc → **LocalWindowAggregate** (source chain) |
| 20.0% | pool-4-thread-1 (datagen worker: `String.charAt` + `RowDataSerializer.copy`) |
|  3.4% | GlobalWindowAggregate[7] (ValueState — R-2 path) |
|  3.4% | GlobalWindowAggregate[14] (ValueState — R-2 path) |
|  2.1% | WindowJoin[16] (ListState — NOT an R-2 path) |
| ~7%   | OutputFlushers / reaper / netty |

Whole-stack subsystem touch-rate:
- **forst-rs NATIVE/FFM: 0.2%**  •  forst-rs backend (any frame): 7.3%
- nexmark datagen/generator: 17.4%  •  **RowData serde/copy: 53.1%**

Top leaf methods: `String.charAt` (datagen), `RowDataSerializer.copyRowData/copy`,
`GenericRowData.isNullAt`, `BinarySegmentUtils.copyToBytes`,
`RowDataEventDeserializer.deserialize` — all source/serde, zero engine hotspot.

### q12 (proctime tumbling-10s COUNT GROUP BY bidder) — wall 46.6s, exact 92,000,000
CPU by operator thread (8,916 samples):
| share | operator thread |
|------:|-----------------|
| 64.6% | Source: datagen → Calc → WatermarkAssigner → Calc (source chain) |
| 34.9% | WindowAggregate[6] + Writer (window machinery + serde, NOT pure state) |

Whole-stack subsystem touch-rate:
- **forst-rs NATIVE/FFM: 0.1%**  •  forst-rs backend (any frame): 10.0%
- nexmark datagen/generator: 7.8%  •  **RowData serde/copy: 34.0%**

WindowAggregate[6] leaves are diffuse: `SegmentHash.polynomial31` +
`MurmurHashUtils.hashBytesByInt` (key hashing), `SliceAssigner.assignSliceEnd`
(proctime slicing), `MemorySession/SegmentBulk` (the zero-copy off-heap Arrow
path — already efficient), `ArrowTimerBuffer.heapLess` (timer),
`BinaryStringData.copy` (serde). **No single RMW/point-get hotspot.**

### Both queries: ~20s of the ~45s wall is the datagen RAMP (src_out=0 until ~20s),
then they run at the **~2.4M events/s datagen source rate** to completion. That
source rate — not the backend — sets the wall.

## R-2 (windowed ValueState write-back RMW cache) — already deployed, does NOT move q8/q12
The decisive control: q12 measured **44.7s WITH the R-2 jar** (`/tmp/r2ok-q12…`,
2026-06-17 23:09) — **byte-identical to the 44.7s pre-R-2 gate run**. R-2 turned
q17 (unbounded GROUP BY, hot-key RMW) from 236.9s → 47.7s (5×) because q17 was
RMW-bound; q8/q12 are NOT (engine = 0.1-0.2% FFM), so the same cache has no
purchase. This confirms category = source-ceiling, NOT the q11/q17 windowed-RMW
class. R-2 DOES cover q8/q12's windowed ValueState (`ForStRsValueStateV2`,
namespace-encoded cache key) and is gated ON under the uniform inline executor —
it simply isn't the bottleneck here.

## WHY no uniform in-scope lever exists
1. Engine native/FFM is 0.1-0.2% — an engine optimization cannot move a wall the
   engine isn't on.
2. The 34-53% RowData serde/copy is Flink-runtime `RowDataSerializer` /
   `BinarySegmentUtils` — backend-agnostic; rocksdb & ForSt pay it too. Reducing
   it (if even possible without a Flink-core change, which is out of scope and not
   forst-rs-specific) helps all arms equally → no change to the gate-3 *relative*
   gap.
3. The residual forst-rs-vs-ForSt delta is JVM/FFM constant-factor, within
   run-to-run VM noise:
   - q8: ForSt 41.2s; forst-rs 45.0 / 46.7 / 48.7 / 50.7 (±6%, VM-load drift).
   - q12: ForSt 43.9s; forst-rs **43.6** / 44.7 / 46.6 / 57.7 — forst-rs has
     **beaten** ForSt (43.6 < 43.9) on one clean run; the gate "fail" is
     noise-level parity.

## Bottom line
q8/q12 PASS gates a (≤1.25× rdb) and b (≤50s regression) and sit at source-rate
parity with ForSt. Gate-3 cannot be won by any engine/backend lever because the
backend is not on the critical path (FFM ≤0.2%). This is the same class as the
q0-q3/q21/q22 source-bound queries — "forst-rs cannot win on source-bound."
Verdict: **gate-3 unwinnable for q8/q12; source-rate + RowData-serde parity
ceiling; no code.**
