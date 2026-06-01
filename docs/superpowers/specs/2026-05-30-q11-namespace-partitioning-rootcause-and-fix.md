# 2026-05-30 — q11/q15 correctness crash ROOT CAUSE: forst-rs has no namespace partitioning

## Status
ROOT CAUSE DEFINITIVELY FOUND (in-scope backend gap). Fix DESIGNED, implementation pending.

## The crash (recap)
`java.lang.IllegalStateException: Window TimeWindow{...} is not in in-flight window set`
at `MergingWindowSet.retireWindow:127` ← `cleanWindowIfNeeded` ← `WindowOperator.onEventTime`
(watermark advance fires a cleanup timer). Steady-state, first attempt, HEAP timers, no
checkpoint/restart. rocksdb runs the SAME query fine → forst-rs-specific.

## Root cause: the NAMESPACE dimension is ignored
Flink keyed state is addressed by `(key, namespace)`. For window operators the namespace
IS the window. `ForStRsInternalKvStateAdapters.AbstractAdapter.setCurrentNamespace(N)`
(line 104) stores `currentNamespace` but every adapter's `bind()` keys state by
`stateName` ONLY — `currentNamespace` NEVER reaches the engine composite key
(`ForStRsReducingState` etc. compute key = `keyComputer()` = kgPrefix+serialize(key), no
namespace). The class doc admits it (lines 48-53: "single implicit namespace ... does NOT
partition the keyspace further") and all three `mergeNamespaces` (List:266/Reducing:434/
Aggregating:505) are NO-OPs ("when real namespace partitioning is wired ...").

### Why TUMBLE/HOP work but SESSION (q11) breaks
Flink-Table SLICING window aggs (TUMBLE/HOP) encode the window in the GROUPING KEY → each
window is a distinct `key` → forst-rs's key-only addressing is sufficient (q3/q4 are
byte-identical to rocksdb). SESSION windows are unsliceable + MERGING → Flink uses the
`WindowOperator` with namespace-PER-WINDOW + `mergeNamespaces(target, sources)` on merge.
With forst-rs ignoring namespace: (a) all of a key's session windows COLLIDE on one state
slot, and (b) merges never combine source→target. The MergingWindowSet (which DOES track
namespaces correctly via its own MapState mapping) then diverges from the collapsed actual
state → a cleanup timer fires for a window the set no longer contains → IllegalStateException.
Also: session aggregate RESULTS are silently WRONG (sessions' data collides/lost), even when
it doesn't crash.

Prior unit tests passed because they only exercised a SINGLE namespace — never the
multi-namespace session pattern.

## Fix: namespace-in-key + real mergeNamespaces (rocksdb model)
1. **Composite key gains the namespace suffix.** key = `kgPrefix + serialize(key) +
   serialize(namespace)`. The per-state classes (`ForStRsReducingState`/`Aggregating`/
   `List`/`Map`/`Value`) take an optional `namespaceBytes` appended after the key in
   `keyComputer`/`keyPrefix`. The adapter serializes `currentNamespace` (via
   `namespaceSerializer`) and sets it on the bound state before each op.
   - Existing non-window state uses `VoidNamespace` (constant) → suffix is a fixed
     constant → behavior unchanged (additive, safe). Validate q3/q4 stay byte-identical.
2. **`mergeNamespaces(target, sources)`** (Reducing/Aggregating/List):
   - Reducing: `acc=null; for src in sources: v=get(src); if v!=null {acc = acc==null? v : reduce(acc,v)}; clear(src)}; if acc!=null {v0=get(target); set(target, v0==null?acc:reduce(v0,acc))}`.
   - Aggregating: same shape but combine ACCUMULATORS via `aggregateFunction.merge` — REQUIRES
     `getInternal`/`updateInternal` (currently throw UnsupportedOperationException) → must
     expose the raw ACC. (`ForStRsAggregatingState` stores ACC; add raw accessors.)
   - List: concatenate source lists into target, clear sources.
   - All operate by `setCurrentNamespace(src/target)` + get/add/clear (now namespace-keyed).
3. **AggregatingAdapter.getInternal/updateInternal** must work (needed by #2 + TTL).

## Validation
- Extend the existing single-namespace unit tests with a MULTI-NAMESPACE merging-window
  suite: write distinct accumulators under namespaces A,B,C; mergeNamespaces(C,[A,B]);
  assert C = combine(A,B) and A,B cleared; assert cross-namespace isolation before merge.
- G3/G4 regression (q3/q4/q5 byte-identical to rocksdb — proves VoidNamespace path
  unchanged), then q11 on 8c/32g: expect NO crash (errors=0) and CORRECT output.

## Scope / risk
Touches the core key encoding of every state type (namespace suffix) + 3 mergeNamespaces +
AggregatingState raw-ACC accessors. Significant but well-defined and the user authorizes
"module-level rewrite for severe bottlenecks". It is THE zero-tolerance correctness fix for
session windows AND removes a silent wrong-results hazard. Implement incrementally
(ReducingState first — q11's likely type — with multi-ns tests, then Aggregating/List/Map/Value).

## PRECISE INJECTION POINT (found in code) — minimal blast radius
`ForStRsReducingState.computeKey()` (line 148) = `keyComputer.get()` (kgPrefix+key) or
`keyPrefix`; NO namespace. Add:
```java
private byte[] namespaceSuffix = null;            // default null → unchanged for direct users
public void setNamespaceSuffix(byte[] ns) { this.namespaceSuffix = ns; }
private byte[] computeKey() {
    byte[] base = (keyComputer != null) ? keyComputer.get() : keyPrefix;
    if (namespaceSuffix == null || namespaceSuffix.length == 0) return base;   // back-compat
    byte[] out = Arrays.copyOf(base, base.length + namespaceSuffix.length);
    System.arraycopy(namespaceSuffix, 0, out, base.length, namespaceSuffix.length);
    return out;
}
```
Same for `ForStRsAggregatingState`/`ForStRsListState`/`ForStRsMapState`/`ForStRsValueState`.
**Blast radius is minimal:** these per-state classes are ALSO used DIRECTLY (V1-sync/V2-async
per-record state, e.g. q11's 92M ValueState ops) — those callers never set the suffix, so
`namespaceSuffix==null` → `computeKey()` byte-identical to today. ONLY the adapter-routed
window state (`ForStRsInternalKvStateAdapters`) sets the suffix. So the change cannot affect
non-window state at all, and non-session window state sets a CONSTANT VoidNamespace suffix
(consistent → correct on fresh runs).
The adapter sets the suffix per-op: `s.setNamespaceSuffix(serialize(currentNamespace)); s.add(v);`
(serialize once + cache when currentNamespace unchanged). `mergeNamespaces` loops src→target
setting the suffix per read/write/clear.

## Build/validate pipeline (Java backend — NOT the Rust dylib)
Maven: `cd flink/flink-state-backends/flink-statebackend-forst-rs && mvn -q -o package` (or the
repo's build) → copy `target/flink-statebackend-forst-rs-2.2.0.jar` to `$FLINK_HOME/lib/`.
Then: new multi-namespace unit test (mvn test) → q3/q4 byte-identical regression → q11 on
8c/32g (expect errors=0 + correct output). This is a DIFFERENT pipeline from this session's
`cargo build -p forst-rs-ffi`; the Rust engine is unchanged by this fix.

## Cross-refs
- [[project_q11_correctness_regression_2026-05-28]] (root cause now found — supersedes the
  scale-race/Flink-runtime theory)
- [[project_full_sweep_2026-05-29]] (8c/32g binding 0.468×; q11/q15 NA from this crash)
