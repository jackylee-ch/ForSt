# P1 Correctness Ticket — `ForStRsValueStateV2` does not encode namespace into storage key

**Status:** Open. P1 correctness — silent data corruption under hopping or overlapping-window patterns.
**Date filed:** 2026-05-19
**Discovered in:** [`2026-05-19-q11q12-state-primitive-audit.md`](./2026-05-19-q11q12-state-primitive-audit.md) §4.3
**Affects:** `ForStRsValueStateV2` in `flink-statebackend-forst-rs/.../state/ForStRsValueStateV2.java`

---

## 1. The bug

`ForStRsValueStateV2.serializeKey` (lines 73-105) and `serializeKeyInto` (lines 154-167) compose the composite storage key as:

```
KEY_PREFIX ("k/") + serialize(K) + "/" + stateName + "/"
```

The **namespace `N` is not encoded**. For `WindowAsyncValueState<W>` whose namespace is the window (typically `Long` slice-end), this means: different window instances for the same operator key map to the **same storage key** in the engine.

`StateRequest` carries the namespace — `request.getNamespace()` is available at request-build time — but the state class ignores it.

## 2. Why current Nexmark queries don't trip this

- **Q12 (PROCTIME tumble, unshared slices):** at any moment, only one slice is active per key. The slice-fire path calls `asyncClear` on the window before the next slice can begin writing. There is no overlap → no key collision.
- **Q11 (SESSION):** sessions for the same key cannot overlap (a session ends when the gap expires; the next session starts after a gap). At most one active session per key → no overlap.
- All other current Nexmark queries on async-state-V2 use the same pattern.

The bug is therefore **dormant**, not absent. It is "accidentally correct" for the queries in the current test matrix.

## 3. Patterns that DO trigger silent corruption

- **Hopping windows:** by construction, hopping slices overlap. A bid record assigned to slice A and a later bid assigned to slice B (which began before A ended) will both write to the same storage key. Whichever PUT lands second wins; the other accumulator is lost. The query produces wrong aggregates with no error.
- **Late-arrival into not-yet-cleared slice:** if a bid arrives for slice A after slice B has started accumulating but before A has been cleared, the GET on slice A returns slice B's accumulator. Wrong aggregation.
- **Custom user code using `ValueState` with non-trivial namespaces:** any user that calls `state.setCurrentNamespace(...)` then `asyncValue()` with concurrent namespaces for the same key is silently corrupted.

## 4. Fix design

The correct fix is to **append the serialized namespace bytes** to the composite key:

```
"k/" + serialize(K) + "/" + stateName + "/" + serialize(N)
```

This requires the state class to hold a `TypeSerializer<N>` (currently only `keySerializer` and `valueSerializer` exist). The namespace serializer is available from the operator at state-creation time — passed via `getOrCreateKeyedState(defaultWindow, createWindowSerializer(), descriptor)` at `AbstractAsyncStateWindowAggProcessor.open` line 75-81. The factory path that creates `ForStRsValueStateV2` must thread the namespace serializer through.

### 4.1 State-format-change risk

Appending namespace bytes is a **storage-format change**: existing on-disk state from prior runs becomes unreadable. For ForSt-RS (alpha) this is acceptable; document the format bump in the changelog.

For Nexmark specifically, state is ephemeral (each run starts clean) so there is no migration concern.

### 4.2 Free perf side-benefit

Encoding namespace into the key enables a future per-namespace `ValueStateCache` (or merge-compute API) to disambiguate same-key-different-namespace lookups without runtime checks. Without namespace in the key, any cache layer is forced to track the namespace separately, doubling the lookup surface.

## 5. Interim defensive measure (this ticket)

Until the fix in §4 lands, add a runtime warning at `ForStRsValueStateV2` construction time when the framework hands us a non-trivial namespace serializer. We cannot rely on detecting hopping-vs-tumble at request time without per-key tracking (expensive). Instead, log a one-time WARN at first non-Void namespace observation. This gives operators visibility that the state class is in the "accidentally correct" regime, so they can audit their window pattern.

**Concrete code:** add a transient `boolean warnedNamespace` to `ForStRsValueStateV2`. In `serializeKey`, if `!warnedNamespace && request.getNamespace() != null && !(request.getNamespace() instanceof org.apache.flink.runtime.state.VoidNamespace)`, log WARN with the namespace's class name and stack trace at first encounter, then set `warnedNamespace = true`. The check is one boolean read per request after the first observation — negligible perf impact.

**What this does NOT do:**

- It does NOT fix the bug. Q11/Q12 will continue to be accidentally correct and silently wrong under hopping.
- It does NOT throw or abort. Throwing would break Q11/Q12 today.

**What this DOES do:**

- Surfaces the issue in logs at runtime so an operator running a hopping-window job sees the warning and reads this ticket.
- Establishes a tracked-known-issue marker in the JVM logs that maps directly to this document.

## 6. Acceptance criteria

- [ ] §4 fix lands: `ForStRsValueStateV2` accepts and uses a `TypeSerializer<N>`, threads through factory and `serializeKey`/`serializeKeyInto`.
- [ ] §5 interim warning lands first as a no-impact safety net.
- [ ] Test: a unit test using a `Long`-namespace `ValueState` writes to namespaces 1 and 2 for the same key, reads both back, asserts they are distinct. Currently fails by design — should pass after the fix.
- [ ] Documentation update in `ForStRsValueStateV2`'s class javadoc explicitly stating that namespace is encoded into the storage key as of the fix-commit SHA.

## 7. Priority justification (P1, not P0)

- **P0** would require shipping the fix before V1.1. Current Nexmark queries are not affected, so the existing perf benchmarks remain valid.
- **P1** because the bug is real and affects any user with a hopping-window job — it is one workload-shape change away from production data loss.
- Not **P2** because the dormant-vs-active distinction is fragile: any future Nexmark query addition (e.g., a new hopping benchmark) would silently produce wrong results without anyone noticing the cause.

The §5 interim warning should land in V1.1 alongside the perf work; the §4 fix should land in V1.2 once the namespace-serializer plumbing is in place.

## 8. Cross-references

- Audit doc: [`2026-05-19-q11q12-state-primitive-audit.md`](./2026-05-19-q11q12-state-primitive-audit.md) §4.3
- CONTRIBUTING.md "MUST: Document the 3-layer state-class call stack" — Q11/Q12 audit is the empirical foundation of that rule.
- Compare to `ForStRsMapStateV2.serializeKey` which appends `userKey` (the map's user key) at the end — analogous structural position to where namespace should go for ValueState.
