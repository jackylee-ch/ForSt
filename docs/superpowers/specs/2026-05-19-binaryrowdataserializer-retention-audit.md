# BinaryRowDataSerializer Retention-Semantics Audit

**Date:** 2026-05-19
**For:** V1.2 work item (B) — byte[] alloc elimination in `VectorizedExecutor.executeGets`
**Source paths:**
- `flink-table-runtime/src/main/java/org/apache/flink/table/runtime/typeutils/BinaryRowDataSerializer.java`
- `flink-table-runtime/src/main/java/org/apache/flink/table/runtime/typeutils/RowDataSerializer.java`

---

## Question

Does `BinaryRowDataSerializer.deserialize(DataInputView source)` **wrap** the source's buffer (forcing us to allocate fresh memory for every result row in `VectorizedExecutor.executeGets`), or does it **copy** internally (making the outer byte[] safely poolable)?

## Answer: **Copies internally.** Outer buffer is safely poolable.

`BinaryRowDataSerializer.deserialize(DataInputView source)` at line 96-103:

```java
public BinaryRowData deserialize(DataInputView source) throws IOException {
    BinaryRowData row = new BinaryRowData(numFields);
    int length = source.readInt();
    byte[] bytes = new byte[length];        // ← internal allocation
    source.readFully(bytes);                // ← copies from source into new byte[]
    row.pointTo(MemorySegmentFactory.wrap(bytes), 0, length);
    return row;
}
```

The returned `BinaryRowData` wraps a **freshly-allocated** byte[] (`bytes`), not the source's underlying buffer. The `DataInputView source` is fully drained at the byte level; nothing in the returned `BinaryRowData` references the input.

`RowDataSerializer.deserialize(DataInputView source)` at line 107-109 delegates straight to `BinaryRowDataSerializer.deserialize`:

```java
public RowData deserialize(DataInputView source) throws IOException {
    return binarySerializer.deserialize(source);
}
```

So the same semantics apply to all `RowData` accumulators (Q11/Q12 windowed aggs use exactly this code path).

## Consequence for (B) — V1.2 work item

The outer per-result `byte[len]` allocation in `VectorizedExecutor.executeGets` (line 321) **can be replaced by a single growable, pooled byte[]** without correctness risk. The serializer copies into its own buffer before returning, so reusing the outer buffer between result slots is safe.

```java
// Today (1M allocations per Q12 flush):
raw = new byte[len];
MemorySegment.copy(outData, ValueLayout.JAVA_BYTE, start, raw, 0, len);
completeGet(reqs[i], tables[i], raw);

// Proposed V1.2 (1 allocation, grown on demand):
if (poolBuf.length < len) {
    poolBuf = new byte[Math.max(poolBuf.length * 2, len)];
}
MemorySegment.copy(outData, ValueLayout.JAVA_BYTE, start, poolBuf, 0, len);
// completeGet must use only the first `len` bytes — DataInputDeserializer.setBuffer
// supports an explicit length, so we don't accidentally read past the slot.
completeGet(reqs[i], tables[i], poolBuf, len);
```

**Required change to the boundary:** `ForStRsInnerTable.deserializeValue(byte[] raw)` becomes `deserializeValue(byte[] raw, int len)`. Each state class updates its `DataInputDeserializer.setBuffer(raw, 0, len)` call.

## But: the savings are smaller than initially estimated

The inner allocation (`new byte[length]` inside `BinaryRowDataSerializer.deserialize`) **is not eliminated by this change** — it lives inside Flink's serializer. The pooled outer buffer only eliminates the *outer* allocation + the *outer* `MemorySegment.copy`.

For Q12 (~1M results per flush, ~24 bytes per accumulator):

| Cost item | Today | After (B) | Savings |
|---|---|---|---|
| Outer `new byte[len]` | ~50 ns × 1M = 50 ms | 0 (pooled) | 50 ms |
| Outer `MemorySegment.copy` | ~50 ns × 1M = 50 ms | unchanged | 0 |
| Inner `new byte[length]` (inside deserialize) | ~50 ns × 1M = 50 ms | unchanged (Flink-internal) | 0 |
| Inner `source.readFully` | ~30 ns × 1M = 30 ms | unchanged | 0 |
| **Total per flush** | ~180 ms | ~130 ms | **~50 ms (~28 %)** |
| Across Q12 (~12 flushes) | ~2.16 s | ~1.56 s | **~0.6 s (~0.5 % of 128 s)** |

The outer pooled-buffer change saves ~0.5 % of Q12 wall-clock — measurable but not transformative. The inner `BinaryRowDataSerializer`-internal allocation is the larger remaining term; eliminating it requires using the `deserialize(BinaryRowData reuse, DataInputView)` overload with a thread-local reuse object, which **adds reuse-plumbing to each state class** and risks correctness bugs if downstream code retains the reuse object reference beyond a single iteration.

## Revised priority for (B)

**Downgrade from "V1.2 medium-risk 2-3 days" to "V1.2 backlog, conditional on (C) shipping first".**

Rationale:
1. Outer-buffer pooling is 2-3 days of work (state-class boundary signature change × N classes + tests) for ~0.5 % Q12 wall-clock.
2. If `merge_compute_into` (C) ships first and delivers ~25-55 % wall-clock (depending on §6 batch sizes — see [Q11/Q12 speedup execution plan](./2026-05-19-q11q12-speedup-execution-plan.md) §2), then (B)'s 0.5 % becomes proportionally smaller (~0.7 % of the new Q12 baseline). Marginal value.
3. The inner-allocation elimination is where the larger remaining savings live, but it requires Flink-runtime-level changes (per [CONTRIBUTING.md "Stop when the dominant cost moves outside your layer"](../../../CONTRIBUTING.md)).

(B) becomes a low-priority cleanup once (C) is in, **not** a primary V1.2 deliverable.

## Cross-references

- [`2026-05-19-q11q12-speedup-execution-plan.md`](./2026-05-19-q11q12-speedup-execution-plan.md) — V1.1/V1.2 work distribution
- CONTRIBUTING.md "SHOULD: Stop when the dominant cost moves outside your layer" — directly applies here
