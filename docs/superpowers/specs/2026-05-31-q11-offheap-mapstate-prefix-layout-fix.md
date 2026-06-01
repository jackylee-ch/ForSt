# q11/q15 correctness fix: off-heap MapState prefix-layout mismatch

Date: 2026-05-31
Status: FIXED + validated (q11 600s, 0 crashes, steady progress on the off-heap perf path)

## Symptom

q11 (and q15 — both session-window / MergingWindowSet operators) reproducibly
crashed with `java.lang.IllegalStateException: Window <...> is not in the
in-flight window set` shortly after the first checkpoint, followed by a job
crash-loop (RESTARTING, source stuck). q12 (tumbling) and the point-get/put
queries were unaffected.

A diagnostic gate `-Dforst.rs.mapstate.legacy=true` (force the legacy,
non-statebuf MapState constructor) made q11 run clean — localizing the bug to
the **off-heap statebuf MapState path** (1c.1, commit 633af3d), not the engine.

## Root cause

The off-heap MapState stores composite keys via
`ForStRsKeyGroupedSerializer.encodeForMapOffheap`:

```
[ keyGroup(2 BE) | serialize(key) | SEP | stateName | SEP | serialize(mapKey) ]
```

`statebuf.flushTo` drains those exact composite bytes to the engine, so engine
keys share that layout. `get()/put()/remove()` encode/look-up through the same
off-heap encoder, so they worked.

But every **prefix-based** operation — `forEachEntry`, `isEmpty`, `clear`,
`entries`/`keys`/`values` — recovers the mapKey by stripping `currentPrefix()`,
and the off-heap constructor wired `currentPrefix()` to:

```java
() -> buildPrefix(stateName)   // = [ "k/" | key | "/" | stateName | "/" ]
```

`buildPrefix` is the **legacy** layout: it leads with the `KEYED_NS_MARKER`
bytes `"k/"` (0x6B 0x2F), whereas the off-heap composite leads with the 2-byte
big-endian **keyGroup**. (SEP and SLASH are both `'/'`, so only the leading
bytes differ — but that is enough.)

Consequence in off-heap mode:
- `segmentStartsWith(row, buildPrefix)` rejected **every** statebuf row.
- `prefixLookupOpen(db, cf, buildPrefix)` matched **nothing** in the engine.

So `isEmpty()` always returned `true`, `clear()` was a no-op, and
`forEachEntry`/`entries()` returned an **empty** set. When the session-window
operator reloaded its `MergingWindowSet` mapping via iteration, it got an empty
map; a subsequent `retireWindow(W)` then found `W` absent → "not in the
in-flight window set" → crash.

## Fix

`ForStRsKeyedStateBackend.getMapState` (off-heap branch): change the
prefix-computer to produce the `encodeForMapOffheap`-matching prefix
`[ keyGroup(2 BE) | key | SEP | stateName | SEP ]`, using the same current-key
source the off-heap put/get encoder uses:

```java
() -> kgSer.encodeForState(
        offheapKeyGroupSupplier.getAsInt(),
        (K) offheapKeySupplier.get(),
        stateName)
```

`encodeForState(kg, key, stateNameBytes)` writes exactly
`writeShort(kg) | serialize(key) | SEP | stateName | SEP` — i.e. the
`encodeForMapOffheap` layout minus the trailing mapKey. One line fixes all six
off-heap prefix sites at once (they all route through `currentPrefix()`).

The legacy constructor (and its `buildPrefix` prefix-computer) is unchanged and
still correct for its own layout; the `-Dforst.rs.mapstate.legacy` gate is kept
as a documented fallback but is no longer needed.

## Validation

- Backend jar rebuilt (JDK25), deployed; `-Dforst.rs.mapstate.legacy=true`
  removed from `config-forst-rs.yaml.tpl` so the off-heap path is exercised.
- q11, 100M, forst-rs-ffm-s3, 8c/32g envelope, MAXSEC=600:
  - **0** `not in the in-flight window set` errors.
  - **0** RESTARTING; job RUNNING the whole window; source progressed
    monotonically 0 → 42.3M.
  - Same clean correctness as the legacy gate, now on the off-heap perf path.

## Not addressed here (separate, perf)

q11 did not consume all 100M within 600s — steady-state source rate collapsed to
~5-10K/s (the known heavy-query burst-then-collapse, an I/O/GC architectural
issue, tracked separately). This fix is **correctness only**: it removes the
crash and restores correct off-heap MapState iteration/clear/isEmpty semantics.
Full q11 output-equality vs RocksDB and a completion time require a longer
MAXSEC run.
