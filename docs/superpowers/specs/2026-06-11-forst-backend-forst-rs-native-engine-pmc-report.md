# ForStBackend + forst-rs Native Engine PMC Report

Date: 2026-06-11

## Scope

This report compares the native-engine side used by Flink `ForStBackend` when the
community ForSt JNI library is replaced by the forst-rs compatible
`libforstjni.so`.

The Flink Java code is used only to identify which native APIs are actually
called: `multiGetAsList`, `RocksIterator`, and `WriteBatch`. This report does
not compare the standalone forst-rs backend against the ForSt backend, and it
does not treat JDK17/JDK25, SQL planner shape, deployment mode, or Docker setup
as the main variable.

CodeGraph was unavailable in this session (`Transport closed`), so the review
falls back to direct source inspection with `rg` and focused file reads.

## PMC Verdict

The current slower-than-ForSt result is not enough to reject the forst-rs engine
architecture, but it is enough to reject the current drop-in replacement path as
performance-ready.

The dominant problem is an API-surface mismatch:

- Flink `ForStBackend` calls the mature RocksDB/ForSt JNI compatibility surface.
- forst-rs has optimized native primitives for vectorized batch get/put,
  borrowed write batches, lazy prefix iterators, chunked iterators, and
  parallel prefix iterator open.
- The `ForStBackend + forst-rs lib` replacement path mostly does not hit those
  optimized primitives. It hits the standard RocksDB-compatible JNI symbols,
  where the current compat implementation is materially weaker than the
  community ForSt native library.

Therefore the root cause is best classified as:

1. **P0: native compat iterator semantics are not RocksDB-class.**
2. **P1: native compat multiGet and WriteBatch do not preserve the expected
   native batching properties.**
3. **P1: forst-rs engine capabilities exist, but the ForStBackend replacement
   lib path bypasses them.**
4. **P2: some engine modules remain immature under heavy state churn, but they
   are not the first explanation for the observed ForStBackend replacement
   slowdown.**

This is a native-side issue, not a SQL benchmark accident. The ForSt Java backend
is a stable caller; the replacement native library must make that API fast.

## Observed Benchmark Shape

The current benchmark evidence is consistent with the source-level diagnosis:

| Area | Result | Interpretation |
| --- | --- | --- |
| Fixed CSV accuracy | Q0-Q22 pass at 1M with query-specific handling for Q6/Q9/Q12 | Correctness is not the current blocker for the replacement lib path. |
| Q0 100M | forst-rs lib `1.006x` | Low-state path is near parity. |
| Q1 100M | forst-rs lib `0.955x` | No engine-wide speedup signal. |
| Q2 100M | forst-rs lib `0.971x` | No engine-wide speedup signal. |
| Q3 10M | forst-rs lib `1.017x` | Stateful join gets only marginal gain. |
| Q4 1M patched map-cache | `0.987x` by source-target time, `0.946x` by wall time | Iterator-heavy MapState path is not improved by cache-size tuning. |
| Q3/Q5 100M | baseline itself times out or cannot provide a useful A/B window | Full 100M Q0-Q22 cannot currently support a 1.5x claim. |

The important signal is not that every query proves forst-rs slower. The signal
is that the only completed low-state queries are around parity, while
state-heavy iterator/join/window paths do not expose the expected native-engine
advantage. That matches the compat JNI gaps below.

## Native API Surface Used By ForStBackend

### multiGet

`ForStGeneralMultiGetOperation` builds Java lists of column family handles and
keys, then calls:

- `RocksDB.multiGetAsList(readOptions, columnFamilyHandles, keys)`

Relevant code:

- `flink-state-backends/flink-statebackend-forst/src/main/java/org/apache/flink/state/forst/ForStGeneralMultiGetOperation.java:84`
- `flink-state-backends/flink-statebackend-forst/src/main/java/org/apache/flink/state/forst/ForStGeneralMultiGetOperation.java:117`

For the community ForSt JNI library, this is expected to be a native multi-get
surface: one Java call, native-side batching, and reduced repeated lookup setup.

In forst-rs compat JNI, the standard `RocksDB.multiGet` implementation reads the
Java byte arrays into `Vec<Vec<u8>>`, resolves the CF list, allocates a Java
result array, then loops one key at a time and calls `frs_get`:

- `crates/forst-rs-ffi/src/compat_jni.rs:8838`
- `crates/forst-rs-ffi/src/compat_jni.rs:8869`
- `crates/forst-rs-ffi/src/compat_jni.rs:8875`

There is a stronger vectorized native API in `frs_vectorized_batch_get`, which
builds key slices once and calls `db.batch_get`:

- `crates/forst-rs-ffi/src/lib.rs:3054`
- `crates/forst-rs-ffi/src/lib.rs:3137`
- `crates/forst-rs-ffi/src/lib.rs:3157`

But `ForStBackend` does not call that API through the current drop-in library
path. So the native engine has a good direction, while the compatibility surface
used by ForStBackend is still a per-key path.

### Iterator / MapState Prefix Scan

`ForStDBIterRequest` uses the standard RocksDB iterator pattern:

1. open an iterator on the column family,
2. seek to a serialized key prefix,
3. loop while key starts with the prefix,
4. read key/value pairs and deserialize them in Java.

Relevant code:

- `flink-state-backends/flink-statebackend-forst/src/main/java/org/apache/flink/state/forst/ForStDBIterRequest.java:116`
- `flink-state-backends/flink-statebackend-forst/src/main/java/org/apache/flink/state/forst/ForStDBIterRequest.java:121`
- `flink-state-backends/flink-statebackend-forst/src/main/java/org/apache/flink/state/forst/ForStDBIterRequest.java:129`
- `flink-state-backends/flink-statebackend-forst/src/main/java/org/apache/flink/state/forst/ForStDBIterRequest.java:132`

`ForStIterateOperation` drives these requests with a cache size of 128:

- `flink-state-backends/flink-statebackend-forst/src/main/java/org/apache/flink/state/forst/ForStIterateOperation.java:34`
- `flink-state-backends/flink-statebackend-forst/src/main/java/org/apache/flink/state/forst/ForStIterateOperation.java:73`

For a RocksDB/ForSt-class native engine, `newIterator(cf)` plus `seek(prefix)`
must behave like a cursor operation. It should not materialize the whole column
family before the prefix is known.

The current forst-rs compat path does exactly the risky thing:

- `Java_org_forstdb_RocksDB_iterator` calls `frs_iterator_open`.
- `frs_iterator_open` performs `db.scan(cf, &[], None)`.
- `db.scan` collects the iterator into a `Vec`.

Relevant code:

- `crates/forst-rs-ffi/src/compat_jni.rs:5967`
- `crates/forst-rs-ffi/src/compat_jni.rs:5992`
- `crates/forst-rs-ffi/src/lib.rs:324`
- `crates/forst-rs-ffi/src/lib.rs:3475`
- `crates/forst-rs-ffi/src/lib.rs:3496`
- `crates/forst-rs-ffi/src/lib.rs:3497`
- `crates/forst-rs-engine/src/db.rs:6138`
- `crates/forst-rs-engine/src/db.rs:6153`

After this full-CF materialization, `seek0` binary-searches the materialized
rows and prefetches one row:

- `crates/forst-rs-ffi/src/compat_jni.rs:6049`
- `crates/forst-rs-ffi/src/compat_jni.rs:6093`
- `crates/forst-rs-ffi/src/compat_jni.rs:6108`
- `crates/forst-rs-ffi/src/lib.rs:3555`
- `crates/forst-rs-ffi/src/lib.rs:3593`

This is the clearest native-side smoking gun. ForStBackend creates many short
prefix scans for MapState. Community ForSt/RocksDB can serve that as cursor
seek and block iteration. forst-rs compat JNI first builds a full materialized
iterator, then seeks inside it. This can easily dominate Q4/Q7/Q9/Q20-like
paths regardless of Java-level cache-size changes.

The engine does have better prefix primitives:

- `prefix_scan_iter` is lazy across active memtable, immutable memtables, and
  SSTs.
- `prefix_scan_iter_owned_arc` exists for FFI streaming/chunked paths.
- `batch_open_prefix_iters_parallel` opens multiple lazy prefix iterators in
  parallel.

Relevant code:

- `crates/forst-rs-engine/src/db.rs:6233`
- `crates/forst-rs-engine/src/db.rs:6253`
- `crates/forst-rs-engine/src/db.rs:6323`
- `crates/forst-rs-engine/src/db.rs:6412`
- `crates/forst-rs-engine/src/db.rs:6466`
- `crates/forst-rs-engine/src/db.rs:6507`

But the standard `RocksIterator` API used by ForStBackend is not wired to those
prefix-aware primitives. The optimized route exists beside the hot path, not in
the hot path.

### Iterator Copy Chain

The compat iterator also pays an avoidable copy chain.

`frs_iterator_next` returns Rust-owned `FrsBytes`; because the compat
`RocksIterator` must support `isValid()/key()/value()` without consuming extra
rows, `fetch_into_handle` copies those bytes into cached `Vec<u8>` values and
then frees the FFI buffers:

- `crates/forst-rs-ffi/src/lib.rs:3609`
- `crates/forst-rs-ffi/src/lib.rs:3643`
- `crates/forst-rs-ffi/src/lib.rs:3649`
- `crates/forst-rs-ffi/src/compat_jni.rs:5894`
- `crates/forst-rs-ffi/src/compat_jni.rs:5932`
- `crates/forst-rs-ffi/src/compat_jni.rs:5936`

The later Java `key()` and `value()` calls must still get Java byte arrays.
Compared with a mature RocksDB JNI iterator over block-cache-backed native
memory, this is a high-allocation compatibility design.

This does not mean zero-copy is the wrong goal. It means the current standard
RocksIterator emulation cannot be the fast path for ForStBackend.

### WriteBatch

`ForStDBWriteBatchWrapper` constructs a native `WriteBatch`, appends
put/delete/merge entries, and flushes through `db.write(options, batch)`:

- `flink-state-backends/flink-statebackend-forst/src/main/java/org/apache/flink/state/forst/ForStDBWriteBatchWrapper.java:54`
- `flink-state-backends/flink-statebackend-forst/src/main/java/org/apache/flink/state/forst/ForStDBWriteBatchWrapper.java:112`
- `flink-state-backends/flink-statebackend-forst/src/main/java/org/apache/flink/state/forst/ForStDBWriteBatchWrapper.java:120`
- `flink-state-backends/flink-statebackend-forst/src/main/java/org/apache/flink/state/forst/ForStDBWriteBatchWrapper.java:128`
- `flink-state-backends/flink-statebackend-forst/src/main/java/org/apache/flink/state/forst/ForStDBWriteBatchWrapper.java:135`

The forst-rs engine write batch representation is directionally good. It uses
`Cow<'a, [u8]>` so FFI hot paths can borrow caller buffers, and it tracks a
single-CF hint to avoid grouping overhead:

- `crates/forst-rs-engine/src/write_batch.rs:31`
- `crates/forst-rs-engine/src/write_batch.rs:50`
- `crates/forst-rs-engine/src/write_batch.rs:96`
- `crates/forst-rs-engine/src/write_batch.rs:209`

But the compat `WriteBatchHandle` buffers entries as owned `Vec<u8>` and
`RocksDB.write0` drains them one by one through `frs_put`, `frs_merge`, or
`frs_delete`:

- `crates/forst-rs-ffi/src/compat_jni.rs:657`
- `crates/forst-rs-ffi/src/compat_jni.rs:677`
- `crates/forst-rs-ffi/src/compat_jni.rs:6561`
- `crates/forst-rs-ffi/src/compat_jni.rs:6584`
- `crates/forst-rs-ffi/src/compat_jni.rs:6903`
- `crates/forst-rs-ffi/src/compat_jni.rs:6943`
- `crates/forst-rs-ffi/src/compat_jni.rs:6945`
- `crates/forst-rs-ffi/src/compat_jni.rs:6961`
- `crates/forst-rs-ffi/src/compat_jni.rs:6982`
- `crates/forst-rs-ffi/src/compat_jni.rs:7002`

There is also an explicit comment that the compat batch is not strict
transactional/atomic today:

- `crates/forst-rs-ffi/src/compat_jni.rs:662`

For performance, this means the current ForStBackend replacement path does not
fully inherit the engine's borrowed batch design. For upstream readiness, the
semantic gap around batch atomicity must also be closed before claiming a
drop-in ForSt/RocksDB replacement.

## Is This An Engine Design Problem?

The answer is nuanced.

It is **not** evidence that a Rust LSM engine, vectorized batch API, zero-copy
FFI, or separated state storage design is the wrong direction. The engine source
contains the right primitives: lazy prefix iterators, value-carrying prefix
iteration, batch get, vectorized batch put/delete, borrowed write batches, and
parallel prefix-iterator open.

It **is** an engine/productization problem for the drop-in ForStBackend path,
because the native compatibility layer is part of the engine product surface.
ForStBackend does not care that a faster API exists elsewhere. If the library is
installed as `libforstjni.so`, the standard RocksDB-compatible APIs must be fast
and semantically equivalent enough for Flink's existing ForSt backend.

PMC classification:

- Engine core direction: **reasonable and still worth pursuing**.
- Current replacement-lib compatibility implementation: **not performance-ready**.
- Current evidence for 1.5x over ForSt on ForStBackend: **not accepted**.
- Main risk: **optimized native primitives are not wired into the compatibility
  surface actually used by ForStBackend**.

## Why forst-rs Can Be Slower Than ForSt In ForStBackend

For the original ForSt native library, the hot native surfaces are aligned with
Flink's caller:

- native multiGet stays native-batched;
- `RocksIterator` open/seek behaves like cursor positioning, not full-CF
  collection;
- `WriteBatch` is a native batch buffer with mature write semantics;
- block cache, prefix seek, iterator, and write paths are optimized for this
  exact JNI API style.

For the current forst-rs compat library:

- standard `multiGet` can degrade to per-key `frs_get`;
- standard `RocksIterator` open can degrade to full-CF materialization;
- seek then runs over the materialized rows;
- iterator next/key/value has extra Rust-Vec and Java-byte-array copies;
- standard `WriteBatch` stores owned Rust `Vec`s and writes entries one by one;
- optimized vectorized/chunked/borrowed APIs exist but are not the APIs
  ForStBackend calls.

That is sufficient to explain "forst-rs slower than ForSt" for the ForStBackend
replacement-lib experiment without blaming JDK selection or SQL execution.

## Is The Current forst-rs Optimization Direction Reliable?

The direction is partially right but currently attached to the wrong public
surface for this experiment.

Reliable parts:

- batch/zero-copy/bulk FFI is the correct class of optimization;
- lazy prefix iteration is the right fix for MapState scan pressure;
- value-carrying iterator work is directionally correct because RocksDB/ForSt
  iterators naturally carry values with keys in the native read path;
- borrowed write batch design is directionally correct for FFI throughput.

Unreliable parts for the ForStBackend replacement target:

- optimizing only the standalone forst-rs backend or FFM vectorized executor
  does not prove the drop-in `libforstjni.so` path is faster;
- changing MapState iterator cache size in Java cannot fix native iterator open
  that materializes a whole CF;
- tuning jemalloc, mini-batch, or write buffer sizes cannot compensate for
  wrong native API semantics;
- native microbenchmarks that call `frs_vectorized_batch_get` or
  `frs_vec_iter_prefix_open*` do not validate `RocksDB.multiGetAsList` and
  `RocksIterator` unless those symbols route to the same optimized machinery.

## Recommended Recovery Plan

### Gate 1: Make compat RocksIterator prefix-aware

The highest-priority fix is to change the standard `RocksDB.iterator` +
`RocksIterator.seek0(prefix)` path so ForStBackend does not pay full-CF scan on
iterator open.

Acceptable designs:

1. Lazy generic iterator: `frs_iterator_open` opens a streaming cursor over the
   CF and `seek0` positions it without pre-collecting the CF.
2. Prefix-specialized compat fast path: delay actual engine iterator creation
   until `seek0`, infer the prefix from the seek key, and route to
   `prefix_scan_iter_owned_arc` or equivalent.
3. Backend-aware native extension: add a ForSt-specific JNI method for prefix
   iterator open and modify the copied `forst-rs-jdk17` backend to call it.

From a PMC standpoint, option 1 is the cleanest drop-in story. Option 3 is
acceptable for an experimental backend fork, but it no longer proves that
`libforstjni.so` is a transparent ForSt native replacement.

### Gate 2: Route standard multiGet to true native batch get

`RocksDB.multiGet` / `multiGetAsList` must call an engine batch-get path, not a
loop of `frs_get`.

Minimum acceptance:

- same input order and null/missing semantics as current Java caller expects;
- multi-CF grouping supported without per-key full setup;
- one batch-level engine call per CF group, or a native multi-CF batch primitive;
- benchmark specifically through the Java `multiGetAsList` entry, not only
  through forst-rs custom FFI.

### Gate 3: Make compat WriteBatch a real native batch path

The compat `WriteBatch` path should preserve Flink's expected batching benefit.

Minimum acceptance:

- avoid per-entry `frs_put`/`frs_delete` dispatch in `RocksDB.write0`;
- route drained entries into engine `batch_write` or a borrowed single-CF batch
  path where possible;
- preserve entry order and expected failure semantics;
- close or explicitly document any atomicity divergence before upstream claims.

### Gate 4: Validate native surfaces before Nexmark 100M

Before spending resources on full 100M Q0-Q22 again, add small native/JNI
surface benchmarks that call exactly the same symbols as ForStBackend:

| Benchmark | Must call | Success target |
| --- | --- | --- |
| Iterator open+seek+128 next | `RocksDB.iterator` + `RocksIterator.seek0` + `key/value/next0` | No full-CF materialization; within parity of community ForSt JNI on 1M-row CF. |
| Prefix-scan small fanout | repeated short MapState-like scans | forst-rs compat no worse than community ForSt JNI. |
| multiGetAsList batch | Java `multiGetAsList` | at least parity before claiming end-to-end speedup. |
| WriteBatch 500 put/delete | Java `WriteBatch` + `db.write` | at least parity and no semantic regression. |

Only after those native surfaces are at parity should the project rerun 100M
Nexmark. Otherwise, the 100M run mostly measures known compatibility gaps.

## Final PMC Judgment

For the specific target "JDK17 + ForStBackend + forst-rs replacement
`libforstjni.so` beats community ForSt native", the current implementation is
not yet on the shortest reliable path to 1.5x.

The shortest reliable path is not more general Nexmark tuning. It is to make the
native compatibility symbols that ForStBackend already calls route into the
optimized forst-rs engine primitives:

1. fix standard iterator open/seek first;
2. fix standard multiGet second;
3. fix standard WriteBatch third;
4. only then run full 100M Q0-Q22 again.

If the project instead wants to exploit the more aggressive vectorized/chunked
APIs immediately, then copying the ForSt backend into a `forst-rs-jdk17` backend
and changing the backend to call forst-rs-specific JNI is a more honest
architecture. That route can be faster, but it should be presented as a new
backend integration, not as a transparent replacement of the original ForSt
native engine.
