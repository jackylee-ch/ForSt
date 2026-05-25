# ForSt-RS Round 0 代码审查与修复报告

日期：2026-05-25

范围：仅审查和修改 ForSt-RS engine（`/Users/lijunqing/Code/stczwd/ForSt`）与 ForSt-RS backend（`/Users/lijunqing/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs`）。未扩展到 Flink runtime 引擎。

## 执行状态

- 已完成 Round 0 五代理审查、合并分类、High 问题局部修复与验证。
- 已完成 Round 1 五代理复审；Round 1 新发现的可局部修复 High 已继续修复并验证。
- 未完成用户要求的 100 轮或连续 5 轮无 High 停止条件；原因是 Round 0 仍存在端到端 Arrow/批量化、Async V2 聚合 convoy 等结构性 High，不能在一次局部修复中安全关闭。

## Round 0 High 问题与处理

### 已修复 High

1. L0 overlapping SST point-get 可能读到旧值
   - 文件：`crates/forst-rs-engine/src/db.rs`
   - 函数：`sst_get`、`peel_merges_from_sst`
   - 问题：L0 文件按 key range 排序，不等价于新旧顺序；新宽范围 SST 可能排在旧窄范围 SST 前面，旧实现按 `iter().rev()` 查找会返回旧值。
   - 修复：收集所有命中的 L0 row，按 `sequence desc, file_number desc` 排序后再处理 Put/Delete/Merge。
   - 回归：`test_l0_newer_wide_range_shadows_older_narrow_range`。

2. multi-CF `batch_write` 部分提交后错误可被消费，后续读/检查点可能继续暴露 torn state
   - 文件：`crates/forst-rs-engine/src/db.rs`
   - 函数：`batch_write`、`record_fatal_error`、`check_fatal_error`
   - 问题：原逻辑把 partial commit escalated error 写入 transient `flush_error`，下一次 `consume_flush_error` 会清空，读路径和 checkpoint 路径也不检查。
   - 修复：新增 sticky `fatal_error`，一旦记录，读、写、flush、scan、batch get、checkpoint 等边界均返回 `ForstError::Internal`，空 batch 也不绕过。
   - 回归：`test_fatal_consistency_error_is_sticky`。

3. L0/Ln compaction 内部 snapshot 污染 `min_active_snapshot`
   - 文件：`crates/forst-rs-engine/src/db.rs`
   - 函数：`compact_l0_for_cf`、`compact_level_for_cf`
   - 问题：compaction 在读取 `min_active` 前创建内部 snapshot，导致无外部 snapshot 时仍把最新版本视作 pinned，bottommost tombstone 无法清理，merge chain 无法折叠。
   - 修复：本轮 compaction retention horizon 仅取 caller-visible snapshots；新 snapshot 会读取已 compacted 的最新状态，不需要保留本轮 compaction 前的全部历史。
   - 验证：`cargo test -p forst-rs-engine l0 -- --nocapture` 12/12 通过。

4. Restore SST local path 可 path traversal
   - 文件：`flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/ForStRsRestoreOperation.java`
   - 函数：`resolveSafeRestoreLocalPath`、`parallelDownloadSsts`
   - 问题：`downloadDir.resolve(hlp.getLocalPath())` 信任 checkpoint localPath，恶意或损坏 handle 可写出 restore download 目录。
   - 修复：只允许单文件名，拒绝 null/blank、绝对路径、`.`、`..`、`/`、`\`，并 normalize 后检查 parent 仍为 downloadDir。
   - 回归：`restoreRejectsEscapingSstLocalPath`、`restoreAcceptsSingleSstFileName`。

5. Rescaling restore timer malformed row 静默跳过
   - 文件：`flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/ForStRsRestoreOperation.java`
   - 函数：`copyTimerRowsForSourceBatch`、`extractTimerRowKeyGroupOrThrow`
   - 问题：`q/` 前缀 timer key 缺少 separator/key-group/timestamp 时被 `continue`，会静默丢 timer。
   - 修复：malformed timer row fail-fast，由 restore 外层包装成 restore failure。
   - 回归：`timerRowKeyParserFailsFastOnMalformedQueueRows`、`timerRowKeyParserReadsEmbeddedKeyGroup`。

6. 同步 timer queue 构造器绕过 totalKeyGroups collision guard
   - 文件：`flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/ForStRsAbstractKeyedStateBackend.java`
   - 函数：`createInternalPriorityQueue`
   - 问题：生产同步路径使用 deprecated `IntSupplier` 构造器，绕过 `totalKeyGroups <= 28975` guard。
   - 修复：改为 `InternalKeyContext` 构造器并传入 `getNumberOfKeyGroups()`。

7. JDK 25 FFM full LSM read path 使用 critical downcall
   - 文件：`flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ffm/ForStRsLinker.java`
   - 函数：constructor bindings、`bindCritical`
   - 问题：`frs_get`、`frs_batch_get_arrow`、`frs_lookup_kv`、`frs_get_into_buf`、`frs_get_fast`、`frs_get_at` 可能触发 SST/S3/分配路径，不应阻塞 JVM safepoint。
   - 修复：上述 full read paths 改为 plain `bind`；仅保留 tiny non-blocking symbols 使用 `bindCritical`，注释同步更新。

8. JDK 25 FFM plain bind 后仍传入 heap `MemorySegment`
   - 文件：`flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ffm/ForStRsLinker.java`
   - 函数：`put`、`delete`、`getInternal`、`getIntoBuf`、`getFast`、`getPinnedSegment` fallback、`getAndPut`、`getAt`
   - 问题：Round 1 复审指出 full read 改为 plain bind 后仍传 heap segment；随后真实 `snapshotThenRestoreRoundTrip` 暴露 `frs_put threw: Heap segment not allowed`。
   - 修复：point write/read/delete/getAndPut/getAt 的 plain downcall 调用点全部改为 confined native arena staging；`frs_get_pinned` 仍保留 critical tiny probe。
   - 验证：`ForStRsRestoreOperationTest#snapshotThenRestoreRoundTrip` 与 restore helper 测试 6/6 通过。

9. Arrow batch 写入路径漏掉 sticky fatal
   - 文件：`crates/forst-rs-engine/src/db.rs`
   - 函数：`batch_put_arrow`
   - 问题：Round 1 复审发现 fatal 后 `batch_put_arrow` 仍可继续分配 sequence 并写 memtable。
   - 修复：`batch_put_arrow` 入口最前面调用 `check_fatal_error()`，空 batch 也不绕过。
   - 回归：`test_fatal_consistency_error_is_sticky` 增加非空/空 Arrow batch 断言。

10. Rescaling restore 中 timer marker `q/` 与 regular-state kg=28975 前缀碰撞
   - 文件：`flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/ForStRsRestoreOperation.java`
   - 函数：`restoreWithRescaling`、`rejectTimerPrefixCollisionOnRescale`
   - 问题：regular state key-group `0x712f` 的前两个字节等于 timer marker `q/`，rescaling restore 的 timer prefix scan 可能误扫普通状态。
   - 修复：短期 fail-fast：rescaling restore 若 source key-group range 包含 `0x712f`，直接拒绝并给出明确错误；no-rescaling fast path 不受影响。长期仍需版本化 timer key format 或独立 namespace。
   - 回归：`rescalingRestoreRejectsTimerPrefixCollisionKeyGroup`。

## 剩余 High / 结构性风险

1. 端到端 Arrow/批量化尚未闭环
   - Rust JNI/FFM 兼容路径仍存在 byte[]/Vec 物化，`batch_get`、部分 list append/merge 路径仍可能退化为 per-key RMW。
   - 需要单独设计：Java Async State batch -> Arrow C Data Interface -> Rust vectorized engine -> SST/S3 vectorized writer/reader 的统一路径。

2. Async V2 Reducing/Aggregating/List 的 same-key convoy 与 checkpoint barrier drain 仍需专项修复
   - Round 0 审查发现 V2 miss path、V1 async wrapper 全局 currentKey 保护、pending chain drain 存在一致性风险。
   - 本轮未动这些文件，避免在未完整建模 mailbox/checkpoint 语义前做局部不安全补丁。

3. FFM plain bind 后需要补充运行时 heap segment 压测
   - 编译和轻量测试通过，但仍需用真实 JDK 25 + native FFI 压测 `MemorySegment.ofArray`、native arena fast path、batch get Arrow path，确认 plain bind 在所有调用点的地址段生命周期安全。

4. Timer wire format 对历史非法 stateName 含 `/` 的 checkpoint 仍不可完全自描述恢复
   - 本轮保证 malformed row 不静默丢失；但旧 checkpoint 若 stateName 本身含 `/`，由于格式无 length-prefix，无法 100% 无歧义恢复。
   - 后续需要版本化 timer key format，或在 manifest 中持久化 timer state-name registry 进行校验。

5. L1+ CF-aware lookup 当前是线性扫描，影响点查/批查扩展性
   - Round 1 性能代理指出 `Version::find_sst_for_key_in_cf` 是 `O(N_level)`；这是正确性修复后的保守实现，但会拉低 large-level point lookup 与 `batch_get_arrow` miss 场景。
   - 后续需要 per-CF range index 或 per-CF sorted view，恢复二分查找。

## 验证结果

- `cargo test -p forst-rs-engine test_l0_newer_wide_range_shadows_older_narrow_range -- --nocapture`：通过。
- `cargo test -p forst-rs-engine test_multiple_l0_ssts_newest_wins -- --nocapture`：通过。
- `cargo test -p forst-rs-engine test_fatal_consistency_error_is_sticky -- --nocapture`：通过。
- `cargo test -p forst-rs-engine l0 -- --nocapture`：12/12 通过。
- `JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home ./mvnw -pl flink-state-backends/flink-statebackend-forst-rs -Pforst-rs-jdk25 ... compile`：通过。
- `JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home ./mvnw -pl flink-state-backends/flink-statebackend-forst-rs -Pforst-rs-jdk25 -Dtest=ForStRsRestoreOperationTest#snapshotThenRestoreRoundTrip+restoreRejectsEscapingSstLocalPath+restoreAcceptsSingleSstFileName+rescalingRestoreRejectsTimerPrefixCollisionKeyGroup+timerRowKeyParserFailsFastOnMalformedQueueRows+timerRowKeyParserReadsEmbeddedKeyGroup test`：6/6 通过。
- `git diff --check`：ForSt 与 flink 两个工作区均通过。

## Round 1 复审摘要

- Rust 正确性代理：确认 L0 point-get 与 compaction `min_active` 修复方向正确；新增 High 为 `batch_put_arrow` 漏 fatal，已修。
- Rust 性能代理：新增 High 为 L1+ CF-aware lookup 线性扫描；本轮列为结构性性能 High，未做仓促重构。
- Backend restore/timer 代理：新增 High 为 `q/` 与 kg=28975 restore 碰撞；已加 rescaling restore fail-fast。
- Backend JDK25/FFM 代理：新增 High 为 plain bind 仍传 heap segment；已改 native staging，并用真实 round-trip 验证。
- Async backend 代理：未返回新的可立即局部修复 High；Round 0 的 Async V2 convoy/checkpoint drain 仍列入结构性 High。
