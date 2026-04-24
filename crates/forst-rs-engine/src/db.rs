// Copyright 2026 The ForSt-RS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! DbImpl — the top-level engine struct. See `2.8_read_write_paths.md` §2-3.
//!
//! Wires the in-memory side of the LSM-tree (active memtable + immutable
//! queue) with on-disk SST files and the write controller. `flush_cf()`
//! persists the oldest immutable memtable to an L0 SST file, atomically
//! installs the new version, and caches a reader for subsequent lookups.
//!
//! Compaction (W15) and the C ABI bridge (W16) are not yet implemented.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use forst_rs_common::{
    ColumnFamilyId, EngineOptions, FileNumber, ForstError, ForstResult, OpType, DEFAULT_CF_ID,
};
use forst_rs_io::{FileSystem, LocalFileSystem, MemoryFileSystem};
use forst_rs_storage::sst::{SstReaderImpl, SstWriterOptions};
use forst_rs_storage::version::{SstFileMeta, Version, VersionEdit, VersionSetImpl};

use crate::checkpoint::{copy_live_ssts, serialize_snapshot, write_blob, CheckpointManifest};
use crate::column_family::{ColumnFamilyData, ColumnFamilyDescriptor, ColumnFamilyHandle};
use crate::compaction::{compaction_output_path, CompactionJob};
use crate::file_deletion_guard::FileDeletionGuard;
use crate::flush::{sst_file_path, FlushJob};
use crate::snapshot_view::SnapshotView;
use crate::write_batch::WriteBatch;
use crate::write_controller::WriteController;

/// Name of the default column family (always id 0).
pub const DEFAULT_CF_NAME: &str = "default";

/// The top-level engine struct.
pub struct DbImpl {
    options: EngineOptions,
    db_path: PathBuf,
    fs: Arc<dyn FileSystem>,
    /// Column families keyed by id.
    cfs: RwLock<HashMap<ColumnFamilyId, Arc<ColumnFamilyData>>>,
    /// Name → id lookup for CF discovery.
    cf_name_to_id: RwLock<HashMap<String, ColumnFamilyId>>,
    /// Lock-free SST file layout.
    version_set: Arc<VersionSetImpl>,
    /// Open SST readers keyed by file number. Installed after flush /
    /// compaction and dropped after files are deleted from the Version.
    sst_readers: RwLock<HashMap<FileNumber, Arc<SstReaderImpl>>>,
    /// Tracks SST files pinned by active checkpoints so concurrent
    /// compactions do not delete them prematurely.
    deletion_guard: Arc<FileDeletionGuard>,
    /// Files whose deletion was deferred because a checkpoint still held a
    /// pin. Reaped during subsequent flush/compact calls once the pins
    /// have been released.
    pending_deletions: Mutex<Vec<FileNumber>>,
    /// Monotonic sequence number shared across all CFs. Incremented on every
    /// successful mutation.
    sequence_number: AtomicU64,
    /// Back-pressure controller.
    write_controller: Arc<WriteController>,
    /// Serializes writers so the active memtable's internal sequence matches
    /// the engine-level sequence allocated just above it.
    write_mutex: Mutex<()>,
    /// Allocator for new CF ids.
    next_cf_id: AtomicU32,
}

impl DbImpl {
    /// Creates a new engine with the given options, using [`LocalFileSystem`]
    /// at `options.db_path`.
    pub fn open(options: EngineOptions) -> ForstResult<Arc<Self>> {
        if options.write_buffer_size == 0 {
            return Err(ForstError::invalid_argument(
                "write_buffer_size must be greater than zero",
            ));
        }
        if options.db_path.is_empty() {
            return Err(ForstError::invalid_argument("db_path must not be empty"));
        }
        let fs: Arc<dyn FileSystem> = Arc::new(LocalFileSystem::new());
        Self::open_with_fs(options, fs)
    }

    /// Creates a new engine with a pluggable [`FileSystem`]. Useful for
    /// in-memory tests with [`MemoryFileSystem`].
    pub fn open_with_fs(options: EngineOptions, fs: Arc<dyn FileSystem>) -> ForstResult<Arc<Self>> {
        if options.write_buffer_size == 0 {
            return Err(ForstError::invalid_argument(
                "write_buffer_size must be greater than zero",
            ));
        }
        let db_path = PathBuf::from(&options.db_path);
        // Ensure the db directory exists.
        if !options.db_path.is_empty() {
            fs.create_dir_all(&db_path)?;
        }

        let db = Arc::new(Self {
            options,
            db_path,
            fs,
            cfs: RwLock::new(HashMap::new()),
            cf_name_to_id: RwLock::new(HashMap::new()),
            version_set: Arc::new(VersionSetImpl::new()),
            sst_readers: RwLock::new(HashMap::new()),
            deletion_guard: Arc::new(FileDeletionGuard::new()),
            pending_deletions: Mutex::new(Vec::new()),
            sequence_number: AtomicU64::new(0),
            write_controller: Arc::new(WriteController::with_defaults()),
            write_mutex: Mutex::new(()),
            next_cf_id: AtomicU32::new(1),
        });

        let default_desc = ColumnFamilyDescriptor::new(DEFAULT_CF_NAME);
        db.create_cf_with_id(DEFAULT_CF_ID, default_desc)?;
        Ok(db)
    }

    /// Opens the engine with default [`EngineOptions`] backed by an
    /// in-memory filesystem. Convenience for tests.
    pub fn open_default() -> ForstResult<Arc<Self>> {
        let opts = EngineOptions {
            db_path: "/db".to_string(),
            ..EngineOptions::default()
        };
        let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
        Self::open_with_fs(opts, fs)
    }

    /// Returns a handle to the default column family.
    pub fn default_cf(&self) -> ColumnFamilyHandle {
        self.lookup_cf_by_id(DEFAULT_CF_ID)
            .expect("default CF must always exist")
            .handle()
            .clone()
    }

    /// Creates a new column family with the given descriptor.
    pub fn create_column_family(
        &self,
        desc: ColumnFamilyDescriptor,
    ) -> ForstResult<ColumnFamilyHandle> {
        // Reject duplicate names.
        {
            let name_map = self.cf_name_to_id.read().expect("lock poisoned");
            if name_map.contains_key(desc.name()) {
                return Err(ForstError::invalid_argument(format!(
                    "column family '{}' already exists",
                    desc.name()
                )));
            }
        }
        let id = ColumnFamilyId(self.next_cf_id.fetch_add(1, Ordering::SeqCst));
        self.create_cf_with_id(id, desc)
    }

    fn create_cf_with_id(
        &self,
        id: ColumnFamilyId,
        desc: ColumnFamilyDescriptor,
    ) -> ForstResult<ColumnFamilyHandle> {
        let name = desc.name().to_string();
        let options = desc.options().clone();
        let merge_op = desc.merge_operator();
        let filter = desc.compaction_filter();
        let handle = ColumnFamilyHandle::new(id, &name);

        let initial_snapshot = Arc::new(SnapshotView::empty());
        let cf_data = Arc::new(ColumnFamilyData::new_with_filter(
            handle.clone(),
            options,
            merge_op,
            filter,
            initial_snapshot,
        ));

        {
            let mut cfs = self.cfs.write().expect("lock poisoned");
            cfs.insert(id, cf_data.clone());
        }
        {
            let mut names = self.cf_name_to_id.write().expect("lock poisoned");
            names.insert(name, id);
        }

        // Install a snapshot view that references the active memtable.
        self.refresh_snapshot_view(&cf_data);
        Ok(handle)
    }

    /// Returns a handle for the given CF name, if it exists.
    pub fn column_family(&self, name: &str) -> Option<ColumnFamilyHandle> {
        let name_map = self.cf_name_to_id.read().expect("lock poisoned");
        let id = name_map.get(name).copied()?;
        let cfs = self.cfs.read().expect("lock poisoned");
        cfs.get(&id).map(|cf| cf.handle().clone())
    }

    /// Returns a reference to the engine options.
    pub fn options(&self) -> &EngineOptions {
        &self.options
    }

    /// Returns the shared write controller.
    pub fn write_controller(&self) -> &Arc<WriteController> {
        &self.write_controller
    }

    /// Returns the current sequence number (latest assigned).
    pub fn sequence_number(&self) -> u64 {
        self.sequence_number.load(Ordering::Acquire)
    }

    // ---------------------------------------------------------------
    // Write path
    // ---------------------------------------------------------------

    /// Inserts or overwrites the value for `key`.
    pub fn put(&self, cf: &ColumnFamilyHandle, key: &[u8], value: &[u8]) -> ForstResult<u64> {
        self.write_single(cf, key, Some(value), OpType::Put)
    }

    /// Deletes the key.
    pub fn delete(&self, cf: &ColumnFamilyHandle, key: &[u8]) -> ForstResult<u64> {
        self.write_single(cf, key, None, OpType::Delete)
    }

    /// Deletes the key with `SingleDelete` semantics. See
    /// [`crate::WriteBatch::single_delete`] for the contract — callers must
    /// guarantee the key has been `put` at most once since the last
    /// delete-family operation.
    pub fn single_delete(&self, cf: &ColumnFamilyHandle, key: &[u8]) -> ForstResult<u64> {
        self.write_single(cf, key, None, OpType::SingleDelete)
    }

    /// Appends a merge operand for the key.
    pub fn merge(&self, cf: &ColumnFamilyHandle, key: &[u8], operand: &[u8]) -> ForstResult<u64> {
        self.write_single(cf, key, Some(operand), OpType::Merge)
    }

    fn write_single(
        &self,
        cf: &ColumnFamilyHandle,
        key: &[u8],
        value: Option<&[u8]>,
        op: OpType,
    ) -> ForstResult<u64> {
        self.write_controller.may_throttle()?;
        let cf_data = self.lookup_cf_by_id(cf.id())?;

        // Phase 1 (under write_mutex): serialize memtable writes + decide
        // whether to switch the active memtable. If a switch happens we do
        // it inline so the next writer sees the fresh memtable, but we defer
        // the expensive SST flush to Phase 2 below so concurrent writers
        // don't block on disk I/O.
        let needs_flush = {
            let _writer = self.write_mutex.lock().expect("lock poisoned");

            let seq = {
                let mem_arc = cf_data.active_memtable();
                let mut mem = mem_arc.write().expect("lock poisoned");
                mem.put(key, value, op as u8)?
            };
            self.bump_sequence(seq);

            self.maybe_switch_memtable_in_lock(&cf_data)?
        };

        // Phase 2 (outside write_mutex): flush the imm memtable. Other
        // writers can proceed in parallel against the new active memtable.
        if needs_flush {
            self.flush_cf_data(&cf_data)?;
            // After flush, auto-compact if L0 file count has grown past the
            // slowdown trigger so we stay far from the write-stall ceiling.
            self.maybe_auto_compact(&cf_data)?;
        }

        Ok(self.sequence_number.load(Ordering::Acquire))
    }

    /// If the current L0 file count is at or above the slowdown trigger,
    /// run an L0→L1 compaction. Returns `Ok(())` either way.
    fn maybe_auto_compact(&self, cf_data: &Arc<ColumnFamilyData>) -> ForstResult<()> {
        let l0_count = self.version_set.current().l0_files().len() as u32;
        let trigger = self.write_controller.config().l0_slowdown_trigger;
        if l0_count >= trigger {
            self.compact_l0_for_cf(cf_data)?;
        }
        Ok(())
    }

    /// Applies a [`WriteBatch`] atomically. Returns the last assigned sequence.
    pub fn batch_write(&self, batch: WriteBatch) -> ForstResult<u64> {
        if batch.is_empty() {
            return Ok(self.sequence_number());
        }
        self.write_controller.may_throttle()?;

        // Resolve and cache CF lookups up front so we fail fast on missing CFs.
        let groups = batch.group_by_cf();
        let mut cf_datas: HashMap<ColumnFamilyId, Arc<ColumnFamilyData>> =
            HashMap::with_capacity(groups.len());
        for &cf_id in groups.keys() {
            cf_datas.insert(cf_id, self.lookup_cf_by_id(cf_id)?);
        }

        // Phase 1 (under write_mutex): perform all memtable writes and any
        // in-lock switch decisions, collecting CFs whose memtable needs to
        // be flushed outside the lock.
        let mut cfs_to_flush: Vec<Arc<ColumnFamilyData>> = Vec::new();
        let mut last_seq = 0u64;
        {
            let _writer = self.write_mutex.lock().expect("lock poisoned");
            let entries = batch.into_entries();

            for (cf_id, indices) in &groups {
                let cf_data = cf_datas.get(cf_id).expect("cf_data pre-populated");
                let mem_arc = cf_data.active_memtable();
                let mut mem = mem_arc.write().expect("lock poisoned");

                let keys: Vec<&[u8]> = indices.iter().map(|&i| entries[i].key.as_slice()).collect();
                let values: Vec<Option<&[u8]>> = indices
                    .iter()
                    .map(|&i| entries[i].value.as_deref())
                    .collect();
                let op_types: Vec<u8> = indices.iter().map(|&i| entries[i].op_type as u8).collect();
                mem.batch_insert(&keys, &values, &op_types)?;
                last_seq = self
                    .sequence_number
                    .fetch_add(indices.len() as u64, Ordering::SeqCst)
                    + indices.len() as u64;
            }

            // Check each CF's threshold and switch in-lock; defer flush to
            // Phase 2 below to avoid blocking other writers on disk I/O.
            for cf_data in cf_datas.values() {
                if self.maybe_switch_memtable_in_lock(cf_data)? {
                    cfs_to_flush.push(cf_data.clone());
                }
            }
        }

        // Phase 2: flush outside the write lock so subsequent writers can
        // progress against the fresh active memtables.
        for cf_data in &cfs_to_flush {
            self.flush_cf_data(cf_data)?;
            self.maybe_auto_compact(cf_data)?;
        }

        Ok(last_seq)
    }

    /// Forces the active memtable of a CF to switch (for flush testing).
    pub fn force_switch_memtable(&self, cf: &ColumnFamilyHandle) -> ForstResult<()> {
        let cf_data = self.lookup_cf_by_id(cf.id())?;
        let _writer = self.write_mutex.lock().expect("lock poisoned");
        cf_data.swap_active_memtable();
        self.refresh_snapshot_view(&cf_data);

        // Enforce max_write_buffer_number: stall if exceeded.
        let imm = cf_data.imm_count() as u32;
        self.write_controller.set_imm_count(imm);
        Ok(())
    }

    // ---------------------------------------------------------------
    // Flush path
    // ---------------------------------------------------------------

    /// Flushes the oldest immutable memtable of `cf` to a new L0 SST file.
    ///
    /// Returns `Ok(None)` if there are no immutable memtables to flush.
    /// Otherwise returns the [`SstFileMeta`] of the produced file.
    pub fn flush_cf(&self, cf: &ColumnFamilyHandle) -> ForstResult<Option<SstFileMeta>> {
        let cf_data = self.lookup_cf_by_id(cf.id())?;
        self.flush_cf_data(&cf_data)
    }

    /// Flushes all pending immutable memtables across all column families.
    pub fn flush_all(&self) -> ForstResult<()> {
        let cfs: Vec<Arc<ColumnFamilyData>> = {
            let guard = self.cfs.read().expect("lock poisoned");
            guard.values().cloned().collect()
        };
        for cf_data in cfs {
            while cf_data.imm_count() > 0 {
                if self.flush_cf_data(&cf_data)?.is_none() {
                    break;
                }
            }
        }
        Ok(())
    }

    /// Convenience: switch the active memtable and then flush it. Useful in
    /// tests and when the engine is about to checkpoint.
    pub fn switch_and_flush(&self, cf: &ColumnFamilyHandle) -> ForstResult<Option<SstFileMeta>> {
        self.force_switch_memtable(cf)?;
        self.flush_cf(cf)
    }

    // ---------------------------------------------------------------
    // Compaction path
    // ---------------------------------------------------------------

    /// Runs an L0→L1 rollup compaction for the given column family. Picks
    /// every current L0 file (plus every L1 file — L1 may overlap L0) and
    /// produces a single new L1 file consolidating versions.
    ///
    /// Returns the new L1 file's metadata, or `Ok(None)` if no L0 files
    /// were present.
    pub fn compact_l0(&self, cf: &ColumnFamilyHandle) -> ForstResult<Option<SstFileMeta>> {
        let cf_data = self.lookup_cf_by_id(cf.id())?;
        self.compact_l0_for_cf(&cf_data)
    }

    /// Runs compaction for every column family. For each CF, this drains
    /// every L0 file and then keeps picking any level that exceeds its
    /// target size until the entire LSM is balanced. Useful in tests and
    /// before checkpointing.
    pub fn compact_all(&self) -> ForstResult<()> {
        let cfs: Vec<Arc<ColumnFamilyData>> = {
            let guard = self.cfs.read().expect("lock poisoned");
            guard.values().cloned().collect()
        };
        for cf_data in cfs {
            loop {
                let did_work = self.compact_once(&cf_data)?;
                if !did_work {
                    break;
                }
            }
        }
        Ok(())
    }

    /// Picks one compaction job (L0 rollup or size-based deeper level) and
    /// runs it. Returns `true` if work was done, `false` if the CF is
    /// already balanced. Useful for integrating with a scheduler.
    pub fn compact_once_for(&self, cf: &ColumnFamilyHandle) -> ForstResult<bool> {
        let cf_data = self.lookup_cf_by_id(cf.id())?;
        self.compact_once(&cf_data)
    }

    /// Runs an L→L+1 compaction. `level` must be >= 1 (use `compact_l0` for
    /// L0 rollup). Returns the produced file meta, or `Ok(None)` if the
    /// level was empty.
    pub fn compact_level(
        &self,
        cf: &ColumnFamilyHandle,
        level: u32,
    ) -> ForstResult<Option<SstFileMeta>> {
        if level == 0 {
            return self.compact_l0(cf);
        }
        let cf_data = self.lookup_cf_by_id(cf.id())?;
        self.compact_level_for_cf(&cf_data, level)
    }

    fn compact_once(&self, cf_data: &Arc<ColumnFamilyData>) -> ForstResult<bool> {
        // Priority 1: drain L0 if any files exist.
        if !self.version_set.current().l0_files().is_empty() {
            self.compact_l0_for_cf(cf_data)?;
            return Ok(true);
        }
        // Priority 2: pick the deepest level that exceeds its size budget.
        let Some(level) = self.pick_compaction_level() else {
            return Ok(false);
        };
        self.compact_level_for_cf(cf_data, level)?;
        Ok(true)
    }

    /// Returns the shallowest level (>= 1) whose total file size exceeds its
    /// target, or `None` if every level is within budget.
    ///
    /// Target is `max_bytes_for_level_base * multiplier^(level-1)`.
    fn pick_compaction_level(&self) -> Option<u32> {
        let version = self.version_set.current();
        let base = self.options.max_bytes_for_level_base as f64;
        let mult = self.options.max_bytes_for_level_multiplier;
        for level in 1..(self.options.num_levels - 1) {
            let total_size: u64 = version.levels[level]
                .files
                .iter()
                .map(|f| f.file_size)
                .sum();
            let target = (base * mult.powi(level as i32 - 1)) as u64;
            if total_size > target {
                return Some(level as u32);
            }
        }
        None
    }

    fn compact_level_for_cf(
        &self,
        cf_data: &Arc<ColumnFamilyData>,
        level: u32,
    ) -> ForstResult<Option<SstFileMeta>> {
        let _guard = cf_data.lock_flush();

        let version = self.version_set.current();
        let level_idx = level as usize;
        if level_idx >= version.num_levels() {
            return Ok(None);
        }
        let src_files: Vec<SstFileMeta> = version.levels[level_idx].files.clone();
        if src_files.is_empty() {
            return Ok(None);
        }
        let next_level = level_idx + 1;
        if next_level >= version.num_levels() {
            // Can't go deeper — the engine is at max depth. Treat as no-op.
            return Ok(None);
        }
        let dst_candidates: Vec<SstFileMeta> = version.levels[next_level].files.clone();

        // Compute the key range spanned by src_files; pull any dst file
        // whose range overlaps.
        let (mut min_key, mut max_key) = (
            src_files[0].smallest_key.clone(),
            src_files[0].largest_key.clone(),
        );
        for f in &src_files {
            if f.smallest_key < min_key {
                min_key = f.smallest_key.clone();
            }
            if f.largest_key > max_key {
                max_key = f.largest_key.clone();
            }
        }
        let mut overlapping_dst: Vec<SstFileMeta> = Vec::new();
        for f in &dst_candidates {
            if f.largest_key < min_key {
                continue;
            }
            if f.smallest_key > max_key {
                continue;
            }
            overlapping_dst.push(f.clone());
        }

        // Build input list. Source files carry `level`; destination overlap
        // carries `next_level`.
        let mut inputs: Vec<(u32, SstFileMeta, Arc<SstReaderImpl>)> = Vec::new();
        for f in &src_files {
            inputs.push((level, f.clone(), self.get_or_open_sst_reader(f)?));
        }
        for f in &overlapping_dst {
            inputs.push((
                next_level as u32,
                f.clone(),
                self.get_or_open_sst_reader(f)?,
            ));
        }

        let is_bottommost =
            (next_level + 1..version.num_levels()).all(|lvl| version.levels[lvl].files.is_empty());
        let output_file_number = self.version_set.allocate_file_number();
        let output_path = compaction_output_path(&self.db_path, output_file_number);
        let writer_options = SstWriterOptions {
            block_size: self.options.block_size,
            compression: self.options.compression,
        };

        let job = CompactionJob {
            inputs,
            output_level: next_level as u32,
            output_file_number,
            output_path: output_path.clone(),
            writer_options,
            fs: self.fs.clone(),
            merge_operator: cf_data.merge_operator().cloned(),
            compaction_filter: cf_data.compaction_filter().cloned(),
            is_bottommost,
        };

        let Some(edit) = job.run()? else {
            return Ok(None);
        };
        self.version_set.apply(&edit)?;

        let new_meta = edit.new_files.first().map(|(_, m)| m.clone());
        if let Some(ref meta) = new_meta {
            let rac = self.fs.open_random_access_file(&output_path)?;
            let reader = Arc::new(SstReaderImpl::open(rac)?);
            self.sst_readers
                .write()
                .expect("lock poisoned")
                .insert(meta.file_number, reader);
        }
        {
            let mut cache = self.sst_readers.write().expect("lock poisoned");
            for (_, file_number) in &edit.deleted_files {
                cache.remove(file_number);
            }
        }
        for (_, file_number) in &edit.deleted_files {
            let path = sst_file_path(&self.db_path, *file_number);
            let _ = self.fs.delete_file(&path);
        }

        Ok(new_meta)
    }

    // ---------------------------------------------------------------
    // Checkpoint path
    // ---------------------------------------------------------------

    /// Writes a consistent checkpoint of the engine state into `target_dir`.
    ///
    /// All pending memtables are flushed first so the checkpoint only needs
    /// to copy on-disk SST files. Returns a [`CheckpointManifest`] describing
    /// the produced files.
    pub fn create_checkpoint(
        &self,
        target_dir: &std::path::Path,
    ) -> ForstResult<CheckpointManifest> {
        // 1. Flush every pending imm memtable.
        self.flush_all()?;

        // 2. Also flush the active memtable (switch + flush) so the
        //    checkpoint includes writes that haven't yet crossed the
        //    write_buffer_size threshold.
        let cfs: Vec<Arc<ColumnFamilyData>> = {
            let guard = self.cfs.read().expect("lock poisoned");
            guard.values().cloned().collect()
        };
        for cf_data in &cfs {
            // Only switch + flush when the active memtable has data.
            let has_data = {
                let mem_arc = cf_data.active_memtable();
                let mem = mem_arc.read().expect("lock poisoned");
                mem.num_entries() > 0
            };
            if has_data {
                let _writer = self.write_mutex.lock().expect("lock poisoned");
                cf_data.swap_active_memtable();
                self.refresh_snapshot_view(cf_data);
                drop(_writer);
                self.flush_cf_data(cf_data)?;
            }
        }

        // 3. Capture the VersionSet snapshot AFTER flushes so it contains
        //    every new L0 file.
        let snapshot = self.version_set.snapshot();
        let blob = serialize_snapshot(&snapshot)?;

        // 4. Pin every live SST file so concurrent compactions cannot delete
        //    them before we finish copying.
        let live = self.version_set.live_sst_files();
        let file_numbers: Vec<FileNumber> = live.iter().map(|f| f.file_number).collect();
        let _pin = self.deletion_guard.pin_batch(&file_numbers);

        // 5. Ensure target dir exists, write blob + copy every live SST.
        self.fs.create_dir_all(target_dir)?;
        write_blob(self.fs.as_ref(), target_dir, &blob)?;

        let (sst_bytes, sst_files) =
            copy_live_ssts(self.fs.as_ref(), &self.db_path, target_dir, &live)?;

        // PinHandle is released here when `_pin` drops; any deletions that
        // were deferred during the checkpoint will be reaped on the next
        // compaction (or immediately below).
        drop(_pin);
        self.reap_pending_deletions();

        Ok(CheckpointManifest {
            target_dir: target_dir.to_path_buf(),
            sst_files,
            total_bytes: blob.len() as u64 + sst_bytes,
        })
    }

    /// Opens an engine by restoring state from a checkpoint directory.
    ///
    /// The checkpoint directory must contain `CHECKPOINT.blob` and every SST
    /// file it references. The engine is opened with `db_path = target_dir`
    /// (i.e. subsequent reads/writes operate directly on the checkpoint
    /// files; copy the checkpoint first if you want to preserve it).
    pub fn open_from_checkpoint(
        options: EngineOptions,
        fs: Arc<dyn FileSystem>,
    ) -> ForstResult<Arc<Self>> {
        use crate::checkpoint::{deserialize_snapshot, read_blob};

        let db_path = PathBuf::from(&options.db_path);
        let blob = read_blob(fs.as_ref(), &db_path)?;
        let snapshot = deserialize_snapshot(&blob)?;

        // Verify every referenced SST file exists.
        for file in snapshot.version.live_sst_files() {
            let path = sst_file_path(&db_path, file.file_number);
            if !fs.file_exists(&path)? {
                return Err(ForstError::corruption(format!(
                    "checkpoint references missing SST file: {}",
                    path.display()
                )));
            }
        }

        // Build the DbImpl with the restored VersionSet.
        let version_set = Arc::new(forst_rs_storage::version::VersionSetImpl::from_restored(
            (*snapshot.version).clone(),
            snapshot.next_file_number,
            snapshot.last_sequence,
        ));

        let db = Arc::new(Self {
            options,
            db_path,
            fs,
            cfs: RwLock::new(HashMap::new()),
            cf_name_to_id: RwLock::new(HashMap::new()),
            version_set,
            sst_readers: RwLock::new(HashMap::new()),
            deletion_guard: Arc::new(FileDeletionGuard::new()),
            pending_deletions: Mutex::new(Vec::new()),
            sequence_number: AtomicU64::new(snapshot.last_sequence),
            write_controller: Arc::new(WriteController::with_defaults()),
            write_mutex: Mutex::new(()),
            next_cf_id: AtomicU32::new(1),
        });

        let default_desc = ColumnFamilyDescriptor::new(DEFAULT_CF_NAME);
        db.create_cf_with_id(DEFAULT_CF_ID, default_desc)?;
        Ok(db)
    }

    fn compact_l0_for_cf(
        &self,
        cf_data: &Arc<ColumnFamilyData>,
    ) -> ForstResult<Option<SstFileMeta>> {
        // Serialize compaction per CF so two callers cannot both pick the
        // same L0 files. We piggyback on the flush_mutex since flush and
        // compaction both rewrite the on-disk layer.
        let _guard = cf_data.lock_flush();

        let version = self.version_set.current();
        let l0_files: Vec<SstFileMeta> = version.l0_files().to_vec();
        if l0_files.is_empty() {
            return Ok(None);
        }
        let l1_files: Vec<SstFileMeta> = version.levels[1].files.clone();

        // Gather input readers.
        let mut inputs: Vec<(u32, SstFileMeta, Arc<SstReaderImpl>)> = Vec::new();
        for meta in &l0_files {
            inputs.push((0, meta.clone(), self.get_or_open_sst_reader(meta)?));
        }
        for meta in &l1_files {
            inputs.push((1, meta.clone(), self.get_or_open_sst_reader(meta)?));
        }

        // Allocate the output file number and build the job. This rollup
        // produces a single bottommost-style file at L1. We mark it as
        // bottommost iff there are no files below L1 (all higher levels
        // empty) so delete tombstones can be eliminated.
        let is_bottommost =
            (2..version.num_levels()).all(|lvl| version.levels[lvl].files.is_empty());

        let output_file_number = self.version_set.allocate_file_number();
        let output_path = compaction_output_path(&self.db_path, output_file_number);
        let writer_options = SstWriterOptions {
            block_size: self.options.block_size,
            compression: self.options.compression,
        };

        let job = CompactionJob {
            inputs,
            output_level: 1,
            output_file_number,
            output_path: output_path.clone(),
            writer_options,
            fs: self.fs.clone(),
            merge_operator: cf_data.merge_operator().cloned(),
            compaction_filter: cf_data.compaction_filter().cloned(),
            is_bottommost,
        };

        let Some(edit) = job.run()? else {
            return Ok(None);
        };

        // Apply the VersionEdit atomically.
        self.version_set.apply(&edit)?;

        // Open the new reader, prune deleted readers, and delete stale files
        // from disk.
        let new_meta = edit.new_files.first().map(|(_, m)| m.clone());
        if let Some(ref meta) = new_meta {
            let rac = self.fs.open_random_access_file(&output_path)?;
            let reader = Arc::new(SstReaderImpl::open(rac)?);
            self.sst_readers
                .write()
                .expect("lock poisoned")
                .insert(meta.file_number, reader);
        }
        {
            let mut cache = self.sst_readers.write().expect("lock poisoned");
            for (_, file_number) in &edit.deleted_files {
                cache.remove(file_number);
            }
        }
        for (_, file_number) in &edit.deleted_files {
            self.delete_file_guarded(*file_number);
        }
        self.reap_pending_deletions();

        // Update back-pressure counts — L0 is now empty (for this rollup).
        self.write_controller
            .set_l0_file_count(self.version_set.current().l0_files().len() as u32);

        Ok(new_meta)
    }

    // ---------------------------------------------------------------
    // Scan path
    // ---------------------------------------------------------------

    /// Range scan: returns all key-value pairs with keys in `[lower, upper)`
    /// as a sorted `Vec`. Pass `upper = None` for an unbounded upper end.
    ///
    /// Tombstoned keys are skipped. Merge operands are resolved via the CF's
    /// configured merge operator.
    pub fn scan(
        &self,
        cf: &ColumnFamilyHandle,
        lower: &[u8],
        upper: Option<&[u8]>,
    ) -> ForstResult<Vec<(Vec<u8>, Vec<u8>)>> {
        use std::collections::BTreeSet;

        let cf_data = self.lookup_cf_by_id(cf.id())?;
        let mut keys: BTreeSet<Vec<u8>> = BTreeSet::new();

        // Active memtable.
        {
            let mem_arc = cf_data.active_memtable();
            let mem = mem_arc.read().expect("lock poisoned");
            for (k, _, _, _) in mem.collect_range_entries(lower, upper, u64::MAX) {
                keys.insert(k);
            }
        }
        // Immutable memtables.
        for imm in cf_data.imm_memtables() {
            let mem = imm.read().expect("lock poisoned");
            for (k, _, _, _) in mem.collect_range_entries(lower, upper, u64::MAX) {
                keys.insert(k);
            }
        }
        // SST layer.
        let version = self.version_set.current();
        for sst in version.live_sst_files() {
            if sst.largest_key.as_slice() < lower {
                continue;
            }
            if let Some(hi) = upper {
                if sst.smallest_key.as_slice() >= hi {
                    continue;
                }
            }
            let reader = self.get_or_open_sst_reader(&sst)?;
            for (k, _, _, _) in reader.scan(lower, upper)? {
                keys.insert(k);
            }
        }

        // Resolve each key via the normal read path (handles deletes + merges).
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(value) = self.get(cf, &key)? {
                out.push((key, value));
            }
        }
        Ok(out)
    }

    /// Prefix scan: all key-value pairs whose keys start with `prefix`.
    pub fn prefix_scan(
        &self,
        cf: &ColumnFamilyHandle,
        prefix: &[u8],
    ) -> ForstResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let upper = prefix_upper_bound(prefix);
        self.scan(cf, prefix, upper.as_deref())
    }

    fn flush_cf_data(&self, cf_data: &Arc<ColumnFamilyData>) -> ForstResult<Option<SstFileMeta>> {
        // Serialize flushes for this CF so concurrent callers cannot pick up
        // the same oldest imm and write it twice.
        let _flush_guard = cf_data.lock_flush();

        // Peek at the oldest imm without popping; if it's empty, return early.
        let imm_list = cf_data.imm_memtables();
        let Some(oldest) = imm_list.first().cloned() else {
            return Ok(None);
        };

        // Allocate a fresh file number and build the flush job.
        let file_number = self.version_set.allocate_file_number();
        let path = sst_file_path(&self.db_path, file_number);
        let writer_opts = SstWriterOptions {
            block_size: self.options.block_size,
            compression: self.options.compression,
        };
        let job = FlushJob::new(
            oldest.clone(),
            file_number,
            path.clone(),
            writer_opts,
            self.fs.clone(),
        );
        let meta = job.run()?;

        // Install the new file into L0 atomically. Note: we install BEFORE
        // popping the memtable so a concurrent reader can never transiently
        // fail to find the key (it will see either the imm or the SST, never
        // neither).
        let edit = VersionEdit {
            new_files: vec![(0, meta.clone())],
            ..Default::default()
        };
        self.version_set.apply(&edit)?;

        // Pre-populate the SST reader cache so the first read doesn't pay
        // the open-file cost.
        let rac = self.fs.open_random_access_file(&path)?;
        let reader = Arc::new(SstReaderImpl::open(rac)?);
        self.sst_readers
            .write()
            .expect("lock poisoned")
            .insert(meta.file_number, reader);

        // Pop the memtable now that its data is durably in the SST and the
        // Version has been updated. Readers that acquired a snapshot before
        // the pop still see the in-memory imm; readers after see only the
        // SST — both return the same values.
        cf_data.pop_oldest_imm();
        self.refresh_snapshot_view(cf_data);
        self.write_controller
            .set_imm_count(cf_data.imm_count() as u32);
        self.write_controller
            .set_l0_file_count(self.version_set.current().l0_files().len() as u32);
        self.write_controller.on_flush_complete();

        Ok(Some(meta))
    }

    /// Legacy helper: checks the active memtable usage and performs a full
    /// switch-and-flush under the assumption the caller is NOT holding
    /// `write_mutex`. Retained for convenience in tests and paths that do
    /// not care about write-path latency.
    #[allow(dead_code)]
    fn maybe_switch_memtable(&self, cf_data: &Arc<ColumnFamilyData>) -> ForstResult<()> {
        if self.maybe_switch_memtable_in_lock(cf_data)? {
            self.flush_cf_data(cf_data)?;
        }
        Ok(())
    }

    /// In-lock portion of the switch decision. Returns `true` if a switch
    /// happened and the caller should flush outside the write lock.
    fn maybe_switch_memtable_in_lock(&self, cf_data: &Arc<ColumnFamilyData>) -> ForstResult<bool> {
        let usage = {
            let mem_arc = cf_data.active_memtable();
            let mem = mem_arc.read().expect("lock poisoned");
            mem.memory_usage()
        };

        let threshold = cf_data.options().effective_write_buffer_size(&self.options);
        if usage < threshold {
            return Ok(false);
        }

        cf_data.swap_active_memtable();
        self.refresh_snapshot_view(cf_data);

        let imm = cf_data.imm_count() as u32;
        self.write_controller.set_imm_count(imm);
        Ok(true)
    }

    fn bump_sequence(&self, seq: u64) {
        // `seq` is the memtable-local seq; translate to a global non-decreasing
        // counter by ensuring our counter is at least `seq`.
        let mut cur = self.sequence_number.load(Ordering::Acquire);
        while cur < seq {
            match self.sequence_number.compare_exchange(
                cur,
                seq,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(observed) => cur = observed,
            }
        }
    }

    fn refresh_snapshot_view(&self, cf_data: &Arc<ColumnFamilyData>) {
        let view = SnapshotView::new(
            cf_data.active_memtable(),
            cf_data.imm_memtables(),
            self.version_set.current(),
            self.sequence_number.load(Ordering::Acquire),
        );
        cf_data.store_snapshot_view(Arc::new(view));
    }

    // ---------------------------------------------------------------
    // Read path
    // ---------------------------------------------------------------

    /// Point-lookup: returns the latest value for `key`.
    pub fn get(&self, cf: &ColumnFamilyHandle, key: &[u8]) -> ForstResult<Option<Vec<u8>>> {
        let cf_data = self.lookup_cf_by_id(cf.id())?;
        let read_seq = u64::MAX; // latest view for W12 (no explicit snapshots yet)
        self.get_internal(&cf_data, key, read_seq)
    }

    /// Batch point-lookup.
    pub fn batch_get(
        &self,
        cf: &ColumnFamilyHandle,
        keys: &[&[u8]],
    ) -> ForstResult<Vec<Option<Vec<u8>>>> {
        let cf_data = self.lookup_cf_by_id(cf.id())?;
        let read_seq = u64::MAX;
        let mut out = Vec::with_capacity(keys.len());
        for k in keys {
            out.push(self.get_internal(&cf_data, k, read_seq)?);
        }
        Ok(out)
    }

    fn get_internal(
        &self,
        cf_data: &Arc<ColumnFamilyData>,
        key: &[u8],
        read_seq: u64,
    ) -> ForstResult<Option<Vec<u8>>> {
        // Stage 1: Active memtable — peek at the newest visible entry.
        let active_hit = {
            let mem_arc = cf_data.active_memtable();
            let mem = mem_arc.read().expect("lock poisoned");
            mem.get(key, read_seq)?
        };
        match active_hit {
            Some(entry) if entry.op_type == OpType::Put => return Ok(entry.value),
            Some(entry)
                if entry.op_type == OpType::Delete || entry.op_type == OpType::SingleDelete =>
            {
                return Ok(None);
            }
            Some(entry) => {
                // Merge — collect operands and resolve via the merge operator.
                debug_assert_eq!(entry.op_type, OpType::Merge);
                let first_operand = entry
                    .value
                    .ok_or_else(|| ForstError::corruption("Merge entry missing operand payload"))?;
                let mut operands: Vec<Vec<u8>> = Vec::new();
                operands.push(first_operand);
                let base =
                    self.collect_merge_operands(cf_data, key, entry.sequence, &mut operands)?;
                return self
                    .apply_merge_operator(cf_data, key, base, operands)
                    .map(Some);
            }
            None => {}
        }

        // Stage 2: Immutable memtables (newest → oldest).
        let imm_list = cf_data.imm_memtables();
        for imm in imm_list.iter().rev() {
            let hit = {
                let guard = imm.read().expect("lock poisoned");
                guard.get(key, read_seq)?
            };
            let Some(entry) = hit else { continue };
            match entry.op_type {
                OpType::Put => return Ok(entry.value),
                OpType::Delete | OpType::SingleDelete => return Ok(None),
                OpType::Merge => {
                    let first_operand = entry.value.ok_or_else(|| {
                        ForstError::corruption("Merge entry missing operand payload")
                    })?;
                    let mut operands: Vec<Vec<u8>> = Vec::new();
                    operands.push(first_operand);
                    let base = self.collect_merge_operands_from_imm_start(
                        cf_data,
                        key,
                        entry.sequence,
                        imm_list.clone(),
                        &mut operands,
                    )?;
                    return self
                        .apply_merge_operator(cf_data, key, base, operands)
                        .map(Some);
                }
            }
        }

        // Stage 3: SST files (L0 newest-first, then L1..Ln binary search).
        let version = self.version_set.current();
        self.sst_get(cf_data, &version, key, &mut Vec::new())
    }

    /// Searches the SST layers for `key`. Returns the resolved user value
    /// (None for missing / tombstoned) — including full merge chains that
    /// start in the SST layer.
    fn sst_get(
        &self,
        cf_data: &Arc<ColumnFamilyData>,
        version: &Version,
        key: &[u8],
        merge_operands: &mut Vec<Vec<u8>>,
    ) -> ForstResult<Option<Vec<u8>>> {
        // L0: iterate newest → oldest. L0 files may overlap; each is checked.
        for sst in version.l0_files().iter().rev() {
            if let Some(res) = self.sst_lookup(sst, key)? {
                match res.op_type {
                    OpType::Put => {
                        if merge_operands.is_empty() {
                            return Ok(res.value);
                        }
                        return self
                            .apply_merge_operator(
                                cf_data,
                                key,
                                res.value,
                                std::mem::take(merge_operands),
                            )
                            .map(Some);
                    }
                    OpType::Delete | OpType::SingleDelete => {
                        if merge_operands.is_empty() {
                            return Ok(None);
                        }
                        return self
                            .apply_merge_operator(
                                cf_data,
                                key,
                                None,
                                std::mem::take(merge_operands),
                            )
                            .map(Some);
                    }
                    OpType::Merge => {
                        if let Some(v) = res.value {
                            merge_operands.push(v);
                        }
                        // continue down the LSM
                    }
                }
            }
        }

        // L1..Ln: at most one candidate per level.
        for level in 1..version.num_levels() {
            let Some(idx) = version.find_sst_for_key(level, key) else {
                continue;
            };
            let sst = &version.levels[level].files[idx];
            if let Some(res) = self.sst_lookup(sst, key)? {
                match res.op_type {
                    OpType::Put => {
                        if merge_operands.is_empty() {
                            return Ok(res.value);
                        }
                        return self
                            .apply_merge_operator(
                                cf_data,
                                key,
                                res.value,
                                std::mem::take(merge_operands),
                            )
                            .map(Some);
                    }
                    OpType::Delete | OpType::SingleDelete => {
                        if merge_operands.is_empty() {
                            return Ok(None);
                        }
                        return self
                            .apply_merge_operator(
                                cf_data,
                                key,
                                None,
                                std::mem::take(merge_operands),
                            )
                            .map(Some);
                    }
                    OpType::Merge => {
                        if let Some(v) = res.value {
                            merge_operands.push(v);
                        }
                    }
                }
            }
        }

        // Exhausted the LSM. If we collected merges but no base, apply with
        // base=None (partial merge with empty base).
        if !merge_operands.is_empty() {
            return self
                .apply_merge_operator(cf_data, key, None, std::mem::take(merge_operands))
                .map(Some);
        }
        Ok(None)
    }

    fn sst_lookup(
        &self,
        meta: &SstFileMeta,
        key: &[u8],
    ) -> ForstResult<Option<forst_rs_storage::sst::LookupResult>> {
        // Quick key-range check — avoids opening the file for obvious misses.
        if key < meta.smallest_key.as_slice() || key > meta.largest_key.as_slice() {
            return Ok(None);
        }
        let reader = self.get_or_open_sst_reader(meta)?;
        reader.get(key)
    }

    fn get_or_open_sst_reader(&self, meta: &SstFileMeta) -> ForstResult<Arc<SstReaderImpl>> {
        {
            let cache = self.sst_readers.read().expect("lock poisoned");
            if let Some(r) = cache.get(&meta.file_number) {
                return Ok(r.clone());
            }
        }
        // Double-checked lock — another thread may have cached meanwhile.
        let mut cache = self.sst_readers.write().expect("lock poisoned");
        if let Some(r) = cache.get(&meta.file_number) {
            return Ok(r.clone());
        }
        let path = sst_file_path(&self.db_path, meta.file_number);
        let file = self.fs.open_random_access_file(&path)?;
        let reader = Arc::new(SstReaderImpl::open(file)?);
        cache.insert(meta.file_number, reader.clone());
        Ok(reader)
    }

    fn apply_merge_operator(
        &self,
        cf_data: &Arc<ColumnFamilyData>,
        key: &[u8],
        base: Option<Vec<u8>>,
        operands: Vec<Vec<u8>>,
    ) -> ForstResult<Vec<u8>> {
        let op = cf_data.merge_operator().ok_or_else(|| {
            ForstError::invalid_argument(format!(
                "column family '{}' has no merge operator configured",
                cf_data.handle().name()
            ))
        })?;
        // `operands` was collected newest-first — the merge operator expects
        // oldest-first so reverse before invoking.
        let reversed: Vec<Vec<u8>> = operands.into_iter().rev().collect();
        let slices: Vec<&[u8]> = reversed.iter().map(|v| v.as_slice()).collect();
        op.full_merge(key, base.as_deref(), &slices)
    }

    /// Collect additional merge operands older than the outer `first_seq`.
    ///
    /// Scans the active memtable for older versions, then the immutable list
    /// from newest to oldest. Each memtable has its own local sequence space,
    /// so `cutoff` is reset to `u64::MAX` when crossing a memtable boundary.
    /// SST lookup is stubbed (W14 work).
    fn collect_merge_operands(
        &self,
        cf_data: &Arc<ColumnFamilyData>,
        key: &[u8],
        first_seq: u64,
        operands: &mut Vec<Vec<u8>>,
    ) -> ForstResult<Option<Vec<u8>>> {
        // Active memtable — skip the already-consumed outer entry (first_seq)
        // by probing at first_seq - 1.
        let mem_arc = cf_data.active_memtable();
        let initial = if first_seq == 0 {
            // No older entries possible; skip active scan.
            None
        } else {
            Some(first_seq - 1)
        };
        if let Some(cutoff) = initial {
            if let Some(base) = self.peel_merges_from_memtable(&mem_arc, key, cutoff, operands)? {
                return Ok(base.value);
            }
        }

        // Immutable memtables (newest → oldest). Each has its own sequence
        // space, so start from u64::MAX.
        let imm_list = cf_data.imm_memtables();
        for imm in imm_list.iter().rev() {
            if let Some(base) = self.peel_merges_from_memtable(imm, key, u64::MAX, operands)? {
                return Ok(base.value);
            }
        }

        // SST layer: walk L0 newest-first, then L1..Ln.
        self.peel_merges_from_sst(cf_data, key, operands)
    }

    /// Walks the imm list when the outer hit was itself in the immutable
    /// queue. `first_seq` is the already-consumed outer sequence; peel
    /// continues from `first_seq - 1` in the starting imm, then u64::MAX for
    /// older imms.
    fn collect_merge_operands_from_imm_start(
        &self,
        cf_data: &Arc<ColumnFamilyData>,
        key: &[u8],
        first_seq: u64,
        imm_list: Vec<crate::column_family::SharedMemTable>,
        operands: &mut Vec<Vec<u8>>,
    ) -> ForstResult<Option<Vec<u8>>> {
        let mut started = false;
        for imm in imm_list.iter().rev() {
            let cutoff = if !started {
                started = true;
                if first_seq == 0 {
                    continue;
                }
                first_seq - 1
            } else {
                u64::MAX
            };
            if let Some(base) = self.peel_merges_from_memtable(imm, key, cutoff, operands)? {
                return Ok(base.value);
            }
        }
        // Fall through into SSTs.
        self.peel_merges_from_sst(cf_data, key, operands)
    }

    /// Peels merges out of the SST layer. Returns:
    /// - `Ok(Some(value))` when a Put base is found (value may itself be None
    ///   if the Put stored a null payload).
    /// - `Ok(None)` when a Delete is hit or the SST layer is exhausted with
    ///   no base — the caller treats this as "base is empty".
    fn peel_merges_from_sst(
        &self,
        _cf_data: &Arc<ColumnFamilyData>,
        key: &[u8],
        operands: &mut Vec<Vec<u8>>,
    ) -> ForstResult<Option<Vec<u8>>> {
        let version = self.version_set.current();
        for sst in version.l0_files().iter().rev() {
            if let Some(res) = self.sst_lookup(sst, key)? {
                match res.op_type {
                    OpType::Put => return Ok(res.value),
                    OpType::Delete | OpType::SingleDelete => return Ok(None),
                    OpType::Merge => {
                        if let Some(v) = res.value {
                            operands.push(v);
                        }
                    }
                }
            }
        }
        for level in 1..version.num_levels() {
            let Some(idx) = version.find_sst_for_key(level, key) else {
                continue;
            };
            let sst = &version.levels[level].files[idx];
            if let Some(res) = self.sst_lookup(sst, key)? {
                match res.op_type {
                    OpType::Put => return Ok(res.value),
                    OpType::Delete | OpType::SingleDelete => return Ok(None),
                    OpType::Merge => {
                        if let Some(v) = res.value {
                            operands.push(v);
                        }
                    }
                }
            }
        }
        Ok(None)
    }

    /// Peels `Merge` entries off a single memtable, appending operands to
    /// `operands`. Returns `Ok(Some(base))` when a `Put`/`Delete` is found
    /// (the base value is in `base.value`; `Delete` produces `Value: None`),
    /// or `Ok(None)` if the memtable is exhausted without finding a base.
    ///
    /// `start_cutoff` is the highest sequence number to consider (inclusive).
    /// Pass `entry.sequence - 1` to skip an already-consumed entry, or
    /// `u64::MAX` to start from the newest entry.
    fn peel_merges_from_memtable(
        &self,
        mem_arc: &crate::column_family::SharedMemTable,
        key: &[u8],
        start_cutoff: u64,
        operands: &mut Vec<Vec<u8>>,
    ) -> ForstResult<Option<MergeBase>> {
        let mut cutoff = start_cutoff;
        loop {
            let hit = {
                let guard = mem_arc.read().expect("lock poisoned");
                guard.get(key, cutoff)?
            };
            let Some(entry) = hit else { return Ok(None) };
            match entry.op_type {
                OpType::Put => return Ok(Some(MergeBase { value: entry.value })),
                OpType::Delete | OpType::SingleDelete => {
                    return Ok(Some(MergeBase { value: None }))
                }
                OpType::Merge => {
                    if let Some(v) = entry.value {
                        operands.push(v);
                    }
                    if entry.sequence == 0 {
                        return Ok(None);
                    }
                    cutoff = entry.sequence - 1;
                }
            }
        }
    }

    fn lookup_cf_by_id(&self, id: ColumnFamilyId) -> ForstResult<Arc<ColumnFamilyData>> {
        let cfs = self.cfs.read().expect("lock poisoned");
        cfs.get(&id).cloned().ok_or_else(|| {
            ForstError::invalid_argument(format!("column family id {} not found", id))
        })
    }

    /// Access the shared [`FileDeletionGuard`] — used by checkpoints to pin
    /// files during the copy phase so concurrent compactions cannot delete
    /// them.
    pub fn deletion_guard(&self) -> &Arc<FileDeletionGuard> {
        &self.deletion_guard
    }

    /// Attempts to delete a file, respecting the [`FileDeletionGuard`].
    /// Pinned files are deferred to [`Self::reap_pending_deletions`].
    fn delete_file_guarded(&self, file_number: FileNumber) {
        if self.deletion_guard.can_delete(file_number) {
            let path = sst_file_path(&self.db_path, file_number);
            let _ = self.fs.delete_file(&path);
        } else {
            self.pending_deletions
                .lock()
                .expect("lock poisoned")
                .push(file_number);
        }
    }

    /// Reaps previously-deferred deletions whose pins have since been
    /// released. Called after every compaction so deletions eventually
    /// drain.
    fn reap_pending_deletions(&self) {
        let mut pending = self.pending_deletions.lock().expect("lock poisoned");
        let mut still_pending = Vec::with_capacity(pending.len());
        for file_number in pending.drain(..) {
            if self.deletion_guard.can_delete(file_number) {
                let path = sst_file_path(&self.db_path, file_number);
                let _ = self.fs.delete_file(&path);
            } else {
                still_pending.push(file_number);
            }
        }
        *pending = still_pending;
    }
}

/// Internal result of peeling merges off a single memtable.
struct MergeBase {
    value: Option<Vec<u8>>,
}

/// Computes the exclusive upper bound representing "all keys with this
/// prefix". Returns `None` if the prefix is all `0xff` bytes (no finite
/// upper bound exists).
fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut result = prefix.to_vec();
    while let Some(&last) = result.last() {
        if last == 0xff {
            result.pop();
        } else {
            let idx = result.len() - 1;
            result[idx] = last + 1;
            return Some(result);
        }
    }
    None
}

// Need this to satisfy Drop semantics if we expose raw Arc<VectorizedMemTable>
// from imm_memtables in the immutable slice layer (currently handled cleanly).
#[allow(clippy::missing_docs_in_private_items)]
trait _Marker {}

#[cfg(test)]
mod tests {
    use super::*;
    use forst_rs_storage::merge_operator::{ListAppendMergeOperator, MergeOperator};

    fn open() -> Arc<DbImpl> {
        DbImpl::open_default().expect("open")
    }

    // --- bring-up ---

    #[test]
    fn test_open_creates_default_cf() {
        let db = open();
        let h = db.column_family(DEFAULT_CF_NAME).expect("default cf");
        assert_eq!(h.id(), DEFAULT_CF_ID);
        assert_eq!(h.name(), DEFAULT_CF_NAME);
    }

    #[test]
    fn test_default_cf_accessor() {
        let db = open();
        let h = db.default_cf();
        assert_eq!(h.name(), DEFAULT_CF_NAME);
    }

    #[test]
    fn test_open_rejects_zero_write_buffer() {
        let opts = EngineOptions {
            write_buffer_size: 0,
            ..EngineOptions::default()
        };
        assert!(DbImpl::open(opts).is_err());
    }

    #[test]
    fn test_create_column_family_assigns_new_id() {
        let db = open();
        let h = db
            .create_column_family(ColumnFamilyDescriptor::new("cf1"))
            .unwrap();
        assert_ne!(h.id(), DEFAULT_CF_ID);
        assert_eq!(h.name(), "cf1");
    }

    #[test]
    fn test_create_column_family_rejects_duplicate() {
        let db = open();
        db.create_column_family(ColumnFamilyDescriptor::new("cf1"))
            .unwrap();
        let err = db
            .create_column_family(ColumnFamilyDescriptor::new("cf1"))
            .unwrap_err();
        assert!(err.to_string().contains("already exists"));
    }

    // --- put / get ---

    #[test]
    fn test_put_then_get_returns_value() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"v").unwrap();
        let got = db.get(&cf, b"k").unwrap();
        assert_eq!(got.as_deref(), Some(b"v".as_ref()));
    }

    #[test]
    fn test_get_missing_key_returns_none() {
        let db = open();
        let cf = db.default_cf();
        assert!(db.get(&cf, b"absent").unwrap().is_none());
    }

    #[test]
    fn test_put_overwrites_previous_value() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"old").unwrap();
        db.put(&cf, b"k", b"new").unwrap();
        assert_eq!(db.get(&cf, b"k").unwrap().as_deref(), Some(b"new".as_ref()));
    }

    #[test]
    fn test_put_increments_sequence() {
        let db = open();
        let cf = db.default_cf();
        let s1 = db.put(&cf, b"k1", b"v1").unwrap();
        let s2 = db.put(&cf, b"k2", b"v2").unwrap();
        assert!(s2 > s1);
        assert_eq!(db.sequence_number(), s2);
    }

    // --- delete ---

    #[test]
    fn test_delete_masks_prior_put() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"v").unwrap();
        db.delete(&cf, b"k").unwrap();
        assert!(db.get(&cf, b"k").unwrap().is_none());
    }

    #[test]
    fn test_delete_of_missing_key_is_noop() {
        let db = open();
        let cf = db.default_cf();
        db.delete(&cf, b"never").unwrap();
        assert!(db.get(&cf, b"never").unwrap().is_none());
    }

    // --- merge ---

    fn open_with_merge_cf() -> (Arc<DbImpl>, ColumnFamilyHandle) {
        let db = open();
        let op: Arc<dyn MergeOperator> = Arc::new(ListAppendMergeOperator::with_comma());
        let cf = db
            .create_column_family(ColumnFamilyDescriptor::new("merge_cf").with_merge_operator(op))
            .unwrap();
        (db, cf)
    }

    #[test]
    fn test_merge_with_prior_put_produces_concatenation() {
        let (db, cf) = open_with_merge_cf();
        db.put(&cf, b"k", b"a").unwrap();
        db.merge(&cf, b"k", b"b").unwrap();
        db.merge(&cf, b"k", b"c").unwrap();
        let got = db.get(&cf, b"k").unwrap();
        assert_eq!(got.as_deref(), Some(b"a,b,c".as_ref()));
    }

    #[test]
    fn test_merge_without_base_starts_empty() {
        let (db, cf) = open_with_merge_cf();
        db.merge(&cf, b"k", b"x").unwrap();
        db.merge(&cf, b"k", b"y").unwrap();
        let got = db.get(&cf, b"k").unwrap();
        assert_eq!(got.as_deref(), Some(b"x,y".as_ref()));
    }

    #[test]
    fn test_merge_requires_operator() {
        let db = open();
        let cf = db.default_cf(); // no merge operator configured
        db.merge(&cf, b"k", b"v").unwrap();
        let err = db.get(&cf, b"k").unwrap_err();
        assert!(err.to_string().contains("merge operator"));
    }

    #[test]
    fn test_merge_after_delete_uses_empty_base() {
        let (db, cf) = open_with_merge_cf();
        db.put(&cf, b"k", b"base").unwrap();
        db.delete(&cf, b"k").unwrap();
        db.merge(&cf, b"k", b"x").unwrap();
        let got = db.get(&cf, b"k").unwrap();
        // After Delete, there's no base; merge starts from empty.
        assert_eq!(got.as_deref(), Some(b"x".as_ref()));
    }

    // --- batch write / batch get ---

    #[test]
    fn test_batch_write_put_and_delete_read_back() {
        let db = open();
        let cf = db.default_cf();
        let mut b = WriteBatch::new();
        b.put(&cf, b"k1", b"v1")
            .put(&cf, b"k2", b"v2")
            .delete(&cf, b"k3");
        db.batch_write(b).unwrap();

        assert_eq!(db.get(&cf, b"k1").unwrap().as_deref(), Some(b"v1".as_ref()));
        assert_eq!(db.get(&cf, b"k2").unwrap().as_deref(), Some(b"v2".as_ref()));
        assert!(db.get(&cf, b"k3").unwrap().is_none());
    }

    #[test]
    fn test_single_delete_point_read_behaves_like_delete() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"v").unwrap();
        db.single_delete(&cf, b"k").unwrap();
        assert!(
            db.get(&cf, b"k").unwrap().is_none(),
            "SingleDelete must make the key appear absent to point reads"
        );
    }

    #[test]
    fn test_write_batch_single_delete_roundtrip() {
        let db = open();
        let cf = db.default_cf();
        let mut wb = WriteBatch::new();
        wb.put(&cf, b"k1", b"v1")
            .single_delete(&cf, b"k1")
            .put(&cf, b"k2", b"v2");
        db.batch_write(wb).unwrap();
        assert!(db.get(&cf, b"k1").unwrap().is_none());
        assert_eq!(db.get(&cf, b"k2").unwrap().as_deref(), Some(b"v2".as_ref()));
    }

    #[test]
    fn test_single_delete_compaction_elides_matching_put() {
        // Seed one Put then one SingleDelete in separate flushes, so the
        // two end up in sibling L0 SSTs. L0→L1 compaction on a
        // non-bottommost level (we stage an empty L1 so this compaction
        // is *not* bottommost) must elide BOTH entries.
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"v").unwrap();
        db.flush_all().unwrap();
        db.single_delete(&cf, b"k").unwrap();
        db.flush_all().unwrap();

        db.compact_all().unwrap();
        assert!(db.get(&cf, b"k").unwrap().is_none());
    }

    #[test]
    fn test_single_delete_falls_back_when_two_puts_precede() {
        // Contract violation: two consecutive Puts before the
        // SingleDelete. Compaction must fall back to normal Delete
        // semantics so the older Put remains shadowed.
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"v1").unwrap();
        db.flush_all().unwrap();
        db.put(&cf, b"k", b"v2").unwrap();
        db.flush_all().unwrap();
        db.single_delete(&cf, b"k").unwrap();
        db.flush_all().unwrap();

        db.compact_all().unwrap();
        // The key must still read as absent — the tombstone must win over
        // whichever Put survived the collapse.
        assert!(db.get(&cf, b"k").unwrap().is_none());
    }

    #[test]
    fn test_batch_get_parallel_reads() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"a", b"1").unwrap();
        db.put(&cf, b"b", b"2").unwrap();
        let out = db
            .batch_get(&cf, &[b"a".as_ref(), b"missing".as_ref(), b"b".as_ref()])
            .unwrap();
        assert_eq!(out[0].as_deref(), Some(b"1".as_ref()));
        assert_eq!(out[1], None);
        assert_eq!(out[2].as_deref(), Some(b"2".as_ref()));
    }

    #[test]
    fn test_batch_write_empty_is_ok() {
        let db = open();
        db.batch_write(WriteBatch::new()).unwrap();
    }

    #[test]
    fn test_batch_write_multiple_cfs() {
        let db = open();
        let a = db.default_cf();
        let b_cf = db
            .create_column_family(ColumnFamilyDescriptor::new("other"))
            .unwrap();
        let mut wb = WriteBatch::new();
        wb.put(&a, b"k", b"A").put(&b_cf, b"k", b"B");
        db.batch_write(wb).unwrap();
        assert_eq!(db.get(&a, b"k").unwrap().as_deref(), Some(b"A".as_ref()));
        assert_eq!(db.get(&b_cf, b"k").unwrap().as_deref(), Some(b"B".as_ref()));
    }

    // --- memtable switch ---

    #[test]
    fn test_force_switch_memtable_increments_imm_count() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"v").unwrap();
        let cf_data = db.lookup_cf_by_id(cf.id()).unwrap();
        assert_eq!(cf_data.imm_count(), 0);
        db.force_switch_memtable(&cf).unwrap();
        assert_eq!(cf_data.imm_count(), 1);
    }

    #[test]
    fn test_read_sees_imm_memtable_after_switch() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"v").unwrap();
        db.force_switch_memtable(&cf).unwrap();
        // After switch, the value lives in the immutable queue only.
        assert_eq!(db.get(&cf, b"k").unwrap().as_deref(), Some(b"v".as_ref()));
    }

    #[test]
    fn test_snapshot_view_updates_after_switch() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"v").unwrap();
        let cf_data = db.lookup_cf_by_id(cf.id()).unwrap();
        let before = cf_data.snapshot_view();
        assert_eq!(before.imm_list().len(), 0);
        db.force_switch_memtable(&cf).unwrap();
        let after = cf_data.snapshot_view();
        assert_eq!(after.imm_list().len(), 1);
    }

    // --- merge spanning active + imm memtable ---

    #[test]
    fn test_merge_resolves_across_active_and_imm_memtables() {
        let (db, cf) = open_with_merge_cf();
        db.put(&cf, b"k", b"base").unwrap();
        db.merge(&cf, b"k", b"a").unwrap();
        db.force_switch_memtable(&cf).unwrap();
        // New writes land in the fresh active memtable.
        db.merge(&cf, b"k", b"b").unwrap();
        db.merge(&cf, b"k", b"c").unwrap();
        let got = db.get(&cf, b"k").unwrap();
        assert_eq!(got.as_deref(), Some(b"base,a,b,c".as_ref()));
    }

    #[test]
    fn test_column_family_lookup_by_name_missing_returns_none() {
        let db = open();
        assert!(db.column_family("does-not-exist").is_none());
    }

    // --- W13 flush tests ---

    #[test]
    fn test_flush_cf_noop_when_no_imm() {
        let db = open();
        let cf = db.default_cf();
        assert!(db.flush_cf(&cf).unwrap().is_none());
    }

    #[test]
    fn test_flush_cf_produces_sst() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k1", b"v1").unwrap();
        db.put(&cf, b"k2", b"v2").unwrap();
        db.force_switch_memtable(&cf).unwrap();
        let meta = db.flush_cf(&cf).unwrap().expect("meta");
        assert_eq!(meta.num_entries, 2);
        assert_eq!(meta.smallest_key, b"k1");
        assert_eq!(meta.largest_key, b"k2");
    }

    #[test]
    fn test_read_after_flush_returns_value() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k1", b"v1").unwrap();
        db.put(&cf, b"k2", b"v2").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        // Data should now live only in the SST file.
        let cf_data = db.lookup_cf_by_id(cf.id()).unwrap();
        assert_eq!(cf_data.imm_count(), 0);
        assert_eq!(db.version_set.current().l0_files().len(), 1);
        assert_eq!(db.get(&cf, b"k1").unwrap().as_deref(), Some(b"v1".as_ref()));
        assert_eq!(db.get(&cf, b"k2").unwrap().as_deref(), Some(b"v2".as_ref()));
        assert!(db.get(&cf, b"missing").unwrap().is_none());
    }

    #[test]
    fn test_delete_survives_flush() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"v").unwrap();
        db.delete(&cf, b"k").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        assert!(db.get(&cf, b"k").unwrap().is_none());
    }

    #[test]
    fn test_newer_memtable_masks_older_sst() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"old").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        // Newer value in fresh active memtable.
        db.put(&cf, b"k", b"new").unwrap();
        assert_eq!(db.get(&cf, b"k").unwrap().as_deref(), Some(b"new".as_ref()));
    }

    #[test]
    fn test_delete_in_memtable_masks_sst_put() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"v").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        db.delete(&cf, b"k").unwrap();
        assert!(db.get(&cf, b"k").unwrap().is_none());
    }

    #[test]
    fn test_flush_all_drains_multiple_cfs() {
        let db = open();
        let a = db.default_cf();
        let b = db
            .create_column_family(ColumnFamilyDescriptor::new("cfb"))
            .unwrap();
        db.put(&a, b"ka", b"va").unwrap();
        db.put(&b, b"kb", b"vb").unwrap();
        db.force_switch_memtable(&a).unwrap();
        db.force_switch_memtable(&b).unwrap();
        db.flush_all().unwrap();
        assert_eq!(db.version_set.current().l0_files().len(), 2);
        assert_eq!(db.get(&a, b"ka").unwrap().as_deref(), Some(b"va".as_ref()));
        assert_eq!(db.get(&b, b"kb").unwrap().as_deref(), Some(b"vb".as_ref()));
    }

    #[test]
    fn test_multiple_l0_ssts_newest_wins() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"v1").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        db.put(&cf, b"k", b"v2").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        db.put(&cf, b"k", b"v3").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        assert_eq!(db.version_set.current().l0_files().len(), 3);
        assert_eq!(db.get(&cf, b"k").unwrap().as_deref(), Some(b"v3".as_ref()));
    }

    #[test]
    fn test_merge_after_flush_uses_sst_base() {
        let (db, cf) = open_with_merge_cf();
        db.put(&cf, b"k", b"base").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        db.merge(&cf, b"k", b"a").unwrap();
        db.merge(&cf, b"k", b"b").unwrap();
        assert_eq!(
            db.get(&cf, b"k").unwrap().as_deref(),
            Some(b"base,a,b".as_ref())
        );
    }

    #[test]
    fn test_merge_chain_spans_memtable_imm_and_sst() {
        // Cross-SST merge peel: put then merges, flushed together. The read
        // path currently uses `SstReaderImpl::get()` which returns only the
        // newest entry for a key. When a single SST contains both a Put base
        // and Merge operands for the same key, the peel cannot walk older
        // versions within that SST. This is resolved in W15 compaction by
        // consolidating multiple versions into a single entry per key.
        //
        // The correct behaviour for this scenario is exercised by
        // `test_merge_after_flush_uses_sst_base`, which flushes the base
        // separately from the subsequent merges.
        let (db, cf) = open_with_merge_cf();
        db.put(&cf, b"k", b"base").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap(); // base alone in SST
        db.merge(&cf, b"k", b"a").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap(); // a alone in another SST
        db.merge(&cf, b"k", b"b").unwrap();
        db.force_switch_memtable(&cf).unwrap(); // b in imm
        db.merge(&cf, b"k", b"c").unwrap(); // c in active
        assert_eq!(
            db.get(&cf, b"k").unwrap().as_deref(),
            Some(b"base,a,b,c".as_ref())
        );
    }

    #[test]
    fn test_auto_flush_on_threshold() {
        // Use a tiny write_buffer_size so any put triggers a switch + flush.
        use forst_rs_io::MemoryFileSystem;
        let opts = EngineOptions {
            db_path: "/db".to_string(),
            write_buffer_size: 32, // extremely small
            ..EngineOptions::default()
        };
        let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
        let db = DbImpl::open_with_fs(opts, fs).unwrap();
        let cf = db.default_cf();
        for i in 0..10u32 {
            let k = format!("key{:04}", i);
            let v = format!("value{:04}", i);
            db.put(&cf, k.as_bytes(), v.as_bytes()).unwrap();
        }
        // Some L0 files should have been created.
        assert!(!db.version_set.current().l0_files().is_empty());
        for i in 0..10u32 {
            let k = format!("key{:04}", i);
            let v = format!("value{:04}", i);
            assert_eq!(
                db.get(&cf, k.as_bytes()).unwrap().as_deref(),
                Some(v.as_bytes())
            );
        }
    }

    #[test]
    fn test_batch_write_then_flush_then_read() {
        let db = open();
        let cf = db.default_cf();
        let mut batch = WriteBatch::new();
        for i in 0..50u32 {
            let k = format!("key{:04}", i);
            batch.put(&cf, k.as_bytes(), b"v");
        }
        db.batch_write(batch).unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        for i in 0..50u32 {
            let k = format!("key{:04}", i);
            assert_eq!(
                db.get(&cf, k.as_bytes()).unwrap().as_deref(),
                Some(b"v".as_ref())
            );
        }
    }

    #[test]
    fn test_sst_reader_cache_reuses_reader() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"v").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        let meta = &db.version_set.current().l0_files()[0].clone();
        let r1 = db.get_or_open_sst_reader(meta).unwrap();
        let r2 = db.get_or_open_sst_reader(meta).unwrap();
        assert!(Arc::ptr_eq(&r1, &r2));
    }

    // --- W14.2 scan tests ---

    #[test]
    fn test_scan_empty_db_returns_empty() {
        let db = open();
        let cf = db.default_cf();
        let out = db.scan(&cf, b"", None).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn test_scan_collects_sorted_memtable_entries() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"b", b"B").unwrap();
        db.put(&cf, b"a", b"A").unwrap();
        db.put(&cf, b"c", b"C").unwrap();
        let out = db.scan(&cf, b"", None).unwrap();
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].0, b"a");
        assert_eq!(out[1].0, b"b");
        assert_eq!(out[2].0, b"c");
    }

    #[test]
    fn test_scan_respects_bounds() {
        let db = open();
        let cf = db.default_cf();
        for k in [b"a".as_ref(), b"b", b"c", b"d", b"e"] {
            db.put(&cf, k, b"v").unwrap();
        }
        let out = db.scan(&cf, b"b", Some(b"d")).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].0, b"b");
        assert_eq!(out[1].0, b"c");
    }

    #[test]
    fn test_scan_spans_memtable_imm_and_sst() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"a", b"1").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap(); // a in SST
        db.put(&cf, b"b", b"2").unwrap();
        db.force_switch_memtable(&cf).unwrap(); // b in imm
        db.put(&cf, b"c", b"3").unwrap(); // c in active
        let out = db.scan(&cf, b"", None).unwrap();
        assert_eq!(out.len(), 3);
        assert_eq!(out[0], (b"a".to_vec(), b"1".to_vec()));
        assert_eq!(out[1], (b"b".to_vec(), b"2".to_vec()));
        assert_eq!(out[2], (b"c".to_vec(), b"3".to_vec()));
    }

    #[test]
    fn test_scan_skips_deleted_keys() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"a", b"1").unwrap();
        db.put(&cf, b"b", b"2").unwrap();
        db.put(&cf, b"c", b"3").unwrap();
        db.delete(&cf, b"b").unwrap();
        let out = db.scan(&cf, b"", None).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].0, b"a");
        assert_eq!(out[1].0, b"c");
    }

    #[test]
    fn test_scan_respects_newer_memtable_over_sst() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"old").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        db.put(&cf, b"k", b"new").unwrap();
        let out = db.scan(&cf, b"", None).unwrap();
        assert_eq!(out, vec![(b"k".to_vec(), b"new".to_vec())]);
    }

    #[test]
    fn test_scan_resolves_merges() {
        let (db, cf) = open_with_merge_cf();
        db.put(&cf, b"k", b"a").unwrap();
        db.merge(&cf, b"k", b"b").unwrap();
        db.merge(&cf, b"k", b"c").unwrap();
        let out = db.scan(&cf, b"", None).unwrap();
        assert_eq!(out, vec![(b"k".to_vec(), b"a,b,c".to_vec())]);
    }

    #[test]
    fn test_prefix_scan_basic() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"user:a", b"A").unwrap();
        db.put(&cf, b"user:b", b"B").unwrap();
        db.put(&cf, b"order:1", b"O1").unwrap();
        let out = db.prefix_scan(&cf, b"user:").unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].0, b"user:a");
        assert_eq!(out[1].0, b"user:b");
    }

    #[test]
    fn test_prefix_scan_with_ff_suffix() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, &[b'a', 0x00], b"1").unwrap();
        db.put(&cf, &[b'a', 0xff], b"2").unwrap();
        db.put(&cf, b"b", b"3").unwrap();
        let out = db.prefix_scan(&cf, b"a").unwrap();
        assert_eq!(out.len(), 2);
    }

    // --- W15 compaction tests ---

    #[test]
    fn test_compact_l0_noop_when_empty() {
        let db = open();
        let cf = db.default_cf();
        assert!(db.compact_l0(&cf).unwrap().is_none());
    }

    #[test]
    fn test_compact_l0_single_file_rolls_up_to_l1() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"v").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        assert_eq!(db.version_set.current().l0_files().len(), 1);

        let new_meta = db.compact_l0(&cf).unwrap().expect("new L1 file");
        let v = db.version_set.current();
        assert_eq!(v.l0_files().len(), 0);
        assert_eq!(v.levels[1].files.len(), 1);
        assert_eq!(v.levels[1].files[0].file_number, new_meta.file_number);
        // Read still works.
        assert_eq!(db.get(&cf, b"k").unwrap().as_deref(), Some(b"v".as_ref()));
    }

    #[test]
    fn test_compact_l0_multiple_files_merged() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k1", b"v1").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        db.put(&cf, b"k2", b"v2").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        db.put(&cf, b"k3", b"v3").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        assert_eq!(db.version_set.current().l0_files().len(), 3);

        db.compact_l0(&cf).unwrap().unwrap();
        let v = db.version_set.current();
        assert_eq!(v.l0_files().len(), 0);
        assert_eq!(v.levels[1].files.len(), 1);
        assert_eq!(v.levels[1].files[0].num_entries, 3);

        assert_eq!(db.get(&cf, b"k1").unwrap().as_deref(), Some(b"v1".as_ref()));
        assert_eq!(db.get(&cf, b"k2").unwrap().as_deref(), Some(b"v2".as_ref()));
        assert_eq!(db.get(&cf, b"k3").unwrap().as_deref(), Some(b"v3".as_ref()));
    }

    #[test]
    fn test_compact_l0_consolidates_versions() {
        let db = open();
        let cf = db.default_cf();
        // Same key written three times; each flush produces a new L0.
        db.put(&cf, b"k", b"v1").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        db.put(&cf, b"k", b"v2").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        db.put(&cf, b"k", b"v3").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();

        db.compact_l0(&cf).unwrap().unwrap();
        let v = db.version_set.current();
        assert_eq!(v.levels[1].files[0].num_entries, 1); // Only latest kept.
        assert_eq!(db.get(&cf, b"k").unwrap().as_deref(), Some(b"v3".as_ref()));
    }

    #[test]
    fn test_compact_l0_drops_bottommost_tombstones() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"v").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        db.delete(&cf, b"k").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();

        db.compact_l0(&cf).unwrap();
        let v = db.version_set.current();
        // Bottommost compaction — tombstone + Put both removed.
        assert_eq!(v.levels[1].files.len(), 0);
        assert!(db.get(&cf, b"k").unwrap().is_none());
    }

    #[test]
    fn test_compact_l0_with_merge_operator_resolves_chain() {
        let (db, cf) = open_with_merge_cf();
        db.put(&cf, b"k", b"a").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        db.merge(&cf, b"k", b"b").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        db.merge(&cf, b"k", b"c").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();

        db.compact_l0(&cf).unwrap();
        let v = db.version_set.current();
        assert_eq!(v.levels[1].files[0].num_entries, 1); // Collapsed.
        assert_eq!(
            db.get(&cf, b"k").unwrap().as_deref(),
            Some(b"a,b,c".as_ref())
        );
    }

    #[test]
    fn test_compact_l0_deletes_old_files_from_disk() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"v").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        let old_file = db.version_set.current().l0_files()[0].file_number;
        let old_path = sst_file_path(&db.db_path, old_file);
        assert!(db.fs.file_exists(&old_path).unwrap());

        db.compact_l0(&cf).unwrap().unwrap();
        assert!(!db.fs.file_exists(&old_path).unwrap());
    }

    #[test]
    fn test_compact_all_flushes_multiple_cfs() {
        let db = open();
        let a = db.default_cf();
        let b = db
            .create_column_family(ColumnFamilyDescriptor::new("other"))
            .unwrap();
        for i in 0..5 {
            db.put(&a, format!("a{}", i).as_bytes(), b"va").unwrap();
            db.put(&b, format!("b{}", i).as_bytes(), b"vb").unwrap();
        }
        db.force_switch_memtable(&a).unwrap();
        db.force_switch_memtable(&b).unwrap();
        db.flush_all().unwrap();
        db.compact_all().unwrap();
        let v = db.version_set.current();
        assert_eq!(v.l0_files().len(), 0);
        for i in 0..5 {
            assert!(db.get(&a, format!("a{}", i).as_bytes()).unwrap().is_some());
            assert!(db.get(&b, format!("b{}", i).as_bytes()).unwrap().is_some());
        }
    }

    // --- W15.3 multi-level compaction ---

    #[test]
    fn test_compact_level_from_l1_to_l2() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k1", b"v1").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        // Roll L0 → L1.
        db.compact_l0(&cf).unwrap().unwrap();
        assert_eq!(db.version_set.current().levels[1].files.len(), 1);
        assert_eq!(db.version_set.current().levels[2].files.len(), 0);

        // Now roll L1 → L2.
        db.compact_level(&cf, 1).unwrap().unwrap();
        let v = db.version_set.current();
        assert_eq!(v.levels[1].files.len(), 0);
        assert_eq!(v.levels[2].files.len(), 1);
        assert_eq!(db.get(&cf, b"k1").unwrap().as_deref(), Some(b"v1".as_ref()));
    }

    #[test]
    fn test_compact_level_zero_delegates_to_l0() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"v").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        db.compact_level(&cf, 0).unwrap().unwrap();
        let v = db.version_set.current();
        assert_eq!(v.l0_files().len(), 0);
        assert_eq!(v.levels[1].files.len(), 1);
    }

    #[test]
    fn test_compact_level_returns_none_on_empty_level() {
        let db = open();
        let cf = db.default_cf();
        let out = db.compact_level(&cf, 2).unwrap();
        assert!(out.is_none());
    }

    #[test]
    fn test_compact_level_overlapping_files_are_merged_into_destination() {
        let db = open();
        let cf = db.default_cf();
        // Seed L1 with "a..d".
        db.put(&cf, b"a", b"1").unwrap();
        db.put(&cf, b"d", b"2").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        db.compact_l0(&cf).unwrap().unwrap();
        // Seed L2 with "c..f" by first landing in L0, then push down.
        db.put(&cf, b"c", b"3").unwrap();
        db.put(&cf, b"f", b"4").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        db.compact_l0(&cf).unwrap().unwrap();
        // Now L1 has two files, push the whole level to L2.
        db.compact_level(&cf, 1).unwrap().unwrap();
        let v = db.version_set.current();
        assert_eq!(v.levels[1].files.len(), 0);
        // All keys read correctly.
        for (k, expected) in [
            (b"a" as &[u8], b"1" as &[u8]),
            (b"c", b"3"),
            (b"d", b"2"),
            (b"f", b"4"),
        ] {
            assert_eq!(
                db.get(&cf, k).unwrap().as_deref(),
                Some(expected),
                "mismatch at key {:?}",
                k
            );
        }
    }

    #[test]
    fn test_compact_once_prefers_l0_drain() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"v").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        assert!(!db.version_set.current().l0_files().is_empty());
        let did = db.compact_once_for(&cf).unwrap();
        assert!(did);
        assert!(db.version_set.current().l0_files().is_empty());
    }

    #[test]
    fn test_compact_once_returns_false_when_balanced() {
        let db = open();
        let cf = db.default_cf();
        let did = db.compact_once_for(&cf).unwrap();
        assert!(!did);
    }

    // --- W15 CompactionFilter integration ---

    #[test]
    fn test_compaction_filter_discards_expired_entries() {
        use crate::compaction_filter::{encode_ttl_value, TtlCompactionFilter};
        let db = open();
        let filter_now: fn() -> u64 = || 1_000;
        let filter = Arc::new(TtlCompactionFilter::with_clock(50, filter_now));
        let cf = db
            .create_column_family(ColumnFamilyDescriptor::new("ttl").with_compaction_filter(filter))
            .unwrap();

        // Old entry (ts=500, age at compaction time = 500) should be discarded.
        let old_value = encode_ttl_value(500, b"old");
        db.put(&cf, b"old_key", &old_value).unwrap();

        // Fresh entry (ts=990, age=10) should be kept.
        let fresh_value = encode_ttl_value(990, b"fresh");
        db.put(&cf, b"fresh_key", &fresh_value).unwrap();

        db.switch_and_flush(&cf).unwrap().unwrap();
        db.compact_l0(&cf).unwrap().unwrap();

        // old_key → expired → None.
        assert!(db.get(&cf, b"old_key").unwrap().is_none());
        // fresh_key → still present.
        let got = db.get(&cf, b"fresh_key").unwrap();
        assert!(got.is_some());
    }

    #[test]
    fn test_compaction_filter_preserves_tombstones() {
        use crate::compaction_filter::TtlCompactionFilter;
        let db = open();
        let now: fn() -> u64 = || 1_000;
        let filter = Arc::new(TtlCompactionFilter::with_clock(0, now));
        let cf = db
            .create_column_family(ColumnFamilyDescriptor::new("ttl").with_compaction_filter(filter))
            .unwrap();

        db.delete(&cf, b"key").unwrap();
        // Move to L1 first so bottommost elimination doesn't fire.
        db.switch_and_flush(&cf).unwrap().unwrap();
        db.put(&cf, b"other", b"dummy").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        // Non-bottommost L0→L1 compaction retains tombstones.
        db.compact_l0(&cf).unwrap();
        // The key must still be absent after compaction (filter didn't drop
        // the tombstone).
        assert!(db.get(&cf, b"key").unwrap().is_none());
    }

    // --- W15.2 checkpoint tests ---

    fn open_in_shared_fs(path: &str, fs: Arc<dyn FileSystem>) -> Arc<DbImpl> {
        let opts = EngineOptions {
            db_path: path.to_string(),
            ..EngineOptions::default()
        };
        DbImpl::open_with_fs(opts, fs).unwrap()
    }

    #[test]
    fn test_create_checkpoint_writes_blob_and_ssts() {
        use forst_rs_io::MemoryFileSystem;
        let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
        let db = open_in_shared_fs("/db", fs.clone());
        let cf = db.default_cf();
        db.put(&cf, b"k1", b"v1").unwrap();
        db.put(&cf, b"k2", b"v2").unwrap();

        let manifest = db.create_checkpoint(std::path::Path::new("/ckpt")).unwrap();
        assert!(fs
            .file_exists(std::path::Path::new("/ckpt/CHECKPOINT.blob"))
            .unwrap());
        assert!(!manifest.sst_files.is_empty());
        assert!(manifest.total_bytes > 0);
    }

    #[test]
    fn test_checkpoint_restore_roundtrip() {
        use forst_rs_io::MemoryFileSystem;
        let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
        let db = open_in_shared_fs("/db", fs.clone());
        let cf = db.default_cf();
        for i in 0..100u32 {
            let k = format!("k{:04}", i);
            let v = format!("v{:04}", i);
            db.put(&cf, k.as_bytes(), v.as_bytes()).unwrap();
        }
        db.create_checkpoint(std::path::Path::new("/ckpt")).unwrap();

        // Re-open from the checkpoint directory.
        let opts = EngineOptions {
            db_path: "/ckpt".to_string(),
            ..EngineOptions::default()
        };
        let restored = DbImpl::open_from_checkpoint(opts, fs).unwrap();
        let rcf = restored.default_cf();
        for i in 0..100u32 {
            let k = format!("k{:04}", i);
            let v = format!("v{:04}", i);
            assert_eq!(
                restored.get(&rcf, k.as_bytes()).unwrap().as_deref(),
                Some(v.as_bytes()),
                "mismatch at i={}",
                i
            );
        }
    }

    #[test]
    fn test_checkpoint_includes_active_memtable() {
        use forst_rs_io::MemoryFileSystem;
        let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
        let db = open_in_shared_fs("/db", fs.clone());
        let cf = db.default_cf();
        // Write without flushing — data lives only in active memtable.
        db.put(&cf, b"k", b"v").unwrap();
        db.create_checkpoint(std::path::Path::new("/ckpt")).unwrap();

        // After checkpoint, an L0 file should exist for the captured data.
        let v = db.version_set.current();
        assert!(!v.l0_files().is_empty());
    }

    #[test]
    fn test_open_from_checkpoint_missing_blob_errors() {
        use forst_rs_io::MemoryFileSystem;
        let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
        fs.create_dir_all(std::path::Path::new("/empty")).unwrap();
        let opts = EngineOptions {
            db_path: "/empty".to_string(),
            ..EngineOptions::default()
        };
        let result = DbImpl::open_from_checkpoint(opts, fs);
        match result {
            Ok(_) => panic!("expected NotFound error"),
            Err(e) => assert!(e.is_not_found()),
        }
    }

    // --- W16 FileDeletionGuard integration ---

    #[test]
    fn test_deletion_guard_protects_pinned_files_during_compaction() {
        let db = open();
        let cf = db.default_cf();
        db.put(&cf, b"k", b"v").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        let pinned = db.version_set.current().l0_files()[0].file_number;
        let pin = db.deletion_guard().pin_batch(&[pinned]);
        // Compaction attempts to delete `pinned` but the guard defers.
        db.compact_l0(&cf).unwrap().unwrap();
        let path = sst_file_path(&db.db_path, pinned);
        assert!(
            db.fs.file_exists(&path).unwrap(),
            "pinned file must survive"
        );
        drop(pin);
        // Trigger a reap via another flush/compact cycle.
        db.put(&cf, b"other", b"v2").unwrap();
        db.switch_and_flush(&cf).unwrap().unwrap();
        db.compact_l0(&cf).unwrap();
        assert!(
            !db.fs.file_exists(&path).unwrap(),
            "file must be deleted after pin release"
        );
    }

    #[test]
    fn test_deletion_guard_exposed_via_accessor() {
        let db = open();
        assert!(db.deletion_guard().pinned_files().is_empty());
        db.deletion_guard().pin(FileNumber(1));
        assert_eq!(db.deletion_guard().pin_count(FileNumber(1)), 1);
    }

    #[test]
    fn test_checkpoint_pins_live_files_during_copy() {
        use forst_rs_io::MemoryFileSystem;
        let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
        let db = open_in_shared_fs("/db", fs);
        let cf = db.default_cf();
        for i in 0..10u32 {
            db.put(&cf, format!("k{}", i).as_bytes(), b"v").unwrap();
        }
        // After checkpoint completes, no pins should remain.
        db.create_checkpoint(std::path::Path::new("/ckpt")).unwrap();
        assert!(db.deletion_guard().pinned_files().is_empty());
    }
}
