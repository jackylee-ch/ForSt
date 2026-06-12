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

//! FRS-PHASE2-S1 (2026-06-13 design §2): the file-mapping / ownership layer —
//! forst-rs's Unified-File-System equivalent (paper §5.1).
//!
//! S3/BOS have no hard links; this metadata layer provides hard-link semantics
//! over object stores: it maintains **logical→physical mappings and reference
//! counts** so a checkpoint can "link" a working-dir SST into a checkpoint
//! namespace as an O(metadata) operation with zero data movement, and physical
//! deletion happens exactly once, when the refcount reaches zero.
//!
//! # Model
//!
//! - **logical path** — a working-dir path (`/db/000042.sst`) or a
//!   checkpoint-namespace path (`/ckpt/chk-7/000042.sst`).
//! - **physical key** — the one object on the DFS that all linked logical
//!   paths share. Working-dir creation registers an *identity* mapping
//!   (logical == physical, refs = 1).
//! - **refcount invariant** — `refs(physical) == number of logical paths
//!   currently mapped to it`. Refs are *derived* from the logical map (never
//!   incremented blindly), which is what makes journal replay idempotent.
//!
//! # Ownership wiring (design §2.2)
//!
//! The previously-dormant [`FileOwnershipTracker`] is the ownership authority:
//! working-dir SSTs register as [`FileOwnership::ShareableOwnedByDb`]; objects
//! adopted from a restored checkpoint enter as [`FileOwnership::NotOwned`]
//! (bytes are never deleted by us at refs==0 — the external owner's lifecycle
//! governs them) until re-registered under the new working namespace.
//!
//! # Durability (design §2.4, risk R1)
//!
//! Every mutation appends a CRC-framed record to an append-only journal next
//! to the working dir *before* any physical delete is issued, so a crash
//! between journal append and physical delete leaves only an orphan that
//! [`FileMappingManager::gc_sweep`] reaps. A full-state snapshot
//! ([`FileMappingManager::snapshot_bytes`]) is embedded into the engine's
//! `CHECKPOINT.blob` at checkpoint time (Stage-2 wiring; the engine-side
//! trailer codec already round-trips it), and the journal tail is replayed on
//! restore.
//!
//! # JM-discard tombstones (design §2.3)
//!
//! JM-side checkpoint discard must never issue a direct S3 delete (the TM
//! owns the physical lifecycle). Instead the discard writes a
//! [`MappingRecord::Tombstone`]; the physical object is deleted when its
//! refcount drains to zero (or immediately if already zero), and
//! [`FileMappingManager::gc_sweep`] consumes tombstones for objects the
//! mapping no longer references.
//!
//! Stage-1 status: this layer is **inert by default** — the engine only
//! routes deletes through it after `DbImpl::attach_file_mapping` is called,
//! which nothing in the production path does until Stage-2 wires the
//! checkpoint to `link()`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use forst_rs_common::error::{ForstError, ForstResult};
use forst_rs_common::types::FileNumber;
use forst_rs_common::{crc32c, get_fixed32, get_fixed64, put_fixed32, put_fixed64};

use crate::filesystem::{FileSystem, WritableFile, WriteMode};
use crate::ownership::{FileOwnership, FileOwnershipTracker, OwnedFile};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Result of [`FileMappingManager::unlink`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnlinkOutcome {
    /// Other logical links still reference the physical object; bytes kept.
    Retained {
        /// Refcount remaining after this unlink.
        refs_remaining: u32,
    },
    /// Last reference dropped — the physical object was deleted (exactly once).
    PhysicalDeleted,
    /// Last reference dropped but the object is [`FileOwnership::NotOwned`]
    /// (adopted from an external checkpoint) and not tombstoned: bytes kept,
    /// external owner governs the lifecycle.
    KeptNotOwned,
}

/// Report from [`FileMappingManager::gc_sweep`].
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct GcReport {
    /// Physical objects deleted by the sweep (orphans + drained tombstones).
    pub reaped: Vec<String>,
    /// Objects kept because a live mapping (refs > 0) references them.
    pub kept_live: usize,
}

/// One durable mapping mutation. Serialized into the append-only journal and
/// replayed (idempotently) on restore.
#[derive(Debug, Clone, PartialEq, Eq)]
enum MappingRecord {
    /// Working-dir create: identity mapping, refs = 1.
    Register {
        logical: PathBuf,
        physical_key: String,
        size: u64,
    },
    /// Checkpoint link: new logical name -> same physical.
    Link {
        dst_logical: PathBuf,
        physical_key: String,
    },
    /// Drop one logical reference.
    Unlink { logical: PathBuf },
    /// Restore link: adopt an existing external physical object.
    Adopt {
        logical: PathBuf,
        physical_key: String,
    },
    /// JM-discard protocol: mark a physical for deletion once refs drain.
    Tombstone { physical_key: String },
}

// ---------------------------------------------------------------------------
// Internal state
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct PhysicalEntry {
    size: u64,
    /// Derived invariant: number of logical paths mapped to this key.
    refs: u32,
    ownership: FileOwnership,
    /// JM-discard tombstone: delete the bytes when refs drain to zero even
    /// if `NotOwned` (the discard is an explicit owner instruction).
    tombstoned: bool,
}

#[derive(Default)]
struct MappingState {
    /// logical path -> physical key.
    logical: HashMap<PathBuf, String>,
    /// physical key -> entry.
    physical: HashMap<String, PhysicalEntry>,
    /// Ownership authority (previously-dormant tracker, now live). Keyed by
    /// the engine [`FileNumber`] parsed from the physical key's `NNNNNN.sst`
    /// basename; keys that don't parse are mapped but not ownership-tracked.
    tracker: FileOwnershipTracker,
}

impl MappingState {
    /// Applies one record. Pure metadata: NEVER touches the filesystem —
    /// physical deletes are decided by the live ops, not by replay (a crash
    /// between journal append and physical delete leaves an orphan for
    /// `gc_sweep`). All branches are written so that re-applying the same
    /// record stream is a no-op (replay idempotence, design §2.4).
    fn apply(&mut self, rec: &MappingRecord) {
        match rec {
            MappingRecord::Register {
                logical,
                physical_key,
                size,
            } => {
                self.insert_logical(logical, physical_key, *size, FileOwnership::ShareableOwnedByDb);
            }
            MappingRecord::Link {
                dst_logical,
                physical_key,
            } => {
                // Replay keeps the physical entry's existing ownership/size.
                let (size, ownership) = self
                    .physical
                    .get(physical_key)
                    .map(|e| (e.size, e.ownership))
                    .unwrap_or((0, FileOwnership::ShareableOwnedByDb));
                self.insert_logical(dst_logical, physical_key, size, ownership);
            }
            MappingRecord::Unlink { logical } => {
                if let Some(key) = self.logical.remove(logical) {
                    let drained = {
                        let entry = self.physical.get_mut(&key).expect("physical invariant");
                        entry.refs = entry.refs.saturating_sub(1);
                        entry.refs == 0
                    };
                    if drained {
                        self.remove_physical(&key);
                    }
                }
            }
            MappingRecord::Adopt {
                logical,
                physical_key,
            } => {
                self.insert_logical(logical, physical_key, 0, FileOwnership::NotOwned);
            }
            MappingRecord::Tombstone { physical_key } => {
                if let Some(entry) = self.physical.get_mut(physical_key) {
                    entry.tombstoned = true;
                }
                // No entry: the object already drained — `gc_sweep`'s orphan
                // pass owns the leftover bytes.
            }
        }
    }

    /// Inserts/refreshes a logical link. Idempotent: re-inserting the same
    /// (logical, key) pair does not double-count refs.
    fn insert_logical(&mut self, logical: &Path, key: &str, size: u64, ownership: FileOwnership) {
        match self.logical.get(logical) {
            Some(existing) if existing == key => return, // exact replay duplicate
            Some(existing) => {
                // Logical path rebound to a different physical (e.g. file
                // number reuse after restore). Drop the old reference first.
                let old = existing.clone();
                self.apply(&MappingRecord::Unlink {
                    logical: logical.to_path_buf(),
                });
                debug_assert_ne!(old, key);
            }
            None => {}
        }
        self.logical.insert(logical.to_path_buf(), key.to_string());
        let entry = self
            .physical
            .entry(key.to_string())
            .or_insert_with(|| PhysicalEntry {
                size,
                refs: 0,
                ownership,
                tombstoned: false,
            });
        entry.refs += 1;
        if size > 0 {
            entry.size = size;
        }
        if entry.refs == 1 {
            // First reference (re)creates the ownership record.
            if let Some(fnum) = parse_file_number(key) {
                let _ = self.tracker.register(OwnedFile {
                    path: PathBuf::from(key),
                    ownership: entry.ownership,
                    file_number: fnum,
                    file_size: entry.size,
                });
            }
        }
    }

    /// Removes a fully-drained physical entry (refs == 0) from the metadata
    /// maps + ownership tracker. Returns its final entry.
    fn remove_physical(&mut self, key: &str) -> Option<PhysicalEntry> {
        let entry = self.physical.remove(key);
        if entry.is_some() {
            if let Some(fnum) = parse_file_number(key) {
                let _ = self.tracker.unregister(fnum);
            }
        }
        entry
    }
}

/// Parses the engine file number from a physical key whose basename is the
/// canonical `NNNNNN.sst` produced by `sst_file_path`. Non-SST keys (manifest,
/// WAL segments) are mapped but not ownership-tracked.
fn parse_file_number(key: &str) -> Option<FileNumber> {
    let base = Path::new(key).file_name()?.to_str()?;
    let stem = base.strip_suffix(".sst")?;
    stem.parse::<u64>().ok().map(FileNumber)
}

// ---------------------------------------------------------------------------
// Journal codec
// ---------------------------------------------------------------------------

/// Journal file header: magic + format version.
const JOURNAL_MAGIC: &[u8; 4] = b"FRMJ";
const JOURNAL_VERSION: u16 = 1;

/// Snapshot blob header (embedded in CHECKPOINT.blob by the engine).
const SNAPSHOT_MAGIC: &[u8; 4] = b"FRMS";
const SNAPSHOT_VERSION: u16 = 1;

/// Defense-in-depth caps mirroring `checkpoint.rs` (OOM-DoS on crafted input).
const MAX_PATH_LEN: u32 = 64 * 1024;
const MAX_SNAPSHOT_ENTRIES: u32 = 10_000_000;

const OP_REGISTER: u8 = 1;
const OP_LINK: u8 = 2;
const OP_UNLINK: u8 = 3;
const OP_ADOPT: u8 = 4;
const OP_TOMBSTONE: u8 = 5;

fn put_str(buf: &mut Vec<u8>, s: &str) {
    put_fixed32(buf, s.len() as u32);
    buf.extend_from_slice(s.as_bytes());
}

fn get_str(src: &[u8]) -> ForstResult<(String, usize)> {
    let (len, n) = get_fixed32(src)?;
    if len > MAX_PATH_LEN {
        return Err(ForstError::corruption(format!(
            "mapping journal string length {} exceeds cap {}",
            len, MAX_PATH_LEN
        )));
    }
    let len = len as usize;
    if src.len() < n + len {
        return Err(ForstError::corruption("mapping journal string truncated"));
    }
    let s = std::str::from_utf8(&src[n..n + len])
        .map_err(|_| ForstError::corruption("mapping journal string not utf-8"))?
        .to_string();
    Ok((s, n + len))
}

fn path_to_str<'a>(p: &'a Path, context: &str) -> ForstResult<&'a str> {
    p.to_str().ok_or_else(|| {
        ForstError::invalid_argument(format!("{context}: path {} is not utf-8", p.display()))
    })
}

impl MappingRecord {
    /// `[op u8][payload][crc32c u32 over op+payload]`, framed by a leading
    /// `u32` total-length so a truncated tail append is detectable.
    fn encode(&self) -> ForstResult<Vec<u8>> {
        let mut body = Vec::with_capacity(64);
        match self {
            MappingRecord::Register {
                logical,
                physical_key,
                size,
            } => {
                body.push(OP_REGISTER);
                put_str(&mut body, path_to_str(logical, "register")?);
                put_str(&mut body, physical_key);
                put_fixed64(&mut body, *size);
            }
            MappingRecord::Link {
                dst_logical,
                physical_key,
            } => {
                body.push(OP_LINK);
                put_str(&mut body, path_to_str(dst_logical, "link")?);
                put_str(&mut body, physical_key);
            }
            MappingRecord::Unlink { logical } => {
                body.push(OP_UNLINK);
                put_str(&mut body, path_to_str(logical, "unlink")?);
            }
            MappingRecord::Adopt {
                logical,
                physical_key,
            } => {
                body.push(OP_ADOPT);
                put_str(&mut body, path_to_str(logical, "adopt")?);
                put_str(&mut body, physical_key);
            }
            MappingRecord::Tombstone { physical_key } => {
                body.push(OP_TOMBSTONE);
                put_str(&mut body, physical_key);
            }
        }
        let crc = crc32c(&body);
        let mut out = Vec::with_capacity(body.len() + 8);
        put_fixed32(&mut out, (body.len() + 4) as u32); // body + crc
        out.extend_from_slice(&body);
        put_fixed32(&mut out, crc);
        Ok(out)
    }

    /// Decodes one record from `src`. Returns `(record, bytes_consumed)`;
    /// `Ok(None)` on a cleanly-truncated tail (crash mid-append).
    fn decode(src: &[u8]) -> ForstResult<Option<(MappingRecord, usize)>> {
        if src.is_empty() {
            return Ok(None);
        }
        if src.len() < 4 {
            return Ok(None); // truncated length prefix == crash tail
        }
        let (frame_len, n) = get_fixed32(src)?;
        if frame_len > MAX_PATH_LEN * 4 {
            return Err(ForstError::corruption(format!(
                "mapping journal frame length {} implausible",
                frame_len
            )));
        }
        let frame_len = frame_len as usize;
        if src.len() < n + frame_len {
            return Ok(None); // truncated frame == crash tail
        }
        let frame = &src[n..n + frame_len];
        if frame_len < 5 {
            return Err(ForstError::corruption("mapping journal frame too small"));
        }
        let (body, crc_bytes) = frame.split_at(frame_len - 4);
        let (stored_crc, _) = get_fixed32(crc_bytes)?;
        if stored_crc != crc32c(body) {
            return Err(ForstError::corruption(
                "mapping journal record checksum mismatch",
            ));
        }
        let op = body[0];
        let mut pos = 1usize;
        let rec = match op {
            OP_REGISTER => {
                let (logical, n) = get_str(&body[pos..])?;
                pos += n;
                let (key, n) = get_str(&body[pos..])?;
                pos += n;
                let (size, _) = get_fixed64(&body[pos..])?;
                MappingRecord::Register {
                    logical: PathBuf::from(logical),
                    physical_key: key,
                    size,
                }
            }
            OP_LINK => {
                let (dst, n) = get_str(&body[pos..])?;
                pos += n;
                let (key, _) = get_str(&body[pos..])?;
                MappingRecord::Link {
                    dst_logical: PathBuf::from(dst),
                    physical_key: key,
                }
            }
            OP_UNLINK => {
                let (logical, _) = get_str(&body[pos..])?;
                MappingRecord::Unlink {
                    logical: PathBuf::from(logical),
                }
            }
            OP_ADOPT => {
                let (logical, n) = get_str(&body[pos..])?;
                pos += n;
                let (key, _) = get_str(&body[pos..])?;
                MappingRecord::Adopt {
                    logical: PathBuf::from(logical),
                    physical_key: key,
                }
            }
            OP_TOMBSTONE => {
                let (key, _) = get_str(&body[pos..])?;
                MappingRecord::Tombstone { physical_key: key }
            }
            other => {
                return Err(ForstError::corruption(format!(
                    "mapping journal unknown op {}",
                    other
                )))
            }
        };
        Ok(Some((rec, n + frame_len)))
    }
}

/// Replays a record region into `state`. Stops cleanly at a truncated tail
/// (crash mid-append); surfaces mid-stream corruption as an error.
fn replay_records(state: &mut MappingState, mut bytes: &[u8]) -> ForstResult<usize> {
    let mut applied = 0usize;
    while let Some((rec, consumed)) = MappingRecord::decode(bytes)? {
        state.apply(&rec);
        bytes = &bytes[consumed..];
        applied += 1;
    }
    Ok(applied)
}

// ---------------------------------------------------------------------------
// FileMappingManager
// ---------------------------------------------------------------------------

/// Durable logical→physical file mapping with refcounts (design §2.2).
///
/// Thread-safe: a single internal mutex orders every metadata mutation,
/// journal append AND the refs==0 physical delete, so a concurrent
/// `link(working → chk)` can never observe bytes deleted by a racing
/// `unlink(working)` — either the link wins (refs > 0, bytes kept) or it
/// loses with `NotFound` (working path already unlinked). This is the
/// link-vs-compaction-delete race gate from design §5 Stage-1.
pub struct FileMappingManager {
    fs: Arc<dyn FileSystem>,
    journal_path: PathBuf,
    inner: Mutex<Inner>,
}

struct Inner {
    state: MappingState,
    /// Open append handle; lazily (re)opened after `restore_snapshot`.
    journal: Option<Box<dyn WritableFile>>,
}

impl FileMappingManager {
    /// Opens (or creates) a mapping manager whose journal lives at
    /// `journal_path` on `fs` (design: "next to the working dir on DFS").
    /// An existing journal is replayed; a truncated tail record (crash
    /// mid-append) is tolerated and dropped.
    pub fn new(fs: Arc<dyn FileSystem>, journal_path: PathBuf) -> ForstResult<Self> {
        let mut state = MappingState::default();
        if fs.file_exists(&journal_path)? {
            let bytes = read_all(fs.as_ref(), &journal_path)?;
            if bytes.len() >= 6 {
                if &bytes[..4] != JOURNAL_MAGIC {
                    return Err(ForstError::corruption("mapping journal bad magic"));
                }
                let version = u16::from_le_bytes([bytes[4], bytes[5]]);
                if version != JOURNAL_VERSION {
                    return Err(ForstError::corruption(format!(
                        "mapping journal unsupported version {version}"
                    )));
                }
                replay_records(&mut state, &bytes[6..])?;
            }
        }
        Ok(Self {
            fs,
            journal_path,
            inner: Mutex::new(Inner {
                state,
                journal: None,
            }),
        })
    }

    /// Working-dir create: identity mapping, refs = 1, ownership
    /// [`FileOwnership::ShareableOwnedByDb`]. Idempotent for the same
    /// `(logical, physical_key)` pair; rebinding a logical path to a
    /// different physical drops the old reference first.
    pub fn register(&self, logical: &Path, physical_key: &str, size: u64) -> ForstResult<()> {
        let rec = MappingRecord::Register {
            logical: logical.to_path_buf(),
            physical_key: physical_key.to_string(),
            size,
        };
        let mut inner = self.inner.lock().expect("lock poisoned");
        if inner.state.logical.get(logical).map(String::as_str) == Some(physical_key) {
            return Ok(()); // exact duplicate — keep the journal compact
        }
        self.append_journal(&mut inner, &rec)?;
        inner.state.apply(&rec);
        Ok(())
    }

    /// Checkpoint link: maps `dst_logical` to `src_logical`'s physical object
    /// — refs += 1, O(metadata), zero data movement (paper §5.1).
    pub fn link(&self, src_logical: &Path, dst_logical: &Path) -> ForstResult<()> {
        let mut inner = self.inner.lock().expect("lock poisoned");
        let key = inner
            .state
            .logical
            .get(src_logical)
            .cloned()
            .ok_or_else(|| {
                ForstError::not_found(format!(
                    "link source {} is not a mapped logical path",
                    src_logical.display()
                ))
            })?;
        if let Some(existing) = inner.state.logical.get(dst_logical) {
            if *existing == key {
                return Ok(()); // idempotent re-link
            }
            return Err(ForstError::invalid_argument(format!(
                "link destination {} already mapped to a different physical",
                dst_logical.display()
            )));
        }
        let rec = MappingRecord::Link {
            dst_logical: dst_logical.to_path_buf(),
            physical_key: key,
        };
        self.append_journal(&mut inner, &rec)?;
        inner.state.apply(&rec);
        Ok(())
    }

    /// Drops one logical reference. At refs == 0 the physical delete is
    /// delegated to the backing FS (exactly once) — unless the object is
    /// [`FileOwnership::NotOwned`] and not tombstoned, in which case the
    /// bytes are kept for their external owner.
    ///
    /// Journal-then-delete ordering: the unlink record is durable BEFORE the
    /// physical delete, so a crash in between leaves an orphan object (reaped
    /// by [`Self::gc_sweep`]) rather than a mapping that references missing
    /// bytes (design risk R1: leak over data-loss).
    pub fn unlink(&self, logical: &Path) -> ForstResult<UnlinkOutcome> {
        let mut inner = self.inner.lock().expect("lock poisoned");
        self.unlink_locked(&mut inner, logical)
    }

    /// [`Self::unlink`] body with the lock already held — composite metadata
    /// ops ([`Self::rename_logical`], [`Self::sweep_temp_logicals`]) call this
    /// so their multi-record sequences are atomic under the single mutex.
    fn unlink_locked(&self, inner: &mut Inner, logical: &Path) -> ForstResult<UnlinkOutcome> {
        let key = inner.state.logical.get(logical).cloned().ok_or_else(|| {
            ForstError::not_found(format!(
                "unlink: {} is not a mapped logical path",
                logical.display()
            ))
        })?;
        let rec = MappingRecord::Unlink {
            logical: logical.to_path_buf(),
        };
        self.append_journal(inner, &rec)?;
        // Capture the entry BEFORE apply removes it at refs==0.
        let entry = inner
            .state
            .physical
            .get(&key)
            .cloned()
            .expect("physical invariant");
        inner.state.apply(&rec);
        if let Some(remaining) = inner.state.physical.get(&key) {
            return Ok(UnlinkOutcome::Retained {
                refs_remaining: remaining.refs,
            });
        }
        // Refs drained to zero. Decide the physical fate.
        if entry.ownership == FileOwnership::NotOwned && !entry.tombstoned {
            return Ok(UnlinkOutcome::KeptNotOwned);
        }
        // Exactly-once: the entry was removed from the map under this lock,
        // so no other thread can reach this branch for the same key again.
        self.fs.delete_file(Path::new(&key))?;
        Ok(UnlinkOutcome::PhysicalDeleted)
    }

    /// FRS-PHASE2-C3U1 (competitive analysis §2.2d, ForSt `toUUIDPath`):
    /// mints a fresh UUID-v4 physical key for `logical` — same parent
    /// directory, `uuid-<32 hex>.sst` basename. The `.sst` suffix is kept
    /// REGARDLESS of the logical's own suffix (a `.NNNNNN.sst.tmp` staging
    /// file mints a `.sst`-suffixed physical) so [`Self::gc_sweep`]'s
    /// `.sst` orphan filter covers every minted object, and the later
    /// staging→final rename is a pure metadata re-point
    /// ([`Self::rename_logical`]) — never a remote rename (S3 has none).
    /// Pure: does not touch the mapping; pair with [`Self::register`].
    pub fn mint_physical_key(logical: &Path) -> ForstResult<String> {
        let parent = logical.parent().unwrap_or_else(|| Path::new(""));
        let key = parent.join(format!("uuid-{}.sst", uuid::Uuid::new_v4().simple()));
        Ok(path_to_str(&key, "mint_physical_key")?.to_string())
    }

    /// FRS-PHASE2-C3U1: POSIX-rename semantics as a pure metadata operation —
    /// `dst` is re-pointed at `src`'s physical object and `src` is dropped,
    /// with ZERO remote ops (the rename-free invariant for UUID-keyed
    /// physicals). Atomic under the single mapping mutex. If `dst` was
    /// already mapped to a DIFFERENT physical its old reference is dropped
    /// first with full physical fate (refs drain ⇒ exactly-once delete) —
    /// the crashed-flush file-number-reuse window: the stale object is
    /// orphaned bytes nothing references. `src` unmapped ⇒ `NotFound`.
    pub fn rename_logical(&self, src: &Path, dst: &Path) -> ForstResult<()> {
        let mut inner = self.inner.lock().expect("lock poisoned");
        let key = inner.state.logical.get(src).cloned().ok_or_else(|| {
            ForstError::not_found(format!(
                "rename_logical: source {} is not a mapped logical path",
                src.display()
            ))
        })?;
        if let Some(existing) = inner.state.logical.get(dst).cloned() {
            if existing == key {
                // dst already points at the same physical: dropping src
                // cannot drain it (dst holds a reference).
                self.unlink_locked(&mut inner, src)?;
                return Ok(());
            }
            self.unlink_locked(&mut inner, dst)?;
        }
        // Link dst BEFORE unlinking src so the physical's refcount never
        // dips to zero mid-rename (no delete window).
        let link = MappingRecord::Link {
            dst_logical: dst.to_path_buf(),
            physical_key: key,
        };
        self.append_journal(&mut inner, &link)?;
        inner.state.apply(&link);
        let unlink = MappingRecord::Unlink {
            logical: src.to_path_buf(),
        };
        self.append_journal(&mut inner, &unlink)?;
        inner.state.apply(&unlink);
        Ok(())
    }

    /// FRS-PHASE2-C3U1 startup sweep: unlinks every mapped logical path whose
    /// basename ends in `.tmp` — flush/compaction staging names
    /// (`.NNNNNN.sst.tmp`) that can only exist in the mapping if a writer
    /// crashed between the UUID mint and the staging→final rename. A `.tmp`
    /// logical is transient by construction, so at manager-(re)open time it is
    /// dead: its working ref would otherwise pin the minted physical forever
    /// (refs never drain — nothing renames or deletes a crashed staging path).
    /// Physical fate follows [`Self::unlink`] (refs drain ⇒ exactly-once
    /// delete). Returns the number of staging mappings swept.
    pub fn sweep_temp_logicals(&self) -> ForstResult<usize> {
        let mut inner = self.inner.lock().expect("lock poisoned");
        let mut stale: Vec<PathBuf> = inner
            .state
            .logical
            .keys()
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.ends_with(".tmp"))
            })
            .cloned()
            .collect();
        stale.sort();
        let swept = stale.len();
        for path in stale {
            self.unlink_locked(&mut inner, &path)?;
        }
        Ok(swept)
    }

    /// Restore link: adopts an existing external physical object under a new
    /// logical path. Enters as [`FileOwnership::NotOwned`] (design §2.2) —
    /// unlink keeps the bytes unless a tombstone says otherwise.
    pub fn adopt(&self, logical: &Path, physical_key: &str) -> ForstResult<()> {
        if !self.fs.file_exists(Path::new(physical_key))? {
            return Err(ForstError::not_found(format!(
                "adopt: physical object {} does not exist",
                physical_key
            )));
        }
        let rec = MappingRecord::Adopt {
            logical: logical.to_path_buf(),
            physical_key: physical_key.to_string(),
        };
        let mut inner = self.inner.lock().expect("lock poisoned");
        if inner.state.logical.get(logical).map(String::as_str) == Some(physical_key) {
            return Ok(());
        }
        self.append_journal(&mut inner, &rec)?;
        inner.state.apply(&rec);
        Ok(())
    }

    /// JM-discard protocol (design §2.3): marks a physical object for
    /// deletion once its refcount drains. Never a direct S3 delete from the
    /// JM — if refs are still held the bytes survive until the last unlink.
    /// Returns `true` if the bytes were deleted immediately (refs already 0).
    pub fn tombstone(&self, physical_key: &str) -> ForstResult<bool> {
        let rec = MappingRecord::Tombstone {
            physical_key: physical_key.to_string(),
        };
        let mut inner = self.inner.lock().expect("lock poisoned");
        self.append_journal(&mut inner, &rec)?;
        inner.state.apply(&rec);
        if inner.state.physical.contains_key(physical_key) {
            return Ok(false); // refs > 0: deferred to the draining unlink
        }
        // No live references — reap now if the object still exists.
        if self.fs.file_exists(Path::new(physical_key))? {
            self.fs.delete_file(Path::new(physical_key))?;
            return Ok(true);
        }
        Ok(false)
    }

    /// Resolves a logical path to its physical key (read-path indirection).
    pub fn resolve(&self, logical: &Path) -> Option<String> {
        self.inner
            .lock()
            .expect("lock poisoned")
            .state
            .logical
            .get(logical)
            .cloned()
    }

    /// Returns whether `logical` is currently mapped.
    pub fn is_registered(&self, logical: &Path) -> bool {
        self.inner
            .lock()
            .expect("lock poisoned")
            .state
            .logical
            .contains_key(logical)
    }

    /// Current refcount of a physical key (0 if unknown).
    pub fn refs(&self, physical_key: &str) -> u32 {
        self.inner
            .lock()
            .expect("lock poisoned")
            .state
            .physical
            .get(physical_key)
            .map(|e| e.refs)
            .unwrap_or(0)
    }

    /// Ownership of a physical key, if tracked.
    pub fn ownership_of(&self, physical_key: &str) -> Option<FileOwnership> {
        self.inner
            .lock()
            .expect("lock poisoned")
            .state
            .physical
            .get(physical_key)
            .map(|e| e.ownership)
    }

    /// FRS-PHASE2-C2U2: whether a physical key carries a JM-discard
    /// tombstone (delete-on-drain). Restore-side guard: an instant restore
    /// must refuse to adopt a tombstoned physical — its bytes are scheduled
    /// for deletion the moment the surviving refs drain.
    pub fn is_tombstoned(&self, physical_key: &str) -> bool {
        self.inner
            .lock()
            .expect("lock poisoned")
            .state
            .physical
            .get(physical_key)
            .map(|e| e.tombstoned)
            .unwrap_or(false)
    }

    /// FRS-PHASE2-C2U2 (design §9 D5 crash-window a): every CURRENTLY-mapped
    /// logical path strictly under `prefix`, sorted for determinism. The
    /// startup sweep uses this to enumerate chk-namespace links
    /// (`<db_path>/checkpoints/<id>/...`) straight from the journal-replayed
    /// state — which is exactly what survives a crash BETWEEN
    /// `sync_journal()` and the blob write (links exist, no blob, JM never
    /// acked the id).
    pub fn logical_paths_under(&self, prefix: &Path) -> Vec<PathBuf> {
        let inner = self.inner.lock().expect("lock poisoned");
        let mut out: Vec<PathBuf> = inner
            .state
            .logical
            .keys()
            .filter(|p| p.starts_with(prefix) && p.as_path() != prefix)
            .cloned()
            .collect();
        out.sort();
        out
    }

    /// Number of live logical mappings.
    pub fn len(&self) -> usize {
        self.inner.lock().expect("lock poisoned").state.logical.len()
    }

    /// True when no logical mappings exist.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Serializes the full mapping state (consistent snapshot) for embedding
    /// into the engine's `CHECKPOINT.blob` (design §2.4). Counterpart:
    /// [`Self::restore_snapshot`].
    pub fn snapshot_bytes(&self) -> ForstResult<Vec<u8>> {
        let inner = self.inner.lock().expect("lock poisoned");
        let state = &inner.state;
        let mut buf = Vec::with_capacity(64 + state.logical.len() * 64);
        buf.extend_from_slice(SNAPSHOT_MAGIC);
        buf.extend_from_slice(&SNAPSHOT_VERSION.to_le_bytes());
        // Deterministic order: sort for byte-stable snapshots.
        let mut logical: Vec<(&PathBuf, &String)> = state.logical.iter().collect();
        logical.sort();
        put_fixed32(&mut buf, logical.len() as u32);
        for (path, key) in logical {
            put_str(&mut buf, path_to_str(path, "snapshot")?);
            put_str(&mut buf, key);
        }
        let mut physical: Vec<(&String, &PhysicalEntry)> = state.physical.iter().collect();
        physical.sort_by_key(|(k, _)| k.as_str());
        put_fixed32(&mut buf, physical.len() as u32);
        for (key, entry) in physical {
            put_str(&mut buf, key);
            put_fixed64(&mut buf, entry.size);
            buf.push(match entry.ownership {
                FileOwnership::PrivateOwnedByDb => 0,
                FileOwnership::ShareableOwnedByDb => 1,
                FileOwnership::NotOwned => 2,
            });
            buf.push(entry.tombstoned as u8);
        }
        let crc = crc32c(&buf);
        put_fixed32(&mut buf, crc);
        Ok(buf)
    }

    /// Replaces the in-memory state from a snapshot produced by
    /// [`Self::snapshot_bytes`] and REWRITES the journal to match (the
    /// snapshot becomes the new journal base — replay after restore sees a
    /// consistent prefix).
    pub fn restore_snapshot(&self, bytes: &[u8]) -> ForstResult<()> {
        let state = decode_snapshot(bytes)?;
        let mut inner = self.inner.lock().expect("lock poisoned");
        // Rewrite the journal as: header + one Register/Link per mapping.
        inner.journal = None;
        if let Some(parent) = self.journal_path.parent() {
            self.fs.create_dir_all(parent)?;
        }
        let mut writer = self
            .fs
            .open_writable_file(&self.journal_path, WriteMode::CreateOrTruncate)?;
        let mut header = Vec::with_capacity(6);
        header.extend_from_slice(JOURNAL_MAGIC);
        header.extend_from_slice(&JOURNAL_VERSION.to_le_bytes());
        writer.append(&header)?;
        let mut sorted: Vec<(&PathBuf, &String)> = state.logical.iter().collect();
        sorted.sort();
        // FRS-PHASE2-C3U1: the FIRST logical of each DB-owned physical is
        // rewritten as Register (it carries size + ShareableOwnedByDb on
        // replay), subsequent logicals as Link. The old identity test
        // (`path == key`) silently downgraded UUID-keyed working files to
        // Link records, losing their size on the next journal replay.
        let mut registered: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for (path, key) in sorted {
            let entry = state.physical.get(key.as_str()).expect("snapshot invariant");
            let rec = if entry.ownership == FileOwnership::NotOwned {
                MappingRecord::Adopt {
                    logical: path.clone(),
                    physical_key: key.clone(),
                }
            } else if registered.insert(key.as_str()) {
                MappingRecord::Register {
                    logical: path.clone(),
                    physical_key: key.clone(),
                    size: entry.size,
                }
            } else {
                MappingRecord::Link {
                    dst_logical: path.clone(),
                    physical_key: key.clone(),
                }
            };
            writer.append(&rec.encode()?)?;
        }
        for (key, entry) in state.physical.iter() {
            if entry.tombstoned {
                writer.append(
                    &MappingRecord::Tombstone {
                        physical_key: key.clone(),
                    }
                    .encode()?,
                )?;
            }
        }
        writer.sync()?;
        // FRS-PHASE2-C3U1: same as `sync_journal` — a synced object-store
        // writer is closed; drop it and let the next append reopen.
        drop(writer);
        inner.journal = None;
        inner.state = state;
        Ok(())
    }

    /// Crash-GC sweep (design §2.4): lists `dir` on the backing FS and reaps
    /// every `.sst` object that no live mapping (refs > 0) references —
    /// orphans from a crash between journal append and physical delete, and
    /// drained tombstones. HARD-STOP guarantee: an object with refs > 0 is
    /// never deleted.
    pub fn gc_sweep(&self, dir: &Path) -> ForstResult<GcReport> {
        let listing = self.fs.list_dir(dir)?;
        let mut report = GcReport::default();
        let inner = self.inner.lock().expect("lock poisoned");
        for meta in listing {
            if meta.is_dir {
                continue;
            }
            let is_sst = meta
                .path
                .extension()
                .map(|e| e == "sst")
                .unwrap_or(false);
            if !is_sst {
                continue;
            }
            let key = match meta.path.to_str() {
                Some(k) => k.to_string(),
                None => continue,
            };
            match inner.state.physical.get(&key) {
                Some(entry) if entry.refs > 0 => {
                    // HARD-STOP: live reference — never reaped.
                    report.kept_live += 1;
                }
                _ => {
                    self.fs.delete_file(&meta.path)?;
                    report.reaped.push(key);
                }
            }
        }
        Ok(report)
    }

    /// Syncs the journal append handle (group-durability point).
    ///
    /// FRS-PHASE2-C3U1: the handle is DROPPED after the sync — object-store
    /// `WritableFile::sync` CLOSES the writer (close is what publishes the
    /// object), so keeping it would fail the next append with
    /// "append after close". The next mutation lazily reopens in `Append`
    /// mode (real append on local FS; read-existing+rewrite emulation on
    /// object stores), which is exactly the recovery the journal format is
    /// built for. One reopen per checkpoint cycle — metadata-cheap.
    pub fn sync_journal(&self) -> ForstResult<()> {
        let mut inner = self.inner.lock().expect("lock poisoned");
        if let Some(w) = inner.journal.as_mut() {
            w.sync()?;
            inner.journal = None;
        }
        Ok(())
    }

    /// Appends one record to the journal, opening (and headering) it lazily.
    fn append_journal(&self, inner: &mut Inner, rec: &MappingRecord) -> ForstResult<()> {
        if inner.journal.is_none() {
            if let Some(parent) = self.journal_path.parent() {
                self.fs.create_dir_all(parent)?;
            }
            let exists = self.fs.file_exists(&self.journal_path)?;
            let mut writer = self
                .fs
                .open_writable_file(&self.journal_path, WriteMode::Append)?;
            if !exists {
                let mut header = Vec::with_capacity(6);
                header.extend_from_slice(JOURNAL_MAGIC);
                header.extend_from_slice(&JOURNAL_VERSION.to_le_bytes());
                writer.append(&header)?;
            }
            inner.journal = Some(writer);
        }
        let encoded = rec.encode()?;
        let writer = inner.journal.as_mut().expect("just set");
        writer.append(&encoded)?;
        writer.flush()?;
        Ok(())
    }
}

impl std::fmt::Debug for FileMappingManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.lock().expect("lock poisoned");
        f.debug_struct("FileMappingManager")
            .field("journal_path", &self.journal_path)
            .field("logical_mappings", &inner.state.logical.len())
            .field("physical_objects", &inner.state.physical.len())
            .finish()
    }
}

/// FRS-PHASE2-S2 (design §9 D7): a read-only view over a mapping snapshot
/// produced by [`FileMappingManager::snapshot_bytes`] (e.g. the trailer
/// embedded in `CHECKPOINT.blob`). The Stage-2 minimal restore uses it to
/// resolve `<chk-k>/NNNNNN.sst` logical paths to their physical keys WITHOUT
/// instantiating a journal-backed manager on the restore target.
pub struct MappingSnapshotView {
    logical: HashMap<PathBuf, String>,
}

impl MappingSnapshotView {
    /// Decodes a snapshot blob (CRC + magic validated; corruption rejected).
    pub fn decode(bytes: &[u8]) -> ForstResult<Self> {
        let state = decode_snapshot(bytes)?;
        Ok(Self {
            logical: state.logical,
        })
    }

    /// Resolves a logical path to its physical key.
    pub fn resolve(&self, logical: &Path) -> Option<&str> {
        self.logical.get(logical).map(String::as_str)
    }

    /// FRS-PHASE2-C2U3: every (logical, physical) entry strictly under
    /// `prefix`, sorted by logical path. The restore side uses it to
    /// enumerate a checkpoint namespace's linked artifacts (e.g. Phase-5
    /// `WAL-NNNNNN.seg` sealed-segment links) without an FS listing — the
    /// linked paths are metadata-only.
    pub fn paths_under(&self, prefix: &Path) -> Vec<(PathBuf, &str)> {
        let mut out: Vec<(PathBuf, &str)> = self
            .logical
            .iter()
            .filter(|(p, _)| p.starts_with(prefix) && p.as_path() != prefix)
            .map(|(p, k)| (p.clone(), k.as_str()))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// Number of logical mappings in the snapshot.
    pub fn len(&self) -> usize {
        self.logical.len()
    }

    /// True when the snapshot carries no logical mappings.
    pub fn is_empty(&self) -> bool {
        self.logical.is_empty()
    }
}

/// FRS-PHASE2-C2U2 (design §2.4 "journal tail replayed on restore"): a
/// READ-ONLY view over a mapping journal's CURRENT state — full replay,
/// including every record appended AFTER the snapshot that a checkpoint blob
/// froze in its trailer. The restore side consults it (when the source
/// journal is reachable on the engine FS) for the truths the blob cannot
/// carry: post-checkpoint JM-discard **tombstones** (a tombstoned physical
/// must never be adopted) and post-checkpoint link/unlink churn.
///
/// Never appends — loading this view cannot mutate the source journal (the
/// restoring process does not own it).
pub struct MappingJournalView {
    state: MappingState,
}

impl MappingJournalView {
    /// Replays the journal at `journal_path` into a read-only view. Returns
    /// `Ok(None)` when no journal exists (legacy / relocated checkpoint —
    /// callers fall back to the blob snapshot alone). A truncated tail
    /// record (crash mid-append) is tolerated exactly like
    /// [`FileMappingManager::new`]; mid-stream corruption is an error.
    pub fn load(fs: &dyn FileSystem, journal_path: &Path) -> ForstResult<Option<Self>> {
        if !fs.file_exists(journal_path)? {
            return Ok(None);
        }
        let bytes = read_all(fs, journal_path)?;
        let mut state = MappingState::default();
        if bytes.len() >= 6 {
            if &bytes[..4] != JOURNAL_MAGIC {
                return Err(ForstError::corruption("mapping journal bad magic"));
            }
            let version = u16::from_le_bytes([bytes[4], bytes[5]]);
            if version != JOURNAL_VERSION {
                return Err(ForstError::corruption(format!(
                    "mapping journal unsupported version {version}"
                )));
            }
            replay_records(&mut state, &bytes[6..])?;
        }
        Ok(Some(Self { state }))
    }

    /// Resolves a logical path to its physical key in the journal-current
    /// state.
    pub fn resolve(&self, logical: &Path) -> Option<&str> {
        self.state.logical.get(logical).map(String::as_str)
    }

    /// Whether `logical` is mapped in the journal-current state.
    pub fn is_registered(&self, logical: &Path) -> bool {
        self.state.logical.contains_key(logical)
    }

    /// Whether `physical_key` carries a live JM-discard tombstone.
    pub fn is_tombstoned(&self, physical_key: &str) -> bool {
        self.state
            .physical
            .get(physical_key)
            .map(|e| e.tombstoned)
            .unwrap_or(false)
    }

    /// Number of live logical mappings in the view.
    pub fn len(&self) -> usize {
        self.state.logical.len()
    }

    /// True when the view carries no logical mappings.
    pub fn is_empty(&self) -> bool {
        self.state.logical.is_empty()
    }
}

/// FRS-PHASE2-S3 (design §5 Stage-3): the UFS read-path indirection — a thin
/// [`FileSystem`] layer that resolves *logical* paths through a
/// [`FileMappingManager`] before delegating to the backing filesystem
/// (paper §5.1: "efficient move or link operations without necessitating
/// physical file relocation").
///
/// Used by the instant-link restore: adopted SSTs live at the SOURCE
/// checkpoint's physical keys; the restored engine keeps addressing its
/// canonical working-dir paths (`<target>/NNNNNN.sst`) and this layer routes
/// the reads to the physical objects — zero downloads, zero copies.
///
/// Resolution applies to READ-side operations only (`open_sequential_file`,
/// `open_random_access_file`, `file_exists`, `get_file_metadata`,
/// `ensure_cached`, `prefetch_concurrent`, `await_upload`,
/// `pre_seed_admission`). Write/namespace operations (`open_writable_file`,
/// `delete_file`, `rename`, `list_dir`, dirs) pass through UNresolved: new
/// files are identity-mapped working files, and physical deletion is governed
/// exclusively by the mapping layer's `unlink` refcounts — a passthrough
/// `delete_file` on a mapped-but-byteless logical path can never reach the
/// shared physical object.
///
/// Unmapped paths pass through untouched, so the layer is transparent for
/// everything except adopted/linked files (identity mappings resolve to
/// themselves).
pub struct MappedFileSystem {
    inner: Arc<dyn FileSystem>,
    mapping: Arc<FileMappingManager>,
    /// FRS-PHASE2-C3U1 (default OFF — [`Self::new`] keeps the read-only
    /// indirection byte-identical to pre-C3U1): UUID-keyed write-side
    /// indirection. When set, SST-class creates mint a `uuid-<hex>.sst`
    /// physical key ([`FileMappingManager::mint_physical_key`]) and the
    /// bytes are written THERE; renames of mapped paths become metadata
    /// re-points ([`FileMappingManager::rename_logical`]) and deletes of
    /// mapped paths route through refcounted `unlink` — no remote rename
    /// ever reaches the backend for the SST lifecycle (ForSt `toUUIDPath`,
    /// competitive analysis §2.2d).
    uuid_keys: bool,
}

impl MappedFileSystem {
    /// Wraps `inner`, resolving logical paths through `mapping`.
    pub fn new(inner: Arc<dyn FileSystem>, mapping: Arc<FileMappingManager>) -> Self {
        Self {
            inner,
            mapping,
            uuid_keys: false,
        }
    }

    /// FRS-PHASE2-C3U1: [`Self::new`] with UUID physical keys enabled for
    /// SST-class writes (see the `uuid_keys` field doc). Sweeps crashed
    /// staging mappings first ([`FileMappingManager::sweep_temp_logicals`]):
    /// a `.tmp` logical surviving in the journal means a writer died between
    /// mint and rename — its mapping would otherwise pin the minted physical
    /// forever AND fail the engine's `CreateNew` re-stage after a
    /// file-number-reusing restart.
    pub fn with_uuid_physical_keys(
        inner: Arc<dyn FileSystem>,
        mapping: Arc<FileMappingManager>,
    ) -> ForstResult<Self> {
        mapping.sweep_temp_logicals()?;
        Ok(Self {
            inner,
            mapping,
            uuid_keys: true,
        })
    }

    /// Whether UUID-keyed write indirection is active.
    pub fn uuid_physical_keys(&self) -> bool {
        self.uuid_keys
    }

    /// Resolves `path` to its physical location, or returns it unchanged
    /// when unmapped (or identity-mapped).
    fn resolve(&self, path: &Path) -> PathBuf {
        match self.mapping.resolve(path) {
            Some(physical) => PathBuf::from(physical),
            None => path.to_path_buf(),
        }
    }

    /// SST-class paths get UUID physical keys: the canonical `NNNNNN.sst`
    /// working name AND its `.NNNNNN.sst.tmp` staging sibling (basename
    /// containing `.sst`). Everything else (MANIFEST, CURRENT, blob, WAL
    /// segments, journals) writes through at its literal path.
    fn is_sst_class(path: &Path) -> bool {
        path.file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.contains(".sst"))
    }

    /// Mints + registers a fresh UUID physical for `logical`, then opens the
    /// writer AT the physical key. Journal-before-bytes: a crash after the
    /// mint leaves a mapping to a missing/partial object — the staging sweep
    /// (`.tmp`) or rebind-on-number-reuse reaps it; never data loss (R1).
    fn mint_and_open(
        &self,
        logical: &Path,
        mode: WriteMode,
    ) -> ForstResult<Box<dyn WritableFile>> {
        let key = FileMappingManager::mint_physical_key(logical)?;
        self.mapping.register(logical, &key, 0)?;
        self.inner.open_writable_file(Path::new(&key), mode)
    }
}

impl std::fmt::Debug for MappedFileSystem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MappedFileSystem")
            .field("inner", &self.inner.name())
            .finish()
    }
}

impl FileSystem for MappedFileSystem {
    fn open_sequential_file(
        &self,
        path: &Path,
    ) -> ForstResult<Box<dyn crate::filesystem::SequentialFile>> {
        self.inner.open_sequential_file(&self.resolve(path))
    }

    fn open_random_access_file(
        &self,
        path: &Path,
    ) -> ForstResult<Box<dyn crate::filesystem::RandomAccessFile>> {
        self.inner.open_random_access_file(&self.resolve(path))
    }

    fn open_writable_file(
        &self,
        path: &Path,
        mode: WriteMode,
    ) -> ForstResult<Box<dyn WritableFile>> {
        if !self.uuid_keys || !Self::is_sst_class(path) {
            return self.inner.open_writable_file(path, mode);
        }
        if let Some(existing) = self.mapping.resolve(path) {
            return match mode {
                // Appends continue at the mapped physical.
                WriteMode::Append => self
                    .inner
                    .open_writable_file(Path::new(&existing), WriteMode::Append),
                WriteMode::CreateNew => Err(ForstError::Io(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    format!(
                        "open_writable_file(CreateNew): {} is already mapped to {}",
                        path.display(),
                        existing
                    ),
                ))),
                // NEVER truncate a (possibly checkpoint-shared) physical in
                // place: drop this logical's reference (refcount decides the
                // old bytes' fate) and mint a fresh object.
                WriteMode::CreateOrTruncate => {
                    self.mapping.unlink(path)?;
                    self.mint_and_open(path, WriteMode::CreateOrTruncate)
                }
            };
        }
        // CreateNew semantics also cover a pre-UUID identity file sitting at
        // the literal path (mixed-mode migration: old identity objects and
        // new UUID objects coexist; mapping-layer only).
        if mode == WriteMode::CreateNew && self.inner.file_exists(path)? {
            return Err(ForstError::Io(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!(
                    "open_writable_file(CreateNew): {} already exists",
                    path.display()
                ),
            )));
        }
        self.mint_and_open(path, mode)
    }

    fn file_exists(&self, path: &Path) -> ForstResult<bool> {
        self.inner.file_exists(&self.resolve(path))
    }

    fn get_file_metadata(&self, path: &Path) -> ForstResult<crate::filesystem::FileMetadata> {
        self.inner.get_file_metadata(&self.resolve(path))
    }

    fn list_dir(&self, dir: &Path) -> ForstResult<Vec<crate::filesystem::FileMetadata>> {
        self.inner.list_dir(dir)
    }

    fn create_dir_all(&self, dir: &Path) -> ForstResult<()> {
        self.inner.create_dir_all(dir)
    }

    fn delete_file(&self, path: &Path) -> ForstResult<()> {
        // FRS-PHASE2-C3U1: under UUID keys a mapped logical has no bytes at
        // its literal path — the delete IS the refcounted unlink (physical
        // gone exactly once, at the last reference). Default mode keeps the
        // historical passthrough (a mapped-but-byteless logical delete can
        // never reach the shared physical object).
        if self.uuid_keys && self.mapping.is_registered(path) {
            return self.mapping.unlink(path).map(|_| ());
        }
        self.inner.delete_file(path)
    }

    fn delete_dir(&self, path: &Path, recursive: bool) -> ForstResult<()> {
        self.inner.delete_dir(path, recursive)
    }

    fn rename(&self, src: &Path, dst: &Path) -> ForstResult<()> {
        if self.uuid_keys {
            if self.mapping.is_registered(src) {
                // Staging→final publication of a minted object: pure
                // metadata re-point, ZERO backend ops (rename-free
                // invariant — S3 has no rename).
                return self.mapping.rename_logical(src, dst);
            }
            if self.mapping.is_registered(dst) {
                // Bytes at an UNMAPPED literal src replace a mapped dst
                // (ingest footer-rewrite shape): move the bytes to a fresh
                // physical, then re-point dst. One backend rename of the
                // staging object; the displaced physical's fate follows its
                // refcount.
                let key = FileMappingManager::mint_physical_key(dst)?;
                self.inner.rename(src, Path::new(&key))?;
                self.mapping.unlink(dst)?;
                return self.mapping.register(dst, &key, 0);
            }
        }
        self.inner.rename(src, dst)
    }

    fn supports_atomic_rename(&self) -> bool {
        self.inner.supports_atomic_rename()
    }

    fn sync_dir(&self, dir: &Path) -> ForstResult<()> {
        self.inner.sync_dir(dir)
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn ensure_cached(&self, path: &Path) -> ForstResult<()> {
        self.inner.ensure_cached(&self.resolve(path))
    }

    fn prefetch_concurrent(&self, paths: &[&Path]) {
        let resolved: Vec<PathBuf> = paths.iter().map(|p| self.resolve(p)).collect();
        let refs: Vec<&Path> = resolved.iter().map(PathBuf::as_path).collect();
        self.inner.prefetch_concurrent(&refs);
    }

    fn await_upload(&self, path: &Path) -> ForstResult<()> {
        self.inner.await_upload(&self.resolve(path))
    }

    fn await_all_uploads(&self) -> ForstResult<()> {
        self.inner.await_all_uploads()
    }

    fn pre_seed_admission(&self, path: &Path) {
        self.inner.pre_seed_admission(&self.resolve(path));
    }
}

fn decode_snapshot(bytes: &[u8]) -> ForstResult<MappingState> {
    if bytes.len() < 10 {
        return Err(ForstError::corruption("mapping snapshot too small"));
    }
    let (payload, crc_bytes) = bytes.split_at(bytes.len() - 4);
    let (stored_crc, _) = get_fixed32(crc_bytes)?;
    if stored_crc != crc32c(payload) {
        return Err(ForstError::corruption("mapping snapshot checksum mismatch"));
    }
    if &payload[..4] != SNAPSHOT_MAGIC {
        return Err(ForstError::corruption("mapping snapshot bad magic"));
    }
    let version = u16::from_le_bytes([payload[4], payload[5]]);
    if version != SNAPSHOT_VERSION {
        return Err(ForstError::corruption(format!(
            "mapping snapshot unsupported version {version}"
        )));
    }
    let mut pos = 6usize;
    let (n_logical, n) = get_fixed32(&payload[pos..])?;
    pos += n;
    if n_logical > MAX_SNAPSHOT_ENTRIES {
        return Err(ForstError::corruption("mapping snapshot entry count implausible"));
    }
    let mut state = MappingState::default();
    let mut pairs: Vec<(PathBuf, String)> = Vec::with_capacity(n_logical as usize);
    for _ in 0..n_logical {
        let (path, n) = get_str(&payload[pos..])?;
        pos += n;
        let (key, n) = get_str(&payload[pos..])?;
        pos += n;
        pairs.push((PathBuf::from(path), key));
    }
    let (n_physical, n) = get_fixed32(&payload[pos..])?;
    pos += n;
    if n_physical > MAX_SNAPSHOT_ENTRIES {
        return Err(ForstError::corruption("mapping snapshot entry count implausible"));
    }
    let mut phys_meta: HashMap<String, (u64, FileOwnership, bool)> =
        HashMap::with_capacity(n_physical as usize);
    for _ in 0..n_physical {
        let (key, n) = get_str(&payload[pos..])?;
        pos += n;
        let (size, n) = get_fixed64(&payload[pos..])?;
        pos += n;
        if payload.len() < pos + 2 {
            return Err(ForstError::corruption("mapping snapshot truncated"));
        }
        let ownership = match payload[pos] {
            0 => FileOwnership::PrivateOwnedByDb,
            1 => FileOwnership::ShareableOwnedByDb,
            2 => FileOwnership::NotOwned,
            other => {
                return Err(ForstError::corruption(format!(
                    "mapping snapshot unknown ownership {other}"
                )))
            }
        };
        let tombstoned = payload[pos + 1] != 0;
        pos += 2;
        phys_meta.insert(key, (size, ownership, tombstoned));
    }
    // Rebuild via insert_logical so refs stay a derived invariant.
    for (path, key) in pairs {
        let (size, ownership, _) = phys_meta
            .get(key.as_str())
            .copied()
            .ok_or_else(|| ForstError::corruption("mapping snapshot dangling logical entry"))?;
        state.insert_logical(&path, &key, size, ownership);
    }
    for (key, (_, _, tombstoned)) in phys_meta {
        if tombstoned {
            if let Some(e) = state.physical.get_mut(&key) {
                e.tombstoned = true;
            }
        }
    }
    Ok(state)
}

fn read_all(fs: &dyn FileSystem, path: &Path) -> ForstResult<Vec<u8>> {
    let mut r = fs.open_sequential_file(path)?;
    let mut out = Vec::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = r.read(&mut buf)?;
        if n == 0 {
            break;
        }
        out.extend_from_slice(&buf[..n]);
    }
    Ok(out)
}

// ===========================================================================
// Tests (design §5 Stage-1 UT gates)
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_fs::MemoryFileSystem;

    fn fs_with_file(path: &str, content: &[u8]) -> Arc<dyn FileSystem> {
        let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
        write_file(fs.as_ref(), path, content);
        fs
    }

    fn write_file(fs: &dyn FileSystem, path: &str, content: &[u8]) {
        if let Some(parent) = Path::new(path).parent() {
            fs.create_dir_all(parent).unwrap();
        }
        let mut w = fs
            .open_writable_file(Path::new(path), WriteMode::CreateOrTruncate)
            .unwrap();
        w.append(content).unwrap();
        w.sync().unwrap();
    }

    fn mgr(fs: &Arc<dyn FileSystem>) -> FileMappingManager {
        FileMappingManager::new(fs.clone(), PathBuf::from("/db/MAPPING.journal")).unwrap()
    }

    // --- FRS-PHASE2-S3: MappedFileSystem (UFS read-path indirection) -------

    #[test]
    fn test_mapped_fs_resolves_reads_unmapped_passthrough() {
        let fs = fs_with_file("/src/000001.sst", b"physical-bytes");
        write_file(fs.as_ref(), "/plain/p.dat", b"plain");
        let m = Arc::new(mgr(&fs));
        m.adopt(Path::new("/restore/000001.sst"), "/src/000001.sst")
            .unwrap();
        let mapped = MappedFileSystem::new(fs.clone(), m);

        // Mapped logical path: every read-side op resolves to the physical.
        assert!(mapped
            .file_exists(Path::new("/restore/000001.sst"))
            .unwrap());
        let raf = mapped
            .open_random_access_file(Path::new("/restore/000001.sst"))
            .unwrap();
        let mut buf = vec![0u8; b"physical-bytes".len()];
        raf.read_at(0, &mut buf).unwrap();
        assert_eq!(&buf, b"physical-bytes");
        assert_eq!(
            mapped
                .get_file_metadata(Path::new("/restore/000001.sst"))
                .unwrap()
                .size,
            b"physical-bytes".len() as u64
        );
        let mut seq = mapped
            .open_sequential_file(Path::new("/restore/000001.sst"))
            .unwrap();
        let mut sbuf = vec![0u8; 64];
        let n = seq.read(&mut sbuf).unwrap();
        assert_eq!(&sbuf[..n], b"physical-bytes");

        // Unmapped paths pass through untouched.
        assert!(mapped.file_exists(Path::new("/plain/p.dat")).unwrap());
        assert!(!mapped.file_exists(Path::new("/restore/other.sst")).unwrap());
    }

    #[test]
    fn test_mapped_fs_writes_and_deletes_never_touch_physical() {
        let fs = fs_with_file("/src/000001.sst", b"physical-bytes");
        let m = Arc::new(mgr(&fs));
        m.adopt(Path::new("/restore/000001.sst"), "/src/000001.sst")
            .unwrap();
        let mapped = MappedFileSystem::new(fs.clone(), m);

        // A passthrough delete on the mapped-but-byteless logical path can
        // never reach the shared physical object (lifecycle belongs to the
        // mapping layer's unlink refcounts).
        let _ = mapped.delete_file(Path::new("/restore/000001.sst"));
        assert!(
            fs.file_exists(Path::new("/src/000001.sst")).unwrap(),
            "physical must survive a passthrough delete of the logical path"
        );

        // Writable opens are passthrough: new files land at their literal
        // working path (identity, unmapped).
        fs.create_dir_all(Path::new("/restore")).unwrap();
        let mut w = mapped
            .open_writable_file(
                Path::new("/restore/000099.sst"),
                WriteMode::CreateOrTruncate,
            )
            .unwrap();
        w.append(b"new-working-bytes").unwrap();
        w.sync().unwrap();
        drop(w);
        assert!(fs.file_exists(Path::new("/restore/000099.sst")).unwrap());
        assert_eq!(
            read_all(fs.as_ref(), Path::new("/src/000001.sst")).unwrap(),
            b"physical-bytes".to_vec(),
            "physical bytes untouched by writes in the mapped namespace"
        );
    }

    // --- link / unlink / refcount ------------------------------------------

    #[test]
    fn test_register_link_unlink_refcount() {
        let fs = fs_with_file("/db/000001.sst", b"bytes-1");
        let m = mgr(&fs);
        m.register(Path::new("/db/000001.sst"), "/db/000001.sst", 7)
            .unwrap();
        assert_eq!(m.refs("/db/000001.sst"), 1);

        m.link(Path::new("/db/000001.sst"), Path::new("/ckpt/chk-1/000001.sst"))
            .unwrap();
        assert_eq!(m.refs("/db/000001.sst"), 2);
        assert_eq!(
            m.resolve(Path::new("/ckpt/chk-1/000001.sst")).as_deref(),
            Some("/db/000001.sst")
        );

        // Unlink at refs > 0 keeps the bytes.
        let out = m.unlink(Path::new("/db/000001.sst")).unwrap();
        assert_eq!(out, UnlinkOutcome::Retained { refs_remaining: 1 });
        assert!(fs.file_exists(Path::new("/db/000001.sst")).unwrap());
        assert!(!m.is_registered(Path::new("/db/000001.sst")));

        // Last unlink deletes the physical object.
        let out = m.unlink(Path::new("/ckpt/chk-1/000001.sst")).unwrap();
        assert_eq!(out, UnlinkOutcome::PhysicalDeleted);
        assert!(!fs.file_exists(Path::new("/db/000001.sst")).unwrap());
        assert_eq!(m.refs("/db/000001.sst"), 0);
    }

    #[test]
    fn test_unlink_unknown_logical_is_not_found_exactly_once_delete() {
        let fs = fs_with_file("/db/000002.sst", b"x");
        let m = mgr(&fs);
        m.register(Path::new("/db/000002.sst"), "/db/000002.sst", 1)
            .unwrap();
        assert_eq!(
            m.unlink(Path::new("/db/000002.sst")).unwrap(),
            UnlinkOutcome::PhysicalDeleted
        );
        // Second unlink of the same logical path must FAIL (exactly-once).
        let err = m.unlink(Path::new("/db/000002.sst")).unwrap_err();
        assert!(err.is_not_found());
    }

    #[test]
    fn test_link_source_must_exist_and_dst_conflict_rejected() {
        let fs = fs_with_file("/db/000003.sst", b"x");
        write_file(fs.as_ref(), "/db/000004.sst", b"y");
        let m = mgr(&fs);
        let err = m
            .link(Path::new("/db/missing.sst"), Path::new("/ckpt/a.sst"))
            .unwrap_err();
        assert!(err.is_not_found());

        m.register(Path::new("/db/000003.sst"), "/db/000003.sst", 1)
            .unwrap();
        m.register(Path::new("/db/000004.sst"), "/db/000004.sst", 1)
            .unwrap();
        m.link(Path::new("/db/000003.sst"), Path::new("/ckpt/c.sst"))
            .unwrap();
        // Idempotent re-link: same dst, same physical — OK, refs unchanged.
        m.link(Path::new("/db/000003.sst"), Path::new("/ckpt/c.sst"))
            .unwrap();
        assert_eq!(m.refs("/db/000003.sst"), 2);
        // Conflicting dst → InvalidArgument.
        let err = m
            .link(Path::new("/db/000004.sst"), Path::new("/ckpt/c.sst"))
            .unwrap_err();
        assert!(err.is_invalid_argument());
    }

    // --- ownership wiring ----------------------------------------------------

    #[test]
    fn test_ownership_wiring_register_shareable_adopt_not_owned() {
        let fs = fs_with_file("/db/000005.sst", b"x");
        write_file(fs.as_ref(), "/remote/chk/000009.sst", b"adopted");
        let m = mgr(&fs);
        m.register(Path::new("/db/000005.sst"), "/db/000005.sst", 1)
            .unwrap();
        assert_eq!(
            m.ownership_of("/db/000005.sst"),
            Some(FileOwnership::ShareableOwnedByDb)
        );

        m.adopt(Path::new("/db/000009.sst"), "/remote/chk/000009.sst")
            .unwrap();
        assert_eq!(
            m.ownership_of("/remote/chk/000009.sst"),
            Some(FileOwnership::NotOwned)
        );
        // Draining an adopted object keeps the bytes (external owner).
        let out = m.unlink(Path::new("/db/000009.sst")).unwrap();
        assert_eq!(out, UnlinkOutcome::KeptNotOwned);
        assert!(fs.file_exists(Path::new("/remote/chk/000009.sst")).unwrap());
    }

    #[test]
    fn test_adopt_missing_physical_fails_loudly() {
        let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
        fs.create_dir_all(Path::new("/db")).unwrap();
        let m = mgr(&fs);
        let err = m
            .adopt(Path::new("/db/000001.sst"), "/remote/gone.sst")
            .unwrap_err();
        assert!(err.is_not_found());
    }

    // --- tombstone (JM-discard protocol) -------------------------------------

    #[test]
    fn test_tombstone_defers_delete_until_refs_drain() {
        let fs = fs_with_file("/db/000006.sst", b"x");
        let m = mgr(&fs);
        m.register(Path::new("/db/000006.sst"), "/db/000006.sst", 1)
            .unwrap();
        m.link(Path::new("/db/000006.sst"), Path::new("/ckpt/chk-2/000006.sst"))
            .unwrap();

        // Tombstone with refs held: bytes survive.
        assert!(!m.tombstone("/db/000006.sst").unwrap());
        assert!(fs.file_exists(Path::new("/db/000006.sst")).unwrap());

        m.unlink(Path::new("/db/000006.sst")).unwrap();
        // Last unlink consumes the tombstone → delete even though refs
        // ordering varied.
        let out = m.unlink(Path::new("/ckpt/chk-2/000006.sst")).unwrap();
        assert_eq!(out, UnlinkOutcome::PhysicalDeleted);
        assert!(!fs.file_exists(Path::new("/db/000006.sst")).unwrap());
    }

    #[test]
    fn test_tombstone_with_no_refs_deletes_immediately() {
        let fs = fs_with_file("/orphan/000007.sst", b"x");
        let m = mgr(&fs);
        assert!(m.tombstone("/orphan/000007.sst").unwrap());
        assert!(!fs.file_exists(Path::new("/orphan/000007.sst")).unwrap());
    }

    #[test]
    fn test_tombstoned_not_owned_object_is_deleted_on_drain() {
        let fs = fs_with_file("/remote/chk/000008.sst", b"x");
        let m = mgr(&fs);
        m.adopt(Path::new("/db/000008.sst"), "/remote/chk/000008.sst")
            .unwrap();
        m.tombstone("/remote/chk/000008.sst").unwrap();
        let out = m.unlink(Path::new("/db/000008.sst")).unwrap();
        // Explicit JM discard overrides NotOwned: delete.
        assert_eq!(out, UnlinkOutcome::PhysicalDeleted);
        assert!(!fs.file_exists(Path::new("/remote/chk/000008.sst")).unwrap());
    }

    // --- journal replay idempotence -------------------------------------------

    #[test]
    fn test_journal_replay_reconstructs_state() {
        let fs = fs_with_file("/db/000010.sst", b"x");
        write_file(fs.as_ref(), "/db/000011.sst", b"y");
        {
            let m = mgr(&fs);
            m.register(Path::new("/db/000010.sst"), "/db/000010.sst", 5)
                .unwrap();
            m.register(Path::new("/db/000011.sst"), "/db/000011.sst", 5)
                .unwrap();
            m.link(Path::new("/db/000010.sst"), Path::new("/ckpt/chk-1/000010.sst"))
                .unwrap();
            m.unlink(Path::new("/db/000011.sst")).unwrap(); // drains → deleted
            m.sync_journal().unwrap();
        }
        // Reopen: replay journal.
        let m2 = mgr(&fs);
        assert_eq!(m2.refs("/db/000010.sst"), 2);
        assert_eq!(m2.refs("/db/000011.sst"), 0);
        assert!(m2.is_registered(Path::new("/ckpt/chk-1/000010.sst")));
        assert!(!m2.is_registered(Path::new("/db/000011.sst")));
    }

    #[test]
    fn test_journal_replay_is_idempotent_under_double_apply() {
        // Applying the same record stream twice must converge to the same
        // state (refs are derived, not blindly incremented).
        let records = vec![
            MappingRecord::Register {
                logical: PathBuf::from("/db/000012.sst"),
                physical_key: "/db/000012.sst".to_string(),
                size: 9,
            },
            MappingRecord::Link {
                dst_logical: PathBuf::from("/ckpt/chk-1/000012.sst"),
                physical_key: "/db/000012.sst".to_string(),
            },
            MappingRecord::Unlink {
                logical: PathBuf::from("/db/000012.sst"),
            },
        ];
        let mut bytes = Vec::new();
        for r in &records {
            bytes.extend_from_slice(&r.encode().unwrap());
        }
        let mut once = MappingState::default();
        replay_records(&mut once, &bytes).unwrap();

        let mut doubled = bytes.clone();
        doubled.extend_from_slice(&bytes);
        let mut twice = MappingState::default();
        replay_records(&mut twice, &doubled).unwrap();

        assert_eq!(once.logical, twice.logical);
        assert_eq!(once.physical.len(), twice.physical.len());
        assert_eq!(
            once.physical.get("/db/000012.sst").map(|e| e.refs),
            twice.physical.get("/db/000012.sst").map(|e| e.refs)
        );
        assert_eq!(once.physical.get("/db/000012.sst").unwrap().refs, 1);
    }

    #[test]
    fn test_journal_truncated_tail_is_tolerated() {
        let fs = fs_with_file("/db/000013.sst", b"x");
        {
            let m = mgr(&fs);
            m.register(Path::new("/db/000013.sst"), "/db/000013.sst", 1)
                .unwrap();
            m.sync_journal().unwrap();
        }
        // Simulate a crash mid-append: append a torn record fragment.
        let mut w = fs
            .open_writable_file(Path::new("/db/MAPPING.journal"), WriteMode::Append)
            .unwrap();
        w.append(&[42u8, 0, 0]).unwrap(); // torn length prefix
        w.sync().unwrap();
        drop(w);
        let m2 = mgr(&fs);
        assert_eq!(m2.refs("/db/000013.sst"), 1);
    }

    // --- snapshot round-trip ---------------------------------------------------

    #[test]
    fn test_snapshot_roundtrip_and_journal_rewrite() {
        let fs = fs_with_file("/db/000014.sst", b"x");
        write_file(fs.as_ref(), "/remote/chk/000015.sst", b"y");
        let m = mgr(&fs);
        m.register(Path::new("/db/000014.sst"), "/db/000014.sst", 3)
            .unwrap();
        m.link(Path::new("/db/000014.sst"), Path::new("/ckpt/chk-3/000014.sst"))
            .unwrap();
        m.adopt(Path::new("/db/000015.sst"), "/remote/chk/000015.sst")
            .unwrap();
        m.tombstone("/remote/chk/000015.sst").unwrap();
        let snap = m.snapshot_bytes().unwrap();

        let m2 = mgr(&fs); // replays the live journal — same state
        assert_eq!(m2.snapshot_bytes().unwrap(), snap);

        // Restore into a fresh manager on an empty journal.
        let fs2: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
        fs2.create_dir_all(Path::new("/db")).unwrap();
        let m3 = FileMappingManager::new(fs2.clone(), PathBuf::from("/db/MAPPING.journal")).unwrap();
        m3.restore_snapshot(&snap).unwrap();
        assert_eq!(m3.refs("/db/000014.sst"), 2);
        assert_eq!(
            m3.ownership_of("/remote/chk/000015.sst"),
            Some(FileOwnership::NotOwned)
        );
        // The rewritten journal alone reconstructs the restored state.
        let m4 = FileMappingManager::new(fs2.clone(), PathBuf::from("/db/MAPPING.journal")).unwrap();
        assert_eq!(m4.snapshot_bytes().unwrap(), m3.snapshot_bytes().unwrap());
    }

    #[test]
    fn test_snapshot_rejects_corruption() {
        let fs = fs_with_file("/db/000016.sst", b"x");
        let m = mgr(&fs);
        m.register(Path::new("/db/000016.sst"), "/db/000016.sst", 1)
            .unwrap();
        let mut snap = m.snapshot_bytes().unwrap();
        let mid = snap.len() / 2;
        snap[mid] ^= 0xFF;
        assert!(decode_snapshot(&snap).is_err());
    }

    // --- concurrent link-vs-delete race (design §5 Stage-1 UT gate) -----------

    #[test]
    fn test_concurrent_link_vs_unlink_race_never_loses_linked_bytes() {
        for round in 0..8 {
            let fs = fs_with_file("/db/000020.sst", b"race-bytes");
            let m = Arc::new(mgr(&fs));
            m.register(Path::new("/db/000020.sst"), "/db/000020.sst", 10)
                .unwrap();

            let linkers: Vec<_> = (0..4)
                .map(|t| {
                    let m = m.clone();
                    std::thread::spawn(move || {
                        let mut wins = 0u32;
                        for i in 0..50 {
                            let dst = PathBuf::from(format!("/ckpt/chk-{round}/t{t}-{i}.sst"));
                            match m.link(Path::new("/db/000020.sst"), &dst) {
                                Ok(()) => wins += 1,
                                Err(e) => assert!(
                                    e.is_not_found(),
                                    "link may only fail NotFound after unlink, got {e}"
                                ),
                            }
                        }
                        wins
                    })
                })
                .collect();
            let unlinker = {
                let m = m.clone();
                std::thread::spawn(move || {
                    // Race the unlink into the middle of the link storm.
                    std::thread::yield_now();
                    m.unlink(Path::new("/db/000020.sst")).unwrap()
                })
            };
            let wins: u32 = linkers.into_iter().map(|h| h.join().unwrap()).sum();
            let unlink_outcome = unlinker.join().unwrap();

            // Invariants: refs == successful links (the working ref was
            // dropped); bytes exist iff someone still references them; the
            // unlink deleted ONLY when no link had landed first.
            assert_eq!(m.refs("/db/000020.sst"), wins);
            let exists = fs.file_exists(Path::new("/db/000020.sst")).unwrap();
            assert_eq!(exists, wins > 0, "bytes must exist iff refs > 0");
            match unlink_outcome {
                UnlinkOutcome::PhysicalDeleted => assert_eq!(wins, 0),
                UnlinkOutcome::Retained { .. } => assert!(wins > 0),
                UnlinkOutcome::KeptNotOwned => panic!("owned file cannot be KeptNotOwned"),
            }
        }
    }

    // --- crash-GC sweep ---------------------------------------------------------

    #[test]
    fn test_gc_sweep_reaps_orphans_never_live_refs() {
        let fs = fs_with_file("/db/000030.sst", b"live");
        write_file(fs.as_ref(), "/db/000031.sst", b"orphan");
        write_file(fs.as_ref(), "/db/000032.sst", b"tombstoned-drained");
        write_file(fs.as_ref(), "/db/MANIFEST", b"not-an-sst");
        let m = mgr(&fs);
        m.register(Path::new("/db/000030.sst"), "/db/000030.sst", 4)
            .unwrap();
        // 000032: simulate crash-between-journal-and-delete — journal says
        // tombstoned with no refs, bytes still present. tombstone() would
        // normally delete; write the record path via register+unlink crash
        // emulation instead: register, unlink journal record applied but
        // physical delete "lost" (we re-create the file to fake the crash).
        m.register(Path::new("/db/000032.sst"), "/db/000032.sst", 4)
            .unwrap();
        m.unlink(Path::new("/db/000032.sst")).unwrap();
        write_file(fs.as_ref(), "/db/000032.sst", b"resurrected-orphan");

        let report = m.gc_sweep(Path::new("/db")).unwrap();
        assert_eq!(report.kept_live, 1);
        let mut reaped = report.reaped.clone();
        reaped.sort();
        assert_eq!(reaped, vec!["/db/000031.sst", "/db/000032.sst"]);
        assert!(fs.file_exists(Path::new("/db/000030.sst")).unwrap());
        assert!(!fs.file_exists(Path::new("/db/000031.sst")).unwrap());
        // Non-SST files are never touched.
        assert!(fs.file_exists(Path::new("/db/MANIFEST")).unwrap());
    }

    // --- misc -------------------------------------------------------------------

    #[test]
    fn test_parse_file_number() {
        assert_eq!(parse_file_number("/db/000042.sst"), Some(FileNumber(42)));
        assert_eq!(parse_file_number("000007.sst"), Some(FileNumber(7)));
        assert_eq!(parse_file_number("/db/CHECKPOINT.blob"), None);
        assert_eq!(parse_file_number("/db/abc.sst"), None);
    }

    #[test]
    fn test_register_idempotent_and_rebind() {
        let fs = fs_with_file("/db/000040.sst", b"a");
        write_file(fs.as_ref(), "/db/000041.sst", b"b");
        let m = mgr(&fs);
        m.register(Path::new("/db/000040.sst"), "/db/000040.sst", 1)
            .unwrap();
        m.register(Path::new("/db/000040.sst"), "/db/000040.sst", 1)
            .unwrap(); // duplicate no-op
        assert_eq!(m.refs("/db/000040.sst"), 1);
        // Rebind logical to a different physical (file-number reuse): the old
        // reference drains in METADATA; the stranded bytes become an orphan
        // that the crash-GC sweep reaps (apply() is replay-pure and never
        // deletes bytes itself).
        m.register(Path::new("/db/000040.sst"), "/db/000041.sst", 1)
            .unwrap();
        assert_eq!(m.refs("/db/000040.sst"), 0);
        assert_eq!(m.refs("/db/000041.sst"), 1);
        assert!(fs.file_exists(Path::new("/db/000040.sst")).unwrap());
        let report = m.gc_sweep(Path::new("/db")).unwrap();
        assert_eq!(report.reaped, vec!["/db/000040.sst".to_string()]);
        assert!(!fs.file_exists(Path::new("/db/000040.sst")).unwrap());
        assert!(fs.file_exists(Path::new("/db/000041.sst")).unwrap());
    }

    // --- FRS-PHASE2-C2U2: journal tail view + chk-namespace enumeration ----

    #[test]
    fn test_journal_view_absent_journal_is_none() {
        let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
        assert!(MappingJournalView::load(fs.as_ref(), Path::new("/nope/MAPPING.journal"))
            .unwrap()
            .is_none());
    }

    /// The view replays the journal's CURRENT state — including records
    /// appended after any snapshot point: links, unlinks AND tombstones.
    #[test]
    fn test_journal_view_replays_current_state_with_tombstones() {
        let fs = fs_with_file("/db/000001.sst", b"a");
        write_file(fs.as_ref(), "/db/000002.sst", b"b");
        let m = mgr(&fs);
        m.register(Path::new("/db/000001.sst"), "/db/000001.sst", 1)
            .unwrap();
        m.register(Path::new("/db/000002.sst"), "/db/000002.sst", 1)
            .unwrap();
        m.link(Path::new("/db/000001.sst"), Path::new("/db/checkpoints/00000000000000000001/000001.sst"))
            .unwrap();
        // Post-"snapshot" tail: unlink one working ref + tombstone the other.
        m.unlink(Path::new("/db/000002.sst")).unwrap();
        assert!(!m.tombstone("/db/000001.sst").unwrap(), "refs held → deferred");
        m.sync_journal().unwrap();

        let v = MappingJournalView::load(fs.as_ref(), Path::new("/db/MAPPING.journal"))
            .unwrap()
            .expect("journal exists");
        assert_eq!(v.resolve(Path::new("/db/000001.sst")), Some("/db/000001.sst"));
        assert_eq!(
            v.resolve(Path::new("/db/checkpoints/00000000000000000001/000001.sst")),
            Some("/db/000001.sst")
        );
        assert!(!v.is_registered(Path::new("/db/000002.sst")), "unlink replayed");
        assert!(v.is_tombstoned("/db/000001.sst"), "tombstone visible in tail");
        assert!(!v.is_tombstoned("/db/000002.sst"));
        assert_eq!(v.len(), 2);
        // Read-only: loading the view never mutates the journal.
        let before = read_all(fs.as_ref(), Path::new("/db/MAPPING.journal")).unwrap();
        let _ = MappingJournalView::load(fs.as_ref(), Path::new("/db/MAPPING.journal")).unwrap();
        let after = read_all(fs.as_ref(), Path::new("/db/MAPPING.journal")).unwrap();
        assert_eq!(before, after);
    }

    /// A torn tail record (crash mid-append) is tolerated exactly like
    /// `FileMappingManager::new`: the clean prefix replays, the tail drops.
    #[test]
    fn test_journal_view_tolerates_torn_tail() {
        let fs = fs_with_file("/db/000001.sst", b"a");
        let m = mgr(&fs);
        m.register(Path::new("/db/000001.sst"), "/db/000001.sst", 1)
            .unwrap();
        m.sync_journal().unwrap();
        // Append a torn frame: a length prefix promising more than exists.
        let mut bytes = read_all(fs.as_ref(), Path::new("/db/MAPPING.journal")).unwrap();
        bytes.extend_from_slice(&[0xFF, 0x00, 0x00, 0x00, 0xAA]); // frame_len=255, 1 byte present
        write_file(fs.as_ref(), "/db/MAPPING.journal", &bytes);

        let v = MappingJournalView::load(fs.as_ref(), Path::new("/db/MAPPING.journal"))
            .unwrap()
            .expect("journal exists");
        assert_eq!(v.len(), 1, "clean prefix replayed, torn tail dropped");
        assert!(v.is_registered(Path::new("/db/000001.sst")));
    }

    #[test]
    fn test_logical_paths_under_filters_and_sorts() {
        let fs = fs_with_file("/db/000001.sst", b"a");
        write_file(fs.as_ref(), "/db/000002.sst", b"b");
        let m = mgr(&fs);
        m.register(Path::new("/db/000002.sst"), "/db/000002.sst", 1)
            .unwrap();
        m.register(Path::new("/db/000001.sst"), "/db/000001.sst", 1)
            .unwrap();
        let chk = Path::new("/db/checkpoints/00000000000000000007");
        m.link(Path::new("/db/000001.sst"), &chk.join("000001.sst"))
            .unwrap();
        m.link(Path::new("/db/000002.sst"), &chk.join("000002.sst"))
            .unwrap();

        let under_root = m.logical_paths_under(Path::new("/db/checkpoints"));
        assert_eq!(
            under_root,
            vec![chk.join("000001.sst"), chk.join("000002.sst")],
            "chk namespace only, sorted"
        );
        // The prefix itself is never returned; working paths are excluded.
        assert!(m
            .logical_paths_under(Path::new("/db"))
            .contains(&PathBuf::from("/db/000001.sst")));
        assert!(m
            .logical_paths_under(Path::new("/elsewhere"))
            .is_empty());
    }

    // --- FRS-PHASE2-C3U1: UUID physical keys (competitive analysis §2.2d) --

    /// Counts every `rename` that reaches the wrapped backend — the
    /// rename-free-invariant probe.
    struct RenameCountingFs {
        inner: Arc<dyn FileSystem>,
        renames: std::sync::atomic::AtomicUsize,
    }

    impl RenameCountingFs {
        fn new(inner: Arc<dyn FileSystem>) -> Self {
            Self {
                inner,
                renames: std::sync::atomic::AtomicUsize::new(0),
            }
        }
        fn renames(&self) -> usize {
            self.renames.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    impl FileSystem for RenameCountingFs {
        fn open_sequential_file(
            &self,
            path: &Path,
        ) -> ForstResult<Box<dyn crate::filesystem::SequentialFile>> {
            self.inner.open_sequential_file(path)
        }
        fn open_random_access_file(
            &self,
            path: &Path,
        ) -> ForstResult<Box<dyn crate::filesystem::RandomAccessFile>> {
            self.inner.open_random_access_file(path)
        }
        fn open_writable_file(
            &self,
            path: &Path,
            mode: WriteMode,
        ) -> ForstResult<Box<dyn WritableFile>> {
            self.inner.open_writable_file(path, mode)
        }
        fn file_exists(&self, path: &Path) -> ForstResult<bool> {
            self.inner.file_exists(path)
        }
        fn get_file_metadata(
            &self,
            path: &Path,
        ) -> ForstResult<crate::filesystem::FileMetadata> {
            self.inner.get_file_metadata(path)
        }
        fn list_dir(&self, dir: &Path) -> ForstResult<Vec<crate::filesystem::FileMetadata>> {
            self.inner.list_dir(dir)
        }
        fn create_dir_all(&self, dir: &Path) -> ForstResult<()> {
            self.inner.create_dir_all(dir)
        }
        fn delete_file(&self, path: &Path) -> ForstResult<()> {
            self.inner.delete_file(path)
        }
        fn delete_dir(&self, path: &Path, recursive: bool) -> ForstResult<()> {
            self.inner.delete_dir(path, recursive)
        }
        fn rename(&self, src: &Path, dst: &Path) -> ForstResult<()> {
            self.renames
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.rename(src, dst)
        }
        fn supports_atomic_rename(&self) -> bool {
            self.inner.supports_atomic_rename()
        }
        fn name(&self) -> &str {
            "rename-counting"
        }
    }

    fn write_through(fs: &dyn FileSystem, path: &str, mode: WriteMode, content: &[u8]) {
        let mut w = fs.open_writable_file(Path::new(path), mode).unwrap();
        w.append(content).unwrap();
        w.sync().unwrap();
    }

    fn is_uuid_key(key: &str) -> bool {
        let base = Path::new(key).file_name().unwrap().to_str().unwrap();
        base.strip_prefix("uuid-")
            .and_then(|rest| rest.strip_suffix(".sst"))
            .is_some_and(|hex| hex.len() == 32 && hex.bytes().all(|b| b.is_ascii_hexdigit()))
    }

    /// UUID mode: an SST-class create mints a `uuid-<32hex>.sst` physical in
    /// the same directory; reads/metadata resolve through the logical name;
    /// the literal logical path holds no bytes.
    #[test]
    fn test_c3u1_uuid_mode_mints_uuid_physicals() {
        let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
        fs.create_dir_all(Path::new("/db")).unwrap();
        let m = Arc::new(mgr(&fs));
        let mapped = MappedFileSystem::with_uuid_physical_keys(fs.clone(), m.clone()).unwrap();
        assert!(mapped.uuid_physical_keys());

        write_through(&mapped, "/db/000001.sst", WriteMode::CreateNew, b"sst-bytes");
        let key = m.resolve(Path::new("/db/000001.sst")).expect("minted");
        assert!(is_uuid_key(&key), "physical {key} must be uuid-shaped");
        assert!(key.starts_with("/db/"), "same parent dir, got {key}");
        assert!(!fs.file_exists(Path::new("/db/000001.sst")).unwrap());
        assert_eq!(
            read_all(&mapped, Path::new("/db/000001.sst")).unwrap(),
            b"sst-bytes".to_vec()
        );
        // Non-SST files write through at their literal path (no mint).
        write_through(&mapped, "/db/MANIFEST-000001", WriteMode::CreateNew, b"m");
        assert!(fs.file_exists(Path::new("/db/MANIFEST-000001")).unwrap());
        assert!(!m.is_registered(Path::new("/db/MANIFEST-000001")));
        // CreateNew on an already-mapped logical refuses (AlreadyExists).
        assert!(mapped
            .open_writable_file(Path::new("/db/000001.sst"), WriteMode::CreateNew)
            .is_err());
        // Default-OFF gate: `new` keeps passthrough writes byte-identical.
        let plain = MappedFileSystem::new(fs.clone(), m.clone());
        assert!(!plain.uuid_physical_keys());
        write_through(&plain, "/db/000002.sst", WriteMode::CreateNew, b"x");
        assert!(fs.file_exists(Path::new("/db/000002.sst")).unwrap());
        assert!(!m.is_registered(Path::new("/db/000002.sst")));
    }

    /// The rename-free invariant on the opendal-fs emulation (the unit
    /// gate): the staging→final SST publication (`.NNNNNN.sst.tmp` →
    /// `NNNNNN.sst`, the flush.rs convention) is a pure metadata re-point —
    /// ZERO renames reach the backend, bytes never move, and a delete of the
    /// final logical follows the refcount.
    #[test]
    fn test_c3u1_uuid_mode_rename_free_on_opendal_fs() {
        let tmp = tempfile::tempdir().unwrap();
        let backend: Arc<dyn FileSystem> =
            Arc::new(crate::opendal_backend::OpendalFileSystem::local(tmp.path()).unwrap());
        let counting = Arc::new(RenameCountingFs::new(backend));
        let fs: Arc<dyn FileSystem> = counting.clone();
        fs.create_dir_all(Path::new("/db")).unwrap();
        let m = Arc::new(mgr(&fs));
        let mapped = MappedFileSystem::with_uuid_physical_keys(fs.clone(), m.clone()).unwrap();

        // Stage exactly like flush.rs: write the staging name, rename into
        // place (the local-FS convention — supports_atomic_rename is true on
        // the opendal Fs scheme, so this IS the path the engine takes).
        assert!(mapped.supports_atomic_rename());
        write_through(&mapped, "/db/.000001.sst.tmp", WriteMode::CreateNew, b"sst-1");
        let staged_key = m.resolve(Path::new("/db/.000001.sst.tmp")).unwrap();
        assert!(is_uuid_key(&staged_key));
        mapped
            .rename(Path::new("/db/.000001.sst.tmp"), Path::new("/db/000001.sst"))
            .unwrap();

        assert_eq!(counting.renames(), 0, "rename-free invariant violated");
        assert_eq!(
            m.resolve(Path::new("/db/000001.sst")).unwrap(),
            staged_key,
            "publication is a re-point, not a data move"
        );
        assert!(!m.is_registered(Path::new("/db/.000001.sst.tmp")));
        assert_eq!(
            read_all(&mapped, Path::new("/db/000001.sst")).unwrap(),
            b"sst-1".to_vec()
        );

        // Checkpoint-link + working delete: bytes survive via the chk ref,
        // physical goes exactly once when the last ref drains.
        m.link(
            Path::new("/db/000001.sst"),
            Path::new("/db/checkpoints/00000000000000000001/000001.sst"),
        )
        .unwrap();
        mapped.delete_file(Path::new("/db/000001.sst")).unwrap();
        assert!(fs.file_exists(Path::new(&staged_key)).unwrap());
        assert_eq!(
            m.unlink(Path::new("/db/checkpoints/00000000000000000001/000001.sst"))
                .unwrap(),
            UnlinkOutcome::PhysicalDeleted
        );
        assert!(!fs.file_exists(Path::new(&staged_key)).unwrap());
        assert_eq!(counting.renames(), 0, "whole lifecycle stays rename-free");
    }

    /// Crashed staging writer (mint journaled, rename never happened): the
    /// next UUID-mode mount sweeps the `.tmp` mapping AND reaps the minted
    /// physical, so a file-number-reusing restart can `CreateNew` the same
    /// staging name again.
    #[test]
    fn test_c3u1_uuid_mode_crashed_staging_swept_on_mount() {
        let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
        fs.create_dir_all(Path::new("/db")).unwrap();
        {
            let m = Arc::new(mgr(&fs));
            let mapped =
                MappedFileSystem::with_uuid_physical_keys(fs.clone(), m.clone()).unwrap();
            write_through(&mapped, "/db/.000007.sst.tmp", WriteMode::CreateNew, b"torn");
            // crash: no rename, manager dropped (journal survives on fs).
        }
        let m2 = Arc::new(mgr(&fs)); // journal replay resurrects the mapping
        let stale_key = m2.resolve(Path::new("/db/.000007.sst.tmp")).unwrap();
        assert!(fs.file_exists(Path::new(&stale_key)).unwrap());
        let mapped2 = MappedFileSystem::with_uuid_physical_keys(fs.clone(), m2.clone()).unwrap();
        assert!(!m2.is_registered(Path::new("/db/.000007.sst.tmp")));
        assert!(
            !fs.file_exists(Path::new(&stale_key)).unwrap(),
            "crashed staging physical must be reaped"
        );
        // Same staging name is creatable again (file-number reuse).
        write_through(&mapped2, "/db/.000007.sst.tmp", WriteMode::CreateNew, b"retry");
        assert_eq!(
            read_all(&mapped2, Path::new("/db/.000007.sst.tmp")).unwrap(),
            b"retry".to_vec()
        );
    }

    /// UUID mappings round-trip through BOTH durability paths: the journal
    /// (manager re-open) and the snapshot (restore_snapshot rewrites the
    /// journal — the old identity-test downgraded UUID registers to Links,
    /// losing sizes).
    #[test]
    fn test_c3u1_uuid_mappings_journal_and_snapshot_roundtrip() {
        let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
        fs.create_dir_all(Path::new("/db")).unwrap();
        let m = Arc::new(mgr(&fs));
        let mapped = MappedFileSystem::with_uuid_physical_keys(fs.clone(), m.clone()).unwrap();
        write_through(&mapped, "/db/000003.sst", WriteMode::CreateNew, b"bytes-3");
        let key = m.resolve(Path::new("/db/000003.sst")).unwrap();
        m.link(
            Path::new("/db/000003.sst"),
            Path::new("/db/checkpoints/00000000000000000002/000003.sst"),
        )
        .unwrap();

        // Journal replay (re-open).
        let m2 = mgr(&fs);
        assert_eq!(m2.resolve(Path::new("/db/000003.sst")).unwrap(), key);
        assert_eq!(m2.refs(&key), 2);
        assert_eq!(m2.ownership_of(&key), Some(FileOwnership::ShareableOwnedByDb));

        // Snapshot → restore_snapshot (journal REWRITE) → re-open again.
        let snap = m2.snapshot_bytes().unwrap();
        let m3 = mgr(&fs);
        m3.restore_snapshot(&snap).unwrap();
        assert_eq!(m3.resolve(Path::new("/db/000003.sst")).unwrap(), key);
        assert_eq!(m3.refs(&key), 2);
        let m4 = mgr(&fs); // replays the REWRITTEN journal
        assert_eq!(m4.resolve(Path::new("/db/000003.sst")).unwrap(), key);
        assert_eq!(m4.refs(&key), 2);
        assert_eq!(m4.ownership_of(&key), Some(FileOwnership::ShareableOwnedByDb));
        assert_eq!(
            read_all(fs.as_ref(), Path::new(&key)).unwrap(),
            b"bytes-3".to_vec()
        );
    }

    /// CreateOrTruncate on a checkpoint-shared logical never truncates the
    /// shared physical in place: the logical re-points to a FRESH mint and
    /// the checkpoint's bytes survive untouched.
    #[test]
    fn test_c3u1_uuid_mode_truncate_never_clobbers_shared_physical() {
        let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
        fs.create_dir_all(Path::new("/db")).unwrap();
        let m = Arc::new(mgr(&fs));
        let mapped = MappedFileSystem::with_uuid_physical_keys(fs.clone(), m.clone()).unwrap();
        write_through(&mapped, "/db/000004.sst", WriteMode::CreateNew, b"original");
        let old_key = m.resolve(Path::new("/db/000004.sst")).unwrap();
        let chk = Path::new("/db/checkpoints/00000000000000000003/000004.sst");
        m.link(Path::new("/db/000004.sst"), chk).unwrap();

        write_through(&mapped, "/db/000004.sst", WriteMode::CreateOrTruncate, b"rewritten");
        let new_key = m.resolve(Path::new("/db/000004.sst")).unwrap();
        assert_ne!(new_key, old_key, "truncate must mint a fresh physical");
        assert_eq!(
            read_all(fs.as_ref(), Path::new(&old_key)).unwrap(),
            b"original".to_vec(),
            "checkpoint-shared bytes untouched"
        );
        assert_eq!(m.resolve(chk).unwrap(), old_key);
        assert_eq!(
            read_all(&mapped, Path::new("/db/000004.sst")).unwrap(),
            b"rewritten".to_vec()
        );
    }
}
