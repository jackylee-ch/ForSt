/*
 * Copyright 2026 The ForSt-RS Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef FORST_RS_H
#define FORST_RS_H

/**
 * @file forst_rs.h
 * @brief C ABI bridge for ForSt-RS.
 *
 * Consumers call these functions via Java FFM (21+) or JNI. See
 * docs/design/2.5_ffm_bridge_design.md for the design rationale.
 *
 * Thread-safety: every function is thread-safe. The engine internally
 * serializes writers via a single mutex; readers are lock-free against
 * the current snapshot.
 *
 * Panic safety: every exported function wraps its Rust body in
 * `catch_unwind`; panics return `FRS_STATUS_PANIC` instead of unwinding
 * across the FFI boundary.
 */

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* -------------------------------------------------------------------- */
/* Status codes                                                         */
/* -------------------------------------------------------------------- */
#define FRS_STATUS_OK                0
#define FRS_STATUS_ERROR             1
#define FRS_STATUS_NULL_ARG          2
#define FRS_STATUS_NOT_FOUND         3
#define FRS_STATUS_INVALID_ARGUMENT  4
#define FRS_STATUS_PANIC             5
#define FRS_STATUS_POISONED          6

/* -------------------------------------------------------------------- */
/* Handle types                                                         */
/* -------------------------------------------------------------------- */

typedef void* FrsDb;
typedef void* FrsCfHandle;

typedef struct {
    uint8_t* data;
    size_t   len;
    size_t   capacity;
} FrsBytes;

/* -------------------------------------------------------------------- */
/* 1. Lifecycle                                                         */
/* -------------------------------------------------------------------- */

int32_t frs_db_open(const char* db_path, FrsDb* out_handle);
int32_t frs_db_open_memory(FrsDb* out_handle);
int32_t frs_db_close(FrsDb handle);
int32_t frs_db_open_from_checkpoint(const char* target_dir, FrsDb* out_handle);
int32_t frs_db_open_from_checkpoint_memory(const char* target_dir, FrsDb* out_handle);

/* -------------------------------------------------------------------- */
/* 2. Column families                                                   */
/* -------------------------------------------------------------------- */

int32_t frs_db_default_cf(FrsDb handle, FrsCfHandle* out_cf);
int32_t frs_db_create_cf(FrsDb handle, const char* name, FrsCfHandle* out_cf);
int32_t frs_db_create_cf_with_merge(FrsDb handle, const char* name,
                                    const char* merge_op_name,
                                    FrsCfHandle* out_cf);
int32_t frs_db_open_cf(FrsDb handle, const char* name, FrsCfHandle* out_cf);
int32_t frs_cf_close(FrsCfHandle handle);

/* -------------------------------------------------------------------- */
/* 3. Point operations                                                  */
/* -------------------------------------------------------------------- */

int32_t frs_put(FrsDb handle, FrsCfHandle cf,
                const uint8_t* key,   size_t key_len,
                const uint8_t* value, size_t value_len);

int32_t frs_delete(FrsDb handle, FrsCfHandle cf,
                   const uint8_t* key, size_t key_len);

int32_t frs_merge(FrsDb handle, FrsCfHandle cf,
                  const uint8_t* key,     size_t key_len,
                  const uint8_t* operand, size_t operand_len);

int32_t frs_get(FrsDb handle, FrsCfHandle cf,
                const uint8_t* key, size_t key_len,
                FrsBytes* out_value);

/* -------------------------------------------------------------------- */
/* 4. Batch operations                                                  */
/* -------------------------------------------------------------------- */

int32_t frs_batch_put(FrsDb handle, FrsCfHandle cf,
                      const uint8_t* const* keys,
                      const size_t* key_lens,
                      const uint8_t* const* values,
                      const size_t* value_lens,
                      size_t count);

int32_t frs_batch_get(FrsDb handle, FrsCfHandle cf,
                      const uint8_t* const* keys,
                      const size_t* key_lens,
                      size_t count,
                      FrsBytes* out_values);

/* -------------------------------------------------------------------- */
/* 5. Memory management                                                 */
/* -------------------------------------------------------------------- */

int32_t frs_bytes_free(FrsBytes* bytes);

/* -------------------------------------------------------------------- */
/* 6. Flush / compact / checkpoint                                      */
/* -------------------------------------------------------------------- */

int32_t frs_flush(FrsDb handle);
int32_t frs_flush_cf(FrsDb handle, FrsCfHandle cf);
int32_t frs_compact_cf(FrsDb handle, FrsCfHandle cf);
int32_t frs_compact_all(FrsDb handle);
int32_t frs_create_checkpoint(FrsDb handle, const char* target_dir);

/* -------------------------------------------------------------------- */
/* 7. Metadata                                                          */
/* -------------------------------------------------------------------- */

int32_t frs_sequence_number(FrsDb handle, uint64_t* out_seq);
int32_t frs_l0_file_count(FrsDb handle, uint32_t* out_count);

/* -------------------------------------------------------------------- */
/* 8. Arrow C Data Interface (zero-copy batch ops)                      */
/* -------------------------------------------------------------------- */
/*
 * Consumers pass standard Arrow C Data Interface structs (ArrowArray +
 * ArrowSchema pointers). See arrow.apache.org/docs/format/CDataInterface.html.
 *
 * After a successful call, the engine invalidates the input structs so
 * the producer must not release them again. Output structs are owned by
 * the caller who must release them via Arrow's release callbacks.
 */

/* Opaque forward declarations — consumers include arrow-c/abi.h for the
 * real definitions and pass pointers of compatible layout. */
struct FFI_ArrowArray;
struct FFI_ArrowSchema;

int32_t frs_batch_put_arrow_schema(struct FFI_ArrowSchema* out_schema);

int32_t frs_batch_put_arrow(FrsDb handle, FrsCfHandle cf,
                            struct FFI_ArrowArray* array,
                            struct FFI_ArrowSchema* schema);

int32_t frs_batch_get_arrow(FrsDb handle, FrsCfHandle cf,
                            struct FFI_ArrowArray* keys_array,
                            struct FFI_ArrowSchema* keys_schema,
                            struct FFI_ArrowArray* out_array,
                            struct FFI_ArrowSchema* out_schema);

/* W26 DeltaJoin Lookup fast-path. Returns a RecordBatch with columns
 * key: Binary, value: Binary containing every entry whose key starts
 * with `prefix`. */
int32_t frs_prefix_scan_arrow(FrsDb handle, FrsCfHandle cf,
                              const uint8_t* prefix, size_t prefix_len,
                              struct FFI_ArrowArray* out_array,
                              struct FFI_ArrowSchema* out_schema);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* FORST_RS_H */
