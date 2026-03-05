/*
 * arrow_c_abi.h — Vendored Arrow C Data Interface (minimal subset).
 *
 * These definitions are copied verbatim from the Apache Arrow project
 * (apache/arrow) and are explicitly designed to be vendored into any project.
 * See: https://arrow.apache.org/docs/format/CDataInterface.html
 *
 * Only ArrowSchema and ArrowArray are included here; ArrowDeviceArray and
 * the stream interfaces are provided by the cudf headers internally.
 *
 * Apache License 2.0
 */

#pragma once
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#ifndef ARROW_C_DATA_INTERFACE
#  define ARROW_C_DATA_INTERFACE

#  define ARROW_FLAG_DICTIONARY_ORDERED 1
#  define ARROW_FLAG_NULLABLE 2
#  define ARROW_FLAG_MAP_KEYS_SORTED 4

struct ArrowSchema {
  const char* format;
  const char* name;
  const char* metadata;
  int64_t flags;
  int64_t n_children;
  struct ArrowSchema** children;
  struct ArrowSchema* dictionary;
  void (*release)(struct ArrowSchema*);
  void* private_data;
};
typedef struct ArrowSchema ArrowSchema;

struct ArrowArray {
  int64_t length;
  int64_t null_count;
  int64_t offset;
  int64_t n_buffers;
  int64_t n_children;
  const void** buffers;
  struct ArrowArray** children;
  struct ArrowArray* dictionary;
  void (*release)(struct ArrowArray*);
  void* private_data;
};
typedef struct ArrowArray ArrowArray;

#endif  /* ARROW_C_DATA_INTERFACE */

#ifndef ARROW_C_DEVICE_DATA_INTERFACE
#  define ARROW_C_DEVICE_DATA_INTERFACE

typedef int32_t ArrowDeviceType;

#  define ARROW_DEVICE_CPU 1
#  define ARROW_DEVICE_CUDA 2
#  define ARROW_DEVICE_CUDA_HOST 3

struct ArrowDeviceArray {
  struct ArrowArray array;
  int64_t device_id;
  ArrowDeviceType device_type;
  void* sync_event;
  int64_t reserved[3];
};
typedef struct ArrowDeviceArray ArrowDeviceArray;

#endif  /* ARROW_C_DEVICE_DATA_INTERFACE */

#ifdef __cplusplus
}
#endif
