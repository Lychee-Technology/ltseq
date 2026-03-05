cmake_minimum_required(VERSION 3.26)

# ── CUDA architecture initialisation (must happen before project()) ───────────
# Default to native (compile for the GPU in the current machine).
if(NOT DEFINED CMAKE_CUDA_ARCHITECTURES)
  set(CMAKE_CUDA_ARCHITECTURES native)
endif()

project(cudf_engine VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# ── Locate cudf via pip-installed package ─────────────────────────────────────
# pip install libcudf-cu13  puts the CMake config here:
#   <site-packages>/libcudf/lib64/cmake/cudf/cudf-config.cmake
# Callers may override with -Dcudf_ROOT=...
if(NOT cudf_ROOT)
  execute_process(
    COMMAND "${Python3_EXECUTABLE}" -c
      "import libcudf, os; print(os.path.join(os.path.dirname(libcudf.__file__), 'lib64'))"
    OUTPUT_VARIABLE CUDF_LIB_DIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET
  )
  if(CUDF_LIB_DIR)
    set(cudf_ROOT "${CUDF_LIB_DIR}")
    message(STATUS "Auto-detected cudf_ROOT from pip: ${cudf_ROOT}")
  endif()
endif()

find_package(cudf REQUIRED HINTS "${cudf_ROOT}/cmake/cudf")

# ── Locate RMM (bundled in librmm-cu13 pip package) ──────────────────────────
if(NOT rmm_ROOT)
  execute_process(
    COMMAND "${Python3_EXECUTABLE}" -c
      "import librmm, os; print(os.path.join(os.path.dirname(librmm.__file__), 'lib64'))"
    OUTPUT_VARIABLE RMM_LIB_DIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET
  )
  if(RMM_LIB_DIR)
    set(rmm_ROOT "${RMM_LIB_DIR}")
  endif()
endif()
# RMM is header-only; the include path is propagated by cudf::cudf.

# ── rapids_logger headers (bundled in rapids_logger pip package) ──────────────
execute_process(
  COMMAND "${Python3_EXECUTABLE}" -c
    "import rapids_logger, os; print(os.path.join(os.path.dirname(rapids_logger.__file__), 'include'))"
  OUTPUT_VARIABLE RAPIDS_LOGGER_INCLUDE
  OUTPUT_STRIP_TRAILING_WHITESPACE
  ERROR_QUIET
)

# ── Protobuf (for Substrait plan decoding) ────────────────────────────────────
find_package(Protobuf REQUIRED)
message(STATUS "Protobuf version: ${Protobuf_VERSION}")
message(STATUS "Protobuf include: ${Protobuf_INCLUDE_DIRS}")

# ── Generate Substrait protobuf C++ sources ───────────────────────────────────
set(SUBSTRAIT_PROTO_DIR "${CMAKE_SOURCE_DIR}/../substrait/proto")
set(SUBSTRAIT_GEN_DIR   "${CMAKE_BINARY_DIR}/substrait_gen")
file(MAKE_DIRECTORY "${SUBSTRAIT_GEN_DIR}")

set(SUBSTRAIT_PROTOS
  substrait/plan.proto
  substrait/algebra.proto
  substrait/type.proto
  substrait/extensions/extensions.proto
  substrait/parameterized_types.proto
  substrait/function.proto
  substrait/type_expressions.proto
  substrait/capabilities.proto
)

set(SUBSTRAIT_CC_SRCS "")
set(SUBSTRAIT_H_SRCS  "")

foreach(proto ${SUBSTRAIT_PROTOS})
  get_filename_component(proto_dir  "${proto}" DIRECTORY)
  get_filename_component(proto_base "${proto}" NAME_WE)
  set(out_cc "${SUBSTRAIT_GEN_DIR}/${proto_dir}/${proto_base}.pb.cc")
  set(out_h  "${SUBSTRAIT_GEN_DIR}/${proto_dir}/${proto_base}.pb.h")
  add_custom_command(
    OUTPUT  "${out_cc}" "${out_h}"
    COMMAND "${Protobuf_PROTOC_EXECUTABLE}"
            --proto_path="${SUBSTRAIT_PROTO_DIR}"
            --cpp_out="${SUBSTRAIT_GEN_DIR}"
            "${SUBSTRAIT_PROTO_DIR}/${proto}"
    DEPENDS "${SUBSTRAIT_PROTO_DIR}/${proto}"
    COMMENT "Generating ${proto}"
  )
  list(APPEND SUBSTRAIT_CC_SRCS "${out_cc}")
  list(APPEND SUBSTRAIT_H_SRCS  "${out_h}")
endforeach()

add_library(substrait_proto STATIC ${SUBSTRAIT_CC_SRCS})
set_target_properties(substrait_proto PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(substrait_proto PUBLIC
  "${SUBSTRAIT_GEN_DIR}"
  "${Protobuf_INCLUDE_DIRS}"
)
target_link_libraries(substrait_proto PUBLIC protobuf::libprotobuf)
target_compile_options(substrait_proto PRIVATE -w)  # suppress proto warnings

# ── cudf_engine shared library ────────────────────────────────────────────────
set(CUDF_ENGINE_SOURCES
  src/cudf_engine.cpp
  src/substrait_planner.cpp
)

add_library(cudf_engine SHARED ${CUDF_ENGINE_SOURCES})

target_include_directories(cudf_engine
  PUBLIC  include
  PRIVATE "${SUBSTRAIT_GEN_DIR}"
          "${Protobuf_INCLUDE_DIRS}"
          "${RAPIDS_LOGGER_INCLUDE}"
)

target_compile_definitions(cudf_engine PRIVATE
  CCCL_IGNORE_DEPRECATED_STREAM_REF_HEADER
  LIBCUDACXX_ENABLE_EXPERIMENTAL_MEMORY_RESOURCE
)

target_link_libraries(cudf_engine
  PRIVATE
    cudf::cudf
    substrait_proto
    protobuf::libprotobuf
)

# ── Collect RAPIDS pip lib dirs for RPATH baking ─────────────────────────────
# Query Python once during configure so that libcudf_engine.so can find its
# transitive runtime deps (libnvcomp.so.5, libkvikio.so) without LD_LIBRARY_PATH.
if(NOT RAPIDS_LIB_DIRS)
  execute_process(
    COMMAND "${Python3_EXECUTABLE}" -c
      "import sysconfig, os; sp=sysconfig.get_path('purelib'); dirs=[os.path.join(sp,p,'lib64') for p in ['libcudf','librmm','libkvikio','rapids_logger','nvidia/libnvcomp']]; print(';'.join(d for d in dirs if os.path.isdir(d)))"
    OUTPUT_VARIABLE RAPIDS_LIB_DIRS
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET
  )
  message(STATUS "RAPIDS RPATH dirs: ${RAPIDS_LIB_DIRS}")
endif()

# Export only the cudf_engine_* C symbols.
set_target_properties(cudf_engine PROPERTIES
  CXX_VISIBILITY_PRESET hidden
  VISIBILITY_INLINES_HIDDEN ON
)

# Bake RPATH so that libnvcomp.so.5, libkvikio.so etc. resolve at runtime.
# Use DT_RPATH (--disable-new-dtags) instead of DT_RUNPATH because RUNPATH
# does NOT propagate to transitive dependencies, but RPATH does.
if(RAPIDS_LIB_DIRS)
  set_target_properties(cudf_engine PROPERTIES
    BUILD_RPATH "${RAPIDS_LIB_DIRS}"
    INSTALL_RPATH "${RAPIDS_LIB_DIRS}"
    BUILD_WITH_INSTALL_RPATH ON
    SKIP_BUILD_RPATH OFF
    SKIP_INSTALL_RPATH OFF
  )
  target_link_options(cudf_engine PRIVATE "LINKER:--disable-new-dtags")
endif()

# ── Install ───────────────────────────────────────────────────────────────────
install(TARGETS cudf_engine LIBRARY DESTINATION lib)
install(FILES include/cudf_engine.h DESTINATION include)
