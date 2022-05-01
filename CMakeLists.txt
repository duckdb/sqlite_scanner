cmake_minimum_required(VERSION 2.8.12)

if(POLICY CMP0026)
  cmake_policy(SET CMP0026 NEW)
endif()

if(POLICY CMP0015)
  cmake_policy(SET CMP0015 NEW)
endif()

if(POLICY CMP0042)
  cmake_policy(SET CMP0042 NEW)
endif()

if(POLICY CMP0051)
  cmake_policy(SET CMP0051 NEW)
endif()

if(POLICY CMP0054)
  cmake_policy(SET CMP0054 NEW)
endif()

if(CMAKE_VERSION VERSION_LESS "3.1")
  add_compile_options(-std=c++11)
else()
  set(CMAKE_CXX_STANDARD 11)
endif()

set(TARGET_NAME sqlite_scanner)

project(${TARGET_NAME})

set(CMAKE_VERBOSE_MAKEFILE OFF)

option(
  OSX_BUILD_UNIVERSAL
  "Build both architectures on OSX and create a single binary containing both."
  FALSE)
if(OSX_BUILD_UNIVERSAL)
  if(NOT APPLE)
    error("This only makes sense on OSX")
  endif()
  set(CMAKE_OSX_ARCHITECTURES
      "x86_64;arm64"
      CACHE STRING "Build architectures for Mac OS X" FORCE)
endif()

include_directories(${DUCKDB_INCLUDE_FOLDER})
include_directories(sqlite)

if("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
  add_compile_options(-Wall -Werror -pedantic -fsanitize=address)
endif()

link_directories(${DUCKDB_LIBRARY_FOLDER})

add_library(${TARGET_NAME} SHARED sqlite_scanner.cpp sqlite/sqlite3.c)
set_target_properties(${TARGET_NAME} PROPERTIES PREFIX "")

if("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
  target_link_libraries(${TARGET_NAME} -fsanitize=address)
endif()

set_target_properties(${TARGET_NAME} PROPERTIES SUFFIX ".duckdb_extension")

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
  set_target_properties(${TARGET_NAME} PROPERTIES LINK_FLAGS
                                                  "-undefined dynamic_lookup")
endif()

if(WIN32)
  target_link_libraries(${TARGET_NAME} duckdb ${DUCKDB_EXTRA_LINK_FLAGS})
endif()

if(MSVC)
  set_target_properties(
    ${TARGET_NAME} PROPERTIES RUNTIME_OUTPUT_DIRECTORY_DEBUG
                              "${CMAKE_BINARY_DIR}/${OUTPUT_DIRECTORY}")
  set_target_properties(
    ${TARGET_NAME} PROPERTIES RUNTIME_OUTPUT_DIRECTORY_RELEASE
                              "${CMAKE_BINARY_DIR}/${OUTPUT_DIRECTORY}")
endif()

if(WIN32)
  set_target_properties(${TARGET_NAME} PROPERTIES LINK_FLAGS_DEBUG
                                                  "/DELAYLOAD:duckdb.dll")
  set(CMAKE_EXE_LINKER_FLAGS_DEBUG
      "${CMAKE_EXE_LINKER_FLAGS_DEBUG}  /DELAYLOAD:duckdb.dll")
  set_target_properties(${TARGET_NAME} PROPERTIES LINK_FLAGS_RELEASE
                                                  "/DELAYLOAD:duckdb.dll")
  set(CMAKE_EXE_LINKER_FLAGS_RELEASE
      "${CMAKE_EXE_LINKER_FLAGS_RELEASE}  /DELAYLOAD:duckdb.dll")
  # This is only strictly required in non-Visual-Studio builds like Ninja:
  target_link_libraries(${TARGET_NAME} delayimp)
endif()
