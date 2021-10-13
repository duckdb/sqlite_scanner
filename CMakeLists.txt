cmake_minimum_required(VERSION 3.1)

if(POLICY CMP0026)
  cmake_policy(SET CMP0026 NEW)
endif()

if(POLICY CMP0042)
  cmake_policy(SET CMP0042 NEW)
endif()

if(POLICY CMP0051)
  cmake_policy(SET CMP0051 NEW)
endif()

set (CMAKE_CXX_STANDARD 11)

project(sqlite_scanner)

# skip building extensions on mingw
if(WIN32 AND NOT MSVC)
  return()
endif()

include_directories(duckdb/src/include)
include_directories(sqlite/build/release/include)
link_directories(duckdb/build/release/src)


add_library(sqlite_scanner SHARED sqlite_scanner.cpp sqlite/build/release/sqlite3.o)
set_target_properties(sqlite_scanner PROPERTIES PREFIX "")
target_link_libraries(sqlite_scanner duckdb)
target_link_libraries(sqlite_scanner )

set_target_properties(sqlite_scanner PROPERTIES SUFFIX
                                                         ".duckdb_extension")

if(MSVC)
  set_target_properties(
    sqlite_scanner PROPERTIES RUNTIME_OUTPUT_DIRECTORY_DEBUG
                                       ${CMAKE_BINARY_DIR})
  set_target_properties(
    sqlite_scanner PROPERTIES RUNTIME_OUTPUT_DIRECTORY_RELEASE
                                       ${CMAKE_BINARY_DIR})
endif()

if(WIN32)
  set_target_properties(sqlite_scanner
                        PROPERTIES LINK_FLAGS_RELEASE "/DELAYLOAD:duckdb.dll")
  set(CMAKE_EXE_LINKER_FLAGS_RELEASE
      "${CMAKE_EXE_LINKER_FLAGS_RELEASE}  /DELAYLOAD:duckdb.dll")
endif()
