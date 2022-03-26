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

if(CMAKE_VERSION VERSION_LESS "3.1")
  add_compile_options(-std=c++11)
else()
  set(CMAKE_CXX_STANDARD 11)
endif()

project(sqlite_scanner)

set(CMAKE_VERBOSE_MAKEFILE ON)

message(${DUCKDB_INCLUDE_FOLDER})

include_directories(${DUCKDB_INCLUDE_FOLDER})
include_directories(sqlite/)
if("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
  add_compile_options(-Wall -Werror -pedantic -fsanitize=address)
endif()

link_directories(${DUCKDB_LIBRARY_FOLDER})

add_library(sqlite_scanner SHARED sqlite_scanner.cpp sqlite/sqlite3.c)
set_target_properties(sqlite_scanner PROPERTIES PREFIX "")

if("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
  target_link_libraries(sqlite_scanner -fsanitize=address)
endif()

set_target_properties(sqlite_scanner PROPERTIES SUFFIX ".duckdb_extension")

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
  set_target_properties(sqlite_scanner PROPERTIES LINK_FLAGS
                                                  "-undefined dynamic_lookup")
endif()

if(MSVC)
  set_target_properties(sqlite_scanner PROPERTIES RUNTIME_OUTPUT_DIRECTORY_DEBUG
                                                  ${CMAKE_BINARY_DIR})
  set_target_properties(
    sqlite_scanner PROPERTIES RUNTIME_OUTPUT_DIRECTORY_RELEASE
                              ${CMAKE_BINARY_DIR})
endif()

if(WIN32)
  set_target_properties(sqlite_scanner PROPERTIES LINK_FLAGS_RELEASE
                                                  "/DELAYLOAD:duckdb.dll")
  set(CMAKE_EXE_LINKER_FLAGS_RELEASE
      "${CMAKE_EXE_LINKER_FLAGS_RELEASE}  /DELAYLOAD:duckdb.dll")
endif()
