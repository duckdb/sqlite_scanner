cmake_minimum_required(VERSION 2.8.12)
set(TARGET_NAME sqlite_scanner)
project(${TARGET_NAME})
include_directories(sqlite)

add_subdirectory(src)

set(PARAMETERS "-no-warnings")
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${ALL_OBJECT_FILES} sqlite/sqlite3.c)
