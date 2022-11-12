cmake_minimum_required(VERSION 2.8.12)
set(TARGET_NAME sqlite_scanner)
project(${TARGET_NAME})
include_directories(sqlite)

set(PARAMETERS "-no-warnings")
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} sqlite_scanner.cpp
                         sqlite/sqlite3.c)
