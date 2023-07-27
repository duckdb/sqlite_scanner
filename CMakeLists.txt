cmake_minimum_required(VERSION 2.8.12)
set(TARGET_NAME sqlite_scanner)
set(EXTENSION_NAME ${TARGET_NAME}_extension)
project(${TARGET_NAME})

include_directories(src/sqlite)

add_subdirectory(src)
set(EXTENSION_OBJECT_FILES ${ALL_OBJECT_FILES} src/sqlite/sqlite3.c)
add_library(${EXTENSION_NAME} STATIC ${EXTENSION_OBJECT_FILES})

set(PARAMETERS "-warnings")
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${EXTENSION_OBJECT_FILES})

install(
        TARGETS ${EXTENSION_NAME}
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")