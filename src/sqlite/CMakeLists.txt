add_library(
  sqlite_lib OBJECT
  sqlite3.c)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:sqlite_lib>
    PARENT_SCOPE)

target_compile_definitions(
  sqlite_lib
  PUBLIC SQLITE_ENABLE_FTS5 SQLITE_ENABLE_FTS4 SQLITE_ENABLE_FTS3_PARENTHESIS
         SQLITE_ENABLE_RTREE)