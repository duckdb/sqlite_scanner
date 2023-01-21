//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sqlite_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "sqlite3.h"

namespace duckdb {

class SQLiteUtils {
public:
	static void Check(int rc, sqlite3 *db);
	static string TypeToString(int sqlite_type);
	static LogicalType TypeToLogicalType(const string &sqlite_type);
	static string SanitizeIdentifier(const string &table_name);
};

}
