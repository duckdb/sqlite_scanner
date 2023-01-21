//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sqlite_db.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sqlite_utils.hpp"

namespace duckdb {
class SQLiteStatement;

class SQLiteDB {
public:
	SQLiteDB();
	SQLiteDB(sqlite3 *db);
	~SQLiteDB();
	// disable copy constructors
	SQLiteDB(const SQLiteDB &other) = delete;
	SQLiteDB &operator=(const SQLiteDB &) = delete;
	//! enable move constructors
	SQLiteDB(SQLiteDB &&other) noexcept;
	SQLiteDB &operator=(SQLiteDB &&) noexcept;

	sqlite3 *db;

public:
	static SQLiteDB Open(const string &path, bool is_read_only = true, bool is_shared = false);
	SQLiteStatement Prepare(const string &query);
	void Execute(const string &query);
	vector<string> GetTables();
	void GetTableInfo(const string &table_name, ColumnList &columns, vector<unique_ptr<Constraint>> &constraints, bool all_varchar);
	idx_t GetMaxRowId(const string &table_name);
	bool ColumnExists(const string &table_name, const string &column_name);

	bool IsOpen();
	void Close();
};

}
