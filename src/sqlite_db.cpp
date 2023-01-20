#include "sqlite_utils.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/column_list.hpp"

namespace duckdb {

SQLiteDB::SQLiteDB() : db(nullptr) {

}

SQLiteDB::SQLiteDB(sqlite3 *db) : db(db) {
}

SQLiteDB::~SQLiteDB() {
	Close();
}

SQLiteDB::SQLiteDB(SQLiteDB &&other) noexcept {
	std::swap(db, other.db);
}

SQLiteDB &SQLiteDB::operator=(SQLiteDB &&other) noexcept {
	std::swap(db, other.db);
	return *this;
}

SQLiteDB SQLiteDB::Open(const string &path, bool is_read_only, bool is_shared) {
	SQLiteDB result;
	int flags = SQLITE_OPEN_PRIVATECACHE;
	if (is_read_only) {
		flags |= SQLITE_OPEN_READONLY;
	} else {
		flags |= SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
	}
	if (!is_shared) {
		// FIXME: we should just make sure we are not re-using the same `sqlite3` object across threads
		flags |= SQLITE_OPEN_NOMUTEX;
	}
	SQLiteUtils::Check(sqlite3_open_v2(path.c_str(), &result.db, flags, nullptr), result.db);
	return result;
}

SQLiteStatement SQLiteDB::Prepare(const string &query) {
	SQLiteStatement stmt;
	stmt.db = db;
	SQLiteUtils::Check(sqlite3_prepare_v2(db, query.c_str(), -1, &stmt.stmt, nullptr), db);
	return stmt;
}

void SQLiteDB::Execute(const string &query) {
	SQLiteUtils::Check(sqlite3_exec(db, query.c_str(), nullptr, nullptr, nullptr), db);
}

bool SQLiteDB::IsOpen() {
	return db;
}

void SQLiteDB::Close() {
	if (!IsOpen()) {
		return;
	}
	sqlite3_close(db);
	db = nullptr;
}

vector<string> SQLiteDB::GetTables() {
	vector<string> result;
	SQLiteStatement stmt = Prepare("SELECT name FROM sqlite_master WHERE type='table'");
	while (stmt.Step()) {
		auto table_name = stmt.GetValue<string>(0);
		result.push_back(move(table_name));
	}
	return result;
}

void SQLiteDB::GetTableInfo(const string &table_name, ColumnList &columns, vector<unique_ptr<Constraint>> &constraints, bool all_varchar) {
	SQLiteStatement stmt;

	idx_t primary_key_index = idx_t(-1);
	vector<string> primary_keys;

	stmt = Prepare(StringUtil::Format("PRAGMA table_info(\"%s\")", table_name));
	while (stmt.Step()) {
		auto cid = stmt.GetValue<int>(0);
		auto sqlite_colname = stmt.GetValue<string>(1);
		auto sqlite_type = StringUtil::Lower(stmt.GetValue<string>(2));
		auto not_null = stmt.GetValue<int>(3);
		auto pk = stmt.GetValue<int>(5);
		StringUtil::Trim(sqlite_type);
		auto column_type = all_varchar ? LogicalType::VARCHAR : SQLiteUtils::TypeToLogicalType(sqlite_type);

		ColumnDefinition column(move(sqlite_colname), move(column_type));
		columns.AddColumn(move(column));
		if (not_null) {
			constraints.push_back(make_unique<NotNullConstraint>(LogicalIndex(cid)));
		}
		if (pk) {
			primary_key_index = cid;
			primary_keys.push_back(sqlite_colname);
		}
	}
	if (!primary_keys.empty()) {
		if (primary_keys.size() == 1) {
			constraints.push_back(make_unique<UniqueConstraint>(LogicalIndex(primary_key_index), true));
		} else {
			constraints.push_back(make_unique<UniqueConstraint>(move(primary_keys), true));
		}
	}
}

bool SQLiteDB::ColumnExists(const string &table_name, const string &column_name) {
	SQLiteStatement stmt;

	stmt = Prepare(StringUtil::Format("PRAGMA table_info(\"%s\")", table_name));
	while (stmt.Step()) {
		auto sqlite_colname = stmt.GetValue<string>(1);
		if (sqlite_colname == column_name) {
			return true;
		}
	}
	return false;
}

idx_t SQLiteDB::GetMaxRowId(const string &table_name) {
	SQLiteStatement stmt;
	stmt = Prepare(StringUtil::Format("SELECT MAX(ROWID) FROM \"%s\"", table_name));
	if (!stmt.Step()) {
		throw std::runtime_error("could not find max rowid?");
	}
	return stmt.GetValue<int64_t>(0);
}

}
