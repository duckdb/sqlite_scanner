#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/parser.hpp"
#include "sqlite_db.hpp"
#include "sqlite_stmt.hpp"
#include "duckdb/common/types/uuid.hpp"

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
	std::swap(vfs, other.vfs);
}

SQLiteDB &SQLiteDB::operator=(SQLiteDB &&other) noexcept {
	std::swap(db, other.db);
	std::swap(vfs, other.vfs);
	return *this;
}

SQLiteDB SQLiteDB::OpenInternal(const string &path, bool is_read_only, bool is_shared, optional_ptr<ClientContext> context) {
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
	flags |= SQLITE_OPEN_EXRESCODE;
	string vfs_name;
	if (context) {
		// register a virtual file system
		vfs_name = result.RegisterVFS(*context);
	}
	auto rc = sqlite3_open_v2(path.c_str(), &result.db, flags, vfs_name.empty() ? nullptr : vfs_name.c_str());
	if (rc != SQLITE_OK) {
		throw std::runtime_error("Unable to open database \"" + path + "\": " + string(sqlite3_errstr(rc)));
	}
	return result;
}

SQLiteDB SQLiteDB::Open(ClientContext &context, const string &path, bool is_read_only, bool is_shared) {
	return SQLiteDB::OpenInternal(path, is_read_only, is_shared, &context);
}

SQLiteDB SQLiteDB::Open(const string &path, bool is_read_only, bool is_shared) {
	return SQLiteDB::OpenInternal(path, is_read_only, is_shared, nullptr);
}


string SQLiteDB::RegisterVFS(ClientContext &context) {
	if (vfs) {
		throw InternalException("RegisterVFS called on DB that already has a VFS");
	}

	auto vfs_name = "_duckdb_sqlite_vfs_" + UUID::ToString(UUID::GenerateRandomUUID());
	vfs = make_uniq<SQLiteVirtualFileSystem>(vfs_name, context);
	sqlite3_vfs_register(vfs->GetSQLiteVFS(), 0);
	return vfs_name;
}

bool SQLiteDB::TryPrepare(const string &query, SQLiteStatement &stmt) {
	stmt.db = db;
	auto rc = sqlite3_prepare_v2(db, query.c_str(), -1, &stmt.stmt, nullptr);
	if (rc != SQLITE_OK) {
		return false;
	}
	return true;
}

SQLiteStatement SQLiteDB::Prepare(const string &query) {
	SQLiteStatement stmt;
	if (!TryPrepare(query, stmt)) {
		string error = "Failed to prepare query \"" + query + "\": " + string(sqlite3_errmsg(db));
		throw std::runtime_error(error);
	}
	return stmt;
}

void SQLiteDB::Execute(const string &query) {
	auto rc = sqlite3_exec(db, query.c_str(), nullptr, nullptr, nullptr);
	if (rc != SQLITE_OK) {
		string error = "Failed to execute query \"" + query + "\": " + string(sqlite3_errmsg(db));
		throw std::runtime_error(error);
	}
}

bool SQLiteDB::IsOpen() {
	return db;
}

void SQLiteDB::Close() {
	if (!IsOpen()) {
		return;
	}
	auto rc = sqlite3_close_v2(db);
	if (rc == SQLITE_BUSY) {
		throw InternalException("Failed to close database - SQLITE_BUSY");
	}
	db = nullptr;
}

vector<string> SQLiteDB::GetEntries(string entry_type) {
	vector<string> result;
	SQLiteStatement stmt = Prepare("SELECT name FROM sqlite_master WHERE type='" + entry_type + "'");
	while (stmt.Step()) {
		auto table_name = stmt.GetValue<string>(0);
		result.push_back(std::move(table_name));
	}
	return result;
}

vector<string> SQLiteDB::GetTables() {
	return GetEntries("table");
}

CatalogType SQLiteDB::GetEntryType(const string &name) {
	SQLiteStatement stmt;
	stmt = Prepare(StringUtil::Format("SELECT type FROM sqlite_master WHERE lower(name)=lower('%s');",
	                                  SQLiteUtils::SanitizeString(name)));
	while (stmt.Step()) {
		auto type = stmt.GetValue<string>(0);
		if (type == "table") {
			return CatalogType::TABLE_ENTRY;
		} else if (type == "view") {
			return CatalogType::VIEW_ENTRY;
		} else if (type == "index") {
			return CatalogType::INDEX_ENTRY;
		} else {
			throw InternalException("Unrecognized SQLite type \"%s\"", name);
		}
	}
	return CatalogType::INVALID;
}

void SQLiteDB::GetIndexInfo(const string &index_name, string &sql, string &table_name) {
	SQLiteStatement stmt;
	stmt = Prepare(StringUtil::Format("SELECT tbl_name, sql FROM sqlite_master WHERE lower(name)=lower('%s');",
	                                  SQLiteUtils::SanitizeString(index_name)));
	while (stmt.Step()) {
		table_name = stmt.GetValue<string>(0);
		sql = stmt.GetValue<string>(1);
		return;
	}
	throw InternalException("GetViewInfo - index \"%s\" not found", index_name);
}

void SQLiteDB::GetViewInfo(const string &view_name, string &sql) {
	SQLiteStatement stmt;
	stmt = Prepare(StringUtil::Format("SELECT sql FROM sqlite_master WHERE lower(name)=lower('%s');",
	                                  SQLiteUtils::SanitizeString(view_name)));
	while (stmt.Step()) {
		sql = stmt.GetValue<string>(0);
		return;
	}
	throw InternalException("GetViewInfo - view \"%s\" not found", view_name);
}

void SQLiteDB::GetTableInfo(const string &table_name, ColumnList &columns, vector<unique_ptr<Constraint>> &constraints,
                            bool all_varchar) {
	SQLiteStatement stmt;

	idx_t primary_key_index = idx_t(-1);
	vector<string> primary_keys;

	bool found = false;

	stmt = Prepare(StringUtil::Format("PRAGMA table_info('%s')", SQLiteUtils::SanitizeString(table_name)));
	while (stmt.Step()) {
		auto cid = stmt.GetValue<int>(0);
		auto sqlite_colname = stmt.GetValue<string>(1);
		auto sqlite_type = StringUtil::Lower(stmt.GetValue<string>(2));
		auto not_null = stmt.GetValue<int>(3);
		auto default_value = stmt.GetValue<string>(4);
		auto pk = stmt.GetValue<int>(5);
		StringUtil::Trim(sqlite_type);
		auto column_type = all_varchar ? LogicalType::VARCHAR : SQLiteUtils::TypeToLogicalType(sqlite_type);

		ColumnDefinition column(std::move(sqlite_colname), std::move(column_type));
		if (!default_value.empty()) {
			auto expressions = Parser::ParseExpressionList(default_value);
			if (expressions.empty()) {
				throw InternalException("Expression list is empty");
			}
			column.SetDefaultValue(std::move(expressions[0]));
		}
		columns.AddColumn(std::move(column));
		if (not_null) {
			constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(cid)));
		}
		if (pk) {
			primary_key_index = cid;
			primary_keys.push_back(sqlite_colname);
		}
		found = true;
	}
	if (!found) {
		throw InternalException("GetTableInfo - table \"%s\" not found", table_name);
	}
	if (!primary_keys.empty()) {
		if (primary_keys.size() == 1) {
			constraints.push_back(make_uniq<UniqueConstraint>(LogicalIndex(primary_key_index), true));
		} else {
			constraints.push_back(make_uniq<UniqueConstraint>(std::move(primary_keys), true));
		}
	}
}

bool SQLiteDB::ColumnExists(const string &table_name, const string &column_name) {
	SQLiteStatement stmt;

	stmt = Prepare(StringUtil::Format("PRAGMA table_info(\"%s\")", SQLiteUtils::SanitizeIdentifier(table_name)));
	while (stmt.Step()) {
		auto sqlite_colname = stmt.GetValue<string>(1);
		if (sqlite_colname == column_name) {
			return true;
		}
	}
	return false;
}

bool SQLiteDB::GetMaxRowId(const string &table_name, idx_t &max_row_id) {
	SQLiteStatement stmt;
	if (!TryPrepare(StringUtil::Format("SELECT MAX(ROWID) FROM \"%s\"", SQLiteUtils::SanitizeIdentifier(table_name)),
	                stmt)) {
		return false;
	}
	if (!stmt.Step()) {
		return false;
	}
	int64_t val = stmt.GetValue<int64_t>(0);;
	if (val <= 0) {
		return false;
	}
	max_row_id = idx_t(val);
	return true;
}

idx_t SQLiteDB::RunPragma(string pragma_name) {
	SQLiteStatement stmt;
	stmt = Prepare("PRAGMA " + pragma_name);
	while (stmt.Step()) {
		return idx_t(stmt.GetValue<int64_t>(0));
	}
	throw InternalException("No result returned from pragma " + pragma_name);
}

} // namespace duckdb
