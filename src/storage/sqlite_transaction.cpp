#include "storage/sqlite_transaction.hpp"
#include "storage/sqlite_catalog.hpp"
#include "storage/sqlite_schema_entry.hpp"
#include "storage/sqlite_table_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {

SQLiteTransaction::SQLiteTransaction(SQLiteCatalog &sqlite_catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context), sqlite_catalog(sqlite_catalog) {
	db = SQLiteDB::Open(sqlite_catalog.path, sqlite_catalog.access_mode == AccessMode::READ_ONLY ? true : false, true);
}

SQLiteTransaction::~SQLiteTransaction() {
}

void SQLiteTransaction::Start() {
	db.Execute("BEGIN TRANSACTION");
}
void SQLiteTransaction::Commit() {
	db.Execute("COMMIT");
}
void SQLiteTransaction::Rollback() {
	db.Execute("ROLLBACK");
}

SQLiteDB &SQLiteTransaction::GetDB() {
	return db;
}

SQLiteTransaction &SQLiteTransaction::Get(ClientContext &context, Catalog &catalog) {
	return (SQLiteTransaction &)Transaction::Get(context, catalog);
}

SQLiteTableEntry *SQLiteTransaction::GetTable(const string &table_name) {
	auto entry = tables.find(table_name);
	if (entry == tables.end()) {
		// table catalog entry not found - look up table in main SQLite database
		CreateTableInfo info(sqlite_catalog.GetMainSchema(), table_name);
		// FIXME: all_varchar from config
		db.GetTableInfo(table_name, info.columns, info.constraints, false);
		if (info.columns.empty()) {
			// table not found in SQLite database
			return nullptr;
		}

		auto table = make_unique<SQLiteTableEntry>(&sqlite_catalog, sqlite_catalog.GetMainSchema(), info);
		auto result = table.get();
		tables[table_name] = move(table);
		return result;
	} else {
		return entry->second.get();
	}
}

void SQLiteTransaction::ClearTableEntry(const string &table_name) {
	tables.erase(table_name);
}

string GetDropSQL(const string &table_name, bool cascade) {
	string result;
	result = "DROP TABLE ";
	result += KeywordHelper::WriteOptionallyQuoted(table_name);
	if (cascade) {
		result += " CASCADE";
	}
	return result;
}

void SQLiteTransaction::DropTable(const string &table_name, bool cascade) {
	tables.erase(table_name);
	db.Execute(GetDropSQL(table_name, cascade));
}

} // namespace duckdb
