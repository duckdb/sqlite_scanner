#include "storage/sqlite_transaction.hpp"
#include "storage/sqlite_catalog.hpp"
#include "storage/sqlite_schema_entry.hpp"
#include "storage/sqlite_table_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"

namespace duckdb {

SQLiteTransaction::SQLiteTransaction(SQLiteCatalog &sqlite_catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context), sqlite_catalog(sqlite_catalog) {
	if (sqlite_catalog.InMemory()) {
		// in-memory database - get a reference to the in-memory connection
		db = sqlite_catalog.GetInMemoryDatabase();
	} else {
		// on-disk database - open a new database connection
		owned_db = SQLiteDB::Open(sqlite_catalog.path,
		                          sqlite_catalog.access_mode == AccessMode::READ_ONLY ? true : false, true);
		db = &owned_db;
	}
}

SQLiteTransaction::~SQLiteTransaction() {
	sqlite_catalog.ReleaseInMemoryDatabase();
}

void SQLiteTransaction::Start() {
	db->Execute("BEGIN TRANSACTION");
}
void SQLiteTransaction::Commit() {
	db->Execute("COMMIT");
}
void SQLiteTransaction::Rollback() {
	db->Execute("ROLLBACK");
}

SQLiteDB &SQLiteTransaction::GetDB() {
	return *db;
}

SQLiteTransaction &SQLiteTransaction::Get(ClientContext &context, Catalog &catalog) {
	return (SQLiteTransaction &)Transaction::Get(context, catalog);
}

CatalogEntry *SQLiteTransaction::GetTableOrView(const string &table_name) {
	auto entry = tables.find(table_name);
	if (entry == tables.end()) {
		// table catalog entry not found - look up table in main SQLite database
		auto type = db->GetTableOrView(table_name);
		if (type == CatalogType::INVALID) {
			// no table or view found
			return nullptr;
		}
		unique_ptr<CatalogEntry> result;
		switch (type) {
		case CatalogType::TABLE_ENTRY: {
			CreateTableInfo info(sqlite_catalog.GetMainSchema(), table_name);
			// FIXME: all_varchar from config
			db->GetTableInfo(table_name, info.columns, info.constraints, false);
			D_ASSERT(!info.columns.empty());

			result = make_unique<SQLiteTableEntry>(&sqlite_catalog, sqlite_catalog.GetMainSchema(), info);
			break;
		}
		case CatalogType::VIEW_ENTRY: {
			string sql;
			db->GetViewInfo(table_name, sql);

			auto view_info = CreateViewInfo::FromCreateView(*context.lock(), sql);
			result = make_unique<ViewCatalogEntry>(&sqlite_catalog, sqlite_catalog.GetMainSchema(), view_info.get());
			break;
		}
		default:
			throw InternalException("Unrecognized table type");
		}
		auto result_ptr = result.get();
		tables[table_name] = move(result);
		return result_ptr;
	} else {
		return entry->second.get();
	}
}

void SQLiteTransaction::ClearTableEntry(const string &table_name) {
	tables.erase(table_name);
}

string GetDropSQL(CatalogType type, const string &table_name, bool cascade) {
	string result;
	result = "DROP ";
	switch (type) {
	case CatalogType::TABLE_ENTRY:
		result += "TABLE ";
		break;
	case CatalogType::VIEW_ENTRY:
		result += "VIEW ";
		break;
	default:
		throw InternalException("Unsupported type for drop");
	}
	result += KeywordHelper::WriteOptionallyQuoted(table_name);
	if (cascade) {
		result += " CASCADE";
	}
	return result;
}

void SQLiteTransaction::DropTableOrView(CatalogType type, const string &table_name, bool cascade) {
	tables.erase(table_name);
	db->Execute(GetDropSQL(type, table_name, cascade));
}

} // namespace duckdb
