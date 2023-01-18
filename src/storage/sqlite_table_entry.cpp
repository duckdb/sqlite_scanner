#include "storage/sqlite_catalog.hpp"
#include "storage/sqlite_table_entry.hpp"
#include "storage/sqlite_transaction.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "sqlite_scanner.hpp"

namespace duckdb {

SQLiteTableEntry::SQLiteTableEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateTableInfo &info) :
   TableCatalogEntry(catalog, schema, info) {}


unique_ptr<BaseStatistics> SQLiteTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	return nullptr;
}

//	string file_name;
//	string table_name;
//
//	vector<string> names;
//	vector<LogicalType> types;

TableFunction SQLiteTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	auto result = make_unique<SqliteBindData>();
	for(auto &col : columns.Logical()) {
		result->names.push_back(col.GetName());
		result->types.push_back(col.GetType());
	}
	auto sqlite_catalog = (SQLiteCatalog *) catalog;
	result->file_name = sqlite_catalog->path;
	result->table_name = name;

	auto &transaction = (SQLiteTransaction &) Transaction::Get(context, *catalog);
	result->max_rowid = transaction.GetDB().GetMaxRowId(name);

	bind_data = move(result);
	return SqliteScanFunction();
}

}
