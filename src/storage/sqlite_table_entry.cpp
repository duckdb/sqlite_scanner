#include "storage/sqlite_catalog.hpp"
#include "storage/sqlite_table_entry.hpp"
#include "storage/sqlite_transaction.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "sqlite_scanner.hpp"
#include "duckdb/storage/table_storage_info.hpp"

namespace duckdb {

SQLiteTableEntry::SQLiteTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info)
    : TableCatalogEntry(catalog, schema, info) {
}

unique_ptr<BaseStatistics> SQLiteTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	return nullptr;
}

void SQLiteTableEntry::BindUpdateConstraints(LogicalGet &, LogicalProjection &, LogicalUpdate &, ClientContext &) {
}

TableFunction SQLiteTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	auto result = make_uniq<SqliteBindData>();
	for (auto &col : columns.Logical()) {
		result->names.push_back(col.GetName());
		result->types.push_back(col.GetType());
	}
	auto &sqlite_catalog = catalog.Cast<SQLiteCatalog>();
	result->file_name = sqlite_catalog.path;
	result->table_name = name;

	auto &transaction = Transaction::Get(context, catalog).Cast<SQLiteTransaction>();
	auto &db = transaction.GetDB();

	if (!db.GetMaxRowId(name, result->max_rowid)) {
		result->max_rowid = idx_t(-1);
		result->rows_per_group = idx_t(-1);
	}
	if (!transaction.IsReadOnly() || sqlite_catalog.InMemory()) {
		// for in-memory databases or if we have transaction-local changes we can only do a single-threaded scan
		// set up the transaction's connection object as the global db
		result->global_db = &db;
		result->rows_per_group = idx_t(-1);
	}

	bind_data = std::move(result);
	return SqliteScanFunction();
}

TableStorageInfo SQLiteTableEntry::GetStorageInfo(ClientContext &context) {
	auto &transaction = Transaction::Get(context, catalog).Cast<SQLiteTransaction>();
	auto &db = transaction.GetDB();
	TableStorageInfo result;
	if (!db.GetMaxRowId(name, result.cardinality)) {
		// probably
		result.cardinality = 10000;
	}
	result.index_info = db.GetIndexInfo(name);
	return result;
}

} // namespace duckdb
