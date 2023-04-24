//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/sqlite_transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "sqlite_db.hpp"

namespace duckdb {
class SQLiteCatalog;
class SQLiteTableEntry;

class SQLiteTransaction : public Transaction {
public:
	SQLiteTransaction(SQLiteCatalog &sqlite_catalog, TransactionManager &manager, ClientContext &context);
	~SQLiteTransaction() override;

	void Start();
	void Commit();
	void Rollback();

	SQLiteDB &GetDB();
	optional_ptr<CatalogEntry> GetCatalogEntry(const string &table_name);
	void DropEntry(CatalogType type, const string &table_name, bool cascade);
	void ClearTableEntry(const string &table_name);

	static SQLiteTransaction &Get(ClientContext &context, Catalog &catalog);

private:
	SQLiteCatalog &sqlite_catalog;
	SQLiteDB *db;
	SQLiteDB owned_db;
	case_insensitive_map_t<unique_ptr<CatalogEntry>> catalog_entries;
};

} // namespace duckdb
