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
	~SQLiteTransaction();

	void Start();
	void Commit();
	void Rollback();

	SQLiteDB &GetDB();
	SQLiteTableEntry *GetTable(const string &table_name);
	void DropTable(const string &table_name, bool cascade);

	static SQLiteTransaction &Get(ClientContext &context, Catalog &catalog);

private:
	SQLiteCatalog &sqlite_catalog;
	SQLiteDB db;
	case_insensitive_map_t<unique_ptr<SQLiteTableEntry>> tables;
};

}
