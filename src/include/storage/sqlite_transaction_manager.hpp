//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/sqlite_transaction_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction_manager.hpp"
#include "storage/sqlite_catalog.hpp"
#include "storage/sqlite_transaction.hpp"

namespace duckdb {

class SQLiteTransactionManager : public TransactionManager {
public:
	SQLiteTransactionManager(AttachedDatabase &db_p, SQLiteCatalog &sqlite_catalog);

	Transaction *StartTransaction(ClientContext &context) override;
	string CommitTransaction(ClientContext &context, Transaction *transaction) override;
	void RollbackTransaction(Transaction *transaction) override;

	void Checkpoint(ClientContext &context, bool force = false) override;

private:
	SQLiteCatalog &sqlite_catalog;
	mutex transaction_lock;
	unordered_map<Transaction *, unique_ptr<SQLiteTransaction>> transactions;
};

} // namespace duckdb
