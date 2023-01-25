#include "storage/sqlite_transaction_manager.hpp"

namespace duckdb {

SQLiteTransactionManager::SQLiteTransactionManager(AttachedDatabase &db_p, SQLiteCatalog &sqlite_catalog)
    : TransactionManager(db_p), sqlite_catalog(sqlite_catalog) {
}

Transaction *SQLiteTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_unique<SQLiteTransaction>(sqlite_catalog, *this, context);
	transaction->Start();
	auto result = transaction.get();
	lock_guard<mutex> l(transaction_lock);
	transactions[result] = move(transaction);
	return result;
}

string SQLiteTransactionManager::CommitTransaction(ClientContext &context, Transaction *transaction) {
	auto sqlite_transaction = (SQLiteTransaction *)transaction;
	sqlite_transaction->Commit();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
	return string();
}

void SQLiteTransactionManager::RollbackTransaction(Transaction *transaction) {
	auto sqlite_transaction = (SQLiteTransaction *)transaction;
	sqlite_transaction->Rollback();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
}

void SQLiteTransactionManager::Checkpoint(ClientContext &context, bool force) {
	throw InternalException("Checkpoint");
}

} // namespace duckdb
