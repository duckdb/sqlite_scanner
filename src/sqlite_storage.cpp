#include "duckdb.hpp"

#include "sqlite3.h"
#include "sqlite_utils.hpp"
#include "sqlite_storage.hpp"
#include "storage/sqlite_catalog.hpp"
#include "storage/sqlite_transaction_manager.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

static unique_ptr<Catalog> SQLiteAttach(StorageExtensionInfo *storage_info, ClientContext &context,
                                        AttachedDatabase &db, const string &name, AttachInfo &info,
                                        AccessMode access_mode) {
	SQLiteOpenOptions options;
	options.access_mode = access_mode;
	for(auto &entry : info.options) {
		if (StringUtil::CIEquals(entry.first, "busy_timeout")) {
			options.busy_timeout = entry.second.GetValue<uint64_t>();
		} else if (StringUtil::CIEquals(entry.first, "journal_mode")) {
			options.journal_mode = entry.second.ToString();
		}
	}
	return make_uniq<SQLiteCatalog>(db, info.path, std::move(options));
}

static unique_ptr<TransactionManager> SQLiteCreateTransactionManager(StorageExtensionInfo *storage_info,
                                                                     AttachedDatabase &db, Catalog &catalog) {
	auto &sqlite_catalog = catalog.Cast<SQLiteCatalog>();
	return make_uniq<SQLiteTransactionManager>(db, sqlite_catalog);
}

SQLiteStorageExtension::SQLiteStorageExtension() {
	attach = SQLiteAttach;
	create_transaction_manager = SQLiteCreateTransactionManager;
}

} // namespace duckdb
