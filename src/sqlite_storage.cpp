#include "duckdb.hpp"

#include "sqlite3.h"
#include "sqlite_utils.hpp"
#include "sqlite_storage.hpp"
#include "storage/sqlite_catalog.hpp"
#include "storage/sqlite_transaction_manager.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

static unique_ptr<Catalog> SQLiteAttach(StorageExtensionInfo *storage_info, AttachedDatabase &db, const string &name,
                                        AttachInfo &info, AccessMode access_mode) {
	Value sqlite_all_varchar;

	auto res = info.options.find("all_varchar");
	if (res != info.options.end()) {
		sqlite_all_varchar = res->second.DefaultCastAs(LogicalTypeId::BOOLEAN);
	} else {
		db.GetDatabase().TryGetCurrentSetting("sqlite_all_varchar",
												sqlite_all_varchar);
	}

	return make_uniq<SQLiteCatalog>(
		db, info.path, access_mode,
		sqlite_all_varchar.IsNull() ? false : BooleanValue::Get(sqlite_all_varchar)
	);
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
