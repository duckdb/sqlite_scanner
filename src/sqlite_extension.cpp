#ifndef DUCKDB_BUILD_LOADABLE_EXTENSION
#define DUCKDB_BUILD_LOADABLE_EXTENSION
#endif
#include "duckdb.hpp"

#include "sqlite_scanner.hpp"
#include "sqlite_storage.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

using namespace duckdb;

namespace duckdb {

struct SQLiteReplacementOpen : public ReplacementOpenData {
	bool HasStorageExtension() override {
		return true;
	}

	unique_ptr<StorageExtension> GetStorageExtension(AttachInfo &info) override {
		return make_unique<SQLiteStorageExtension>();
	}
};

} // namespace duckdb

extern "C" {
DUCKDB_EXTENSION_API void sqlite_scanner_init(duckdb::DatabaseInstance &db) {
	Connection con(db);
	con.BeginTransaction();
	auto &context = *con.context;
	auto &catalog = Catalog::GetSystemCatalog(context);

	SqliteScanFunction sqlite_fun;
	CreateTableFunctionInfo sqlite_info(sqlite_fun);
	catalog.CreateTableFunction(context, &sqlite_info);

	SqliteAttachFunction attach_func;

	CreateTableFunctionInfo attach_info(attach_func);
	catalog.CreateTableFunction(context, &attach_info);

	auto &config = DBConfig::GetConfig(db);
	config.AddExtensionOption("sqlite_all_varchar", "Load all SQLite columns as VARCHAR columns", LogicalType::BOOLEAN);

	config.storage_extensions["sqlite"] = make_unique<SQLiteStorageExtension>();

	con.Commit();
}

DUCKDB_EXTENSION_API const char *sqlite_scanner_version() {
	return DuckDB::LibraryVersion();
}

unique_ptr<ReplacementOpenData> sqlite_scanner_replacement_open_pre(DBConfig &config,
                                                                    ReplacementOpenStaticData *static_data) {
	return make_unique<SQLiteReplacementOpen>();
}

void sqlite_scanner_replacement_open_post(DatabaseInstance &instance, ReplacementOpenData *open_data) {
	throw InternalException("replacement_open_post should never be called");
}
}
