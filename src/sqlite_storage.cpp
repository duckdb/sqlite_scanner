#include "duckdb.hpp"

#include "sqlite3.h"
#include "sqlite_utils.hpp"
#include "sqlite_storage.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"

namespace duckdb {

class SQLiteCatalog : public Catalog {
public:
	explicit SQLiteCatalog(AttachedDatabase &db_p, const string &path, AccessMode access_mode) : Catalog(db_p) {
		db = SQLiteDB::Open(path, access_mode == AccessMode::READ_ONLY ? true : false);
	}

public:
	void Initialize(bool load_builtin) override {
	}

	CatalogEntry *CreateSchema(CatalogTransaction transaction, CreateSchemaInfo *info) override {
		throw InternalException("CreateSchema");
	}

	void ScanSchemas(ClientContext &context, std::function<void(CatalogEntry *)> callback) override {
		throw InternalException("ScanSchemas");
	}

	SchemaCatalogEntry *GetSchema(CatalogTransaction transaction, const string &schema_name,
								  bool if_exists = false,
								  QueryErrorContext error_context = QueryErrorContext()) override {
		throw InternalException("GetSchema");
	}

private:
	void DropSchema(ClientContext &context, DropInfo *info) override {
		throw InternalException("DropSchema");

	}

private:
	SQLiteDB db;
};

static unique_ptr<Catalog> SQLiteAttach(AttachedDatabase &db, const string &name, AttachInfo &info, AccessMode access_mode) {
	return make_unique<SQLiteCatalog>(db, info.path, access_mode);
}

SQLiteStorageExtension::SQLiteStorageExtension() {
	attach = SQLiteAttach;
}

}

