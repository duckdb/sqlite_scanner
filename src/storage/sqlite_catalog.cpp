#include "storage/sqlite_catalog.hpp"
#include "storage/sqlite_schema_entry.hpp"

namespace duckdb {

SQLiteCatalog::SQLiteCatalog(AttachedDatabase &db_p, const string &path, AccessMode access_mode)
    : Catalog(db_p), path(path), access_mode(access_mode) {
}

SQLiteCatalog::~SQLiteCatalog() {
}

void SQLiteCatalog::Initialize(bool load_builtin) {
	main_schema = make_unique<SQLiteSchemaEntry>(this);
}

CatalogEntry *SQLiteCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo *info) {
	throw BinderException("SQLite databases do not support creating new schemas");
}

void SQLiteCatalog::ScanSchemas(ClientContext &context, std::function<void(CatalogEntry *)> callback) {
	callback(main_schema.get());
}

SchemaCatalogEntry *SQLiteCatalog::GetSchema(CatalogTransaction transaction, const string &schema_name, bool if_exists,
                                             QueryErrorContext error_context) {
	if (schema_name == DEFAULT_SCHEMA || schema_name == INVALID_SCHEMA) {
		return main_schema.get();
	}
	if (if_exists) {
		return nullptr;
	}
	throw BinderException("SQLite databases only have a single schema - \"%s\"", DEFAULT_SCHEMA);
}

void SQLiteCatalog::DropSchema(ClientContext &context, DropInfo *info) {
	throw BinderException("SQLite databases do not support dropping schemas");
}

} // namespace duckdb
