#include "storage/sqlite_catalog.hpp"
#include "storage/sqlite_schema_entry.hpp"

namespace duckdb {

SQLiteCatalog::SQLiteCatalog(AttachedDatabase &db_p, const string &path, AccessMode access_mode)
    : Catalog(db_p), path(path), access_mode(access_mode), in_memory(path == ":memory:"), active_in_memory(false) {
	if (InMemory()) {
		in_memory_db = SQLiteDB::Open(path, false, true);
	}
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

bool SQLiteCatalog::InMemory() {
	return in_memory;
}

SQLiteDB *SQLiteCatalog::GetInMemoryDatabase() {
	if (!InMemory()) {
		throw InternalException("GetInMemoryDatabase() called on a non-in-memory database");
	}
	lock_guard<mutex> l(in_memory_lock);
	if (active_in_memory) {
		throw TransactionException("Only a single transaction can be active on an in-memory SQLite database at a time");
	}
	active_in_memory = true;
	return &in_memory_db;
}

void SQLiteCatalog::ReleaseInMemoryDatabase() {
	if (!InMemory()) {
		return;
	}
	lock_guard<mutex> l(in_memory_lock);
	if (!active_in_memory) {
		throw InternalException(
		    "ReleaseInMemoryDatabase called but there is no active transaction on an in-memory database");
	}
	active_in_memory = false;
}

void SQLiteCatalog::DropSchema(ClientContext &context, DropInfo *info) {
	throw BinderException("SQLite databases do not support dropping schemas");
}

} // namespace duckdb
