#include "storage/sqlite_schema_entry.hpp"
#include "storage/sqlite_table_entry.hpp"
#include "storage/sqlite_transaction.hpp"
#include "duckdb/catalog/dependency_list.hpp"

namespace duckdb {

SQLiteSchemaEntry::SQLiteSchemaEntry(Catalog *catalog) :
	SchemaCatalogEntry(catalog, DEFAULT_SCHEMA, true) {
}

CatalogEntry *SQLiteSchemaEntry::AddEntryInternal(CatalogTransaction transaction, unique_ptr<StandardEntry> entry,
							   OnCreateConflict on_conflict, DependencyList dependencies) {
	throw InternalException("AddEntryInternal");
}
CatalogEntry *SQLiteSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo *info) {
	throw InternalException("CreateTable");
}
CatalogEntry *SQLiteSchemaEntry::CreateFunction(CatalogTransaction transaction, CreateFunctionInfo *info) {
	throw InternalException("CreateFunction");
}
void SQLiteSchemaEntry::Alter(ClientContext &context, AlterInfo *info) {
	throw InternalException("Alter");
}
void SQLiteSchemaEntry::Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry *)> &callback) {
	throw InternalException("Scan");
}
void SQLiteSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry *)> &callback) {
	throw InternalException("Scan");
}
void SQLiteSchemaEntry::DropEntry(ClientContext &context, DropInfo *info) {
	throw InternalException("DropEntry");
}
CatalogEntry *SQLiteSchemaEntry::GetEntry(CatalogTransaction transaction, CatalogType type, const string &name) {
	if (type != CatalogType::TABLE_ENTRY) {
		throw BinderException("Only tables are supported for now");
	}
	if (!transaction.transaction) {
		throw InternalException("No transaction!?");
	}
	auto sqlite_transaction = (SQLiteTransaction *) transaction.transaction;
	return sqlite_transaction->GetTable(name);
}
SimilarCatalogEntry SQLiteSchemaEntry::GetSimilarEntry(CatalogTransaction transaction, CatalogType type, const string &name) {
	throw InternalException("GetSimilarEntry");
}

}