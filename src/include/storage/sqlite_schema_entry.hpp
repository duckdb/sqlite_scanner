//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/sqlite_schema_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"

namespace duckdb {
class SQLiteTransaction;

class SQLiteSchemaEntry : public SchemaCatalogEntry {
public:
	SQLiteSchemaEntry(Catalog *catalog);

public:
	CatalogEntry *CreateTable(CatalogTransaction transaction, BoundCreateTableInfo *info) override;
	CatalogEntry *CreateFunction(CatalogTransaction transaction, CreateFunctionInfo *info) override;
	CatalogEntry *CreateIndex(ClientContext &context, CreateIndexInfo *info, TableCatalogEntry *table) override;
	CatalogEntry *CreateView(CatalogTransaction transaction, CreateViewInfo *info) override;
	CatalogEntry *CreateSequence(CatalogTransaction transaction, CreateSequenceInfo *info) override;
	CatalogEntry *CreateTableFunction(CatalogTransaction transaction, CreateTableFunctionInfo *info) override;
	CatalogEntry *CreateCopyFunction(CatalogTransaction transaction, CreateCopyFunctionInfo *info) override;
	CatalogEntry *CreatePragmaFunction(CatalogTransaction transaction, CreatePragmaFunctionInfo *info) override;
	CatalogEntry *CreateCollation(CatalogTransaction transaction, CreateCollationInfo *info) override;
	CatalogEntry *CreateType(CatalogTransaction transaction, CreateTypeInfo *info) override;
	void Alter(ClientContext &context, AlterInfo *info) override;
	void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry *)> &callback) override;
	void Scan(CatalogType type, const std::function<void(CatalogEntry *)> &callback) override;
	void DropEntry(ClientContext &context, DropInfo *info) override;
	CatalogEntry *GetEntry(CatalogTransaction transaction, CatalogType type, const string &name) override;

private:
	void AlterTable(SQLiteTransaction &transaction, RenameTableInfo &info);
	void AlterTable(SQLiteTransaction &transaction, RenameColumnInfo &info);
	void AlterTable(SQLiteTransaction &transaction, AddColumnInfo &info);
	void AlterTable(SQLiteTransaction &transaction, RemoveColumnInfo &info);

	void TryDropEntry(ClientContext &context, CatalogType catalog_type, const string &name);
};

} // namespace duckdb
