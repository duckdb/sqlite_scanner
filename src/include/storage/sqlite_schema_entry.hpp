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

class SQLiteSchemaEntry : public SchemaCatalogEntry {
public:
	SQLiteSchemaEntry(Catalog *catalog);

public:
	CatalogEntry *CreateTable(CatalogTransaction transaction, BoundCreateTableInfo *info) override;
	CatalogEntry *CreateFunction(CatalogTransaction transaction, CreateFunctionInfo *info) override;
	void Alter(ClientContext &context, AlterInfo *info) override;
	void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry *)> &callback) override;
	void Scan(CatalogType type, const std::function<void(CatalogEntry *)> &callback) override;
	void DropEntry(ClientContext &context, DropInfo *info) override;
	CatalogEntry *GetEntry(CatalogTransaction transaction, CatalogType type, const string &name) override;
};

}
