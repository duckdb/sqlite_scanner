//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/sqlite_index_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"

namespace duckdb {

class SQLiteIndexEntry : public IndexCatalogEntry {
public:
	SQLiteIndexEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateIndexInfo *info, string table_name);

	string table_name;

public:
	string GetSchemaName() override;
	string GetTableName() override;
};

} // namespace duckdb
