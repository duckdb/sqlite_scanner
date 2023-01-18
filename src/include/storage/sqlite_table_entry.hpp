//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/sqlite_table_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

class SQLiteTableEntry : public TableCatalogEntry {
public:
	SQLiteTableEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateTableInfo &info);

public:
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;

	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
};

}
