#include "storage/sqlite_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"

namespace duckdb {

SQLiteIndexEntry::SQLiteIndexEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateIndexInfo *info,
                                   string table_name_p)
    : IndexCatalogEntry(catalog, schema, info), table_name(move(table_name_p)) {
}

string SQLiteIndexEntry::GetSchemaName() {
	return schema->name;
}

string SQLiteIndexEntry::GetTableName() {
	return table_name;
}

} // namespace duckdb
