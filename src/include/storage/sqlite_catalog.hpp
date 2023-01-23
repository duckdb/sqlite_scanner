//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/sqlite_catalog.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/enums/access_mode.hpp"

namespace duckdb {
class SQLiteSchemaEntry;

class SQLiteCatalog : public Catalog {
public:
	explicit SQLiteCatalog(AttachedDatabase &db_p, const string &path, AccessMode access_mode);
	~SQLiteCatalog();

	string path;
	AccessMode access_mode;

public:
	void Initialize(bool load_builtin) override;

	CatalogEntry *CreateSchema(CatalogTransaction transaction, CreateSchemaInfo *info) override;

	void ScanSchemas(ClientContext &context, std::function<void(CatalogEntry *)> callback) override;

	SchemaCatalogEntry *GetSchema(CatalogTransaction transaction, const string &schema_name,
								  bool if_exists = false,
								  QueryErrorContext error_context = QueryErrorContext()) override;

	SQLiteSchemaEntry *GetMainSchema() {
		return main_schema.get();
	}

	unique_ptr<PhysicalOperator> PlanInsert(ClientContext &context, LogicalInsert &op, unique_ptr<PhysicalOperator> plan) override;
	unique_ptr<PhysicalOperator> PlanCreateTableAs(ClientContext &context, LogicalCreateTable &op, unique_ptr<PhysicalOperator> plan) override;
	unique_ptr<PhysicalOperator> PlanDelete(ClientContext &context, LogicalDelete &op, unique_ptr<PhysicalOperator> plan) override;
	unique_ptr<PhysicalOperator> PlanUpdate(ClientContext &context, LogicalUpdate &op, unique_ptr<PhysicalOperator> plan) override;

private:
	void DropSchema(ClientContext &context, DropInfo *info) override;

private:
	unique_ptr<SQLiteSchemaEntry> main_schema;
};

}
