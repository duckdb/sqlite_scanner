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
#include "sqlite_db.hpp"

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

	SchemaCatalogEntry *GetSchema(CatalogTransaction transaction, const string &schema_name, bool if_exists = false,
	                              QueryErrorContext error_context = QueryErrorContext()) override;

	SQLiteSchemaEntry *GetMainSchema() {
		return main_schema.get();
	}

	unique_ptr<PhysicalOperator> PlanInsert(ClientContext &context, LogicalInsert &op,
	                                        unique_ptr<PhysicalOperator> plan) override;
	unique_ptr<PhysicalOperator> PlanCreateTableAs(ClientContext &context, LogicalCreateTable &op,
	                                               unique_ptr<PhysicalOperator> plan) override;
	unique_ptr<PhysicalOperator> PlanDelete(ClientContext &context, LogicalDelete &op,
	                                        unique_ptr<PhysicalOperator> plan) override;
	unique_ptr<PhysicalOperator> PlanUpdate(ClientContext &context, LogicalUpdate &op,
	                                        unique_ptr<PhysicalOperator> plan) override;
	unique_ptr<LogicalOperator> BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
	                                            unique_ptr<LogicalOperator> plan) override;

	//! Whether or not this is an in-memory SQLite database
	bool InMemory();
	//! Returns a reference to the in-memory database (if any)
	SQLiteDB *GetInMemoryDatabase();
	//! Release the in-memory database (if there is any)
	void ReleaseInMemoryDatabase();

private:
	void DropSchema(ClientContext &context, DropInfo *info) override;

private:
	unique_ptr<SQLiteSchemaEntry> main_schema;
	//! Whether or not the database is in-memory
	bool in_memory;
	//! In-memory database - if any
	SQLiteDB in_memory_db;
	//! The lock maintaing access to the in-memory database
	mutex in_memory_lock;
	//! Whether or not there is any active transaction on the in-memory database
	bool active_in_memory;
};

} // namespace duckdb
