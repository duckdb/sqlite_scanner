#include "storage/sqlite_update.hpp"
#include "storage/sqlite_table_entry.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "storage/sqlite_catalog.hpp"
#include "storage/sqlite_transaction.hpp"
#include "sqlite_db.hpp"
#include "sqlite_stmt.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> SQLiteCatalog::PlanUpdate(ClientContext &context, LogicalUpdate &op, unique_ptr<PhysicalOperator> plan) {
	throw BinderException("FIXME: update is unsupported for SQLite tables");
}

}
