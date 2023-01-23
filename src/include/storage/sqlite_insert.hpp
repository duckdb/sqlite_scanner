//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/sqlite_insert.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/index_vector.hpp"

namespace duckdb {

class SQLiteInsert : public PhysicalOperator {
public:
	//! INSERT INTO
	SQLiteInsert(LogicalOperator &op, TableCatalogEntry *table, physical_index_vector_t<idx_t> column_index_map);
	//! CREATE TABLE AS
	SQLiteInsert(LogicalOperator &op, SchemaCatalogEntry *schema, unique_ptr<BoundCreateTableInfo> info);

	//! The table to insert into
	TableCatalogEntry *table;
	//! Table schema, in case of CREATE TABLE AS
	SchemaCatalogEntry *schema;
	//! Create table info, in case of CREATE TABLE AS
	unique_ptr<BoundCreateTableInfo> info;
	//! column_index_map
	physical_index_vector_t<idx_t> column_index_map;

public:
	// Source interface
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	void GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
	             LocalSourceState &lstate) const override;

public:
	// Sink interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
	                    DataChunk &input) const override;

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return false;
	}

	string GetName() const override;
	string ParamsToString() const override;
};

} // namespace duckdb
