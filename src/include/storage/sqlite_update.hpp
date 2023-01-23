//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/sqlite_update.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/index_vector.hpp"

namespace duckdb {

class SQLiteUpdate : public PhysicalOperator {
public:
	SQLiteUpdate(LogicalOperator &op, TableCatalogEntry &table, vector<PhysicalIndex> columns);

	//! The table to delete from
	TableCatalogEntry &table;
	//! The set of columns to update
	vector<PhysicalIndex> columns;

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


}
