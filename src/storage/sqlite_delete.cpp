#include "storage/sqlite_delete.hpp"
#include "storage/sqlite_table_entry.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "storage/sqlite_catalog.hpp"
#include "storage/sqlite_transaction.hpp"
#include "sqlite_db.hpp"
#include "sqlite_stmt.hpp"

namespace duckdb {

SQLiteDelete::SQLiteDelete(LogicalOperator &op, TableCatalogEntry &table) :
	PhysicalOperator(PhysicalOperatorType::EXTENSION, op.types, 1), table(table) {}

//===--------------------------------------------------------------------===//
// States
//===--------------------------------------------------------------------===//
class SQLiteDeleteGlobalState : public GlobalSinkState {
public:
	explicit SQLiteDeleteGlobalState(SQLiteTableEntry &table)
	    : table(table), delete_count(0) {
	}

	SQLiteTableEntry &table;
	SQLiteStatement statement;
	idx_t delete_count;
};

string GetDeleteSQL(const string &table_name) {
	string result;
	result = "DELETE FROM " + KeywordHelper::WriteOptionallyQuoted(table_name);
	result += " WHERE rowid = ?";
	return result;
}

unique_ptr<GlobalSinkState> SQLiteDelete::GetGlobalSinkState(ClientContext &context) const {
	auto &sqlite_table = (SQLiteTableEntry &) table;

	auto &transaction = SQLiteTransaction::Get(context, *sqlite_table.catalog);
	auto result = make_unique<SQLiteDeleteGlobalState>(sqlite_table);
	result->statement = transaction.GetDB().Prepare(GetDeleteSQL(sqlite_table.name));
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType SQLiteDelete::Sink(ExecutionContext &context, GlobalSinkState &state_p, LocalSinkState &lstate,
					DataChunk &input) const {
	auto &gstate = (SQLiteDeleteGlobalState &) state_p;

	input.Flatten();
	auto &row_identifiers = input.data[0];
	auto row_data = FlatVector::GetData<row_t>(row_identifiers);
	for(idx_t i = 0; i < input.size(); i++) {
		gstate.statement.Bind<int64_t>(0, row_data[i]);
		gstate.statement.Step();
		gstate.statement.Reset();
	}
	gstate.delete_count += input.size();
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
class SQLiteDeleteSourceState : public GlobalSourceState {
public:
	bool finished = false;
};

unique_ptr<GlobalSourceState> SQLiteDelete::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<SQLiteDeleteSourceState>();
}

void SQLiteDelete::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                             LocalSourceState &lstate) const {
	auto &state = (SQLiteDeleteSourceState &)gstate;
	auto &insert_gstate = (SQLiteDeleteGlobalState &)*sink_state;
	if (state.finished) {
		return;
	}
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(insert_gstate.delete_count));
	state.finished = true;
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
string SQLiteDelete::GetName() const {
	return "DELETE";
}

string SQLiteDelete::ParamsToString() const {
	return table.name;
}

//===--------------------------------------------------------------------===//
// Plan
//===--------------------------------------------------------------------===//
unique_ptr<PhysicalOperator> SQLiteCatalog::PlanDelete(ClientContext &context, LogicalDelete &op, unique_ptr<PhysicalOperator> plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for deletion of a SQLite table");
	}
	auto insert = make_unique<SQLiteDelete>(op, *op.table);
	insert->children.push_back(std::move(plan));
	return std::move(insert);
}

}
