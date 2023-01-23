#include "storage/sqlite_update.hpp"
#include "storage/sqlite_table_entry.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "storage/sqlite_catalog.hpp"
#include "storage/sqlite_transaction.hpp"
#include "sqlite_db.hpp"
#include "sqlite_stmt.hpp"

namespace duckdb {

SQLiteUpdate::SQLiteUpdate(LogicalOperator &op, TableCatalogEntry &table, vector<PhysicalIndex> columns_p) :
	PhysicalOperator(PhysicalOperatorType::EXTENSION, op.types, 1), table(table), columns(move(columns_p)) {}

//===--------------------------------------------------------------------===//
// States
//===--------------------------------------------------------------------===//
class SQLiteUpdateGlobalState : public GlobalSinkState {
public:
	explicit SQLiteUpdateGlobalState(SQLiteTableEntry &table)
	    : table(table), update_count(0) {
	}

	SQLiteTableEntry &table;
	SQLiteStatement statement;
	idx_t update_count;
};

string GetUpdateSQL(SQLiteTableEntry &table, const vector<PhysicalIndex> &index) {
	string result;
	result = "UPDATE " + KeywordHelper::WriteOptionallyQuoted(table.name);
	result += " SET ";
	for(idx_t i = 0; i < index.size(); i++) {
		if (i > 0) {
			result += ", ";
		}
		auto &col = table.GetColumn(LogicalIndex(index[i].index));
		result += KeywordHelper::WriteOptionallyQuoted(col.GetName());
		result += " = ?";
	}
	result += " WHERE rowid = ?";
	return result;
}

unique_ptr<GlobalSinkState> SQLiteUpdate::GetGlobalSinkState(ClientContext &context) const {
	auto &sqlite_table = (SQLiteTableEntry &) table;

	auto &transaction = SQLiteTransaction::Get(context, *sqlite_table.catalog);
	auto result = make_unique<SQLiteUpdateGlobalState>(sqlite_table);
	result->statement = transaction.GetDB().Prepare(GetUpdateSQL(sqlite_table, columns));
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType SQLiteUpdate::Sink(ExecutionContext &context, GlobalSinkState &state_p, LocalSinkState &lstate,
					DataChunk &input) const {
	auto &gstate = (SQLiteUpdateGlobalState &) state_p;

	input.Flatten();
	auto &row_identifiers = input.data[input.ColumnCount() - 1];
	auto row_data = FlatVector::GetData<row_t>(row_identifiers);
	auto &stmt = gstate.statement;
	auto update_columns = input.ColumnCount() - 1;
	for(idx_t r = 0; r < input.size(); r++) {
		// bind the SET values
		for(idx_t c = 0; c < update_columns; c++) {
			auto &col = input.data[c];
			stmt.BindValue(col, c, r);
		}
		// bind the row identifier
		stmt.Bind<int64_t>(update_columns, row_data[r]);
		stmt.Step();
		stmt.Reset();
	}
	gstate.update_count += input.size();
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
class SQLiteUpdateSourceState : public GlobalSourceState {
public:
	bool finished = false;
};

unique_ptr<GlobalSourceState> SQLiteUpdate::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<SQLiteUpdateSourceState>();
}

void SQLiteUpdate::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                             LocalSourceState &lstate) const {
	auto &state = (SQLiteUpdateSourceState &)gstate;
	auto &insert_gstate = (SQLiteUpdateGlobalState &)*sink_state;
	if (state.finished) {
		return;
	}
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(insert_gstate.update_count));
	state.finished = true;
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
string SQLiteUpdate::GetName() const {
	return "UPDATE";
}

string SQLiteUpdate::ParamsToString() const {
	return table.name;
}

//===--------------------------------------------------------------------===//
// Plan
//===--------------------------------------------------------------------===//
unique_ptr<PhysicalOperator> SQLiteCatalog::PlanUpdate(ClientContext &context, LogicalUpdate &op, unique_ptr<PhysicalOperator> plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for updates of a SQLite table");
	}
	for(auto &expr : op.expressions) {
		if (expr->type == ExpressionType::VALUE_DEFAULT) {
			throw BinderException("SET DEFAULT is not yet supported for updates of a SQLite table");
		}
	}
	auto insert = make_unique<SQLiteUpdate>(op, *op.table, move(op.columns));
	insert->children.push_back(std::move(plan));
	return std::move(insert);
}

}
