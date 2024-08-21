#include "storage/sqlite_insert.hpp"
#include "storage/sqlite_catalog.hpp"
#include "storage/sqlite_transaction.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "storage/sqlite_table_entry.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "sqlite_db.hpp"
#include "sqlite_stmt.hpp"

namespace duckdb {

SQLiteInsert::SQLiteInsert(LogicalOperator &op, TableCatalogEntry &table,
                           physical_index_vector_t<idx_t> column_index_map_p)
    : PhysicalOperator(PhysicalOperatorType::EXTENSION, op.types, 1), table(&table), schema(nullptr),
      column_index_map(std::move(column_index_map_p)) {
}

SQLiteInsert::SQLiteInsert(LogicalOperator &op, SchemaCatalogEntry &schema, unique_ptr<BoundCreateTableInfo> info)
    : PhysicalOperator(PhysicalOperatorType::EXTENSION, op.types, 1), table(nullptr), schema(&schema),
      info(std::move(info)) {
}

//===--------------------------------------------------------------------===//
// States
//===--------------------------------------------------------------------===//
class SQLiteInsertGlobalState : public GlobalSinkState {
public:
	explicit SQLiteInsertGlobalState(ClientContext &context, SQLiteTableEntry *table) : insert_count(0) {
	}

	SQLiteTableEntry *table;
	SQLiteStatement statement;
	idx_t insert_count;
};

string GetInsertSQL(const SQLiteInsert &insert, SQLiteTableEntry *entry) {
	string result;
	result = "INSERT INTO " + KeywordHelper::WriteOptionallyQuoted(entry->name);
	auto &columns = entry->GetColumns();
	idx_t column_count;
	if (!insert.column_index_map.empty()) {
		column_count = 0;
		result += " (";
		vector<PhysicalIndex> column_indexes;
		column_indexes.resize(columns.LogicalColumnCount(), PhysicalIndex(DConstants::INVALID_INDEX));
		for (idx_t c = 0; c < insert.column_index_map.size(); c++) {
			auto column_index = PhysicalIndex(c);
			auto mapped_index = insert.column_index_map[column_index];
			if (mapped_index == DConstants::INVALID_INDEX) {
				// column not specified
				continue;
			}
			column_indexes[mapped_index] = column_index;
			column_count++;
		}
		for (idx_t c = 0; c < column_count; c++) {
			if (c > 0) {
				result += ", ";
			}
			auto &col = columns.GetColumn(column_indexes[c]);
			result += KeywordHelper::WriteOptionallyQuoted(col.GetName());
		}
		result += ")";
	} else {
		column_count = columns.LogicalColumnCount();
	}
	result += " VALUES (";
	for (idx_t i = 0; i < column_count; i++) {
		if (i > 0) {
			result += ", ";
		}
		result += "?";
	}
	result += ");";
	return result;
}

unique_ptr<GlobalSinkState> SQLiteInsert::GetGlobalSinkState(ClientContext &context) const {
	SQLiteTableEntry *insert_table;
	if (!table) {
		auto &schema_ref = *schema.get_mutable();
		insert_table =
		    &schema_ref.CreateTable(schema_ref.GetCatalogTransaction(context), *info)->Cast<SQLiteTableEntry>();
	} else {
		insert_table = &table.get_mutable()->Cast<SQLiteTableEntry>();
	}
	auto &transaction = SQLiteTransaction::Get(context, insert_table->catalog);
	auto result = make_uniq<SQLiteInsertGlobalState>(context, insert_table);
	result->statement = transaction.GetDB().Prepare(GetInsertSQL(*this, insert_table));
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType SQLiteInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = sink_state->Cast<SQLiteInsertGlobalState>();
	chunk.Flatten();
	auto &stmt = gstate.statement;
	for (idx_t r = 0; r < chunk.size(); r++) {
		for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
			auto &col = chunk.data[c];
			stmt.BindValue(col, c, r);
		}
		// execute and clear bindings
		stmt.Step();
		stmt.Reset();
	}
	gstate.insert_count += chunk.size();
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
SourceResultType SQLiteInsert::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
	auto &insert_gstate = sink_state->Cast<SQLiteInsertGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(insert_gstate.insert_count));

	return SourceResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
string SQLiteInsert::GetName() const {
	return table ? "INSERT" : "CREATE_TABLE_AS";
}

InsertionOrderPreservingMap<string> SQLiteInsert::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table Name"] = table ? table->name : info->Base().table;
	return result;
}

//===--------------------------------------------------------------------===//
// Plan
//===--------------------------------------------------------------------===//
unique_ptr<PhysicalOperator> AddCastToSQLiteTypes(ClientContext &context, unique_ptr<PhysicalOperator> plan) {
	// check if we need to cast anything
	bool require_cast = false;
	auto &child_types = plan->GetTypes();
	for (auto &type : child_types) {
		auto sqlite_type = SQLiteUtils::ToSQLiteType(type);
		if (sqlite_type != type) {
			require_cast = true;
			break;
		}
	}
	if (require_cast) {
		vector<LogicalType> sqlite_types;
		vector<unique_ptr<Expression>> select_list;
		for (idx_t i = 0; i < child_types.size(); i++) {
			auto &type = child_types[i];
			unique_ptr<Expression> expr;
			expr = make_uniq<BoundReferenceExpression>(type, i);

			auto sqlite_type = SQLiteUtils::ToSQLiteType(type);
			if (sqlite_type != type) {
				// add a cast
				expr = BoundCastExpression::AddCastToType(context, std::move(expr), sqlite_type);
			}
			sqlite_types.push_back(std::move(sqlite_type));
			select_list.push_back(std::move(expr));
		}
		// we need to cast: add casts
		auto proj =
		    make_uniq<PhysicalProjection>(std::move(sqlite_types), std::move(select_list), plan->estimated_cardinality);
		proj->children.push_back(std::move(plan));
		plan = std::move(proj);
	}

	return plan;
}

unique_ptr<PhysicalOperator> SQLiteCatalog::PlanInsert(ClientContext &context, LogicalInsert &op,
                                                       unique_ptr<PhysicalOperator> plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for insertion into SQLite table");
	}
	if (op.action_type != OnConflictAction::THROW) {
		throw BinderException("ON CONFLICT clause not yet supported for insertion into SQLite table");
	}

	plan = AddCastToSQLiteTypes(context, std::move(plan));

	auto insert = make_uniq<SQLiteInsert>(op, op.table, op.column_index_map);
	insert->children.push_back(std::move(plan));
	return std::move(insert);
}

unique_ptr<PhysicalOperator> SQLiteCatalog::PlanCreateTableAs(ClientContext &context, LogicalCreateTable &op,
                                                              unique_ptr<PhysicalOperator> plan) {
	plan = AddCastToSQLiteTypes(context, std::move(plan));

	auto insert = make_uniq<SQLiteInsert>(op, op.schema, std::move(op.info));
	insert->children.push_back(std::move(plan));
	return std::move(insert);
}

} // namespace duckdb
