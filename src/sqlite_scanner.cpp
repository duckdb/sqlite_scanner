#include "duckdb.hpp"

#include "sqlite_db.hpp"
#include "sqlite_stmt.hpp"
#include "sqlite_scanner.hpp"
#include <stdint.h>
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include <cmath>

namespace duckdb {

struct SqliteLocalState : public LocalTableFunctionState {
	SQLiteDB *db;
	SQLiteDB owned_db;
	SQLiteStatement stmt;
	bool done = false;
	vector<column_t> column_ids;
	//! The amount of rows we scanned as part of this row group
	idx_t scan_count = 1;

	~SqliteLocalState() {
	}
};

struct SqliteGlobalState : public GlobalTableFunctionState {
	explicit SqliteGlobalState(idx_t max_threads) : max_threads(max_threads) {
	}

	mutex lock;
	idx_t position = 0;
	idx_t max_threads;
	idx_t rows_per_group = 0;

	idx_t MaxThreads() const override {
		return max_threads;
	}
};

static unique_ptr<FunctionData> SqliteBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {

	auto result = make_uniq<SqliteBindData>();
	result->file_name = input.inputs[0].GetValue<string>();
	result->table_name = input.inputs[1].GetValue<string>();

	SQLiteDB db;
	SQLiteStatement stmt;
	SQLiteOpenOptions options;
	options.access_mode = AccessMode::READ_ONLY;
	db = SQLiteDB::Open(result->file_name, options);

	ColumnList columns;
	vector<unique_ptr<Constraint>> constraints;

	result->all_varchar = false;
	Value sqlite_all_varchar;
	if (context.TryGetCurrentSetting("sqlite_all_varchar", sqlite_all_varchar)) {
		result->all_varchar = BooleanValue::Get(sqlite_all_varchar);
	}
	db.GetTableInfo(result->table_name, columns, constraints, result->all_varchar);
	for (auto &column : columns.Logical()) {
		names.push_back(column.GetName());
		return_types.push_back(column.GetType());
	}

	if (names.empty()) {
		throw std::runtime_error("no columns for table " + result->table_name);
	}

	if (!db.GetRowIdInfo(result->table_name, result->row_id_info)) {
		result->rows_per_group = optional_idx();
	}

	result->names = names;
	result->types = return_types;

	return std::move(result);
}

static void SqliteInitInternal(ClientContext &context, const SqliteBindData &bind_data, SqliteLocalState &local_state,
                               idx_t rowid_min, idx_t rowid_max) {
	D_ASSERT(rowid_min <= rowid_max);

	local_state.done = false;
	// we may have leftover statements or connections from a previous call to this
	// function
	local_state.stmt.Close();
	if (!local_state.db) {
		SQLiteOpenOptions options;
		options.access_mode = AccessMode::READ_ONLY;
		local_state.owned_db = SQLiteDB::Open(bind_data.file_name.c_str(), options);
		local_state.db = &local_state.owned_db;
	}

	auto col_names = StringUtil::Join(
	    local_state.column_ids.data(), local_state.column_ids.size(), ", ", [&](const idx_t column_id) {
		    return column_id == (column_t)-1 ? "ROWID"
		                                     : '"' + SQLiteUtils::SanitizeIdentifier(bind_data.names[column_id]) + '"';
	    });

	auto sql =
	    StringUtil::Format("SELECT %s FROM \"%s\"", col_names, SQLiteUtils::SanitizeIdentifier(bind_data.table_name));
	if (bind_data.rows_per_group.IsValid()) {
		// we are scanning a subset of the rows - generate a WHERE clause based on
		// the rowid
		auto where_clause = StringUtil::Format(" WHERE ROWID BETWEEN %d AND %d", rowid_min, rowid_max);
		sql += where_clause;
	} else {
		// we are scanning the entire table - no need for a WHERE clause
		D_ASSERT(rowid_min == 0);
	}
	local_state.stmt = local_state.db->Prepare(sql.c_str());
}

static unique_ptr<NodeStatistics> SqliteCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	D_ASSERT(bind_data_p);
	auto &bind_data = bind_data_p->Cast<SqliteBindData>();
	if (!bind_data.row_id_info.max_rowid.IsValid()) {
		return nullptr;
	}
	auto row_count = bind_data.row_id_info.max_rowid.GetIndex() - bind_data.row_id_info.min_rowid.GetIndex();
	return make_uniq<NodeStatistics>(row_count);
}

static idx_t SqliteMaxThreads(ClientContext &context, const FunctionData *bind_data_p) {
	D_ASSERT(bind_data_p);
	auto &bind_data = bind_data_p->Cast<SqliteBindData>();
	if (bind_data.global_db) {
		return 1;
	}
	if (!bind_data.row_id_info.max_rowid.IsValid()) {
		return 1;
	}
	auto row_count = bind_data.row_id_info.max_rowid.GetIndex() - bind_data.row_id_info.min_rowid.GetIndex();
	return row_count / bind_data.rows_per_group.GetIndex();
}

static bool SqliteParallelStateNext(ClientContext &context, const SqliteBindData &bind_data, SqliteLocalState &lstate,
                                    SqliteGlobalState &gstate) {
	lock_guard<mutex> parallel_lock(gstate.lock);
	if (!bind_data.rows_per_group.IsValid()) {
		// not doing a parallel scan - scan everything at once
		if (gstate.position > 0) {
			// already scanned
			return false;
		}
		SqliteInitInternal(context, bind_data, lstate, 0, 0);
		gstate.position = static_cast<idx_t>(-1);
		lstate.scan_count = 0;
		return true;
	}
	auto max_row_id = bind_data.row_id_info.max_rowid.GetIndex();
	if (gstate.position < max_row_id) {
		if (lstate.scan_count == 0 && gstate.rows_per_group < max_row_id) {
			// we scanned no rows in our previous slice - double the rows per group
			gstate.rows_per_group *= 2;
		}
		if (gstate.rows_per_group == 0) {
			throw InternalException("SqliteParallelStateNext - gstate.rows_per_group not set");
		}
		auto start = gstate.position;
		auto end = MinValue<idx_t>(max_row_id, start + gstate.rows_per_group - 1);
		SqliteInitInternal(context, bind_data, lstate, start, end);
		gstate.position = end + 1;
		lstate.scan_count = 0;
		return true;
	}
	return false;
}

static unique_ptr<LocalTableFunctionState>
SqliteInitLocalState(ExecutionContext &context, TableFunctionInitInput &input, GlobalTableFunctionState *global_state) {
	auto &bind_data = input.bind_data->Cast<SqliteBindData>();
	auto &gstate = global_state->Cast<SqliteGlobalState>();
	auto result = make_uniq<SqliteLocalState>();
	result->column_ids = input.column_ids;
	result->db = bind_data.global_db;
	if (!SqliteParallelStateNext(context.client, bind_data, *result, gstate)) {
		result->done = true;
	}
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> SqliteInitGlobalState(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<SqliteBindData>();
	auto result = make_uniq<SqliteGlobalState>(SqliteMaxThreads(context, input.bind_data.get()));
	result->position = 0;
	if (bind_data.rows_per_group.IsValid()) {
		auto min_row_id = bind_data.row_id_info.min_rowid.GetIndex();
		if (min_row_id > 0) {
			result->position = min_row_id - 1;
		}
		result->rows_per_group = bind_data.rows_per_group.GetIndex();
	}
	return std::move(result);
}

static void SqliteScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.local_state->Cast<SqliteLocalState>();
	auto &gstate = data.global_state->Cast<SqliteGlobalState>();
	auto &bind_data = data.bind_data->Cast<SqliteBindData>();

	while (output.size() == 0) {
		if (state.done) {
			if (!SqliteParallelStateNext(context, bind_data, state, gstate)) {
				return;
			}
		}

		idx_t out_idx = 0;
		while (true) {
			if (out_idx == STANDARD_VECTOR_SIZE) {
				output.SetCardinality(out_idx);
				return;
			}
			auto &stmt = state.stmt;
			auto has_more = stmt.Step();
			if (!has_more) {
				state.done = true;
				output.SetCardinality(out_idx);
				break;
			}
			state.scan_count++;
			for (idx_t col_idx = 0; col_idx < output.ColumnCount(); col_idx++) {
				auto &out_vec = output.data[col_idx];
				auto sqlite_column_type = stmt.GetType(col_idx);
				if (sqlite_column_type == SQLITE_NULL) {
					auto &mask = FlatVector::Validity(out_vec);
					mask.Set(out_idx, false);
					continue;
				}

				auto val = stmt.GetValue<sqlite3_value *>(col_idx);
				switch (out_vec.GetType().id()) {
				case LogicalTypeId::BIGINT:
					stmt.CheckTypeMatches(bind_data, val, sqlite_column_type, SQLITE_INTEGER, col_idx);
					FlatVector::GetData<int64_t>(out_vec)[out_idx] = sqlite3_value_int64(val);
					break;
				case LogicalTypeId::DOUBLE:
					stmt.CheckTypeIsFloatOrInteger(val, sqlite_column_type, col_idx);
					FlatVector::GetData<double>(out_vec)[out_idx] = sqlite3_value_double(val);
					break;
				case LogicalTypeId::VARCHAR:
					stmt.CheckTypeMatches(bind_data, val, sqlite_column_type, SQLITE_TEXT, col_idx);
					FlatVector::GetData<string_t>(out_vec)[out_idx] = StringVector::AddString(
					    out_vec, (const char *)sqlite3_value_text(val), sqlite3_value_bytes(val));
					break;
				case LogicalTypeId::DATE:
					stmt.CheckTypeMatches(bind_data, val, sqlite_column_type, SQLITE_TEXT, col_idx);
					FlatVector::GetData<date_t>(out_vec)[out_idx] =
					    Date::FromCString((const char *)sqlite3_value_text(val), sqlite3_value_bytes(val));
					break;
				case LogicalTypeId::TIMESTAMP:
					// SQLite does not have a timestamp type - but it has "conventions"
					// See https://www.sqlite.org/lang_datefunc.html
					// The conventions are:
					// A text string that is an ISO 8601 date/time value
					// The number of days including fractional days since -4713-11-24 12:00:00
					// The number of seconds including fractional seconds since 1970-01-01 00:00:00
					// for now we only support ISO-8601 and unix timestamps
					if (sqlite_column_type == SQLITE_INTEGER) {
						// unix timestamp
						FlatVector::GetData<timestamp_t>(out_vec)[out_idx] =
						    Timestamp::FromEpochSeconds(sqlite3_value_int64(val));
					} else if (sqlite_column_type == SQLITE_FLOAT) {
						int64_t timestamp_micros =
						    Cast::Operation<double, int64_t>(sqlite3_value_double(val) * 1000000.0);
						FlatVector::GetData<timestamp_t>(out_vec)[out_idx] =
						    Timestamp::FromEpochMicroSeconds(timestamp_micros);
					} else if (sqlite_column_type == SQLITE_TEXT) {
						// ISO-8601
						FlatVector::GetData<timestamp_t>(out_vec)[out_idx] =
						    Timestamp::FromCString((const char *)sqlite3_value_text(val), sqlite3_value_bytes(val));
					} else {
						throw NotImplementedException(
						    "Unimplemented SQLite type for column of type TIMESTAMP\n* SET sqlite_all_varchar=true to "
						    "load all columns as VARCHAR and skip type conversions");
					}
					break;
				case LogicalTypeId::BLOB:
					FlatVector::GetData<string_t>(out_vec)[out_idx] = StringVector::AddStringOrBlob(
					    out_vec, (const char *)sqlite3_value_blob(val), sqlite3_value_bytes(val));
					break;
				default:
					throw std::runtime_error(out_vec.GetType().ToString());
				}
			}
			out_idx++;
		}
	}
}

static string SqliteToString(const FunctionData *bind_data_p) {
	D_ASSERT(bind_data_p);
	auto &bind_data = bind_data_p->Cast<SqliteBindData>();
	return StringUtil::Format("%s:%s", bind_data.file_name, bind_data.table_name);
}

/*
static unique_ptr<BaseStatistics>
SqliteStatistics(ClientContext &context, const FunctionData *bind_data_p,
                 column_t column_index) {
  auto &bind_data = (SqliteBindData &)*bind_data_p;
  auto stats = BaseStatistics::CreateEmpty(bind_data.types[column_index]);
  stats->validity_stats =
      make_uniq<ValidityStatistics>(!bind_data.not_nulls[column_index]);
  return stats;
}
*/

SqliteScanFunction::SqliteScanFunction()
    : TableFunction("sqlite_scan", {LogicalType::VARCHAR, LogicalType::VARCHAR}, SqliteScan, SqliteBind,
                    SqliteInitGlobalState, SqliteInitLocalState) {
	cardinality = SqliteCardinality;
	to_string = SqliteToString;
	projection_pushdown = true;
}

struct AttachFunctionData : public TableFunctionData {
	AttachFunctionData() {
	}

	bool finished = false;
	bool overwrite = false;
	string file_name = "";
};

static unique_ptr<FunctionData> AttachBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {

	auto result = make_uniq<AttachFunctionData>();
	result->file_name = input.inputs[0].GetValue<string>();

	for (auto &kv : input.named_parameters) {
		if (kv.first == "overwrite") {
			result->overwrite = BooleanValue::Get(kv.second);
		}
	}

	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");
	return std::move(result);
}

static void AttachFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<AttachFunctionData>();
	if (data.finished) {
		return;
	}

	SQLiteOpenOptions options;
	options.access_mode = AccessMode::READ_ONLY;
	SQLiteDB db = SQLiteDB::Open(data.file_name, options);
	auto dconn = Connection(context.db->GetDatabase(context));
	{
		auto tables = db.GetTables();
		for (auto &table_name : tables) {
			dconn.TableFunction("sqlite_scan", {Value(data.file_name), Value(table_name)})
			    ->CreateView(table_name, data.overwrite, false);
		}
	}
	{
		SQLiteStatement stmt = db.Prepare("SELECT sql FROM sqlite_master WHERE type='view'");
		while (stmt.Step()) {
			auto view_sql = stmt.GetValue<string>(0);
			dconn.Query(view_sql);
		}
	}
	data.finished = true;
}

SqliteAttachFunction::SqliteAttachFunction()
    : TableFunction("sqlite_attach", {LogicalType::VARCHAR}, AttachFunction, AttachBind) {
	named_parameters["overwrite"] = LogicalType::BOOLEAN;
}

} // namespace duckdb
