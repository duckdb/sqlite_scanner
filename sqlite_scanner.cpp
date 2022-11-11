#ifndef DUCKDB_BUILD_LOADABLE_EXTENSION
#define DUCKDB_BUILD_LOADABLE_EXTENSION
#endif
#include "duckdb.hpp"

#include "sqlite3.h"
#include <stdint.h>
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"

#include <cmath>

using namespace duckdb;

static int SQLITE_OPEN_FLAGS = SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX | SQLITE_OPEN_PRIVATECACHE;

struct SqliteBindData : public FunctionData {
	string file_name;
	string table_name;

	vector<string> names;
	vector<LogicalType> types;

	idx_t max_rowid = 0;
	vector<bool> not_nulls;
	bool all_varchar = false;

	idx_t rows_per_group = 100000;

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_unique<SqliteBindData>();
		copy->file_name = file_name;
		copy->table_name = table_name;
		copy->names = names;
		copy->types = types;
		copy->max_rowid = max_rowid;
		copy->not_nulls = not_nulls;
		copy->rows_per_group = rows_per_group;

		return move(copy);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto other = (SqliteBindData &)other_p;
		return other.file_name == file_name && other.table_name == table_name && other.names == names &&
		       other.types == types && other.max_rowid == max_rowid && other.not_nulls == not_nulls &&
		       other.rows_per_group == rows_per_group;
	}
};

struct SqliteLocalState : public LocalTableFunctionState {
	sqlite3 *db = nullptr;
	sqlite3_stmt *res = nullptr;
	bool done = false;
	vector<column_t> column_ids;

	~SqliteLocalState() {
		sqlite3_finalize(res);
		sqlite3_close(db);
		res = nullptr;
		db = nullptr;
	}
};

struct SqliteGlobalState : public GlobalTableFunctionState {
	SqliteGlobalState(idx_t max_threads) : max_threads(max_threads) {
	}

	mutex lock;
	idx_t position = 0;
	idx_t max_threads;

	idx_t MaxThreads() const override {
		return max_threads;
	}
};

static void check_ok(int rc, sqlite3 *db) {
	if (rc != SQLITE_OK) {
		throw std::runtime_error(string(sqlite3_errmsg(db)));
	}
}

static LogicalType SQLiteTypeToLogicalType(const string &sqlite_type) {
	// type affinity rules are taken from here: https://www.sqlite.org/datatype3.html

	// If the declared type contains the string "INT" then it is assigned INTEGER affinity.
	if (StringUtil::Contains(sqlite_type, "int")) {
		return LogicalType::BIGINT;
	}
	// If the declared type of the column contains any of the strings "CHAR", "CLOB", or "TEXT" then that column has
	// TEXT affinity. Notice that the type VARCHAR contains the string "CHAR" and is thus assigned TEXT affinity.
	if (StringUtil::Contains(sqlite_type, "char") || StringUtil::Contains(sqlite_type, "clob") ||
	    StringUtil::Contains(sqlite_type, "text")) {
		return LogicalType::VARCHAR;
	}

	// If the declared type for a column contains the string "BLOB" or if no type is specified then the column has
	// affinity BLOB.
	if (StringUtil::Contains(sqlite_type, "blob") || sqlite_type.empty()) {
		return LogicalType::BLOB;
	}

	// If the declared type for a column contains any of the strings "REAL", "FLOA", or "DOUB" then the column has REAL
	// affinity.
	if (StringUtil::Contains(sqlite_type, "real") || StringUtil::Contains(sqlite_type, "floa") ||
	    StringUtil::Contains(sqlite_type, "doub")) {
		return LogicalType::DOUBLE;
	}
	// Otherwise, the affinity is NUMERIC.
	// now numeric sounds simple, but it is rather complex:
	// A column with NUMERIC affinity may contain values using all five storage classes.
	// ...
	// we add some more extra rules to try to be somewhat sane

	if (sqlite_type == "date") {
		return LogicalType::DATE;
	}

	// datetime, timestamp
	if (StringUtil::Contains(sqlite_type, "time")) {
		return LogicalType::TIMESTAMP;
	}

	// decimal, numeric
	if (StringUtil::Contains(sqlite_type, "dec") || StringUtil::Contains(sqlite_type, "num")) {
		return LogicalType::DOUBLE;
	}

	// alright, give up and fallback to varchar
	return LogicalType::VARCHAR;
}

static unique_ptr<FunctionData> SqliteBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {

	auto result = make_unique<SqliteBindData>();
	result->file_name = input.inputs[0].GetValue<string>();
	result->table_name = input.inputs[1].GetValue<string>();

	sqlite3 *db;
	sqlite3_stmt *res;
	check_ok(sqlite3_open_v2(result->file_name.c_str(), &db, SQLITE_OPEN_FLAGS, nullptr), db);
	check_ok(sqlite3_prepare_v2(db, StringUtil::Format("PRAGMA table_info(\"%s\")", result->table_name).c_str(), -1,
	                            &res, nullptr),
	         db);

	result->all_varchar = false;
	Value sqlite_all_varchar;
	if (context.TryGetCurrentSetting("sqlite_all_varchar", sqlite_all_varchar)) {
		result->all_varchar = BooleanValue::Get(sqlite_all_varchar);
	}
	vector<bool> not_nulls;
	while (sqlite3_step(res) == SQLITE_ROW) {
		auto sqlite_colname = string((const char *)sqlite3_column_text(res, 1));
		auto sqlite_type = StringUtil::Lower(string((const char *)sqlite3_column_text(res, 2)));
		auto not_null = sqlite3_column_int(res, 3);
		StringUtil::Trim(sqlite_type);
		names.push_back(sqlite_colname);
		result->not_nulls.push_back((bool)not_null);
		return_types.push_back(result->all_varchar ? LogicalType::VARCHAR : SQLiteTypeToLogicalType(sqlite_type));
	}
	check_ok(sqlite3_finalize(res), db);

	if (names.empty()) {
		throw std::runtime_error("no columns for table " + result->table_name);
	}

	check_ok(sqlite3_prepare_v2(db, StringUtil::Format("SELECT MAX(ROWID) FROM \"%s\"", result->table_name).c_str(), -1,
	                            &res, nullptr),
	         db);
	if (sqlite3_step(res) != SQLITE_ROW) {
		throw std::runtime_error("could not find max rowid?");
	}
	result->max_rowid = sqlite3_column_int64(res, 0);

	check_ok(sqlite3_finalize(res), db);
	check_ok(sqlite3_close(db), db);

	result->names = names;
	result->types = return_types;

	return move(result);
}

static void SqliteInitInternal(ClientContext &context, const SqliteBindData *bind_data, SqliteLocalState &local_state,
                               idx_t rowid_min, idx_t rowid_max) {
	D_ASSERT(bind_data);
	D_ASSERT(rowid_min <= rowid_max);

	local_state.done = false;

	// we may have leftover statements or connections from a previous call to this function
	if (local_state.res) {
		check_ok(sqlite3_finalize(local_state.res), local_state.db);
		local_state.res = nullptr;
	}

	if (local_state.db) {
		check_ok(sqlite3_close(local_state.db), local_state.db);
		local_state.db = nullptr;
	}

	check_ok(sqlite3_open_v2(bind_data->file_name.c_str(), &local_state.db, SQLITE_OPEN_FLAGS, nullptr),
	         local_state.db);

	auto col_names = StringUtil::Join(
	    local_state.column_ids.data(), local_state.column_ids.size(), ", ", [&](const idx_t column_id) {
		    return column_id == (column_t)-1 ? "ROWID" : '"' + bind_data->names[column_id] + '"';
	    });

	auto sql = StringUtil::Format("SELECT %s FROM \"%s\" WHERE ROWID BETWEEN %d AND %d", col_names,
	                              bind_data->table_name, rowid_min, rowid_max);

	check_ok(sqlite3_prepare_v2(local_state.db, sql.c_str(), -1, &local_state.res, nullptr), local_state.db);
}

static unique_ptr<NodeStatistics> SqliteCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	D_ASSERT(bind_data_p);

	auto bind_data = (const SqliteBindData *)bind_data_p;
	return make_unique<NodeStatistics>(bind_data->max_rowid);
}

static idx_t SqliteMaxThreads(ClientContext &context, const FunctionData *bind_data_p) {
	D_ASSERT(bind_data_p);
	auto bind_data = (const SqliteBindData *)bind_data_p;
	return bind_data->max_rowid / bind_data->rows_per_group;
}

static bool SqliteParallelStateNext(ClientContext &context, const FunctionData *bind_data_p, SqliteLocalState &lstate,
                                    SqliteGlobalState &gstate) {
	D_ASSERT(bind_data_p);
	auto bind_data = (const SqliteBindData *)bind_data_p;
	lock_guard<mutex> parallel_lock(gstate.lock);
	if (gstate.position < bind_data->max_rowid) {
		auto start = gstate.position;
		auto end = start + bind_data->rows_per_group - 1;
		SqliteInitInternal(context, bind_data, lstate, start, end);
		gstate.position = end + 1;
		return true;
	}
	return false;
}

static unique_ptr<LocalTableFunctionState>
SqliteInitLocalState(ExecutionContext &context, TableFunctionInitInput &input, GlobalTableFunctionState *global_state) {

	auto &gstate = (SqliteGlobalState &)*global_state;
	auto result = make_unique<SqliteLocalState>();
	result->column_ids = input.column_ids;
	if (!SqliteParallelStateNext(context.client, input.bind_data, *result, gstate)) {
		result->done = true;
	}
	return move(result);
}

static unique_ptr<GlobalTableFunctionState> SqliteInitGlobalState(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
	auto result = make_unique<SqliteGlobalState>(SqliteMaxThreads(context, input.bind_data));
	result->position = 0;
	return move(result);
}

static string SqliteTypeToString(int sqlite_type) {
	switch (sqlite_type) {
	case SQLITE_ANY:
		return "any";
	case SQLITE_INTEGER:
		return "integer";
	case SQLITE_TEXT:
		return "text";
	case SQLITE_BLOB:
		return "blob";
	case SQLITE_FLOAT:
		return "float";
	default:
		return "unknown";
	}
}

static void AssertTypeMatches(std::string &column_name, sqlite3_value *value, int sqlite_column_type,
                              int expected_type) {
	if (sqlite_column_type != expected_type) {
		auto value_as_text = string((const char *)sqlite3_value_text(value));
		auto message = "Invalid type in column \"" + column_name + "\": column was declared as " +
		               SqliteTypeToString(expected_type) + ", found \"" + value_as_text + "\" of type \"" +
		               SqliteTypeToString(sqlite_column_type) + "\" instead.";
		throw Exception(ExceptionType::MISMATCH_TYPE, message);
	}
}
static void AssertTypeIsFloatOrInteger(std::string &column_name, sqlite3_value *value, int sqlite_column_type) {
	if (sqlite_column_type != SQLITE_FLOAT && sqlite_column_type != SQLITE_INTEGER) {
		auto value_as_text = string((const char *)sqlite3_value_text(value));
		auto message = "Invalid type in column \"" + column_name + "\": expected float or integer, found \"" +
		               value_as_text + "\" of type \"" + SqliteTypeToString(sqlite_column_type) + "\" instead.";
		throw Exception(ExceptionType::MISMATCH_TYPE, message);
	}
}

static void SqliteScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = (SqliteLocalState &)*data.local_state;
	auto &gstate = (SqliteGlobalState &)*data.global_state;
	auto bind_data = (const SqliteBindData *)data.bind_data;

	while (output.size() == 0) {
		if (state.done) {
			if (!SqliteParallelStateNext(context, data.bind_data, state, gstate)) {
				return;
			}
		}

		idx_t out_idx = 0;
		while (true) {
			if (out_idx == STANDARD_VECTOR_SIZE) {
				output.SetCardinality(out_idx);
				return;
			}
			auto rc = sqlite3_step(state.res);
			if (rc == SQLITE_DONE) {
				state.done = true;
				output.SetCardinality(out_idx);
				return;
			}
			if (rc == SQLITE_ERROR) {
				throw std::runtime_error(sqlite3_errmsg(state.db));
			}
			D_ASSERT(rc == SQLITE_ROW);
			for (idx_t col_idx = 0; col_idx < output.ColumnCount(); col_idx++) {
				auto &out_vec = output.data[col_idx];
				auto &mask = FlatVector::Validity(out_vec);
				auto val = sqlite3_column_value(state.res, (int)col_idx);
				auto column_name = string(sqlite3_column_name(state.res, (int)col_idx));

				auto not_null = bind_data->not_nulls[state.column_ids[col_idx]];
				auto sqlite_column_type = sqlite3_value_type(val);
				if (sqlite_column_type == SQLITE_NULL) {
					if (not_null) {
						throw std::runtime_error("Column was declared as NOT NULL but got one anyway");
					}
					mask.Set(out_idx, false);
					continue;
				}

				switch (out_vec.GetType().id()) {
				case LogicalTypeId::BIGINT:
					AssertTypeMatches(column_name, val, sqlite_column_type, SQLITE_INTEGER);
					FlatVector::GetData<int64_t>(out_vec)[out_idx] = sqlite3_value_int64(val);
					break;
				case LogicalTypeId::DOUBLE:
					AssertTypeIsFloatOrInteger(column_name, val, sqlite_column_type);
					FlatVector::GetData<double>(out_vec)[out_idx] = sqlite3_value_double(val);
					break;
				case LogicalTypeId::VARCHAR:
					if (!bind_data->all_varchar) {
						AssertTypeMatches(column_name, val, sqlite_column_type, SQLITE_TEXT);
					}
					FlatVector::GetData<string_t>(out_vec)[out_idx] = StringVector::AddString(
					    out_vec, (const char *)sqlite3_value_text(val), sqlite3_value_bytes(val));
					break;
				case LogicalTypeId::DATE:
					AssertTypeMatches(column_name, val, sqlite_column_type, SQLITE_TEXT);
					FlatVector::GetData<date_t>(out_vec)[out_idx] =
					    Date::FromCString((const char *)sqlite3_value_text(val), sqlite3_value_bytes(val));
					break;
				case LogicalTypeId::TIMESTAMP:
					AssertTypeMatches(column_name, val, sqlite_column_type, SQLITE_TEXT);
					FlatVector::GetData<timestamp_t>(out_vec)[out_idx] =
					    Timestamp::FromCString((const char *)sqlite3_value_text(val), sqlite3_value_bytes(val));
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
	auto bind_data = (const SqliteBindData *)bind_data_p;
	return StringUtil::Format("%s:%s", bind_data->file_name, bind_data->table_name);
}

/*
static unique_ptr<BaseStatistics>
SqliteStatistics(ClientContext &context, const FunctionData *bind_data_p,
                 column_t column_index) {
  auto &bind_data = (SqliteBindData &)*bind_data_p;
  auto stats = BaseStatistics::CreateEmpty(bind_data.types[column_index]);
  stats->validity_stats =
      make_unique<ValidityStatistics>(!bind_data.not_nulls[column_index]);
  return stats;
}
*/

class SqliteScanFunction : public TableFunction {
public:
	SqliteScanFunction()
	    : TableFunction("sqlite_scan", {LogicalType::VARCHAR, LogicalType::VARCHAR}, SqliteScan, SqliteBind,
	                    SqliteInitGlobalState, SqliteInitLocalState) {
		cardinality = SqliteCardinality;
		to_string = SqliteToString;
		projection_pushdown = true;
	}
};

struct AttachFunctionData : public TableFunctionData {
	AttachFunctionData() {
	}

	bool finished = false;
	bool overwrite = false;
	string file_name = "";
};

static unique_ptr<FunctionData> AttachBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {

	auto result = make_unique<AttachFunctionData>();
	result->file_name = input.inputs[0].GetValue<string>();

	for (auto &kv : input.named_parameters) {
		if (kv.first == "overwrite") {
			result->overwrite = BooleanValue::Get(kv.second);
		}
	}

	return_types.push_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");
	return move(result);
}

static void AttachFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = (AttachFunctionData &)*data_p.bind_data;
	if (data.finished) {
		return;
	}

	sqlite3 *db;
	sqlite3_stmt *res;
	check_ok(sqlite3_open_v2(data.file_name.c_str(), &db, SQLITE_OPEN_FLAGS, nullptr), db);
	auto dconn = Connection(context.db->GetDatabase(context));

	{

		check_ok(sqlite3_prepare_v2(db, "SELECT name FROM sqlite_master WHERE type='table'", -1, &res, nullptr), db);

		while (sqlite3_step(res) == SQLITE_ROW) {
			auto table_name = string((const char *)sqlite3_column_text(res, 0));

			dconn.TableFunction("sqlite_scan", {Value(data.file_name), Value(table_name)})
			    ->CreateView(table_name, data.overwrite, false);
		}
		check_ok(sqlite3_finalize(res), db);
	}
	{
		check_ok(sqlite3_prepare_v2(db, "SELECT sql FROM sqlite_master WHERE type='view'", -1, &res, nullptr), db);

		while (sqlite3_step(res) == SQLITE_ROW) {
			auto view_sql = string((const char *)sqlite3_column_text(res, 0));
			dconn.Query(view_sql);
		}
		check_ok(sqlite3_finalize(res), db);
	}
	check_ok(sqlite3_close(db), db);
	data.finished = true;
}

extern "C" {
DUCKDB_EXTENSION_API void sqlite_scanner_init(duckdb::DatabaseInstance &db) {
	Connection con(db);
	con.BeginTransaction();
	auto &context = *con.context;
	auto &catalog = Catalog::GetCatalog(context);

	SqliteScanFunction sqlite_fun;
	CreateTableFunctionInfo sqlite_info(sqlite_fun);
	catalog.CreateTableFunction(context, &sqlite_info);

	TableFunction attach_func("sqlite_attach", {LogicalType::VARCHAR}, AttachFunction, AttachBind);
	attach_func.named_parameters["overwrite"] = LogicalType::BOOLEAN;

	CreateTableFunctionInfo attach_info(attach_func);
	catalog.CreateTableFunction(context, &attach_info);

	auto &config = DBConfig::GetConfig(db);
	config.AddExtensionOption("sqlite_all_varchar", "Load all SQLite columns as VARCHAR columns", LogicalType::BOOLEAN);

	con.Commit();
}

DUCKDB_EXTENSION_API const char *sqlite_scanner_version() {
	return DuckDB::LibraryVersion();
}
}
