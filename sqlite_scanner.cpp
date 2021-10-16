#define DUCKDB_BUILD_LOADABLE_EXTENSION
#include "duckdb.hpp"

#include "sqlite3.h"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parallel/parallel_state.hpp"
#include "duckdb/storage/statistics/validity_statistics.hpp"

using namespace duckdb;

struct SqliteBindData : public FunctionData {
  string file_name;
  string table_name;
  idx_t max_rowid;
  vector<column_t> column_ids;
  vector<bool> not_nulls;
  vector<string> names;
  vector<LogicalType> types;

  idx_t rows_per_group =
      1000000; // TODO this should be a parameter to the scan function
};

struct SqliteOperatorData : public FunctionOperatorData {
  sqlite3 *db;
  sqlite3_stmt *res;
  bool done;
};

struct SqliteParallelState : public ParallelState {
  mutex lock;
  idx_t row_group_index;
};

static void check_ok(int rc, sqlite3 *db) {
  if (rc != SQLITE_OK) {
    throw std::runtime_error(string(sqlite3_errmsg(db)));
  }
}

unique_ptr<FunctionData>
SqliteBind(ClientContext &context, vector<Value> &inputs,
           unordered_map<string, Value> &named_parameters,
           vector<LogicalType> &input_table_types,
           vector<string> &input_table_names, vector<LogicalType> &return_types,
           vector<string> &names) {

  auto file_name = inputs[0].GetValue<string>();
  auto table_name = inputs[1].GetValue<string>();

  sqlite3 *db;
  sqlite3_stmt *res;
  check_ok(
      sqlite3_open_v2(file_name.c_str(), &db, SQLITE_OPEN_READONLY, nullptr),
      db);
  check_ok(sqlite3_prepare_v2(
               db, string("PRAGMA table_info(" + table_name + ")").c_str(), -1,
               &res, 0),
           db);

  vector<bool> not_nulls;

  while (sqlite3_step(res) == SQLITE_ROW) {
    auto sqlite_colname = string((const char *)sqlite3_column_text(res, 1));
    auto sqlite_type =
        StringUtil::Upper(string((const char *)sqlite3_column_text(res, 2)));
    auto not_null = sqlite3_column_int(res, 3);

    names.push_back(sqlite_colname);
    not_nulls.push_back((bool)not_null);

    if (sqlite_type == "SMALLINT") {
      return_types.push_back(LogicalType::SMALLINT);
    } else if (sqlite_type == "INTEGER" || sqlite_type == "INT") {
      return_types.push_back(LogicalType::INTEGER);
    } else if (sqlite_type == "DATE") {
      return_types.push_back(LogicalType::DATE);
    } else if (sqlite_type == "TIMESTAMP") {
      return_types.push_back(LogicalType::TIMESTAMP);
    } else if (sqlite_type == "REAL" || sqlite_type == "NUMERIC") {
      return_types.push_back(LogicalType::DOUBLE);
    } else if (sqlite_type.rfind("DECIMAL", 0) == 0) {
      // TODO parse this thing
      return_types.push_back(LogicalType::DECIMAL(15, 2));
    } else if (sqlite_type.rfind("CHAR", 0) == 0 ||
               sqlite_type.rfind("VARCHAR", 0) == 0 ||
               sqlite_type == "BLOB SUB_TYPE TEXT") {
      return_types.push_back(LogicalType::VARCHAR);
    } else if (sqlite_type == "BLOB") {
      return_types.push_back(LogicalType::BLOB);

    } else {
      throw std::runtime_error("Unsupported type " + sqlite_type);
    }
  }
  sqlite3_reset(res);

  check_ok(sqlite3_prepare_v2(
               db, string("SELECT MAX(ROWID) FROM " + table_name).c_str(), -1,
               &res, 0),
           db);
  if (sqlite3_step(res) != SQLITE_ROW) {
    throw std::runtime_error("could not find max rowid?");
  }
  idx_t max_rowid = sqlite3_column_int64(res, 0);

  sqlite3_reset(res);
  sqlite3_close(db);

  auto result = make_unique<SqliteBindData>();
  result->not_nulls = not_nulls;
  result->file_name = file_name;
  result->table_name = table_name;
  result->max_rowid = max_rowid;
  result->names = names;
  result->types = return_types;

  return move(result);
}

static void SqliteInitInternal(ClientContext &context,
                               const SqliteBindData *bind_data,
                               SqliteOperatorData *local_state, idx_t rowid_min,
                               idx_t rowid_max) {
  D_ASSERT(bind_data);
  D_ASSERT(local_state);
  D_ASSERT(rowid_min < rowid_max);

  check_ok(sqlite3_open_v2(bind_data->file_name.c_str(), &local_state->db,
                           SQLITE_OPEN_READONLY, nullptr),
           local_state->db);

  string col_names = "";
  for (idx_t col_id_idx = 0; col_id_idx < bind_data->column_ids.size();
       col_id_idx++) {
    auto column_id = bind_data->column_ids[col_id_idx];
    if (column_id == COLUMN_IDENTIFIER_ROW_ID) {
      col_names += "ROWID";
    } else {
      col_names += bind_data->names[column_id];
    }
    if (col_id_idx < bind_data->column_ids.size() - 1) {
      col_names += ", ";
    }
  }

  auto sql = StringUtil::Format(
      "SELECT %s FROM %s WHERE ROWID BETWEEN %d AND %d", col_names,
      bind_data->table_name, rowid_min, rowid_max);

  check_ok(sqlite3_prepare_v2(local_state->db, sql.c_str(), -1,
                              &local_state->res, 0),
           local_state->db);

  local_state->done = false;
}

static unique_ptr<FunctionOperatorData>
SqliteInit(ClientContext &context, const FunctionData *bind_data_p,
           const vector<column_t> &column_ids, TableFilterCollection *filters) {
  D_ASSERT(bind_data_p);
  auto bind_data = (SqliteBindData *)bind_data_p;
  bind_data->column_ids = column_ids;
  // TODO deal with filters
  auto result = make_unique<SqliteOperatorData>();
  SqliteInitInternal(context, bind_data, result.get(), 0, bind_data->max_rowid);
  return move(result);
}

static unique_ptr<NodeStatistics>
SqliteCardinality(ClientContext &context, const FunctionData *bind_data_p) {
  auto &bind_data = (SqliteBindData &)*bind_data_p;
  return make_unique<NodeStatistics>(bind_data.max_rowid);
}

static idx_t SqliteMaxThreads(ClientContext &context,
                              const FunctionData *bind_data_p) {
  auto &bind_data = (SqliteBindData &)*bind_data_p;
  return bind_data.max_rowid / bind_data.rows_per_group;
}

static unique_ptr<ParallelState>
SqliteInitParallelState(ClientContext &context, const FunctionData *bind_data_p,
                        const vector<column_t> &column_ids,
                        TableFilterCollection *filters) {
  auto result = make_unique<SqliteParallelState>();
  result->row_group_index = 0;
  return move(result);
}

static bool SqliteParallelStateNext(ClientContext &context,
                                    const FunctionData *bind_data_p,
                                    FunctionOperatorData *state_p,
                                    ParallelState *parallel_state_p) {
  D_ASSERT(bind_data_p);
  D_ASSERT(state_p);
  D_ASSERT(parallel_state_p);

  auto bind_data = (SqliteBindData *)bind_data_p;
  auto &parallel_state = (SqliteParallelState &)*parallel_state_p;
  auto local_state = (SqliteOperatorData *)state_p;

  lock_guard<mutex> parallel_lock(parallel_state.lock);

  if (parallel_state.row_group_index <
      (bind_data->max_rowid / bind_data->rows_per_group + 1)) {
    SqliteInitInternal(
        context, bind_data, local_state,
        parallel_state.row_group_index * bind_data->rows_per_group,
        (parallel_state.row_group_index + 1) * bind_data->rows_per_group - 1);
    parallel_state.row_group_index++;
    return true;
  }
  return false;
}

static unique_ptr<FunctionOperatorData>
SqliteParallelInit(ClientContext &context, const FunctionData *bind_data_p,
                   ParallelState *parallel_state_p,
                   const vector<column_t> &column_ids,
                   TableFilterCollection *filters) {
  auto result = make_unique<SqliteOperatorData>();
  auto bind_data = (SqliteBindData *)bind_data_p;

  bind_data->column_ids = column_ids;
  if (!SqliteParallelStateNext(context, bind_data_p, result.get(),
                               parallel_state_p)) {
    return nullptr;
  }
  return move(result);
}

static void SqliteCleanup(ClientContext &context, const FunctionData *bind_data,
                          FunctionOperatorData *operator_state) {
  auto &state = (SqliteOperatorData &)*operator_state;

  if (state.done) {
    return;
  }
  check_ok(sqlite3_finalize(state.res), state.db);
  check_ok(sqlite3_close(state.db), state.db);
  state.db = nullptr;
  state.res = nullptr;
  state.done = true;
}

void SqliteScan(ClientContext &context, const FunctionData *bind_data_p,
                FunctionOperatorData *operator_state, DataChunk *input,
                DataChunk &output) {
  auto &state = (SqliteOperatorData &)*operator_state;
  auto &bind_data = (SqliteBindData &)*bind_data_p;

  if (state.done) {
    return;
  }

  idx_t out_idx = 0;
  while (true) {
    if (out_idx == STANDARD_VECTOR_SIZE) {
      output.SetCardinality(out_idx);
      return;
    }
    auto rc = sqlite3_step(state.res);
    if (rc == SQLITE_DONE) {
      output.SetCardinality(out_idx);
      SqliteCleanup(context, bind_data_p, operator_state);
      return;
    }
    if (rc == SQLITE_ERROR) {
      throw std::runtime_error(sqlite3_errmsg(state.db));
    }
    D_ASSERT(rc == SQLITE_ROW);
    for (idx_t col_idx = 0; col_idx < output.ColumnCount(); col_idx++) {
      auto &out_vec = output.data[col_idx];
      auto &mask = FlatVector::Validity(out_vec);
      auto val = sqlite3_column_value(state.res, col_idx);

      auto not_null = bind_data.not_nulls[bind_data.column_ids[col_idx]];
      auto sqlite_column_type = sqlite3_value_type(val);
      if (sqlite_column_type == SQLITE_NULL) {
        if (not_null) {
          throw std::runtime_error(
              "Column was declared as NOT NULL but got one anyway");
        }
        mask.Set(out_idx, false);
        continue;
      }

      switch (out_vec.GetType().id()) {
      case LogicalTypeId::SMALLINT:
        if (sqlite_column_type != SQLITE_INTEGER) {
          throw std::runtime_error("Expected integer, got something else");
        }
        // TODO do we need an overflow check here?
        FlatVector::GetData<int16_t>(out_vec)[out_idx] = sqlite3_value_int(val);
        break;

      case LogicalTypeId::INTEGER:
        if (sqlite_column_type != SQLITE_INTEGER) {
          throw std::runtime_error("Expected integer, got something else");
        }
        // TODO do we need an overflow check here?

        FlatVector::GetData<int32_t>(out_vec)[out_idx] = sqlite3_value_int(val);
        break;

      case LogicalTypeId::BIGINT:
        if (sqlite_column_type != SQLITE_INTEGER) {
          throw std::runtime_error("Expected integer, got something else");
        }
        FlatVector::GetData<int64_t>(out_vec)[out_idx] =
            sqlite3_value_int64(val);
        break;
      case LogicalTypeId::DOUBLE:
        if (sqlite_column_type != SQLITE_FLOAT &&
            sqlite_column_type != SQLITE_INTEGER) {
          throw std::runtime_error(
              "Expected float or integer, got something else");
        }
        FlatVector::GetData<double>(out_vec)[out_idx] =
            sqlite3_value_double(val);
        break;
      case LogicalTypeId::VARCHAR:
        if (sqlite_column_type != SQLITE_TEXT) {
          throw std::runtime_error("Expected string, got something else");
        }
        FlatVector::GetData<string_t>(out_vec)[out_idx] =
            StringVector::AddString(out_vec,
                                    (const char *)sqlite3_value_text(val),
                                    sqlite3_value_bytes(val));
        break;

      case LogicalTypeId::DATE:
        if (sqlite_column_type != SQLITE_TEXT) {
          throw std::runtime_error("Expected string, got something else");
        }
        FlatVector::GetData<date_t>(out_vec)[out_idx] = Date::FromCString(
            (const char *)sqlite3_value_text(val), sqlite3_value_bytes(val));
        break;

      case LogicalTypeId::TIMESTAMP:
        if (sqlite_column_type != SQLITE_TEXT) {
          throw std::runtime_error("Expected string, got something else");
        }
        FlatVector::GetData<timestamp_t>(out_vec)[out_idx] =
            Timestamp::FromCString((const char *)sqlite3_value_text(val),
                                   sqlite3_value_bytes(val));
        break;

      case LogicalTypeId::DECIMAL:
        if (sqlite_column_type != SQLITE_FLOAT &&
            sqlite_column_type != SQLITE_INTEGER) {
          throw std::runtime_error(
              "Expected float or integer, got something else");
        }
        switch (out_vec.GetType().InternalType()) {
        case PhysicalType::INT64:
          // TODO this * 100 of course depends on the DECIMAL type
          FlatVector::GetData<int64_t>(out_vec)[out_idx] =
              sqlite3_value_double(val) * 100;
          break;

        default:
          throw std::runtime_error(
              TypeIdToString(out_vec.GetType().InternalType()));
        }
        break;

      default:
        throw std::runtime_error(out_vec.GetType().ToString());
      }
    }
    out_idx++;
  }
}

static void SqliteFuncParallel(ClientContext &context,
                               const FunctionData *bind_data,
                               FunctionOperatorData *operator_state,
                               DataChunk *input, DataChunk &output,
                               ParallelState *parallel_state_p) {
  SqliteScan(context, bind_data, operator_state, input, output);
}

// TODO add remaining types
// TODO selection pushdown
// TODO what if table changes between bind and scan? keep transaction?

static string SqliteTotString(const FunctionData *bind_data_p) {
  auto &bind_data = (SqliteBindData &)*bind_data_p;
  return StringUtil::Format("%s:%s", bind_data.file_name, bind_data.table_name);
}
//
// static unique_ptr<BaseStatistics> SqliteStatistics(ClientContext &context,
// const FunctionData *bind_data_p,
//                                                         column_t
//                                                         column_index) {
//    auto &bind_data = (SqliteBindData &)*bind_data_p;
//    // TODO this does not seem to work, only declaring NULLness
//    auto stats = make_unique<BaseStatistics>(bind_data.types[column_index]);
//    stats->validity_stats =
//    make_unique<ValidityStatistics>(!bind_data.not_nulls[column_index]);
//    return stats;
//}

class SqliteScanFunction : public TableFunction {
public:
  SqliteScanFunction()
      : TableFunction("sqlite_scan",
                      {LogicalType::VARCHAR, LogicalType::VARCHAR}, SqliteScan,
                      SqliteBind, SqliteInit, /*statistics */ nullptr,
                      SqliteCleanup,
                      /* dependency */ nullptr, SqliteCardinality,
                      /* pushdown_complex_filter */ nullptr, SqliteTotString,
                      SqliteMaxThreads, SqliteInitParallelState,
                      SqliteFuncParallel, SqliteParallelInit,
                      SqliteParallelStateNext, true, false, nullptr) {}
};

extern "C" {
DUCKDB_EXTENSION_API void sqlite_scanner_init(duckdb::DatabaseInstance &db) {
  SqliteScanFunction sqlite_fun;
  CreateTableFunctionInfo sqlite_info(sqlite_fun);
  Connection con(db);
  con.BeginTransaction();
  auto &context = *con.context;
  auto &catalog = Catalog::GetCatalog(context);
  catalog.CreateTableFunction(context, &sqlite_info);
  con.Commit();
}

DUCKDB_EXTENSION_API const char *sqlite_scanner_version() {
  return DuckDB::LibraryVersion();
}
}
