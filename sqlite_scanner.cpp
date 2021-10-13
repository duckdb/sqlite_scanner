#define DUCKDB_BUILD_LOADABLE_EXTENSION
#include "duckdb.hpp"
#include "sqlite3.h"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parallel/parallel_state.hpp"

using namespace duckdb;

struct SqliteBindData : public FunctionData {
  string file_name;
  string table_name;
  idx_t max_rowid;
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

  while (sqlite3_step(res) == SQLITE_ROW) {
    auto sqlite_colname = string((const char *)sqlite3_column_text(res, 1));
    auto sqlite_type = string((const char *)sqlite3_column_text(res, 2));
    names.push_back(sqlite_colname);

    if (sqlite_type == "INTEGER") {
      return_types.push_back(LogicalType::INTEGER);
    } else if (sqlite_type == "DATE") {
      return_types.push_back(LogicalType::DATE);
    } else if (sqlite_type.rfind("DECIMAL", 0) == 0) {
      return_types.push_back(LogicalType::DECIMAL(15, 2));
    } else if (sqlite_type.rfind("CHAR", 0) == 0 ||
               sqlite_type.rfind("VARCHAR", 0) == 0) {
      return_types.push_back(LogicalType::VARCHAR);
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
  result->file_name = file_name;
  result->table_name = table_name;
  result->max_rowid = max_rowid;
  return move(result);
}

static void SqliteInitInternal(ClientContext &context,
                               const SqliteBindData *bind_data,
                               SqliteOperatorData *local_state, idx_t rowid_min,
                               idx_t rowid_max) {
  D_ASSERT(bind_data);
  D_ASSERT(local_state);
  D_ASSERT(rowid_min < rowid_max);

  sqlite3_reset(local_state->res);
  sqlite3_close(local_state->db);

  check_ok(sqlite3_open_v2(bind_data->file_name.c_str(), &local_state->db,
                           SQLITE_OPEN_READONLY, nullptr),
           local_state->db);
  check_ok(sqlite3_prepare_v2(local_state->db,
                              string("SELECT * FROM " + bind_data->table_name +
                                     " WHERE ROWID BETWEEN ? AND ?")
                                  .c_str(),
                              -1, &local_state->res, 0),
           local_state->db);

  check_ok(sqlite3_bind_int64(local_state->res, 1, rowid_min), local_state->db);
  check_ok(sqlite3_bind_int64(local_state->res, 2, rowid_max), local_state->db);

  // TODO we need to close these again, too
  local_state->done = false;
}

static unique_ptr<FunctionOperatorData>
SqliteInit(ClientContext &context, const FunctionData *bind_data_p,
           const vector<column_t> &column_ids, TableFilterCollection *filters) {
  D_ASSERT(bind_data_p);
  auto bind_data = (SqliteBindData *)bind_data_p;
  // TODO deal with column_ids and filters
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
  auto &bind_data = (SqliteBindData &)*bind_data_p;
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
  sqlite3_reset(state.res);
  sqlite3_close(state.db);
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
      if (sqlite3_column_type(state.res, col_idx) == SQLITE_NULL) {
        mask.Set(out_idx, false);
        continue;
      }
      switch (out_vec.GetType().id()) {
      case LogicalTypeId::INTEGER: {
        auto out_ptr = FlatVector::GetData<int32_t>(out_vec);
        out_ptr[out_idx] = sqlite3_column_int(state.res, col_idx);
        break;
      }
      case LogicalTypeId::VARCHAR: {
        auto out_ptr = FlatVector::GetData<string_t>(out_vec);
        out_ptr[out_idx] = StringVector::AddString(
            out_vec, (const char *)sqlite3_column_text(state.res, col_idx));
        break;
      }
      case LogicalTypeId::DATE: {
        auto out_ptr = FlatVector::GetData<date_t>(out_vec);
        out_ptr[out_idx] =
            Date::EpochToDate(sqlite3_column_int64(state.res, col_idx));
        break;
      }
      case LogicalTypeId::DECIMAL: {
        switch (out_vec.GetType().InternalType()) {
        case PhysicalType::INT64: {
          auto out_ptr = FlatVector::GetData<int64_t>(out_vec);
          out_ptr[out_idx] =
              (int64_t)sqlite3_column_double(state.res, col_idx) * 100;
          break;
        }
        default:
          throw std::runtime_error(
              TypeIdToString(out_vec.GetType().InternalType()));
        }
        break;
      }
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
// TODO cleanup connection
// TODO parallel scans (use ROWID)
// TODO projection pushdown
// TODO selection pushdown
// TODO progress bar
// TODO what if table changes between bind and scan? keep transaction?

class SqliteScanFunction : public TableFunction {
public:
  SqliteScanFunction()
      : TableFunction(
            "sqlite_scan", {LogicalType::VARCHAR, LogicalType::VARCHAR},
            SqliteScan, SqliteBind, SqliteInit, /* statistics */ nullptr,
            /* cleanup */ SqliteCleanup,
            /* dependency */ nullptr, SqliteCardinality,
            /* pushdown_complex_filter */ nullptr, /* to_string */ nullptr,
            SqliteMaxThreads, SqliteInitParallelState, SqliteFuncParallel,
            SqliteParallelInit, SqliteParallelStateNext, false, false,
            nullptr) {}
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
