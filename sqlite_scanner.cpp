#define DUCKDB_BUILD_LOADABLE_EXTENSION
#include "duckdb.hpp"

#include "sqlite3.h"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parallel/parallel_state.hpp"
#include "duckdb/storage/statistics/validity_statistics.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/query_node/select_node.hpp"

using namespace duckdb;

struct SqliteBindData : public FunctionData {
  string file_name;
  string table_name;

  vector<string> names;
  vector<LogicalType> types;

  idx_t max_rowid = 0;
  vector<bool> not_nulls;
  vector<uint64_t> decimal_multipliers;

  idx_t rows_per_group = 100000;
};

struct SqliteOperatorData : public FunctionOperatorData {
  sqlite3 *db = nullptr;
  sqlite3_stmt *res = nullptr;
  bool done = false;
  vector<column_t> column_ids;
};

struct SqliteParallelState : public ParallelState {
  mutex lock;
  idx_t row_group_index = 0;
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

  auto result = make_unique<SqliteBindData>();
  result->file_name = inputs[0].GetValue<string>();
  result->table_name = inputs[1].GetValue<string>();

  sqlite3 *db;
  sqlite3_stmt *res;
  check_ok(sqlite3_open_v2(result->file_name.c_str(), &db, SQLITE_OPEN_READONLY,
                           nullptr),
           db);
  check_ok(sqlite3_prepare_v2(db,
                              StringUtil::Format("PRAGMA table_info(\"%s\")",
                                                 result->table_name)
                                  .c_str(),
                              -1, &res, nullptr),
           db);

  vector<bool> not_nulls;
  vector<string> sqlite_types;

  while (sqlite3_step(res) == SQLITE_ROW) {
    auto sqlite_colname = string((const char *)sqlite3_column_text(res, 1));
    auto sqlite_type = string((const char *)sqlite3_column_text(res, 2));
    // Sqlite specialty, untyped columns
    if (sqlite_type.empty()) {
      sqlite_type = "BLOB";
    }
    if (sqlite_type == "BLOB SUB_TYPE TEXT") {
      sqlite_type = "STRING"; // grr
    }
    auto not_null = sqlite3_column_int(res, 3);

    names.push_back(sqlite_colname);
    result->not_nulls.push_back((bool)not_null);
    sqlite_types.push_back(sqlite_type);
  }
  check_ok(sqlite3_finalize(res), db);

  if (names.empty()) {
    throw std::runtime_error("no columns for table " + result->table_name);
  }

  // we use the duckdb/postgres parser to parse these types, no need to
  // duplicate
  auto cast_string = StringUtil::Join(
      sqlite_types.data(), sqlite_types.size(), ", ",
      [](const string &st) { return StringUtil::Format("?::%s", st); });
  auto cast_expressions = Parser::ParseExpressionList(cast_string);

  result->decimal_multipliers.resize(names.size());
  idx_t column_index = 0;
  for (auto &e : cast_expressions) {
    D_ASSERT(e->type == ExpressionType::OPERATOR_CAST);
    auto &cast = (CastExpression &)*e;
    return_types.push_back(cast.cast_type);

    // precompute decimal conversion multipliers
    if (cast.cast_type.id() == LogicalTypeId::DECIMAL) {
      uint8_t width = 0, scale = 0;
      cast.cast_type.GetDecimalProperties(width, scale);
      result->decimal_multipliers[column_index] = (uint64_t)pow(10, scale);
    }
    column_index++;
  }

  check_ok(
      sqlite3_prepare_v2(db,
                         StringUtil::Format("SELECT MAX(ROWID) FROM \"%s\"",
                                            result->table_name)
                             .c_str(),
                         -1, &res, nullptr),
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

static void SqliteInitInternal(ClientContext &context,
                               const SqliteBindData *bind_data,
                               SqliteOperatorData *local_state, idx_t rowid_min,
                               idx_t rowid_max) {
  D_ASSERT(bind_data);
  D_ASSERT(local_state);
  D_ASSERT(rowid_min <= rowid_max);

  local_state->done = false;

  check_ok(sqlite3_open_v2(bind_data->file_name.c_str(), &local_state->db,
                           SQLITE_OPEN_READONLY, nullptr),
           local_state->db);

  auto col_names = StringUtil::Join(
      local_state->column_ids.data(), local_state->column_ids.size(), ", ",
      [&](const idx_t column_id) {
        return column_id == COLUMN_IDENTIFIER_ROW_ID
                   ? "ROWID"
                   : '"' + bind_data->names[column_id] + '"';
      });

  auto sql = StringUtil::Format(
      "SELECT %s FROM \"%s\" WHERE ROWID BETWEEN %d AND %d", col_names,
      bind_data->table_name, rowid_min, rowid_max);

  check_ok(sqlite3_prepare_v2(local_state->db, sql.c_str(), -1,
                              &local_state->res, nullptr),
           local_state->db);
}

static unique_ptr<FunctionOperatorData>
SqliteInit(ClientContext &context, const FunctionData *bind_data_p,
           const vector<column_t> &column_ids, TableFilterCollection *) {
  D_ASSERT(bind_data_p);
  auto bind_data = (const SqliteBindData *)bind_data_p;
  auto result = make_unique<SqliteOperatorData>();
  result->column_ids = column_ids;

  SqliteInitInternal(context, bind_data, result.get(), 0, bind_data->max_rowid);
  return move(result);
}

static unique_ptr<NodeStatistics>
SqliteCardinality(ClientContext &context, const FunctionData *bind_data_p) {
  D_ASSERT(bind_data_p);

  auto bind_data = (const SqliteBindData *)bind_data_p;
  return make_unique<NodeStatistics>(bind_data->max_rowid);
}

static idx_t SqliteMaxThreads(ClientContext &context,
                              const FunctionData *bind_data_p) {
  D_ASSERT(bind_data_p);

  auto bind_data = (const SqliteBindData *)bind_data_p;
  return bind_data->max_rowid / bind_data->rows_per_group;
}

static unique_ptr<ParallelState>
SqliteInitParallelState(ClientContext &context, const FunctionData *,
                        const vector<column_t> &column_ids,
                        TableFilterCollection *) {
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

  auto bind_data = (const SqliteBindData *)bind_data_p;
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
                   TableFilterCollection *) {
  auto result = make_unique<SqliteOperatorData>();
  result->column_ids = column_ids;
  if (!SqliteParallelStateNext(context, bind_data_p, result.get(),
                               parallel_state_p)) {
    return nullptr;
  }
  return move(result);
}

static void SqliteCleanup(ClientContext &context, const FunctionData *,
                          FunctionOperatorData *operator_state) {
  auto &state = (SqliteOperatorData &)*operator_state;
  if (!state.db) {
    return;
  }
  check_ok(sqlite3_finalize(state.res), state.db);
  check_ok(sqlite3_close(state.db), state.db);
  state.db = nullptr;
  state.res = nullptr;
  state.done = true;
}

void SqliteScan(ClientContext &context, const FunctionData *bind_data_p,
                FunctionOperatorData *operator_state, DataChunk *,
                DataChunk &output) {

  D_ASSERT(operator_state);
  D_ASSERT(bind_data_p);

  auto &state = (SqliteOperatorData &)*operator_state;
  auto bind_data = (const SqliteBindData *)bind_data_p;

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

      auto not_null = bind_data->not_nulls[state.column_ids[col_idx]];
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
      case LogicalTypeId::SMALLINT: {
        if (sqlite_column_type != SQLITE_INTEGER) {
          throw std::runtime_error("Expected integer, got something else");
        }
        auto raw_int = sqlite3_value_int(val);
        if (raw_int > NumericLimits<int16_t>::Maximum()) {
          throw std::runtime_error("int16 value out of range");
        }
        FlatVector::GetData<int16_t>(out_vec)[out_idx] = (int16_t)raw_int;
        break;
      }

      case LogicalTypeId::INTEGER:
        if (sqlite_column_type != SQLITE_INTEGER) {
          throw std::runtime_error("Expected integer, got something else");
        }
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

      case LogicalTypeId::BLOB:
        FlatVector::GetData<string_t>(out_vec)[out_idx] =
            StringVector::AddStringOrBlob(out_vec,
                                          (const char *)sqlite3_value_blob(val),
                                          sqlite3_value_bytes(val));
        break;
      case LogicalTypeId::DECIMAL: {
        if (sqlite_column_type != SQLITE_FLOAT &&
            sqlite_column_type != SQLITE_INTEGER) {
          throw std::runtime_error(
              "Expected float or integer, got something else");
        }
        auto &multiplier =
            bind_data->decimal_multipliers[state.column_ids[col_idx]];
        switch (out_vec.GetType().InternalType()) {
        case PhysicalType::INT16:
          FlatVector::GetData<int16_t>(out_vec)[out_idx] =
              round(sqlite3_value_double(val) * multiplier);
          break;
        case PhysicalType::INT32:
          FlatVector::GetData<int32_t>(out_vec)[out_idx] =
              round(sqlite3_value_double(val) * multiplier);
          break;
        case PhysicalType::INT64:
          FlatVector::GetData<int64_t>(out_vec)[out_idx] =
              round(sqlite3_value_double(val) * multiplier);
          break;

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

static string SqliteToString(const FunctionData *bind_data_p) {
  D_ASSERT(bind_data_p);
  auto bind_data = (const SqliteBindData *)bind_data_p;
  return StringUtil::Format("%s:%s", bind_data->file_name,
                            bind_data->table_name);
}
//
// static unique_ptr<BaseStatistics> SqliteStatistics(ClientContext &context,
// const FunctionData *bind_data_p,
//                                                         column_t
//                                                         column_index) {
//    auto &bind_data = (SqliteBindData &)*bind_data_p;
//    auto stats = BaseStatistics::CreateEmpty(bind_data.types[column_index]);
//    stats->validity_stats =
//    make_unique<ValidityStatistics>(!bind_data.not_nulls[column_index]);
//    return stats;
//}

class SqliteScanFunction : public TableFunction {
public:
  SqliteScanFunction()
      : TableFunction("sqlite_scan",
                      {LogicalType::VARCHAR, LogicalType::VARCHAR}, SqliteScan,
                      SqliteBind, SqliteInit, nullptr, SqliteCleanup,
                      /* dependency */ nullptr, SqliteCardinality,
                      /* pushdown_complex_filter */ nullptr, SqliteToString,
                      SqliteMaxThreads, SqliteInitParallelState, nullptr,
                      SqliteParallelInit, SqliteParallelStateNext, true, false,
                      nullptr) {}
};

struct AttachFunctionData : public TableFunctionData {
  AttachFunctionData() {}

  bool finished = false;
  string schema = DEFAULT_SCHEMA;
  string suffix = "";
  bool overwrite = false;
  string file_name = "";
};

static unique_ptr<FunctionData>
AttachBind(ClientContext &context, vector<Value> &inputs,
           unordered_map<string, Value> &named_parameters,
           vector<LogicalType> &input_table_types,
           vector<string> &input_table_names, vector<LogicalType> &return_types,
           vector<string> &names) {
  auto result = make_unique<AttachFunctionData>();
  result->file_name = inputs[0].GetValue<string>();

  for (auto &kv : named_parameters) {
    if (kv.first == "schema") {
      result->schema = kv.second.str_value;
    } else if (kv.first == "suffix") {
      result->suffix = kv.second.str_value;
    } else if (kv.first == "overwrite") {
      result->overwrite = kv.second.value_.boolean;
    }
  }

  return_types.push_back(LogicalType::BOOLEAN);
  names.emplace_back("Success");
  return move(result);
}

static void AttachFunction(ClientContext &context,
                           const FunctionData *bind_data,
                           FunctionOperatorData *operator_state,
                           DataChunk *input, DataChunk &output) {
  auto &data = (AttachFunctionData &)*bind_data;
  if (data.finished) {
    return;
  }

  sqlite3 *db;
  sqlite3_stmt *res;
  check_ok(sqlite3_open_v2(data.file_name.c_str(), &db, SQLITE_OPEN_READONLY,
                           nullptr),
           db);
  check_ok(sqlite3_prepare_v2(
               db, "SELECT tbl_name FROM sqlite_master WHERE type='table'", -1,
               &res, nullptr),
           db);

  while (sqlite3_step(res) == SQLITE_ROW) {
    auto table_name = string((const char *)sqlite3_column_text(res, 0));
    CreateViewInfo view_info;
    view_info.schema = data.schema;
    view_info.view_name = table_name;
    view_info.temporary = true;
    view_info.on_conflict = data.overwrite
                                ? OnCreateConflict::REPLACE_ON_CONFLICT
                                : OnCreateConflict::ERROR_ON_CONFLICT;

    auto view_query = StringUtil::Format(
        "SELECT * FROM sqlite_scan('%s', '%s')", data.file_name, table_name);

    Parser parser;
    parser.ParseQuery(view_query);

    auto &statement = parser.statements[0];
    view_info.query = unique_ptr<SelectStatement>(
        (SelectStatement *)statement->Copy().release());

    auto binder = Binder::CreateBinder(context);
    auto bound_statement = binder->Bind(*statement);
    view_info.types = bound_statement.types;

    context.db->GetCatalog().CreateView(context, &view_info);
  }
  check_ok(sqlite3_finalize(res), db);
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

  TableFunction attach_func("sqlite_attach", {LogicalType::VARCHAR},
                            AttachFunction, AttachBind);
  attach_func.named_parameters["overwrite"] = LogicalType::BOOLEAN;
  attach_func.named_parameters["schema"] = LogicalType::VARCHAR;
  attach_func.named_parameters["suffix"] = LogicalType::VARCHAR;

  CreateTableFunctionInfo attach_info(attach_func);
  catalog.CreateTableFunction(context, &attach_info);

  con.Commit();
}

DUCKDB_EXTENSION_API const char *sqlite_scanner_version() {
  return DuckDB::LibraryVersion();
}
}
