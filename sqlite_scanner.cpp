#define DUCKDB_BUILD_LOADABLE_EXTENSION
#include "duckdb.hpp"
#include "sqlite3.h"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

using namespace duckdb;

struct SqliteBindData : public FunctionData {
    string file_name;
    string table_name;
};


struct SqliteOperatorData : public FunctionOperatorData {
    sqlite3 *db;
    sqlite3_stmt *res;
    bool done;
};

static void check_ok(int rc, sqlite3 *db) {
    if (rc != SQLITE_OK) {
        throw std::runtime_error(string(sqlite3_errmsg(db)));
    }
}

unique_ptr<FunctionData> SqliteBind(ClientContext &context, vector<Value> &inputs,
                                             unordered_map<string, Value> &named_parameters,
                                             vector<LogicalType> &input_table_types, vector<string> &input_table_names,
                                             vector<LogicalType> &return_types, vector<string> &names) {

    auto file_name = inputs[0].GetValue<string>();
    auto table_name = inputs[1].GetValue<string>();

    sqlite3 *db;
    sqlite3_stmt *res;
    check_ok(sqlite3_open_v2(file_name.c_str(), &db, SQLITE_OPEN_READONLY, nullptr), db);
    check_ok(sqlite3_prepare_v2(db, string("PRAGMA table_info("+table_name+")").c_str(), -1, &res, 0), db);

    while(sqlite3_step(res) == SQLITE_ROW) {
        auto sqlite_colname = string((const char*)sqlite3_column_text(res, 1));
        auto sqlite_type = string((const char*)sqlite3_column_text(res, 2));
        names.push_back(sqlite_colname);

        if (sqlite_type == "INTEGER") {
            return_types.push_back(LogicalType::INTEGER);
        } else {
            throw std::runtime_error("Unsupported type " + sqlite_type);
        }
    }

    sqlite3_close(db);

    auto result = make_unique<SqliteBindData>();
    result->file_name = file_name;
    result->table_name = table_name;
    return move(result);
}

unique_ptr<FunctionOperatorData> SqliteInit(ClientContext &context, const FunctionData *bind_data_p,
                                                     const vector<column_t> &column_ids,
                                                     TableFilterCollection *filters) {
    auto &bind_data = (SqliteBindData &)*bind_data_p;
    auto result = make_unique<SqliteOperatorData>();

    check_ok(sqlite3_open_v2(bind_data.file_name.c_str(), &result->db, SQLITE_OPEN_READONLY, nullptr), result->db);
    check_ok(sqlite3_prepare_v2(result->db, string("SELECT * FROM "+bind_data.table_name).c_str(), -1, &result->res, 0), result->db);
    result->done = false;
    return move(result);
}

void SqliteScan(ClientContext &context, const FunctionData *bind_data_p,
                                   FunctionOperatorData *operator_state, DataChunk *input, DataChunk &output) {
    auto &data = (SqliteOperatorData &)*operator_state;
    auto &bind_data = (SqliteBindData &)*bind_data_p;

    if (data.done) {
        return;
    }

    idx_t out_idx = 0;
    while (true) {
        if (out_idx == STANDARD_VECTOR_SIZE) {
            break;
        }
        auto rc = sqlite3_step(data.res);
        if (rc == SQLITE_DONE) {
            output.SetCardinality(out_idx);
            data.done = true;
            return;
        }
        if (rc == SQLITE_ERROR) {
            throw std::runtime_error(sqlite3_errmsg(data.db));
        }
        D_ASSERT(rc == SQLITE_ROW);
        for (idx_t col_idx = 0; col_idx < output.ColumnCount(); col_idx++) {
            auto& out_vec = output.data[col_idx];
            auto &mask = FlatVector::Validity(out_vec);
            if (sqlite3_column_type(data.res, col_idx) == SQLITE_NULL) {
                mask.Set(out_idx, false);
                continue;
            }
            switch (out_vec.GetType().id()) {
                case LogicalTypeId::INTEGER: {
                    auto out_ptr = FlatVector::GetData<int32_t>(out_vec);
                    out_ptr[out_idx] = sqlite3_column_int(data.res, col_idx);
                    break;
                }
                default:
                    throw std::runtime_error(out_vec.GetType().ToString());
            }
        }
        out_idx++;
    }
}

// TODO add remaining types
// TODO cleanup connection
// TODO parallel scans (use ROWID)
// TODO projection pushdown
// TODO selection pushdown
// TODO progress bar

class SqliteScanFunction : public TableFunction {
public:
    SqliteScanFunction()
            : TableFunction("sqlite_scan", {LogicalType::VARCHAR, LogicalType::VARCHAR}, SqliteScan,
                           SqliteBind, SqliteInit, /* statistics */ nullptr,
            /* cleanup */ nullptr,
            /* dependency */ nullptr, nullptr,
            /* pushdown_complex_filter */ nullptr, /* to_string */ nullptr, nullptr, nullptr, nullptr, nullptr,
                            nullptr, false, false, nullptr) {
    }};



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
