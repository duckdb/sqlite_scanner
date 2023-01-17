//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sqlite_scanner.hpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb.hpp"

namespace duckdb {

class SqliteScanFunction : public TableFunction {
public:
	SqliteScanFunction();
};

class SqliteAttachFunction : public TableFunction {
public:
	SqliteAttachFunction();
};

}
