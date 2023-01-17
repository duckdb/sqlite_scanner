//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sqlite_storage.hpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

class SQLiteStorageExtension : public StorageExtension {
public:
	SQLiteStorageExtension();
};

}
