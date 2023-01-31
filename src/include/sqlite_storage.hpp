//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sqlite_storage.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

class SQLiteStorageExtension : public StorageExtension {
public:
	SQLiteStorageExtension();
};

} // namespace duckdb
