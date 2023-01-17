#include "sqlite_utils.hpp"

namespace duckdb {

static int SQLITE_OPEN_FLAGS = SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX | SQLITE_OPEN_PRIVATECACHE;

SQLiteDB::SQLiteDB() : db(nullptr) {

}

SQLiteDB::SQLiteDB(sqlite3 *db) : db(db) {
}

SQLiteDB::~SQLiteDB() {
	Close();
}

SQLiteDB::SQLiteDB(SQLiteDB &&other) noexcept {
	std::swap(db, other.db);
}

SQLiteDB &SQLiteDB::operator=(SQLiteDB &&other) noexcept {
	std::swap(db, other.db);
	return *this;
}

SQLiteDB SQLiteDB::Open(const string &path) {
	SQLiteDB result;
	SQLiteUtils::Check(sqlite3_open_v2(path.c_str(), &result.db, SQLITE_OPEN_FLAGS, nullptr), result.db);
	return result;
}

SQLiteStatement SQLiteDB::Prepare(const string &query) {
	SQLiteStatement stmt;
	SQLiteUtils::Check(sqlite3_prepare_v2(db, query.c_str(), -1, &stmt.stmt, nullptr), db);
	return stmt;
}

bool SQLiteDB::IsOpen() {
	return db;
}

void SQLiteDB::Close() {
	if (!IsOpen()) {
		return;
	}
	sqlite3_close(db);
	db = nullptr;
}

}
