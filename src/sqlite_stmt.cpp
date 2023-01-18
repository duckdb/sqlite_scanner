#include "sqlite_utils.hpp"

namespace duckdb {

SQLiteStatement::SQLiteStatement() : db(nullptr), stmt(nullptr) {

}

SQLiteStatement::SQLiteStatement(sqlite3 *db, sqlite3_stmt *stmt) : db(db), stmt(stmt) {
}

SQLiteStatement::~SQLiteStatement() {
	Close();
}

SQLiteStatement::SQLiteStatement(SQLiteStatement &&other) noexcept {
	std::swap(db, other.db);
	std::swap(stmt, other.stmt);
}

SQLiteStatement &SQLiteStatement::operator=(SQLiteStatement &&other) noexcept {
	std::swap(db, other.db);
	std::swap(stmt, other.stmt);
	return *this;
}

int SQLiteStatement::Step() {
	D_ASSERT(stmt);
	auto rc = sqlite3_step(stmt);
	if (rc == SQLITE_ERROR) {
		throw std::runtime_error(string(sqlite3_errmsg(db)));
	}
	D_ASSERT(rc == SQLITE_ROW || rc == SQLITE_DONE);
	return rc == SQLITE_ROW;
}
int SQLiteStatement::GetType(idx_t col) {
	D_ASSERT(stmt);
	return sqlite3_column_type(stmt, col);
}
bool SQLiteStatement::IsOpen() {
	return stmt;
}
void SQLiteStatement::Close() {
	if (!IsOpen()) {
		return;
	}
	sqlite3_finalize(stmt);
	stmt = nullptr;
}

void SQLiteStatement::CheckTypeMatches(sqlite3_value *val, int sqlite_column_type, int expected_type, idx_t col_idx) {
	D_ASSERT(stmt);
	if (sqlite_column_type != expected_type) {
		auto column_name = string(sqlite3_column_name(stmt, int(col_idx)));
		auto value_as_text = string((char *) sqlite3_value_text(val));
		auto message = "Invalid type in column \"" + column_name + "\": column was declared as " +
					   SQLiteUtils::TypeToString(expected_type) + ", found \"" + value_as_text + "\" of type \"" +
					   SQLiteUtils::TypeToString(sqlite_column_type) + "\" instead.";
		throw Exception(ExceptionType::MISMATCH_TYPE, message);
	}
}

void SQLiteStatement::CheckTypeIsFloatOrInteger(sqlite3_value *val, int sqlite_column_type, idx_t col_idx) {
	if (sqlite_column_type != SQLITE_FLOAT && sqlite_column_type != SQLITE_INTEGER) {
		auto column_name = string(sqlite3_column_name(stmt, int(col_idx)));
		auto value_as_text = string((const char *)sqlite3_value_text(val));
		auto message = "Invalid type in column \"" + column_name + "\": expected float or integer, found \"" +
		               value_as_text + "\" of type \"" + SQLiteUtils::TypeToString(sqlite_column_type) + "\" instead.";
		throw Exception(ExceptionType::MISMATCH_TYPE, message);
	}
}

void SQLiteStatement::ClearBindings() {
	SQLiteUtils::Check(sqlite3_clear_bindings(stmt), db);
}
void SQLiteStatement::Reset() {
	SQLiteUtils::Check(sqlite3_reset(stmt), db);
}

template<>
string SQLiteStatement::GetValue(idx_t col) {
	D_ASSERT(stmt);
	return string((char *)sqlite3_column_text(stmt, col));
}

template<>
int SQLiteStatement::GetValue(idx_t col) {
	D_ASSERT(stmt);
	return sqlite3_column_int(stmt, col);
}

template<>
int64_t SQLiteStatement::GetValue(idx_t col) {
	D_ASSERT(stmt);
	return sqlite3_column_int64(stmt, col);
}

template<>
sqlite3_value * SQLiteStatement::GetValue(idx_t col) {
	D_ASSERT(stmt);
	return sqlite3_column_value(stmt, col);
}

template<>
void SQLiteStatement::Bind(idx_t col, int32_t value) {
	SQLiteUtils::Check(sqlite3_bind_int(stmt, col + 1, value), db);
}

template<>
void SQLiteStatement::Bind(idx_t col, int64_t value) {
	SQLiteUtils::Check(sqlite3_bind_int64(stmt, col + 1, value), db);
}

template<>
void SQLiteStatement::Bind(idx_t col, double value) {
	SQLiteUtils::Check(sqlite3_bind_double(stmt, col + 1, value), db);
}

template<>
void SQLiteStatement::Bind(idx_t col, string_t value) {
	SQLiteUtils::Check(sqlite3_bind_text(stmt, col + 1, value.GetDataUnsafe(), value.GetSize(), nullptr), db);
}

}
