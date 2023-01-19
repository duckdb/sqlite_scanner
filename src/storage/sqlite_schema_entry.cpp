#include "storage/sqlite_schema_entry.hpp"
#include "storage/sqlite_table_entry.hpp"
#include "storage/sqlite_transaction.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"

namespace duckdb {

SQLiteSchemaEntry::SQLiteSchemaEntry(Catalog *catalog) :
	SchemaCatalogEntry(catalog, DEFAULT_SCHEMA, true) {
}

SQLiteTransaction &GetSQLiteTransaction(CatalogTransaction transaction) {
	if (!transaction.transaction) {
		throw InternalException("No transaction!?");
	}
	return (SQLiteTransaction &) *transaction.transaction;
}

string GetCreateTableSQL(CreateTableInfo &info) {
	string result;
	result = "CREATE TABLE ";
	result += KeywordHelper::WriteOptionallyQuoted(info.table);
	result += "(";

	bool first_column = true;
	for(auto &col : info.columns.Logical()) {
		if (col.Generated()) {
			throw BinderException("SQLite does not support generated columns");
		}
		if (!first_column) {
			result += ", ";
		}
		result += KeywordHelper::WriteOptionallyQuoted(col.GetName());
		result += " ";
		result += col.GetType().ToString();
	}
	result += ");";
	// FIXME: constraints
	// FIXME: more complex types
	return result;
}

CatalogEntry *SQLiteSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo *info) {
	auto &sqlite_transaction = GetSQLiteTransaction(transaction);
	auto &base_info = info->Base();
	auto table_name = base_info.table;
	sqlite_transaction.GetDB().Execute(GetCreateTableSQL(base_info));
	return GetEntry(transaction, CatalogType::TABLE_ENTRY, table_name);
}

CatalogEntry *SQLiteSchemaEntry::CreateFunction(CatalogTransaction transaction, CreateFunctionInfo *info) {
	throw InternalException("CreateFunction");
}
void SQLiteSchemaEntry::Alter(ClientContext &context, AlterInfo *info) {
	throw InternalException("Alter");
}
void SQLiteSchemaEntry::Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry *)> &callback) {
	if (type != CatalogType::TABLE_ENTRY) {
		throw BinderException("Only tables are supported for now");
	}
	auto &transaction = SQLiteTransaction::Get(context, *catalog);
	auto tables = transaction.GetDB().GetTables();
	for(auto &table_name : tables) {
		callback(GetEntry(GetCatalogTransaction(context), type, table_name));
	}
}
void SQLiteSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry *)> &callback) {
	throw InternalException("Scan");
}

DropErrorType SQLiteSchemaEntry::DropEntry(ClientContext &context, DropInfo *info) {
	if (info->type != CatalogType::TABLE_ENTRY) {
		throw BinderException("Only tables are supported for now");
	}
	auto table = GetEntry(GetCatalogTransaction(context), info->type, info->name);
	if (!table) {
		return DropErrorType::ENTRY_DOES_NOT_EXIST;
	}
	auto &transaction = SQLiteTransaction::Get(context, *catalog);
	transaction.DropTable(info->name, info->cascade);
	return DropErrorType::SUCCESS;
}

CatalogEntry *SQLiteSchemaEntry::GetEntry(CatalogTransaction transaction, CatalogType type, const string &name) {
	if (type != CatalogType::TABLE_ENTRY) {
		throw BinderException("Only tables are supported for now");
	}
	auto &sqlite_transaction = GetSQLiteTransaction(transaction);
	return sqlite_transaction.GetTable(name);
}

}