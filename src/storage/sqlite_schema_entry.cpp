#include "storage/sqlite_schema_entry.hpp"
#include "storage/sqlite_table_entry.hpp"
#include "storage/sqlite_transaction.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"

namespace duckdb {

SQLiteSchemaEntry::SQLiteSchemaEntry(Catalog *catalog) : SchemaCatalogEntry(catalog, DEFAULT_SCHEMA, true) {
}

SQLiteTransaction &GetSQLiteTransaction(CatalogTransaction transaction) {
	if (!transaction.transaction) {
		throw InternalException("No transaction!?");
	}
	return (SQLiteTransaction &)*transaction.transaction;
}

string GetCreateTableSQL(Catalog *catalog, SchemaCatalogEntry *schema, CreateTableInfo &info) {
	auto result = make_unique<SQLiteTableEntry>(catalog, schema, info);
	return result->ToSQL();
}

CatalogEntry *SQLiteSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo *info) {
	auto &sqlite_transaction = GetSQLiteTransaction(transaction);
	auto &base_info = info->Base();
	auto table_name = base_info.table;
	if (base_info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		// CREATE OR REPLACE - drop any existing entries first (if any)
		DropInfo info;
		info.type = CatalogType::TABLE_ENTRY;
		info.name = table_name;
		info.cascade = false;
		info.if_exists = true;
		DropEntry(transaction.GetContext(), &info);
	}
	sqlite_transaction.GetDB().Execute(GetCreateTableSQL(catalog, this, base_info));
	return GetEntry(transaction, CatalogType::TABLE_ENTRY, table_name);
}

CatalogEntry *SQLiteSchemaEntry::CreateFunction(CatalogTransaction transaction, CreateFunctionInfo *info) {
	throw InternalException("CreateFunction");
}

void SQLiteSchemaEntry::AlterTable(SQLiteTransaction &sqlite_transaction, RenameTableInfo &info) {
	string sql = "ALTER TABLE ";
	sql += KeywordHelper::WriteOptionallyQuoted(info.name);
	sql += " RENAME TO ";
	sql += KeywordHelper::WriteOptionallyQuoted(info.new_table_name);
	sqlite_transaction.GetDB().Execute(sql);
}

void SQLiteSchemaEntry::AlterTable(SQLiteTransaction &sqlite_transaction, RenameColumnInfo &info) {
	string sql = "ALTER TABLE ";
	sql += KeywordHelper::WriteOptionallyQuoted(info.name);
	sql += " RENAME COLUMN  ";
	sql += KeywordHelper::WriteOptionallyQuoted(info.old_name);
	sql += " TO ";
	sql += KeywordHelper::WriteOptionallyQuoted(info.new_name);
	sqlite_transaction.GetDB().Execute(sql);
}

void SQLiteSchemaEntry::AlterTable(SQLiteTransaction &sqlite_transaction, AddColumnInfo &info) {
	if (info.if_column_not_exists) {
		if (sqlite_transaction.GetDB().ColumnExists(info.name, info.new_column.GetName())) {
			return;
		}
	}
	string sql = "ALTER TABLE ";
	sql += KeywordHelper::WriteOptionallyQuoted(info.name);
	sql += " ADD COLUMN  ";
	sql += KeywordHelper::WriteOptionallyQuoted(info.new_column.Name());
	sql += " ";
	sql += info.new_column.Type().ToString();
	sqlite_transaction.GetDB().Execute(sql);
}

void SQLiteSchemaEntry::AlterTable(SQLiteTransaction &sqlite_transaction, RemoveColumnInfo &info) {
	if (info.if_column_exists) {
		if (!sqlite_transaction.GetDB().ColumnExists(info.name, info.removed_column)) {
			return;
		}
	}
	string sql = "ALTER TABLE ";
	sql += KeywordHelper::WriteOptionallyQuoted(info.name);
	sql += " DROP COLUMN  ";
	sql += KeywordHelper::WriteOptionallyQuoted(info.removed_column);
	sqlite_transaction.GetDB().Execute(sql);
}

void SQLiteSchemaEntry::Alter(ClientContext &context, AlterInfo *info) {
	if (info->type != AlterType::ALTER_TABLE) {
		throw BinderException("Only altering tables is supported for now");
	}
	auto &alter = (AlterTableInfo &)*info;
	auto &transaction = SQLiteTransaction::Get(context, *catalog);
	switch (alter.alter_table_type) {
	case AlterTableType::RENAME_TABLE:
		AlterTable(transaction, (RenameTableInfo &)alter);
		break;
	case AlterTableType::RENAME_COLUMN:
		AlterTable(transaction, (RenameColumnInfo &)alter);
		break;
	case AlterTableType::ADD_COLUMN:
		AlterTable(transaction, (AddColumnInfo &)alter);
		break;
	case AlterTableType::REMOVE_COLUMN:
		AlterTable(transaction, (RemoveColumnInfo &)alter);
		break;
	default:
		throw BinderException("Unsupported ALTER TABLE type - SQLite tables only support RENAME TABLE, RENAME COLUMN, "
		                      "ADD COLUMN and DROP COLUMN");
	}
	transaction.ClearTableEntry(info->name);
}

void SQLiteSchemaEntry::Scan(ClientContext &context, CatalogType type,
                             const std::function<void(CatalogEntry *)> &callback) {
	if (type != CatalogType::TABLE_ENTRY) {
		throw BinderException("Only tables are supported for now");
	}
	auto &transaction = SQLiteTransaction::Get(context, *catalog);
	auto tables = transaction.GetDB().GetTables();
	for (auto &table_name : tables) {
		callback(GetEntry(GetCatalogTransaction(context), type, table_name));
	}
}
void SQLiteSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry *)> &callback) {
	throw InternalException("Scan");
}

void SQLiteSchemaEntry::DropEntry(ClientContext &context, DropInfo *info) {
	if (info->type != CatalogType::TABLE_ENTRY) {
		throw BinderException("Only tables are supported for now");
	}
	auto table = GetEntry(GetCatalogTransaction(context), info->type, info->name);
	if (!table) {
		throw InternalException("Failed to drop entry \"%s\" - could not find entry", info->name);
	}
	auto &transaction = SQLiteTransaction::Get(context, *catalog);
	transaction.DropTable(info->name, info->cascade);
}

CatalogEntry *SQLiteSchemaEntry::GetEntry(CatalogTransaction transaction, CatalogType type, const string &name) {
	if (type != CatalogType::TABLE_ENTRY) {
		return nullptr;
	}
	auto &sqlite_transaction = GetSQLiteTransaction(transaction);
	return sqlite_transaction.GetTable(name);
}

} // namespace duckdb