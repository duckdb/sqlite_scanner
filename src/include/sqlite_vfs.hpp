//===----------------------------------------------------------------------===//
//                         DuckDB
//
// sqlite_vfs.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sqlite_utils.hpp"

namespace duckdb {
class SQLiteStatement;

class SQLiteVirtualFileSystem {
public:
	SQLiteVirtualFileSystem(string name, ClientContext &context);
	~SQLiteVirtualFileSystem();

	sqlite3_vfs *GetSQLiteVFS();

public:
	static SQLiteVirtualFileSystem &GetVFS(sqlite3_vfs*);
	FileSystem &GetFileSystem();
	FileOpener *GetOpener();

	// VFS methods
	static int SQLiteVFSOpen(sqlite3_vfs*, const char *zName, sqlite3_file*, int flags, int *pOutFlags);
	static int SQLiteVFSDelete(sqlite3_vfs*, const char *zName, int syncDir);
	static int SQLiteVFSAccess(sqlite3_vfs*, const char *zName, int flags, int *pResOut);
	static int SQLiteVFSFullPathname(sqlite3_vfs*, const char *zName, int nOut, char *zOut);
	static int SQLiteVFSGetLastError(sqlite3_vfs*, int, char *);

	// I/O methods
	static int SQLiteVFSClose(sqlite3_file*);
	static int SQLiteVFSRead(sqlite3_file*, void*, int iAmt, sqlite3_int64 iOfst);
	static int SQLiteVFSWrite(sqlite3_file*, const void*, int iAmt, sqlite3_int64 iOfst);
	static int SQLiteVFSTruncate(sqlite3_file*, sqlite3_int64 size);
	static int SQLiteVFSSync(sqlite3_file*, int flags);
	static int SQLiteVFSFileSize(sqlite3_file*, sqlite3_int64 *pSize);
	static int SQLiteVFSLock(sqlite3_file*, int);
	static int SQLiteVFSUnlock(sqlite3_file*, int);
	static int SQLiteVFSCheckReservedLock(sqlite3_file*, int *pResOut);
	static int SQLiteVFSFileControl(sqlite3_file*, int op, void *pArg);
	static int SQLiteVFSSectorSize(sqlite3_file*);
	static int SQLiteVFSDeviceCharacteristics(sqlite3_file*);
	static int SQLiteVFSShmMap(sqlite3_file*, int iPg, int pgsz, int, void volatile**);
	static int SQLiteVFSShmLock(sqlite3_file*, int offset, int n, int flags);
	static void SQLiteVFSShmBarrier(sqlite3_file*);
	static int SQLiteVFSxShmUnmap(sqlite3_file*, int deleteFlag);
	static int SQLiteVFSxFetch(sqlite3_file*, sqlite3_int64 iOfst, int iAmt, void **pp);
	static int SQLiteVFSxUnfetch(sqlite3_file*, sqlite3_int64 iOfst, void *p);

private:
	string name;
	ClientContext &context;
	unique_ptr<sqlite3_vfs> vfs;
	unique_ptr<sqlite3_io_methods> io_methods;
	string last_error;
};

} // namespace duckdb
