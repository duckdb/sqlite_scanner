#include "sqlite_vfs.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/file_opener.hpp"

namespace duckdb {

struct vfs_sqlite_file {
	sqlite3_file base;
	SQLiteVirtualFileSystem *vfs;
	FileHandle *handle;
};

SQLiteVirtualFileSystem::SQLiteVirtualFileSystem(string name_p, ClientContext &context_p) :
	name(std::move(name_p)), context(context_p) {
	optional_ptr<sqlite3_vfs> default_vfs = sqlite3_vfs_find(nullptr);

	vfs = make_uniq<sqlite3_vfs>();

	// VFS methods
	vfs->iVersion = default_vfs->iVersion;
	vfs->szOsFile = sizeof(vfs_sqlite_file);
	vfs->mxPathname = default_vfs->mxPathname;
	vfs->zName = name.c_str();
	vfs->pAppData = (void *) this;

	vfs->xOpen = SQLiteVFSOpen;
	vfs->xDelete = SQLiteVFSDelete;
	vfs->xAccess = SQLiteVFSAccess;
	vfs->xFullPathname = SQLiteVFSFullPathname;
	vfs->xGetLastError = SQLiteVFSGetLastError;

	// inherit these from the default - we don't care
	vfs->xDlOpen = default_vfs->xDlOpen;
	vfs->xDlError = default_vfs->xDlError;
	vfs->xDlSym = default_vfs->xDlSym;
	vfs->xDlClose = default_vfs->xDlClose;
	vfs->xRandomness = default_vfs->xRandomness;
	vfs->xSleep = default_vfs->xSleep;
	vfs->xCurrentTime = default_vfs->xCurrentTime;
	vfs->xCurrentTimeInt64 = default_vfs->xCurrentTimeInt64;
	vfs->xSetSystemCall = default_vfs->xSetSystemCall;
	vfs->xGetSystemCall = default_vfs->xGetSystemCall;
	vfs->xNextSystemCall = default_vfs->xNextSystemCall;

	// I/O methods
	io_methods = make_uniq<sqlite3_io_methods>();
	io_methods->iVersion = default_vfs->iVersion;
	io_methods->xClose = SQLiteVFSClose;
	io_methods->xRead = SQLiteVFSRead;
	io_methods->xWrite = SQLiteVFSWrite;
	io_methods->xTruncate = SQLiteVFSTruncate;
	io_methods->xSync = SQLiteVFSSync;
	io_methods->xFileSize = SQLiteVFSFileSize;
	io_methods->xLock = SQLiteVFSLock;
	io_methods->xUnlock = SQLiteVFSUnlock;
	io_methods->xCheckReservedLock = SQLiteVFSCheckReservedLock;
	io_methods->xFileControl = SQLiteVFSFileControl;
	io_methods->xSectorSize = SQLiteVFSSectorSize;
	io_methods->xDeviceCharacteristics = SQLiteVFSDeviceCharacteristics;
	io_methods->xShmMap = SQLiteVFSShmMap;
	io_methods->xShmLock = SQLiteVFSShmLock;
	io_methods->xShmBarrier = SQLiteVFSShmBarrier;
	io_methods->xShmUnmap = SQLiteVFSxShmUnmap;
	io_methods->xFetch = SQLiteVFSxFetch;
	io_methods->xUnfetch = SQLiteVFSxUnfetch;

	sqlite3_vfs_register(vfs.get(), 0);
}

SQLiteVirtualFileSystem::~SQLiteVirtualFileSystem() {
	if (!vfs) {
		return;
	}
	sqlite3_vfs_unregister(vfs.get());
}

sqlite3_vfs *SQLiteVirtualFileSystem::GetSQLiteVFS() {
	return vfs.get();
}

SQLiteVirtualFileSystem &SQLiteVirtualFileSystem::GetVFS(sqlite3_vfs *vfs) {
	if (!vfs->pAppData) {
		throw InternalException("SQLiteVirtualFileSystem - vfs->pAppData not set");
	}
	return * (SQLiteVirtualFileSystem *) vfs->pAppData;
}

FileSystem &SQLiteVirtualFileSystem::GetFileSystem() {
	return FileSystem::GetFileSystem(context);
}

FileOpener *SQLiteVirtualFileSystem::GetOpener() {
	return FileOpener::Get(context);
}

int SQLiteVirtualFileSystem::SQLiteVFSOpen(sqlite3_vfs *vfs_ptr, const char *zName, sqlite3_file *file_p, int sqlite_flags, int *pOutFlags) {
	printf("SQLiteVFSOpen\n");
	if (!zName || !vfs_ptr) {
		return SQLITE_ERROR;
	}
	auto &vfs = SQLiteVirtualFileSystem::GetVFS(vfs_ptr);
	auto &fs = vfs.GetFileSystem();
	uint8_t flags = 0;
	FileLockType lock = FileLockType::NO_LOCK;
	if (sqlite_flags & SQLITE_OPEN_READWRITE) {
		flags |= FileFlags::FILE_FLAGS_READ;
		flags |= FileFlags::FILE_FLAGS_WRITE;
	}
	if (sqlite_flags & SQLITE_OPEN_READONLY) {
		flags |= FileFlags::FILE_FLAGS_READ;
		if (pOutFlags) {
			*pOutFlags |= SQLITE_OPEN_READONLY;
		}
	}
	if (sqlite_flags & SQLITE_OPEN_CREATE) {
		flags |= FileFlags::FILE_FLAGS_FILE_CREATE;
	}
	if (sqlite_flags & SQLITE_OPEN_EXCLUSIVE) {
		lock = FileLockType::WRITE_LOCK;
	}
	auto file_handle = fs.OpenFile(zName, flags, lock, FileCompressionType::UNCOMPRESSED, vfs.GetOpener());
	auto file = reinterpret_cast<vfs_sqlite_file *>(file_p);
	if (!vfs.io_methods) {
		throw InternalException("SQLiteVirtualFileSystem - No I/O methods defined");
	}
	file->base.pMethods = vfs.io_methods.get();
	file->vfs = &vfs;
	file->handle = file_handle.release();

	return SQLITE_OK;
}

int SQLiteVirtualFileSystem::SQLiteVFSDelete(sqlite3_vfs*, const char *zName, int syncDir) {
	printf("SQLiteVFSDelete\n");
	return SQLITE_ERROR;
}
int SQLiteVirtualFileSystem::SQLiteVFSAccess(sqlite3_vfs*, const char *zName, int flags, int *pResOut) {
	printf("SQLiteVFSAccess\n");
	return SQLITE_ERROR;
}
int SQLiteVirtualFileSystem::SQLiteVFSFullPathname(sqlite3_vfs*, const char *zName, int nOut, char *zOut) {
	printf("SQLiteVFSFullPathname\n");
	auto len = strlen(zName);
	if (len + 1 >= nOut) {
		return SQLITE_ERROR;
	}
	memcpy(zOut, zName, len);
	zOut[len] = '\0';
	return SQLITE_OK;
}
int SQLiteVirtualFileSystem::SQLiteVFSGetLastError(sqlite3_vfs*, int , char *) {
	printf("SQLiteVFSGetLastError\n");
	return errno;
}

int SQLiteVirtualFileSystem::SQLiteVFSClose(sqlite3_file *file_p) {
	printf("SQLiteVFSClose\n");
	if (!file_p) {
		return SQLITE_MISUSE;
	}
	auto file = reinterpret_cast<vfs_sqlite_file *>(file_p);
	if (file->handle) {
		auto uniq_file = unique_ptr<FileHandle>(file->handle);
		file->handle = nullptr;
		uniq_file->Close();
	}
	return SQLITE_OK;
}

int SQLiteVirtualFileSystem::SQLiteVFSRead(sqlite3_file *file_p, void *pBuf, int iAmt, sqlite3_int64 iOfst) {
	printf("Read call of %d at offset %llu\n", iAmt, iOfst);
	if (!file_p || !pBuf) {
		return SQLITE_MISUSE;
	}
	auto file = reinterpret_cast<vfs_sqlite_file *>(file_p);
	file->handle->Read(pBuf, iAmt, iOfst);
	return SQLITE_OK;
}
int SQLiteVirtualFileSystem::SQLiteVFSWrite(sqlite3_file *file_p, const void *pBuf, int iAmt, sqlite3_int64 iOfst) {
	printf("SQLiteVFSWrite\n");
	if (!file_p || !pBuf) {
		return SQLITE_MISUSE;
	}
	auto file = reinterpret_cast<vfs_sqlite_file *>(file_p);
	file->handle->Write((void *) pBuf, iAmt, iOfst);
	return SQLITE_OK;
}
int SQLiteVirtualFileSystem::SQLiteVFSTruncate(sqlite3_file *file_p, sqlite3_int64 size) {
	printf("SQLiteVFSTruncate\n");
	if (!file_p) {
		return SQLITE_MISUSE;
	}
	auto file = reinterpret_cast<vfs_sqlite_file *>(file_p);
	file->handle->Truncate(size);
	return SQLITE_OK;
}

int SQLiteVirtualFileSystem::SQLiteVFSSync(sqlite3_file *file_p, int flags) {
	printf("SQLiteVFSSync\n");
	if (!file_p) {
		return SQLITE_MISUSE;
	}
	auto file = reinterpret_cast<vfs_sqlite_file *>(file_p);
	file->handle->Sync();
	return SQLITE_OK;
}
int SQLiteVirtualFileSystem::SQLiteVFSFileSize(sqlite3_file *file_p, sqlite3_int64 *pSize) {
	printf("SQLiteVFSFileSize\n");
	if (!file_p || !pSize) {
		return SQLITE_MISUSE;
	}
	auto file = reinterpret_cast<vfs_sqlite_file *>(file_p);
	*pSize = int64_t(file->handle->GetFileSize());
	return SQLITE_OK;
}
int SQLiteVirtualFileSystem::SQLiteVFSLock(sqlite3_file*, int) {
	printf("SQLiteVFSLock\n");
	return SQLITE_OK;
}
int SQLiteVirtualFileSystem::SQLiteVFSUnlock(sqlite3_file*, int) {
	printf("SQLiteVFSUnlock\n");
	return SQLITE_OK;
}
int SQLiteVirtualFileSystem::SQLiteVFSCheckReservedLock(sqlite3_file*, int *pResOut) {
	printf("SQLiteVFSCheckReservedLock\n");
	*pResOut = 0;
	return SQLITE_OK;
}
int SQLiteVirtualFileSystem::SQLiteVFSFileControl(sqlite3_file*, int op, void *pArg) {
	printf("SQLiteVFSFileControl\n");
	return 0;
}
int SQLiteVirtualFileSystem::SQLiteVFSSectorSize(sqlite3_file*) {
	printf("SQLiteVFSSectorSize\n");
	return 0;
}
int SQLiteVirtualFileSystem::SQLiteVFSDeviceCharacteristics(sqlite3_file*) {
	printf("SQLiteVFSDeviceCharacteristics\n");
	return 0;
}
int SQLiteVirtualFileSystem::SQLiteVFSShmMap(sqlite3_file*, int iPg, int pgsz, int, void volatile**) {
	throw InternalException("SQLite VFS shared memory not implemented");
}
int SQLiteVirtualFileSystem::SQLiteVFSShmLock(sqlite3_file*, int offset, int n, int flags) {
	throw InternalException("SQLite VFS shared memory not implemented");
}

void SQLiteVirtualFileSystem::SQLiteVFSShmBarrier(sqlite3_file*) {
	throw InternalException("SQLite VFS shared memory not implemented");
}

int SQLiteVirtualFileSystem::SQLiteVFSxShmUnmap(sqlite3_file*, int deleteFlag) {
	throw InternalException("SQLite VFS shared memory not implemented");
}
int SQLiteVirtualFileSystem::SQLiteVFSxFetch(sqlite3_file*, sqlite3_int64 iOfst, int iAmt, void **pp) {
	printf("SQLiteVFSxFetch\n");
	return SQLITE_ERROR;
}
int SQLiteVirtualFileSystem::SQLiteVFSxUnfetch(sqlite3_file*, sqlite3_int64 iOfst, void *p) {
	printf("SQLiteVFSxUnfetch\n");
	return SQLITE_ERROR;
}

}
