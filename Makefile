.PHONY: all debug clean sqlite duckdb format release duckdb_release sqlite_release
all: debug

clean:
	rm -rf build
	rm -rf sqlite/build
	rm -rf duckdb/build

sqlite:
	mkdir -p sqlite/build/debug && \
	cd sqlite/build/debug && \
	../../configure --disable-tcl --enable-debug && \
	make -j

sqlite_release:
	mkdir -p sqlite/build/release && \
	cd sqlite/build/release && \
	../../configure --disable-tcl && \
	make -j

duckdb:
	cd duckdb && \
	DISABLE_SANITIZER=1 BUILD_TPCH=1 make debug

duckdb_release:
	cd duckdb && \
	DISABLE_SANITIZER=1 BUILD_TPCH=1 make

debug: 
	mkdir -p build/debug && \
	cd build/debug && \
	cmake  -DCMAKE_BUILD_TYPE=Debug ../.. && \
	cmake --build .

release: duckdb_release sqlite_release
	mkdir -p build/release && \
	cd build/release && \
	cmake  -DCMAKE_BUILD_TYPE=Release ../.. && \
	cmake --build .

test: debug
	./duckdb/build/debug/duckdb < test.sql

format:
	clang-format --sort-includes=0 -style=file -i sqlite_scanner.cpp 