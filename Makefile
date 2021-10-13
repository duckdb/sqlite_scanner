.PHONY: all debug clean duckdb format release duckdb_release
all: debug

clean:
	rm -rf build
	rm -rf duckdb/build


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

release:
	mkdir -p build/release && \
	cd build/release && \
	cmake  -DCMAKE_BUILD_TYPE=RelWithDebInfo ../.. && \
	cmake --build .

test: release
	./duckdb/build/release/duckdb < test.sql

format:
	clang-format --sort-includes=0 -style=file -i sqlite_scanner.cpp 