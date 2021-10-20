.PHONY: all clean format debug release duckdb_debug duckdb_release
all: release

clean:
	rm -rf build
	rm -rf duckdb/build

duckdb_debug:
	cd duckdb && \
	BUILD_TPCH=1 make debug

duckdb_release:
	cd duckdb && \
	BUILD_TPCH=1 make release

debug: duckdb_debug
	mkdir -p build/debug && \
	cd build/debug && \
	cmake  -DCMAKE_BUILD_TYPE=Debug ../.. && \
	cmake --build .

release: duckdb_release
	mkdir -p build/release && \
	cd build/release && \
	cmake  -DCMAKE_BUILD_TYPE=Release ../.. && \
	cmake --build .

test: release
	./duckdb/build/release/duckdb < test.sql

format:
	clang-format --sort-includes=0 -style=file -i sqlite_scanner.cpp
	cmake-format -i CMakeLists.txt