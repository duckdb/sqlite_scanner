.PHONY: all clean format debug release duckdb_debug duckdb_release pull
all: release

pull:
	git submodule init
	git submodule update --recursive --remote	

clean:
	rm -rf build
	rm -rf duckdb/build

duckdb_debug:
	cd duckdb && \
	BUILD_TPCH=1 make debug

duckdb_release:
	cd duckdb && \
	BUILD_TPCH=1 make release

debug: pull duckdb_debug
	mkdir -p build/debug && \
	cd build/debug && \
	cmake  -DCMAKE_BUILD_TYPE=Debug ../.. && \
	cmake --build .

release: pull duckdb_release
	mkdir -p build/release && \
	cd build/release && \
	cmake  -DCMAKE_BUILD_TYPE=RelWithDebInfo ../.. && \
	cmake --build .

test: release
	../duckdb/build/debug/test/unittest --test-dir . "[lite_scanner]"

format:
	clang-format --sort-includes=0 -style=file -i sqlite_scanner.cpp
	cmake-format -i CMakeLists.txt