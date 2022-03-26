.PHONY: all clean format debug release duckdb_debug duckdb_release pull update

all: release

OSX_BUILD_UNIVERSAL_FLAG=
ifeq (${OSX_BUILD_UNIVERSAL}, 1)
	OSX_BUILD_UNIVERSAL_FLAG=-DOSX_BUILD_UNIVERSAL=1
endif


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

debug: pull 
	mkdir -p build/debug && \
	cd build/debug && \
	cmake  -DCMAKE_BUILD_TYPE=Debug -DDUCKDB_INCLUDE_FOLDER=duckdb/src/include -DDUCKDB_LIBRARY_FOLDER=duckdb/build/debug/src ${OSX_BUILD_UNIVERSAL_FLAG}  ../.. && \
	cmake --build .

release: pull 
	mkdir -p build/release && \
	cd build/release && \
	cmake  -DCMAKE_BUILD_TYPE=RelWithDebInfo -DDUCKDB_INCLUDE_FOLDER=duckdb/src/include -DDUCKDB_LIBRARY_FOLDER=duckdb/build/release/src ${OSX_BUILD_UNIVERSAL_FLAG} ../.. && \
	cmake --build .

test: release duckdb_release
	../duckdb/build/release/test/unittest --test-dir . "[lite_scanner]"

format:
	clang-format --sort-includes=0 -style=file -i sqlite_scanner.cpp
	cmake-format -i CMakeLists.txt

update:
	git submodule update --remote --merge
