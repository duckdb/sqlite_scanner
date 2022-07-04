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

debug: pull
	mkdir -p build/debug && \
	cd build/debug && \
	cmake -DCMAKE_BUILD_TYPE=Debug ${OSX_BUILD_UNIVERSAL_FLAG} ../../duckdb/CMakeLists.txt -DEXTERNAL_EXTENSION_DIRECTORY=../../sqlitescanner -B. && \
	cmake --build . --parallel

release: pull 
	mkdir -p build/release && \
	cd build/release && \
	cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo ${OSX_BUILD_UNIVERSAL_FLAG} ../../duckdb/CMakeLists.txt -DEXTERNAL_EXTENSION_DIRECTORY=../../sqlitescanner -B. && \
	cmake --build . --parallel

test: release
	./build/release/test/unittest --test-dir . "[lite_scanner]"

format:
	cp duckdb/.clang-format .
	clang-format --sort-includes=0 -style=file -i sqlite_scanner.cpp
	cmake-format -i CMakeLists.txt
	rm .clang-format

update:
	git submodule update --remote --merge
