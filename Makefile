.PHONY: all clean format debug release pull update

all: release

OSX_BUILD_UNIVERSAL_FLAG=
GENERATOR=
ifeq ($(GEN),ninja)
	GENERATOR=-G "Ninja"
endif
ifeq (${OSX_BUILD_UNIVERSAL}, 1)
	OSX_BUILD_UNIVERSAL_FLAG=-DOSX_BUILD_UNIVERSAL=1
endif
BUILD_FLAGS=-DEXTENSION_STATIC_BUILD=1 -DBUILD_TPCH_EXTENSION=1 ${OSX_BUILD_UNIVERSAL_FLAG}


pull:
	git submodule init
	git submodule update --recursive --remote	

clean:
	rm -rf build

debug: pull
	mkdir -p build/debug && \
	cd build/debug && \
	cmake $(GENERATOR) -DCMAKE_BUILD_TYPE=Debug ${BUILD_FLAGS} ../../duckdb/CMakeLists.txt -DEXTERNAL_EXTENSION_DIRECTORIES=../../sqlite_scanner -B. && \
	cmake --build . --config Debug

release: pull 
	mkdir -p build/release && \
	cd build/release && \
	cmake $(GENERATOR) -DCMAKE_BUILD_TYPE=RelWithDebInfo ${BUILD_FLAGS} ../../duckdb/CMakeLists.txt -DEXTERNAL_EXTENSION_DIRECTORIES=../../sqlite_scanner -B. && \
	cmake --build . --config Release

test: release
	./build/release/test/unittest --test-dir .

format:
	cp duckdb/.clang-format .
	find src -iname *.hpp -o -iname *.cpp | xargs clang-format --sort-includes=0 -style=file -i
	find . -iname CMakeLists.txt | xargs cmake-format -i
	rm .clang-format

update:
	git submodule update --remote --merge
