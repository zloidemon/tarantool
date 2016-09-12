check_library_exists (gcov __gcov_flush  ""  HAVE_GCOV)

set(ENABLE_GCOV_DEFAULT OFF)
option(ENABLE_GCOV "Enable integration with gcov, a code coverage program" ${ENABLE_GCOV_DEFAULT})

if (ENABLE_GCOV)
    if (NOT HAVE_GCOV)
    message (FATAL_ERROR
         "ENABLE_GCOV option requested but gcov library is not found")
    endif()

    add_compile_flags("C;CXX"
        "-fprofile-arcs"
        "-ftest-coverage"
    )

    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fprofile-arcs")
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -ftest-coverage")
    set (CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fprofile-arcs")
    set (CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -ftest-coverage")

   # add_library(gcov SHARED IMPORTED)
endif()

if (NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(ENABLE_GPROF_DEFAULT ON)
else()
    set(ENABLE_GPROF_DEFAULT OFF)
endif()
option(ENABLE_GPROF "Enable integration with gprof, a performance analyzing tool" ${GPROF_DEFAULT})

if (ENABLE_GPROF)
    add_compile_flags("C;CXX" "-pg")
endif()

option(ENABLE_VALGRIND "Enable integration with valgrind, a memory analyzing tool" OFF)
if (ENABLE_VALGRIND)
    check_include_file(valgrind/valgrind.h HAVE_VALGRIND_VALGRIND_H)
    if (NOT HAVE_VALGRIND_VALGRIND_H)
        message (FATAL_ERROR
             "ENABLE_VALGRIND option is set but valgrind/valgrind.h is not found")
        endif()
endif()

option(ENABLE_ASAN "Enable AddressSanitizer, a fast memory error detector based on compiler instrumentation" OFF)
if (ENABLE_ASAN)
    add_compile_flags("C;CXX" -fsanitize=address -mllvm -asan-stack=0)
    # Apparently, C/CXX flags are passed to the linker as well.
    # Normally, that would be ok (irt -fsanitize=address).
    # However, we want a more recent sanitizer runtime than the one that
    # ships with a compiler (the most recent ASAN knows about fibers!)
    # hence we have to undo -fsanitize=address and link with the ASAN
    # library explicitly.
    set(ldflags_asan "-fno-sanitize=address ${CMAKE_BINARY_DIR}/libclang_rt.asan_osx_dynamic.dylib")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${ldflags_asan}")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} ${ldflags_asan}")
endif()
