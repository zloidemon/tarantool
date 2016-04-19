#
# A macro to build the sqlite submodule
macro(trnsqlite_build)
    set(SQLITE_BUILD_DIRECTORY ${PROJECT_BINARY_DIR}/third_party/sqlite/)
    set(SQLITE_SOURCE_DIRECTORY ${PROJECT_SOURCE_DIR}/third_party/sqlite)
    # create directory for out of source build
    file(MAKE_DIRECTORY ${SQLITE_BUILD_DIRECTORY}) 
    
    # configure sqlite
    execute_process(COMMAND ${SQLITE_SOURCE_DIRECTORY}/configure
                    WORKING_DIRECTORY ${SQLITE_BUILD_DIRECTORY})
    
    # make amalgamation
    execute_process(COMMAND make sqlite3.c
                    WORKING_DIRECTORY ${SQLITE_BUILD_DIRECTORY})
    # create target testfixture
    add_custom_target(testfixture make testfixture
                     WORKING_DIRECTORY ${SQLITE_BUILD_DIRECTORY})
    add_dependencies(testfixture sqlite_make)

    set(sqlite_src ${SQLITE_BUILD_DIRECTORY}/sqlite3.c)
     
    add_library(trnsqlite SHARED ${sqlite_src})
    SET_TARGET_PROPERTIES(trnsqlite PROPERTIES COMPILE_FLAGS "-fPIC -DSQLITE_PRIVATE=\"\" -DSQLITE_TEST=1 -DSQLITE_CORE=1")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O0 -g")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -g")
	include_directories(${SQLITE_BUILD_DIRECTORY}/tsrc)
    set(LIBSQLITE_INCLUDE_DIR ${SQLITE_BUILD_DIRECTORY}/tsrc)
    set(LIBSQLITE_LIBRARIES trnsqlite)

    message(STATUS "Use bundled sqlite library: ${LIBSQLITE_LIBRARIES}")
    
    add_dependencies(trnsqlite testfixture) 
    unset(SQLITE_BUILD_DIRECTORY)
    unset(SQLITE_SOURCE_DIRECTORY)
    unset(sqlite_src)
endmacro(trnsqlite_build)
