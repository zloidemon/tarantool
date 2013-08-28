##
## Core JS Library
##
set(js_sources
    js.cc
)
# Needed for mhash
set_source_files_properties(${js_sources} PROPERTIES
    COMPILE_FLAGS "-Wno-unused-function")
set_source_files_compile_flags(${js_sources})
add_library(js STATIC ${js_sources})
target_link_libraries(js ${LIBV8_LIBRARIES})

##
## Modules
##

lua_source(js_console_sources console.js)

# Mod Console
set(js_console_sources
    ${js_console_sources}
    console.cc
)

add_custom_target(generate_console_js_sources
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/src/box
    DEPENDS ${js_console_sources})
set_property(DIRECTORY PROPERTY ADDITIONAL_MAKE_CLEAN_FILES ${js_console_sources})

set_source_files_compile_flags(${js_console_sources})
add_library(js_console STATIC ${js_console_sources})
target_link_libraries(js_console js)

# Mod Require
set(js_require_sources
    require.cc
)

set_source_files_compile_flags(${js_require_sources})
add_library(js_require STATIC ${js_require_sources})
target_link_libraries(js_require js)

# Mod Fiber
set(js_fiber_sources
    fiber.cc
)

set_source_files_compile_flags(${js_fiber_sources})
add_library(js_fiber STATIC ${js_fiber_sources})
target_link_libraries(js_fiber js)

# Mod Stub
set(js_stub_sources
    stub.cc
)

set_source_files_compile_flags(${js_stub_sources})
add_library(js_stub STATIC ${js_stub_sources})
target_link_libraries(js_stub js)

# Mod Lua
set(js_lua_sources
    lua.cc
)

set_source_files_compile_flags(${js_lua_sources})
add_library(js_lua STATIC ${js_lua_sources})
target_link_libraries(js_lua js)

# Mod Session
set(js_session_sources
    session.cc
)

set_source_files_compile_flags(${js_session_sources})
add_library(js_session STATIC ${js_session_sources})
target_link_libraries(js_session js)

target_link_libraries(js
    js_stub
    js_console
    js_require
    js_fiber
    js_lua
    js_session
    js_box
)

##
## Tarantool <-> JS bridge
##
set(tarantool_js_sources
    init.cc
)
set_source_files_compile_flags(${tarantool_js_sources})
add_library(tarantool_js STATIC ${tarantool_js_sources})
target_link_libraries(tarantool_js js)
