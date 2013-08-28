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

# Mod Platform
set(js_platform_sources
    platform.cc
)

set_source_files_compile_flags(${js_platform_sources})
add_library(js_platform STATIC ${js_platform_sources})
target_link_libraries(js_platform js)

# Mod Require
set(js_require_sources
    require.cc
)

set_source_files_compile_flags(${js_require_sources})
add_library(js_require STATIC ${js_require_sources})
target_link_libraries(js_require js js_platform)

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

target_link_libraries(js
    js_stub
    js_platform
    js_require
    js_fiber
    js_lua
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
