# Mod Box
set(js_sources)
if (ENABLE_JS)
lua_source(js_sources box.js)
endif (ENABLE_JS)

set(js_box_sources
    box.cc
    box_tuple.cc
    ${js_sources}
)

add_custom_target(generate_box_js_sources
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/src/box
    DEPENDS ${js_sources})
set_property(DIRECTORY PROPERTY ADDITIONAL_MAKE_CLEAN_FILES ${js_sources})

set_source_files_compile_flags(${js_box_sources})
add_library(js_box STATIC ${js_box_sources})
