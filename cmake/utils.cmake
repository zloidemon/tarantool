macro(add_compile_flags langs)
    foreach(_lang ${langs})
        string (REPLACE ";" " " _flags "${ARGN}")
        set ("CMAKE_${_lang}_FLAGS" "${CMAKE_${_lang}_FLAGS} ${_flags}")
        unset (${_lang})
        unset (${_flags})
    endforeach()
endmacro(add_compile_flags)

macro(set_source_files_compile_flags)
    foreach(file ${ARGN})
        get_filename_component(_file_ext ${file} EXT)
        set(_lang "")
        if ("${_file_ext}" STREQUAL ".m")
            set(_lang OBJC)
            # CMake believes that Objective C is a flavor of C++, not C,
            # and uses g++ compiler for .m files.
            # LANGUAGE property forces CMake to use CC for ${file}
            set_source_files_properties(${file} PROPERTIES LANGUAGE C)
        elseif("${_file_ext}" STREQUAL ".mm")
            set(_lang OBJCXX)
        endif()

        if (_lang)
            get_source_file_property(_flags ${file} COMPILE_FLAGS)
            if ("${_flags}" STREQUAL "NOTFOUND")
                set(_flags "${CMAKE_${_lang}_FLAGS}")
            else()
                set(_flags "${_flags} ${CMAKE_${_lang}_FLAGS}")
            endif()
            # message(STATUS "Set (${file} ${_flags}")
            set_source_files_properties(${file} PROPERTIES COMPILE_FLAGS
                "${_flags}")
        endif()
    endforeach()
    unset(_file_ext)
    unset(_lang)
endmacro(set_source_files_compile_flags)

# lua_source(VAR_NAME file1.lua ...)
#
# Emit rules for compiling lua sources; the list of generated files
# is saved in VAR_NAME in the caller scope. Note: files are created
# in ${CMAKE_CURRENT_BINARY_DIR}/lua.
#
# Arguments starting with @ are commands:
#   @@lint@@, @@nolint@@ - lint on or off,
#   @<vinstall_path> - source file name is recorded in compiled module,
#                      modify recorded name to appear as if the file was
#                      installed in <vinstall_path>, ex: @bultin/foo
function(lua_source varname)
    set(atat @@) # to prevent @VAR@ expansion in "@@lint@@" string
    set(generated)
    set(vinstall_path)
    set(lint 1)
    set(luajit_cmd
        env LUA_PATH=${CMAKE_BINARY_DIR}/third_party/luajit/src/?.lua
        ${CMAKE_BINARY_DIR}/third_party/luajit/src/luajit)
    foreach(srcfile ${ARGN})
        if ("${srcfile}" MATCHES "^@")
            if ("${srcfile}" STREQUAL "${atat}lint${atat}")
                set(lint 1)
            elseif ("${srcfile}" STREQUAL "${atat}nolint${atat}")
                set(lint 0)
            else ()
                set(vinstall_path "${srcfile}")
            endif ()
        else ()
            get_filename_component(srcfile ${srcfile} ABSOLUTE)
            get_filename_component(filename ${srcfile} NAME)
            get_filename_component(filename_we ${srcfile} NAME_WE)
            set (dstdir "${CMAKE_CURRENT_BINARY_DIR}/lua")
            set (rawfile "${dstdir}/${filename_we}.raw")
            set (tmpfile "${dstdir}/${filename}.new.h")
            set (dstfile "${dstdir}/${filename}.h")
            if (NOT IS_DIRECTORY ${dstdir})
                file(MAKE_DIRECTORY ${dstdir})
            endif()

            if ("${vinstall_path}" STREQUAL "")
                set(instname_stage)
            else ()
                set(instname_stage COMMAND
                    ${CMAKE_SOURCE_DIR}/extra/lua_instname.py
                    "-r${vinstall_path}/${filename}" ${rawfile})
            endif ()

            if ("${lint}" EQUAL 1)
                set(lint_stage COMMAND ${luajit_cmd}
                    ${CMAKE_SOURCE_DIR}/extra/lint.lua ${rawfile})
            else ()
                set(lint_stage)
            endif ()

            # 1. compile and save binary bytecode in ${rawfile}
            # 2. lint
            # 3. fix recorded file name in ${rawfile}
            # 4. convert ${rawfile} to a C header file
            #    (put bytecode in a static array)
            ADD_CUSTOM_COMMAND(OUTPUT ${dstfile}
                COMMAND ${luajit_cmd} -bg ${srcfile} ${rawfile}
                ${lint_stage}
                ${instname_stage}
                COMMAND ${luajit_cmd} -bg ${rawfile} ${tmpfile}
                COMMAND ${CMAKE_COMMAND} -E copy ${tmpfile} ${dstfile}
                COMMAND ${CMAKE_COMMAND} -E remove ${rawfile} ${tmpfile}
                DEPENDS ${CMAKE_SOURCE_DIR}/extra/lint.lua
                DEPENDS ${CMAKE_SOURCE_DIR}/extra/lua_instname.py
                DEPENDS ${srcfile} libluajit)

            set(generated ${generated} ${dstfile})
        endif ()
    endforeach()

    set(${varname} ${generated} PARENT_SCOPE)
endfunction()

function(bin_source varname srcfile dstfile)
    set(var ${${varname}})
    set(${varname} ${var} ${dstfile} PARENT_SCOPE)
    set (srcfile "${CMAKE_CURRENT_SOURCE_DIR}/${srcfile}")
    set (dstfile "${CMAKE_CURRENT_SOURCE_DIR}/${dstfile}")
    set (tmpfile "${dstfile}.tmp")
    get_filename_component(module ${dstfile} NAME_WE)

    ADD_CUSTOM_COMMAND(OUTPUT ${dstfile}
        COMMAND ${ECHO} 'const unsigned char ${module}_bin[] = {' > ${tmpfile}
        COMMAND ${CMAKE_BINARY_DIR}/extra/bin2c "${srcfile}" >> ${tmpfile}
        COMMAND ${ECHO} '}\;' >> ${tmpfile}
        COMMAND ${CMAKE_COMMAND} -E copy_if_different ${tmpfile} ${dstfile}
        COMMAND ${CMAKE_COMMAND} -E remove ${tmpfile}
        DEPENDS ${srcfile} bin2c)

endfunction()

