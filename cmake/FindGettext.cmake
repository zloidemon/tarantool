find_program(XGETTEXT_EXECUTABLE xgettext)
find_program(GETTEXT_MSGMERGE_EXECUTABLE msgmerge)
find_program(GETTEXT_MSGFMT_EXECUTABLE msgfmt)

set(GETTEXT_FOUND FALSE)

if (GETTEXT_MSGMERGE_EXECUTABLE AND GETTEXT_MSGFMT_EXECUTABLE AND XGETTEXT_EXECUTABLE)
    set(GETTEXT_FOUND TRUE)
endif()

if (GETTEXT_FOUND)
    message(STATUS "Gettext found")
    set(languages en ru)
    set(_mos)
    foreach(_lang ${languages})
        set(_wrkdir ${CMAKE_CURRENT_BINARY_DIR})
        set(_mo ${_wrkdir}/${_lang}.mo)
        set(_po ${CMAKE_SOURCE_DIR}/po/${_lang}.po)
        set(_newpot ${_wrkdir}/${_lang}.pot)
        set(_src ${CMAKE_SOURCE_DIR}/src/lua/help.lua)
        add_custom_command(
            OUTPUT ${_newpot}
            COMMAND ${XGETTEXT_EXECUTABLE} --no-location --keyword=_ --language=lua --from-code=UTF-8 --package-name=${PROJECT_NAME} -o ${_newpot} ${_src}
            DEPENDS ${_src}
            WORKING_DIRECTORY ${_wrkdir}
            COMMENT "Extract translatable messages to ${_newpot}"
        )

        add_custom_command(
            OUTPUT ${_mo}
            COMMAND ${GETTEXT_MSGMERGE_EXECUTABLE} --quiet --update --backup=none -s ${_po} ${_newpot}
            COMMAND ${GETTEXT_MSGFMT_EXECUTABLE} -o ${_mo} ${_po}
            DEPENDS ${_newpot} ${_po}
            WORKING_DIRECTORY ${_wrkdir}
            COMMENT "Update translated files and compile"
        )
        install(FILES ${_mo} DESTINATION share/locale/${_lang}/LC_MESSAGES RENAME ${PROJECT_NAME}.mo)
        list(APPEND _mos ${_mo})
    endforeach()
    add_custom_target(translation "ALL" DEPENDS ${_mos})
endif()
