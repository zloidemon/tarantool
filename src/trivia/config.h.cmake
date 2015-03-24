#ifndef TARANTOOL_CONFIG_H_INCLUDED
#define TARANTOOL_CONFIG_H_INCLUDED
/*
 * This file is generated by CMake. The original file is called
 * config.h.cmake. Please do not modify.
 */
/*
 * A string with major-minor-patch-commit-id identifier of the
 * release.
 */
#define PACKAGE_VERSION "@PACKAGE_VERSION@"
#define PACKAGE_VERSION_MAJOR @CPACK_PACKAGE_VERSION_MAJOR@
#define PACKAGE_VERSION_MINOR @CPACK_PACKAGE_VERSION_MINOR@
#define PACKAGE_VERSION_PATCH @CPACK_PACKAGE_VERSION_PATCH@

#define PACKAGE "@PACKAGE@"
/*  Defined if building for Linux */
#cmakedefine TARGET_OS_LINUX 1
/*  Defined if building for FreeBSD */
#cmakedefine TARGET_OS_FREEBSD 1
/*  Defined if building for Darwin */
#cmakedefine TARGET_OS_DARWIN 1
/*
 * Defined if gcov instrumentation should be enabled.
 */
#cmakedefine ENABLE_GCOV 1
/*
 * Defined if configured with ENABLE_TRACE (debug trace into
 * a file specified by TARANTOOL_TRACE environment variable.
 */
#cmakedefine ENABLE_TRACE 1
/*
 * Defined if configured with ENABLE_BACKTRACE ('show fiber'
 * showing fiber call stack.
 */
#cmakedefine ENABLE_BACKTRACE 1
/*
 * Set if the system has bfd.h header and GNU bfd library.
 */
#cmakedefine HAVE_BFD 1
#cmakedefine HAVE_MAP_ANON 1
#cmakedefine HAVE_MAP_ANONYMOUS 1
#if !defined(HAVE_MAP_ANONYMOUS) && defined(HAVE_MAP_ANON)
/*
 * MAP_ANON is deprecated, MAP_ANONYMOUS should be used instead.
 * Unfortunately, it's not universally present (e.g. not present
 * on FreeBSD.
 */
#define MAP_ANONYMOUS MAP_ANON
#endif
/*
 * Defined if O_DSYNC mode exists for open(2).
 */
#cmakedefine HAVE_O_DSYNC 1
#if defined(HAVE_O_DSYNC)
    #define WAL_SYNC_FLAG O_DSYNC
#else
    #define WAL_SYNC_FLAG O_SYNC
#endif
/*
 * Defined if fdatasync(2) call is present.
 */
#cmakedefine HAVE_FDATASYNC 1

#ifndef HAVE_FDATASYNC
	#define fdatasync fsync
#endif

/*
 * Defined if this platform has BSD specific funopen()
 */
#cmakedefine HAVE_FUNOPEN 1

/*
 * Defined if this platform has GNU specific fopencookie()
 */
#cmakedefine HAVE_FOPENCOOKIE 1

/*
 * Defined if this platform has GNU specific memmem().
 */
#cmakedefine HAVE_MEMMEM 1
/*
 * Defined if this platform has GNU specific memrchr().
 */
#cmakedefine HAVE_MEMRCHR 1
/*
 * Defined if this platform has sendfile(..).
 */
#cmakedefine HAVE_SENDFILE 1
/*
 * Defined if this platform has Linux specific sendfile(..).
 */
#cmakedefine HAVE_SENDFILE_LINUX 1
/*
 * Defined if this platform has BSD specific sendfile(..).
 */
#cmakedefine HAVE_SENDFILE_BSD 1
/*
 * Set if this is a GNU system and libc has __libc_stack_end.
 */
#cmakedefine HAVE_LIBC_STACK_END 1
/*
 * Defined if this is a big-endian system.
 */
#cmakedefine HAVE_BYTE_ORDER_BIG_ENDIAN 1
/*
 * Defined if this platform supports openmp and it is enabled
 */
#cmakedefine HAVE_OPENMP 1
/*
*  Defined if compatible with GNU readline installed.
*/
#cmakedefine HAVE_GNU_READLINE 1
/*
*  Defined if this platform has gettext.
*/
#cmakedefine HAVE_GETTEXT 1

/*
 * Set if compiler has __builtin_XXX methods.
 */
#cmakedefine HAVE_BUILTIN_CTZ 1
#cmakedefine HAVE_BUILTIN_CTZLL 1
#cmakedefine HAVE_BUILTIN_CLZ 1
#cmakedefine HAVE_BUILTIN_CLZLL 1
#cmakedefine HAVE_BUILTIN_POPCOUNT 1
#cmakedefine HAVE_BUILTIN_POPCOUNTLL 1
#cmakedefine HAVE_BUILTIN_BSWAP32 1
#cmakedefine HAVE_BUILTIN_BSWAP64 1
#cmakedefine HAVE_FFSL 1
#cmakedefine HAVE_FFSLL 1

/*
 * pthread have problems with -std=c99
 */
#cmakedefine HAVE_NON_C99_PTHREAD_H 1

#cmakedefine ENABLE_BUNDLED_LIBEV 1
#cmakedefine ENABLE_BUNDLED_LIBEIO 1
#cmakedefine ENABLE_BUNDLED_LIBCORO 1

#cmakedefine HAVE_PTHREAD_YIELD 1
#cmakedefine HAVE_SCHED_YIELD 1

#cmakedefine HAVE_PRCTL_H 1

#cmakedefine HAVE_OPEN_MEMSTREAM 1
#cmakedefine HAVE_FMEMOPEN 1

#cmakedefine HAVE_UUIDGEN 1

/*
 * predefined /etc directory prefix.
 */
#define SYSCONF_DIR "@CMAKE_INSTALL_SYSCONFDIR@"
#define INSTALL_PREFIX "@CMAKE_INSTALL_PREFIX@"
#define BUILD_TYPE "@CMAKE_BUILD_TYPE@"
#define BUILD_INFO "@TARANTOOL_BUILD@"
#define BUILD_OPTIONS "cmake . @TARANTOOL_OPTIONS@"
#define COMPILER_INFO "@CMAKE_C_COMPILER@ @CMAKE_CXX_COMPILER@"
#define TARANTOOL_C_FLAGS "@TARANTOOL_C_FLAGS@"
#define TARANTOOL_CXX_FLAGS "@TARANTOOL_CXX_FLAGS@"

/*
 * Modules
 */
#define MODULE_LIBDIR "@MODULE_FULL_LIBDIR@"
#define MODULE_LUADIR "@MODULE_FULL_LUADIR@"
#define MODULE_INCLUDEDIR "@MODULE_FULL_INCLUDEDIR@"
#define MODULE_LUAPATH "@MODULE_LUAPATH@"
#define MODULE_LIBPATH "@MODULE_LIBPATH@"

#define DEFAULT_CFG_FILENAME "tarantool.cfg"
#define DEFAULT_CFG SYSCONF_DIR "/" DEFAULT_CFG_FILENAME

/*
 * vim: syntax=c
 */
#endif /* TARANTOOL_CONFIG_H_INCLUDED */
