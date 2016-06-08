--[[ for run tsl test you should run main from:
     <tarantool_path>/third_party/sqlite/build/testfixture.so
     int main(int argc, char *argv[]):
         param: argc: count of arguments = 2
         param: argv: array with two strings
                argv[0] -- path to tarantool src dir
                argv[1] -- path to tcl test file for run 
]]--

-- configuring tarantool
box.cfg {
    listen = os.getenv("LISTEN"),
    logger="tarantool.log",
    slab_alloc_arena=0.1,
}

require('console').listen(os.getenv('ADMIN'))

--requere ffi module
ffi = require('ffi')

-- loading textfixture - dynamic librarary for running tests
package.cpath = '../../third_party/sqlite/src/?.so;'..
                '../../third_party/sqlite/src/?.dylib;'..
                package.cpath
fixture = ffi.load(package.searchpath('libtestfixture', package.cpath))

-- define function
ffi.cdef('int main(int argc, char *argv[])')
