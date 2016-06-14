#!/usr/bin/env tarantool

local ffi = require('ffi')
local console = require('console')

local tarantool_path = io.popen("which tarantool", "r"):read()

function get_test_name(path_to_file)
	local last_part = ''
	for w in path_to_file:gmatch('([^/]+)') do -- split by /
		last_part = w
	end
	local tmp = {}
	for part in last_part:gmatch('([^.]+)') do -- split by .
		table.insert(tmp, part)
	end
	local result = ''
	for i = 1, (table.getn(tmp)) do
		if i == 1 then
			result = tmp[i]
		else
			result = result .. '.' .. tmp[i]
		end
	end
	return result
end

--[[ for run tsl test you should run main from:
     <tarantool_path>/third_party/sqlite/build/testfixture.so
     int main(int argc, char *argv[]):
         param: argc: count of arguments = 2
         param: argv: array with two strings
                argv[0] -- path to tarantool src dir
                argv[1] -- path to tcl test file for run 
]]--

-- cleanup
os.execute("rm -f *.snap *.xlog*")

-- configuring tarantool
box.cfg {
    listen = os.getenv("LISTEN"),
    logger="tarantool.log",
    slab_alloc_arena=0.1,
}

console.listen(os.getenv('ADMIN'))

-- loading textfixture - dynamic librarary for running tests
package.cpath = '../../third_party/sqlite/src/?.so;'..
                '../../third_party/sqlite/src/?.dylib;'..
                package.cpath
fixture = ffi.load(package.searchpath('libtestfixture', package.cpath))

-- define function
ffi.cdef('int main(int argc, char *argv[])')

-- initialization params
local arg0 = ffi.cast('char *',  tarantool_path)
local arg1 = ffi.cast('char *', get_test_name(arg[1]))
local argv = ffi.new('char *[2]')
argv[0] = arg0
argv[1] = arg1

-- run main
os.exit(fixture.main(2, argv))
