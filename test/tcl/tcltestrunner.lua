#!/usr/bin/env tarantool

os.execute("rm -f *.snap *.xlog*")

tarantool_path = io.popen("which tarantool", "r"):read()

require("top")

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

-- initialization params
local arg0 = ffi.cast('char *',  tarantool_path)
local arg1 = ffi.cast('char *', get_test_name(arg[1]))
local argv = ffi.new('char *[2]')
argv[0] = arg0
argv[1] = arg1

-- run main
fixture.main(2, argv)
print('here!!')

os.exit(0)
