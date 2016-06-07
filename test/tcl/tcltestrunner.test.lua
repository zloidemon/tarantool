#!/usr/bin/env tarantool

tarantool_path = io.popen("which tarantool", "r"):read()

require("top")

-- initialization params
local arg0 = ffi.cast('char *',  tarantool_path)
local arg1 = ffi.cast('char *', "./select6.sqlite.test")
local argv = ffi.new('char *[2]')
argv[0] = arg0
argv[1] = arg1

-- run main
fixture.main(2, argv)

os.exit(0)
