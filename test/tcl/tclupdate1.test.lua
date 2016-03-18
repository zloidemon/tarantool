#!/usr/bin/env tarantool

tarantool_path = io.popen("which tarantool", "r"):read()

require("top")

-- init space test1
if box.space.test1 then
    box.space.test1:drop()
end

-- init space test2
if box.space.test2 then
    box.space.test2:drop()
end

-- init space test3
if box.space.test3 then
    box.space.test3:drop()
end

-- init space test4
if box.space.test4 then
    box.space.test4:drop()
end

-- initialization params
local arg0 = ffi.cast('char *',  tarantool_path)
local arg1 = ffi.cast('char *', "./update1.test")
local argv = ffi.new('char *[2]')
argv[0] = arg0
argv[1] = arg1

-- run main
fixture.main(2, argv)

os.exit(0)