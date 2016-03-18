#!/usr/bin/env tarantool

tarantool_path = io.popen("which tarantool", "r"):read()

require("top")

-- init space test1
if box.space.test1 then
    box.space.test1:drop()
end
-- box.schema.space.create('test1')
-- box.space.test1:create_index('primary', {parts={1, 'NUM'}, type='TREE'})
-- format = {}
-- format[1] = {name='id', type='num'}
-- box.space.test1:format(format)
-- for i = 1, 3 do
--     box.space.test1:insert({i})
-- end

-- init space test2
if box.space.test2 then
    box.space.test2:drop()
end
-- box.schema.space.create('test2')
-- box.space.test2:create_index('primary', {parts={1, 'NUM'}, type='TREE'})
-- box.space.test2:create_index('secondary', {parts={1, 'NUM', 2, 'STR'}, type='TREE'})
-- box.space.test2:create_index('third', {parts={3, 'STR', 4, 'NUM'}, type='TREE'})
-- box.space.test2:create_index('fourth', {parts={6, 'NUM'}, type='TREE'})
-- format = {}
-- format[1] = {name='id', type='num'}
-- format[2] = {name='name', type='str'}
-- format[3] = {name='surname', type='str'}
-- format[4] = {name='bar', type='num'}
-- format[5] = {name='foo', type='num'}
-- format[6] = {name='qwerty', type='num'}
-- box.space.test2:format(format)
-- box.space.test2:insert({1, 'Vlad', 'Shpilevoy', 100, 200, 300})
-- box.space.test2:insert({2, 'Ivan', 'Petrov', 200, 300, 400})
-- box.space.test2:insert({3, 'Maria', 'Popova', 300, 400, 500})
-- box.space.test2:insert({4, 'Albert', 'Sukaev', 400, 500, 600})
-- box.space.test2:insert({5, 'Ksenia', 'Ivanova', 100, 200, 700})
-- box.space.test2:insert({6, 'Brian', 'Hankok', 200, 300, 800})


-- init space test3
if box.space.test3 then
    box.space.test3:drop()
end
-- box.schema.space.create('test3')
-- box.space.test3:create_index('primary', {parts={1, 'NUM'}, type='TREE'})
-- box.space.test3:create_index('secondary', {parts={1, 'NUM', 2, 'STR'}, type='TREE'})
-- box.space.test3:create_index('third', {parts={3, 'STR', 4, 'NUM'}, type='TREE'})
-- box.space.test3:create_index('fourth', {parts={6, 'NUM'}, type='TREE'})
-- format = {}
-- format[1] = {name='id', type='num'}
-- format[2] = {name='name', type='str'}
-- format[3] = {name='surname', type='str'}
-- format[4] = {name='bar', type='num'}
-- format[5] = {name='foo', type='num'}
-- format[6] = {name='qwerty', type='num'}
-- box.space.test3:format(format)
-- box.space.test3:insert({1, 'Vlad', 'Shpilevoy', 100, 200, 300})
-- box.space.test3:insert({2, 'Ivan', 'Petrov', 200, 300, 400})
-- box.space.test3:insert({3, 'Maria', 'Popova', 300, 400, 500})
-- box.space.test3:insert({4, 'Albert', 'Sukaev', 400, 500, 600})
-- box.space.test3:insert({5, 'Ksenia', 'Ivanova', 100, 200, 700})
-- box.space.test3:insert({6, 'Brian', 'Hankok', 200, 300, 800})


-- init space test4:
if box.space.test4 then
    box.space.test4:drop()
end
-- box.schema.space.create('test4')
-- box.space.test4:create_index('primary', {parts={1, 'NUM'}, type='TREE'})
-- box.space.test4:create_index('secondary', {parts={1, 'NUM', 2, 'STR'}, type='TREE'})
-- box.space.test4:create_index('third', {parts={3, 'STR', 4, 'NUM'}, type='TREE'})
-- box.space.test4:create_index('fourth', {parts={6, 'NUM'}, type='TREE'})
-- format = {}
-- format[1] = {name='id', type='num'}
-- format[2] = {name='name', type='str'}
-- format[3] = {name='surname', type='str'}
-- format[4] = {name='bar', type='num'}
-- format[5] = {name='foo', type='num'}
-- format[6] = {name='qwerty', type='num'}
-- box.space.test4:format(format)
-- box.space.test4:insert({1, 'Vlad', 'Shpilevoy', 100, 200, 300})
-- box.space.test4:insert({2, 'Ivan', 'Petrov', 200, 300, 400})
-- box.space.test4:insert({3, 'Maria', 'Popova', 300, 400, 500})
-- box.space.test4:insert({4, 'Albert', 'Sukaev', 400, 500, 600})
-- box.space.test4:insert({5, 'Ksenia', 'Ivanova', 100, 200, 700})
-- box.space.test4:insert({6, 'Brian', 'Hankok', 200, 300, 800})


-- init space test5:
if box.space.test5 then
    box.space.test5:drop()
end
-- box.schema.space.create('test5')
-- box.space.test5:create_index('primary', {parts={1, 'NUM'}, type='TREE'})
-- box.space.test5:create_index('secondary', {parts={1, 'NUM', 2, 'STR'}, type='TREE'})
-- box.space.test5:create_index('third', {parts={3, 'STR', 4, 'NUM'}, type='TREE'})
-- box.space.test5:create_index('fourth', {parts={6, 'NUM'}, type='TREE'})
-- format = {}
-- format[1] = {name='id', type='num'}
-- format[2] = {name='name', type='str'}
-- format[3] = {name='surname', type='str'}
-- format[4] = {name='bar', type='num'}
-- format[5] = {name='foo', type='num'}
-- format[6] = {name='qwerty', type='num'}
-- box.space.test5:format(format)
-- box.space.test5:insert({1, 'Vlad', 'Shpilevoy', 100, 200, 300})
-- box.space.test5:insert({2, 'Ivan', 'Petrov', 200, 300, 400})
-- box.space.test5:insert({3, 'Maria', 'Popova', 300, 400, 500})
-- box.space.test5:insert({4, 'Albert', 'Sukaev', 400, 500, 600})
-- box.space.test5:insert({5, 'Ksenia', 'Ivanova', 100, 200, 700})
-- box.space.test5:insert({6, 'Brian', 'Hankok', 200, 300, 800})

-- initialization params
local arg0 = ffi.cast('char *',  tarantool_path)
local arg1 = ffi.cast('char *', "./delete1.test")
local argv = ffi.new('char *[2]')
argv[0] = arg0
argv[1] = arg1

-- run main
fixture.main(2, argv)

os.exit(0)