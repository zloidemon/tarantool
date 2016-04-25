--[[
function hex_dump(buf)
    for byte = 1, #buf, 16 do
        local chunk = buf:sub(byte, byte + 15)
        io.write(string.format('%08X  ', byte - 1))
        chunk:gsub('.', function (c)
            io.write(string.format('%02X ', string.byte(c)))
        end)
        io.write(string.rep(' ', 3 * (16 - #chunk)))
        io.write(' ', chunk:gsub('%c','.'), "\n")
    end
end
]]--

function hex_dump(buf)
    return buf
end

local function random(x, y)
    while true do
        if x == nil then
            local rv = math.random()
            if rv >= 0 and rv <= 1 then
                return rv
            end
        elseif y == nil then
            local rv = math.random(x)
            if rv >= 1 and rv <= x then
                return rv
            end
        else
            local rv = math.random(x, y)
            if rv >= x and rv <= y then
                return rv
            end
        end
    end
end

local mf_new = require('msgpackffi_new')
local mf = require('msgpackffi')

-- local bc = require('jit.bc')
-- require('jit.dump').on('tbimXa', 'test.out-1')
-- require('jit.opt').start(3)

local random_types = {}

local function random_boolean()
    return (random(0, 1) == 0)
end

local function random_pos_int()
    return random(0, 1000000000)
end

local function random_int()
    return random(-1000000000, 1000000000)
end

local function random_float()
    return random(-1000000000, 1000000000) * random()
end

local function random_string()
    local len = random(10, 200)
    local out = {}
    for k = 1, len, 1 do
        table.insert(out, k)
    end
    return string.char(unpack(out))
end

local function random_table(depth)
    depth = depth or 1
    if depth > 10 then
        return nil
    end
    local out = {}
    local len = random(1, 20)
    for k = 1, len, 1 do
        table.insert(out, random_types[random(#random_types)](depth + 1))
    end
    return out
end

local function random_map(depth)
    depth = depth or 1
    if depth > 10 then
        return nil
    end
    local out = {}
    local len = random(1, 5)
    for k = 1, len, 1 do
        out[random_string()] = random_types[random(#random_types)](depth + 1)
    end
    return out
end

local function generate_number_table(len)
    local out = {}
    for k = 1, len, 1 do
        table.insert(out, {random_pos_int(), random_pos_int()})
    end
    return out
end

local function generate_table()
    return {
        random_pos_int(),
        random_pos_int(),
        random_pos_int(),
        random_pos_int(),
        random_pos_int(),
        generate_number_table(math.random(10, 100))
    }
end

table.insert(random_types, random_boolean)
table.insert(random_types, random_boolean)
table.insert(random_types, random_int)
table.insert(random_types, random_int)
table.insert(random_types, random_int)
table.insert(random_types, random_int)
table.insert(random_types, random_int)
table.insert(random_types, random_int)
table.insert(random_types, random_float)
table.insert(random_types, random_float)
table.insert(random_types, random_float)
table.insert(random_types, random_string)
table.insert(random_types, random_string)
table.insert(random_types, random_table)
table.insert(random_types, random_map)

local function timeit(func, ...)
    local time = os.clock()
    func(...)
    return os.clock() - time
end

function example()
    local check = {}
    local function construct()
        for i = 1, 100000 do
            local rv = random_table()
            -- print(require('json').encode(rv))
            -- local rv = generate_table()
            table.insert(check, rv)
        end
    end
    local function test_1()
        for i = 1, 400000 do
            mf_new.encode(check[(i % 100000) + 1])
        end
    end
    local function test_2()
        for i = 1, 400000 do
            mf.encode(check[(i % 100000) + 1])
        end
    end
    print('constructing: ', timeit(construct))
    -- require('jit.p').start()
    require('jit.v').on('test.out-1')
    -- require('jit.dump').on(nil, 'test.out-1')
    print('testing new: ',      timeit(test_1))
    print('testing new: ',      timeit(test_1))
    print('testing new: ',      timeit(test_1))
    print('testing new: ',      timeit(test_1))
    print('testing new: ',      timeit(test_1))
    print('testing new: ',      timeit(test_1))
    -- require('jit.p').stop()
    require('jit.v').off()
    -- require('jit.v').on('test.out-2')
    -- require('jit.dump').on(nil, 'test.out-1')
    print('testing old: ',      timeit(test_2))
    print('testing old: ',      timeit(test_2))
    print('testing old: ',      timeit(test_2))
    print('testing old: ',      timeit(test_2))
    print('testing old: ',      timeit(test_2))
    print('testing old: ',      timeit(test_2))
    -- require('jit.v').off()
--[[
    for i = 1, 1000000 do
        hex_dump(mf_new.encode('string'))
        hex_dump(mf_new.encode(2.5))
        hex_dump(mf_new.encode({'string', 'string', 'string', {'string', 'string', 'string'}}))
--        hex_dump(mf_new.encode({key = 'value', val = 'value2'}))
        hex_dump(mf_new.encode(2))
        hex_dump(mf_new.encode(-2))
    end
]]--
end

example()

-- hex_dump(mf_new.encode({'string', 'string', 'string'}))
