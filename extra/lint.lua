-- Simple Lint: check for unintended globals
--
-- One can spot all globals by checking the bytecode:
-- search for GSET and GGET instructions. Based on jit/bc.lua

local allow_globals = {
    _G           = true,
    _TARANTOOL   = true,
    assert       = true,
    bit          = true,
    box          = true,
    debug        = true,
    dostring     = true,
    error        = true,
    getmetatable = true,
    help         = true,
    io           = true,
    ipairs       = true,
    jit          = true,
    loadstring   = true,
    math         = true,
    next         = true,
    package      = true,
    pairs        = true,
    pcall        = true,
    print        = true,
    rawget       = true,
    rawset       = true,
    require      = true,
    select       = true,
    setmetatable = true,
    string       = true,
    table        = true,
    tonumber     = true,
    tostring     = true,
    tutorial     = true,
    type         = true,
    unpack       = true,
}

local jutil = require("jit.util")
local funcinfo, funcbc, funck = jutil.funcinfo, jutil.funcbc, jutil.funck
local band, shr = bit.band, bit.rshift
local sub, format = string.sub, string.format

-- GGET and GSET opcodes, numeric values different in v2.0 and v2.1
local function gget_gset() foo = bar end
local GGET = band(funcbc(gget_gset, 1), 0xff)
local GSET = band(funcbc(gget_gset, 2), 0xff)

local errors = 0

local function check(func, pc, op, gkey)
    if allow_globals[gkey] then return end
    local fi = funcinfo(func, pc)
    local li
    if sub(fi.source,1,1)=='@' then
        li = sub(fi.source, 2)
    else
        li = format('[string "%s"]', fi.source)
    end
    li = li..format(':%d', fi.currentline)
    print(format('%s: Suspicious use of a global variable: %s', li, gkey))
    errors = errors + 1
end

-- depending heavily on jit.util:
--  * funcinfo(fn) - self-explanatory;
--  * funck(fn, i) - get an item from constants table;
--                   i >= 0 integer constants,
--                   i <  0 object constants, including strings and
--                          prototypes for nested functions;
--  * funcbc(fn, i) - i-th bytecode instruction.
local function validate(func)
    local fi = funcinfo(func)
    if fi.children then
        for n=-1,-1000000000,-1 do
            local k = funck(func, n)
            if not k then break end
            if type(k) == "proto" then validate(k) end
        end
    end
    for pc=1,1000000000 do
        local ins = funcbc(func, pc)
        if not ins then return end
        local op = band(ins, 0xff)
        if op==GGET or op==GSET then
            local gkey = funck(func, -shr(ins, 16) - 1)
            check(func, pc, op, gkey)
        end
    end
end

validate(loadfile(arg[1])); os.exit(errors)
