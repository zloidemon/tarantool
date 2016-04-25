local ffi = require('ffi')
local buffer = require('buffer')

local builtin     = ffi.C
local MAXNESTING  = 16
local table_new   = require('table.new')
local table_clear = require('table.clear')

ffi.cdef[[
    char       *mp_encode_nil    (char *data);
    void        mp_decode_nil    (const char **data);

    bool        mp_decode_bool   (const char **data);
    char       *mp_encode_bool   (char *data, bool val);

    char       *mp_encode_int    (const char *data, int64_t val);
    char       *mp_encode_uint   (const char *data, uint64_t val);
    uint32_t    mp_sizeof_int    (int64_t val);
    uint32_t    mp_sizeof_uint   (uint64_t val);
    int64_t     mp_decode_int    (const char **data);
    uint64_t    mp_decode_uint   (const char **data);

    char       *mp_encode_str    (char *data, const char *str, uint32_t len);
    char       *mp_encode_strl   (char *data, uint32_t len);
    uint32_t    mp_sizeof_str    (uint32_t len);
    uint32_t    mp_sizeof_strl   (uint32_t len);
    const char *mp_decode_str    (char **data, uint32_t *len);
    uint32_t    mp_decode_strl   (char **data);

    char       *mp_encode_float  (char *data, float num);
    char       *mp_encode_double (char *data, double num);
    float       mp_decode_float  (const char **data);
    double      mp_decode_double (const char **data);

    char       *mp_encode_map    (char *data, uint32_t size);
    char       *mp_encode_array  (char *data, uint32_t size);
    uint32_t    mp_sizeof_map    (uint32_t size);
    uint32_t    mp_sizeof_array  (uint32_t size);
    uint32_t    mp_decode_map    (const char **data);
    uint32_t    mp_decode_array  (const char **data);
]]

local function get_map_length(obj)
    local len = 0
    for k, v in pairs(obj) do
        len = len + 1
    end
    return len
end

--[[------------------------------------------------------------------------]]--
--[[--                      Common encode functions                       --]]--
--[[--                     ('msgpuck' or 'internal')                      --]]--
--[[------------------------------------------------------------------------]]--

-- common
local encode_float   = nil
local encode_double  = nil
local encode_number  = nil

-- uncommon
local encode_nil     = nil
local encode_boolean = nil
local encode_int     = nil
local encode_uint    = nil
local encode_string  = nil
local encode_arrayl  = nil
local encode_mapl    = nil

do
    encode_float   = function(buf, obj)
        local p = buf:alloc(5)
        builtin.mp_encode_float(p, obj)
    end
    encode_double  = function(buf, obj)
        local p = buf:alloc(9)
        builtin.mp_encode_double(p, obj)
    end
    encode_number  = function(buf, obj)
        if obj >= 0 then
            encode_uint(buf, obj)
        else
            encode_int(buf, obj)
        end
    end
end

local ENCODER = 'msgpuck'

if ENCODER == 'msgpuck' then
    encode_nil     = function(buf)
        local p = buf:alloc(1)
        builtin.mp_encode_nil(p)
    end
    encode_boolean = function(buf, val)
        local p  = buf:alloc(1)
        builtin.mp_encode_bool(p, val and 1 or 0)
    end
    encode_int     = function(buf, val)
        local p  = buf:reserve(9)
        buf.wpos = builtin.mp_encode_int(p, val)
    end
    encode_uint    = function(buf, val)
        local p  = buf:reserve(9)
        buf.wpos = builtin.mp_encode_uint(p, val)
    end
    encode_string  = function(buf, val)
        local string_sz = #val
        local p  = buf:reserve(5 + string_sz)
        buf.wpos = builtin.mp_encode_str(p, val, string_sz)
    end
    encode_arrayl  = function(buf, val)
        local array_sz = #val
        local p  = buf:reserve(5)
        buf.wpos = builtin.mp_encode_array(p, array_sz)
    end
    -- good bye JIT
    encode_mapl    = function(buf, val)
        local map_sz = get_map_length(val)
        local p  = buf:reserve(5)
        buf.wpos = builtin.mp_encode_map(p, map_sz)
    end
elseif ENCODER == 'internal' then
    local int8_ptr_t       = ffi.typeof('int8_t *')
    local uint8_ptr_t      = ffi.typeof('uint8_t *')
    local uint16_ptr_t     = ffi.typeof('uint16_t *')
    local uint32_ptr_t     = ffi.typeof('uint32_t *')
    local uint64_ptr_t     = ffi.typeof('uint64_t *')
    local const_char_ptr_t = ffi.typeof('const char *')

    local strict_alignment = (jit.arch == 'arm')

    local tmpint
    if strict_alignment then
    tmpint = ffi.new('union tmpint[1]')
    end

    local function bswap_u16(num)
        return bit.rshift(bit.bswap(tonumber(num)), 16)
    end

    local function encode_fix(buf, code, num)
        local p = buf:alloc(1)
        p[0] = bit.bor(code, tonumber(num))
    end

    local function encode_u8(buf, code, num)
        local p = buf:alloc(2); p[0] = code
        ffi.cast(uint8_ptr_t, p + 1)[0] = num
    end

    local encode_u16
    if strict_alignment then
        encode_u16 = function(buf, code, num)
            tmpint[0].u16 = bswap_u16(num)
            local p = buf:alloc(3); p[0] = code
            ffi.copy(p + 1, tmpint, 2)
        end
    else
        encode_u16 = function(buf, code, num)
            local p = buf:alloc(3); p[0] = code
            ffi.cast(uint16_ptr_t, p + 1)[0] = bswap_u16(num)
        end
    end

    local encode_u32
    if strict_alignment then
        encode_u32 = function(buf, code, num)
            tmpint[0].u32 = ffi.cast('uint32_t', bit.bswap(tonumber(num)))
            local p = buf:alloc(5); p[0] = code
            ffi.copy(p + 1, tmpint, 4)
        end
    else
        encode_u32 = function(buf, code, num)
            local p = buf:alloc(5); p[0] = code
            ffi.cast(uint32_ptr_t, p + 1)[0] = ffi.cast('uint32_t', bit.bswap(tonumber(num)))
        end
    end

    local encode_u64
    if strict_alignment then
        encode_u64 = function(buf, code, num)
            tmpint[0].u64 = bit.bswap(ffi.cast('uint64_t', num))
            local p = buf:alloc(9); p[0] = code
            ffi.copy(p + 1, tmpint, 8)
        end
    else
        encode_u64 = function(buf, code, num)
            local p = buf:alloc(9); p[0] = code
            ffi.cast(uint64_ptr_t, p + 1)[0] = bit.bswap(ffi.cast('uint64_t', num))
        end
    end

    encode_nil     = function(buf)
        local p = buf:alloc(1)
        p[0] = 0xc0
    end
    encode_boolean = function(buf, val)
        encode_fix(buf, 0xc2, val and 1 or 0)
    end
    encode_int     = function(buf, val)
        if val >= -0x20 then
            encode_fix(buf, 0xe0, val)
        elseif val >= -0x80 then
            encode_u8(buf, 0xd0, val)
        elseif val >= -0x8000 then
            encode_u16(buf, 0xd1, val)
        elseif val >= -0x80000000 then
            encode_u32(buf, 0xd2, val)
        else
            encode_u64(buf, 0xd3, 0LL + val)
        end
    end
    encode_uint    = function(buf, val)
        if val <= 0x7f then
            encode_fix(buf, 0, val)
        elseif val <= 0xff then
            encode_u8(buf, 0xcc, val)
        elseif val <= 0xffff then
            encode_u16(buf, 0xcd, val)
        elseif val <= 0xffffffff then
            encode_u32(buf, 0xce, val)
        else
            encode_u64(buf, 0xcf, 0ULL + val)
        end
    end
    encode_string  = function(buf, val)
        local len = #val
        buf:reserve(5 + len)
        if len <= 31 then
            encode_fix(buf, 0xa0, len)
        elseif len <= 0xff then
            encode_u8(buf, 0xd9, len)
        elseif len <= 0xffff then
            encode_u16(buf, 0xda, len)
        else
            encode_u32(buf, 0xdb, len)
        end
        local p = buf:alloc(len)
        ffi.copy(p, val, len)
    end
    encode_arrayl  = function(buf, val)
        local array_sz = #val
        if array_sz <= 0xf then
            encode_fix(buf, 0x90, array_sz)
        elseif array_sz <= 0xffff then
            encode_u16(buf, 0xdc, array_sz)
        else
            encode_u32(buf, 0xdd, array_sz)
        end
    end
    -- good bye JIT
    encode_mapl    = function(buf, val)
        local map_sz = get_map_length(val)
        if map_sz <= 0xf then
            encode_fix(buf, 0x80, map_sz)
        elseif map_sz <= 0xffff then
            encode_u16(buf, 0xde, map_sz)
        else
            encode_u32(buf, 0xdf, map_sz)
        end
    end
else
    assert(false, 'bad encoder type, expected "msgpack" or "internal"')
end

--[[------------------------------------------------------------------------]]--
--[[--                       Encoding state machine                       --]]--
--[[------------------------------------------------------------------------]]--

local type_to_state = {}

type_to_state['nil']     = 2
type_to_state['boolean'] = 3
type_to_state['number']  = 4 -- 4-(double)->8, 4-(uint)->10, 4-(int)->11
type_to_state['string']  = 5
type_to_state['table']   = 6 -- 6-(array)->13->15, 6-(map)->14->16
type_to_state['cdata']   = 7

local encode_ext_cdata = {}

-- Set trigger that called when encoding cdata
local function on_encode(ctype_or_udataname, callback)
    if type(ctype_or_udataname) ~= "cdata" or type(callback) ~= "function" then
        error("Usage: on_encode(ffi.typeof('mytype'), function(buf, obj)")
    end
    local ctypeid = tonumber(ffi.typeof(ctype_or_udataname))
    local prev = encode_ext_cdata[ctypeid]
    encode_ext_cdata[ctypeid] = callback
    return prev
end

--[[
on_encode(ffi.typeof('uint8_t'),             encode_int)
on_encode(ffi.typeof('uint16_t'),            encode_int)
on_encode(ffi.typeof('uint32_t'),            encode_int)
on_encode(ffi.typeof('uint64_t'),            encode_int)
on_encode(ffi.typeof('int8_t'),              encode_int)
on_encode(ffi.typeof('int16_t'),             encode_int)
on_encode(ffi.typeof('int32_t'),             encode_int)
on_encode(ffi.typeof('int64_t'),             encode_int)
on_encode(ffi.typeof('char'),                encode_int)
on_encode(ffi.typeof('const char'),          encode_int)
on_encode(ffi.typeof('unsigned char'),       encode_int)
on_encode(ffi.typeof('const unsigned char'), encode_int)
on_encode(ffi.typeof('bool'),                encode_bool_cdata)
on_encode(ffi.typeof('float'),               encode_float)
on_encode(ffi.typeof('double'),              encode_double)
]]--

local encode_r_jit_table = table_new(MAXNESTING, 0)
for i = 1, MAXNESTING, 1 do
    -- first place is for object
    -- second place is for index (iterator state for next)
    encode_r_jit_table[i] = table_new(4, 0)
end

-- `next` isn't JIT'ted, as workaround for this - we must write this function
-- in LuaJIT internal API (Force JIT not to be aborted)
local function encode_r_jit(buf, obj)
    local dyn = encode_r_jit_table
    local callback, state, depth = nil, 1, 1
    local value_obj = nil
    -- temporary storage for current iteration

    for i = 1, 1000000, 1 do
        while true do
            -- we can't understand object type
            if state == nil then
                local tobj = type(obj) == 'cdata' and ffi.typeof(tobj) or type(obj)
                error(string.format('bad object type for encoding "%s"', tobj))
            -- get state by type
            elseif state == 1 then
                state = type_to_state[type(obj)]
            -- nil stub
            elseif state == 2 then
                encode_nil(buf)
                state = 18
            -- boolean
            elseif state == 3 then
                encode_boolean(buf, obj)
                state = 18
            -- number type: if it is 'double', then goto 8 else execute 9
            elseif state == 4 then
                if (obj % 1 == 0 and (obj > -1e63 and obj < 1e64)) then
                    if obj >= 0 then
                        state = 10
                    else
                        state = 11
                    end
                else
                    state = 8
                end
            -- string
            elseif state == 5 then
                encode_string(buf, obj)
                state = 18
            -- table (dead end, we must understand, is it array or map)
            elseif state == 6 then
                local array_sz = #obj
                -- get serialization hints
                local table_mt_serialize = getmetatable(obj)
                table_mt_serialize = (type(table_mt_serialize) == 'table' and
                                      table_mt_serialize.__serialize or nil)

                -- hints
                local is_map   = tostring(table_mt_serialize) == 'map' or
                                tostring(table_mt_serialize) == 'mapping'

                local is_array = tostring(table_mt_serialize) == 'array' or
                                tostring(table_mt_serialize) == 'seq' or
                                tostring(table_mt_serialize) == 'sequence'

                -- case, when hints are not defined and len(array) > 0
                is_array = is_array or (not is_map and #obj > 0)

                state = is_array and 13 or 14
            -- cdata, get callback and execute it
            elseif state == 7 then
                callback = encode_ext_data[ffi.typeof(state)]
                -- if callback is nil, then goto error
                if --[[ likely ]] callback then
                    callback()
                    callback = nil
                    state = 18
                else
                    state = nil
                end
            -- double
            elseif state == 8 then
                encode_double(buf, obj)
                state = 18
            -- uint
            elseif state == 10 then
                encode_uint(buf, obj)
                state = 18
            -- int
            elseif state == 11 then
                encode_int(buf, obj)
                state = 18
            -- array
            elseif state == 13 then
                local carr = dyn[depth]
                carr[1] = obj
                carr[2] = 0
                carr[3] = 15
                carr[4] = #obj
                carr[5] = false
                encode_arrayl(buf, obj)
                state = 15
                depth = depth + 1
            -- map (breaks JIT'ting)
            elseif state == 14 then
                local cmap = dyn[depth]
                cmap[1] = obj
                cmap[2] = nil
                cmap[3] = 16
                cmap[4] = nil
                cmap[5] = false
                encode_mapl(buf, obj)
                state = 16
                depth = depth + 1
            -- next step with array
            elseif state == 15 then
                local carr = dyn[depth - 1]
                local pos  = carr[2] + 1
                if carr[4] + 1 == pos then
                    carr[3] = 18
                    carr[5] = true
                    state   = 18
                else
                    carr[2] = pos
                    obj     = carr[1][pos]
                    state   = 1
                end
            -- next key step with map (breaks JIT'ting)
            elseif state == 16 then
                local cmap = dyn[depth - 1]
                local pos  = cmap[2]
                obj, cmap[4] = next(cmap[1], pos)
                if obj == nil then
                    state   = 18
                    cmap[3] = 18
                    cmap[5] = true
                else
                    state   = 1
                    cmap[2] = obj
                    cmap[3] = 19
                end
            -- next value step with map
            elseif state == 19 then
                local cmap = dyn[depth - 1]
                local pos  = cmap[2]
                obj     = cmap[4]
                cmap[3] = 16
                state   = 1
            -- we're done
            elseif state == 17 then
                -- cleanup table of state
                for i = 1, MAXNESTING, 1 do
                    table_clear(encode_r_jit_table[i])
                end
                return
            -- return to previous state
            elseif state == 18 then
                local celm = dyn[depth - 1]
                if celm and celm[5] then
                    depth = depth - 1
                end
                if celm then
                    state = celm[3]
                else
                    state = 17
                end
            else
                error('unknown state')
            end
        end
    end
end

local function encode_jit(obj)
    local tmpbuf = buffer.IBUF_SHARED
    tmpbuf:reset()
    encode_r_jit(tmpbuf, obj, 0)
    local sz = tonumber(tmpbuf.wpos - tmpbuf.rpos)
    local p = tmpbuf.rpos
    local r = ffi.string(p, sz)
    tmpbuf:recycle()
    return r
end

return {
    encode_r = encode_r_jit,
    encode = encode_jit
}
