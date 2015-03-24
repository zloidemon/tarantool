-- init.lua -- internal file

-- Override pcall to support Tarantool exceptions

local ffi = require('ffi')
ffi.cdef[[
char *
tarantool_error_message(void);
double
tarantool_uptime(void);
typedef int32_t pid_t;
pid_t getpid(void);
]]

local pcall_lua = pcall

local function pcall_wrap(status, ...)
    if status == false and ... == 'C++ exception' then
        return false, ffi.string(ffi.C.tarantool_error_message())
    end
    return status, ...
end
pcall = function(fun, ...)
    return pcall_wrap(pcall_lua(fun, ...))
end

dostring = function(s, ...)
    local chunk, message = loadstring(s)
    if chunk == nil then
        error(message, 2)
    end
    return chunk(...)
end

local function uptime()
    return tonumber(ffi.C.tarantool_uptime());
end

local function pid()
    return tonumber(ffi.C.getpid())
end

function _(str)
    local gtx = {}
    local s, e = pcall(function() gtx = require('gettext') end)
    if gtx.gettext then
        gtx.bindtextdomain('tarantool')
        gtx.bind_textdomain_codeset('tarantool', 'utf-8')
        gtx.textdomain('tarantool')
        return gtx.gettext(str)
    end
    return str
end

return {
    uptime = uptime;
    pid = pid;
}
