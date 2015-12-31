-- errno.lua (internal file)

local ffi = require 'ffi'

ffi.cdef[[
    char *strerror(int errnum);
    int errno_get();
    int errno_set(int new_errno);
]]

local errno = require('errno')
errno.strerror = function(errno)
    if errno == nil then
        errno = ffi.C.errno_get()
    end
    return ffi.string(ffi.C.strerror(tonumber(errno)))
end

setmetatable(errno, {
    __newindex  = function() error("Can't create new errno constants") end,
    __call = function(self, new_errno)
        local res
        if new_errno then
            return ffi.C.errno_set(new_errno)
        end
        return ffi.C.errno_get()
    end
})

return errno
