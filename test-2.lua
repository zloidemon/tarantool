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

-- local mf = require('msgpackffi_new')
local mf = require('msgpackffi')

-- local bc = require('jit.bc')
-- require('jit.v').on('test.out-2')
-- require('jit.dump').on('tbimXa', 'test.out-2')
require('jit.dump').on(nil, 'test.out-2')
-- require('jit.opt').start(3)

function example()
    for i = 1, 1000000 do
        hex_dump(mf.encode('string'))
        hex_dump(mf.encode(2.5))
        hex_dump(mf.encode({'string', 'string', 'string', {'string', 'string', 'string'}}))
--        hex_dump(mf.encode({key = 'value', val = 'value2'}))
        hex_dump(mf.encode(2))
        hex_dump(mf.encode(-2))
    end
end

example()

-- hex_dump(mf.encode({'string', 'string', 'string'}))
