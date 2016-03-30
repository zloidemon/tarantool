local ffi      = require('ffi')
local buffer   = require('buffer')
local imxlib   = require('imxlib')
local socket   = require('socket')
local fiber    = require('fiber')
local msgpack  = require('msgpack')
local digest   = require('digest')
local errno    = require('errno')
local internal -- XXX please fix the loader

local band          = bit.band
local match, find   = string.match, string.find
local gfind         = string.gfind
local sub, gsub     = string.sub, string.gsub
local uuid_pattern  = gsub('^qq%-q%-q%-q%-qqq$', 'q', '%%x%%x%%x%%x')
local base64_decode = digest.base64_decode
local ibuf_decode   = msgpack.ibuf_decode
local encode_auth
local encode_select
local encode_eval
local parse_response

--
-- A contrived way to define 'real' constants.
-- Mike Paul's-approved.
--
ffi.cdef([[
    struct tarantool_iproto_constants
    {
        static const int STATUS_KEY    = 0x00;
        static const int SYNC_KEY      = 0x01;
        static const int SCHEMA_ID_KEY = 0x05;
        static const int DATA_KEY      = 0x30;
        static const int ERROR_KEY     = 0x31;

        static const int STATUS_MASK   = 0x7FFF;

        static const int GREETING_SIZE = 128;

        static const int VSPACE_ID     = 281;
        static const int VINDEX_ID     = 289;
    };
]])
local iproto = ffi.new('struct tarantool_iproto_constants')

-- select errors from box.error
local E_UNKNOWN              = 0
local E_NO_CONNECTION        = 77
local E_TIMEOUT              = 78
local E_WRONG_SCHEMA_VERSION = 109

local function greeting_decode(greating)
    local ok, res = pcall(function()
        local li = gfind(greating, '[^\n]+')
        local line1, line2 = li(), li()
        local ps, pe = find(line1, '%b()')
        local before_proto, proto, after_proto
        if ps then
            before_proto = sub(line1, 1, ps - 1)
            proto        = sub(line1, ps + 1, pe - 1)
            after_proto  = sub(line1, pe + 1)
        else
            before_proto = line1
        end
        local ti = gfind(before_proto, '[^ ]+')
        local t1, t2 = ti(), ti()
        if t1 ~= 'Tarantool' or not match(t2, '^%d+%.%d+%.%d+') or ti() then
            return nil
        end
        local di = gfind(t2, '%d+')
        local major = tonumber(di())
        local minor = tonumber(di())
        local patch = tonumber(di())
        local intver = major * 10000 + minor * 100 + patch
        local res = { version = t2, intver = intver, protocol = proto }

        if intver < 10607 and not proto then
            res.protocol = 'Binary'
        else
            if not proto then
                return nil
            elseif proto ~= 'Binary' then
                return res
            end
            local u = gfind(after_proto, '[^ ]+')()
            if not match(u, uuid_pattern) then
                return nil
            end
            res.uuid = u
        end
        local s = base64_decode(gfind(line2, '[^ ]+')())
        if #s < 20 then
            return nil
        end
        res.salt = s
        return res
    end)
    return ok and res
end

ffi.cdef([[
    struct tarantool_iomux
    {
        ptrdiff_t    f;
        struct ibuf *s;
        struct ibuf *r;
        int          inject_error;
        char         _[?];
    };
]])

local imx_type           = ffi.typeof('struct tarantool_iomux')
local imx_size           = imxlib.imx_size
local imx_create         = imxlib.imx_create
local imx_destroy        = imxlib.imx_destroy
local imx_configure      = imxlib.imx_configure
local imx_sender_checkin = imxlib.imx_sender_checkin
local imx_line_service   = imxlib.imx_line_service

local function imx_new(sendb, recvb)
    local p = ffi.new(imx_type, imx_size - ffi.sizeof(imx_type, 0))
    imx_create(p)
    p.s = sendb
    p.r = recvb
    return ffi.gc(p, function() return imx_destroy(p), sendb, recvb end)
end

local function next_id(id)
    if id + 1 > 0x7FFFFFFF then
        return 1
    else
        return id + 1
    end
end

local function weak_value_table(...)
    return setmetatable({...}, { __mode = 'v' })
end

function connect(host, port, opts)

    -- XXX please fix the loader
    if not internal then
        internal = require('net.box.lib')
        encode_auth  = internal.encode_auth
        encode_select = internal.encode_select
        encode_eval = internal.encode_eval
        parse_response = internal.parse_response
    end

    local state           = ''
    local err, errmsg
    local state_ev        = fiber.event()
    local sendb           = buffer.ibuf(buffer.READAHEAD)
    local recvb           = buffer.ibuf(buffer.READAHEAD)
    local imx             = imx_new(sendb, recvb)
    local requests        = weak_value_table()
    local request_ev      = fiber.event()
    local worker
    local sock
    local request_id      = 1
    local enable_schema   = opts and opts.enable_schema
    local user            = opts and opts.user
    local password        = opts and opts.password
    local reconnect_after = opts and opts.reconnect_after
    local get_self
    local hook


    -- STATE SWITCHING --
    -- wait_state is for API users; internally iomux + request_ev are used
    -- for request coordination and that's it
    local function set_state(s, e, m)
        state = s
        err = e
        errmsg = m
        local self = get_self()
        self.state = s
        self.err = e
        self.errmsg = m
        hook('state_changed', s, e, m)
        state_ev:signal()
        if state == 'closed' or state == 'error' or
           state == 'error_reconnect'
        then
            self.protocol = nil
            for _, request in pairs(requests) do
                request[2] = e
                request[3] = m
            end
            requests = weak_value_table()
            request_ev:signal()
        end
        if state == 'active' then
            imx_configure(imx, { state = 'P' })
        elseif state == 'closed' or state == 'error' then
            imx_configure(imx, { state = 'X' })
        else
            imx_configure(imx, { state = 'H' })
        end
    end

    local function wait_state(s, deadline)
        while state ~= s and state ~= 'closed' and state ~= 'error' do
            if not state_ev:wait_deadline(deadline) then
                return false
            end
        end
        return state == s
    end

    -- CONNECT/CLOSE --
    local protocol_sm

    local function connect()
        if state ~= '' then
            return state ~= 'closed' and state ~= 'error'
        end
        set_state('connecting')
        fiber.create(function()
            worker = fiber.self()
            fiber.name(string.format('net_connector(%s:%s)', host, port))
            local ok, err = pcall(protocol_sm)
            if not ok and state ~= 'closed' and state ~= 'error' then
                set_state('error', E_UNKNOWN, err)
            end
            worker = nil
            if sock then
                imx_configure(imx, { fd = -1 })
                sock:close()
                sock = nil
            end
            sendb:recycle()
            recvb:recycle()
        end)
        return true
    end

    local function close() -- used in a finalizer, MUST NOT yield
        if state ~= 'error' and state ~= 'closed' then
            set_state('closed', E_NO_CONNECTION, 'Connection closed')
        end
        if worker then
            worker:cancel('async')
            worker = nil
        end
    end

    local function inject_error()
        imx.inject_error = 1
        if worker then
            worker:wakeup()
        end
    end

    -- REQUEST/RESPONSE --
    local function new_request_id()
        local id = request_id
        request_id = next_id(request_id)
        return id
    end

    local function begin_request(deadline)
        if sendb.rpos - sendb.wpos >= imx.f then
            connect()
            local st = imx_sender_checkin(imx, deadline)
            if st == 'T' then
                return E_TIMEOUT, 'Timeout exceeded'
            elseif st == 'X' then
                return err, errmsg
            end
        end
        return nil, sendb
    end

    local function wait_response(id, deadline)
        local request = { fiber.self(), nil, nil, nil }
        requests[id] = request
        if not request_ev:wait_deadline(deadline) then
            requests[id] = nil
            return E_TIMEOUT, 'Timeout exceeded'
        else
            return request[2], request[3], request[4]
        end
    end

    local function dispatch_response(id, a1, a2)
        local request = requests[id]
        requests[id] = nil
        if request then
            local client = request[1]
            request[3] = a1
            request[4] = a2
            if client:status() ~= 'dead' then
                client:wakeup()
            end
        end
    end

    -- IO (WORKER FIBER) --
    local function send_and_recv(sz_or_boundary, deadline)
        local err, extra = imx_line_service(imx, sz_or_boundary, deadline)
        if not err then
            return nil, extra
        elseif err == 'E' then
            return E_NO_CONNECTION, errno.strerror(extra)
        elseif err == 'T' then
            return E_TIMEOUT, 'Timeout exceeded'
        elseif err == '.' then
            return E_NO_CONNECTION, 'Peer closed'
        elseif err == '!' then
            return E_NO_CONNECTION, 'Error injection'
        else
            return E_UNKNOWN, 'Unknown error'
        end
    end

    local function send_and_recv_iproto(deadline)
        local packet_len, hdr, body = parse_response(recvb)
        if packet_len == nil then
            return E_NO_CONNECTION, 'Invalid response packet'
        end
        if recvb.wpos - recvb.rpos >= packet_len then
            recvb.rpos = recvb.rpos + packet_len
            return nil, hdr, body
        end
        local err, extra = send_and_recv(packet_len, deadline)
        if err then
            return err, extra
        end
        return send_and_recv_iproto(deadline)
    end

    -- PROTOCOL STATE MACHINE (WORKER FIBER) --
    local console_sm, iproto_auth_sm, iproto_schema_sm, iproto_sm, error_sm

    -- sm entry point:
    protocol_sm = function ()
        sock = socket.tcp_connect(host, port)
        if sock == nil then
            return error_sm(E_NO_CONNECTION, errno.strerror(errno()))
        end
        imx_configure(imx, {fd = sock:fd()})
        local err, msg = send_and_recv(iproto.GREETING_SIZE, fiber.time() + 0.3)
        if err then
            return error_sm(err, msg)
        end
        local h = ffi.string(recvb.rpos, iproto.GREETING_SIZE)
        recvb.rpos = recvb.rpos + iproto.GREETING_SIZE
        local g = greeting_decode(h)
        if not g then
            return error_sm(E_NO_CONNECTION, 'Can\'t decode handshake')
        end
        err, msg = hook('handshake', g, h)
        if err then
            return error_sm(err, msg)
        end
        get_self().protocol = g.protocol
        if g.protocol == 'Lua console' then
            local rid = request_id
            set_state('active') -- unblock senders, may queue a few requests
            return console_sm(rid)
        elseif g.protocol == 'Binary' then
            return iproto_auth_sm(g.salt)
        else
            return error_sm(E_NO_CONNECTION, 'Unknown protocol: ' .. g.protocol)
        end
    end

    console_sm = function(rid)
        local delim = '\n...\n'
        local err, delim_pos = send_and_recv(delim)
        if err then
            return error_sm(err, delim_pos)
        else
            dispatch_response(rid, ffi.string(recvb.rpos, delim_pos + #delim))
            recvb.rpos = recvb.rpos + delim_pos + #delim
            return console_sm(next_id(rid))
        end
    end

    iproto_auth_sm = function(salt)
        if not user or not password then
            return iproto_schema_sm()
        end
        set_state('auth')
        encode_auth(sendb, new_request_id(), nil, user, password, salt)
        local err, hdr, body = send_and_recv_iproto()
        if err then
            return error_sm(err, hdr)
        end
        if hdr[iproto.STATUS_KEY] ~= 0 then
            return error_sm(E_NO_CONNECTION, body[iproto.ERROR_KEY])
        end
        return iproto_schema_sm(hdr[iproto.SCHEMA_ID_KEY])
    end

    iproto_schema_sm = function (schema_id)
        if not enable_schema then
            set_state('active')
            return iproto_sm()
        end
        if state ~= 'fetch_schema' then
            set_state('fetch_schema')
        end
        -- schema_id maybe nil!
        local select1_id = new_request_id()
        local select2_id = new_request_id()
        local schema = {}
        local id_map = { [select1_id] = 1, [select2_id] = 2 }
        encode_select(sendb, select1_id, schema_id, iproto.VSPACE_ID, 0, 2, 0, 0xFFFFFFFF, nil)
        encode_select(sendb, select2_id, schema_id, iproto.VINDEX_ID, 0, 2, 0, 0xFFFFFFFF, nil)
        repeat
            local err, hdr, body = send_and_recv_iproto()
            if err then
                return error_sm(err, hdr)
            end
            local id    = hdr[iproto.SYNC_KEY]
            local xslot = id_map[id]
            if not xslot then
                -- response to a client request
                dispatch_response(id, hdr, body)
            else
                -- response to a schema query we've submitted
                local status = hdr[iproto.STATUS_KEY]
                local response_schema_id = hdr[iproto.SCHEMA_ID_KEY]
                -- error check
                if status ~= 0 then
                    if band(status, iproto.STATUS_MASK) == E_WRONG_SCHEMA_VERSION then
                        -- restart schema loader
                        return iproto_schema_sm(response_schema_id)
                    end
                    return error_sm(E_NO_CONNECTION, body[iproto.ERROR_KEY])
                end
                -- we could have sent requests without knowing a schema_id
                if schema_id ~= response_schema_id then
                    if schema_id then
                        -- restart schema loader
                        return iproto_schema_sm(response_schema_id)
                    end
                    schema_id = response_schema_id
                end
                schema[xslot] = body[iproto.DATA_KEY]
                if #schema == 2 then
                    break
                end
            end
        until false
        local err, extra = hook('schema_updated', schema_id, schema[1], schema[2])
        if err then
            error_sm(err, extra)
        end
        set_state('active')
        return iproto_sm(schema_id)
    end

    iproto_sm = function(schema_id)
        local err, hdr, body = send_and_recv_iproto()
        if err then
            return error_sm(err, hdr)
        end
        local status = hdr[iproto.STATUS_KEY]
        if status ~= 0 and
           band(status, iproto.STATUS_MASK) == E_WRONG_SCHEMA_VERSION and
           enable_schema and hdr[iproto.SCHEMA_ID_KEY] ~= schema_id
        then
            set_state('fetch_schema')
            dispatch_response(hdr[iproto.SYNC_KEY], hdr, body)
            return iproto_schema_sm(hdr[iproto.SCHEMA_ID_KEY])
        end
        dispatch_response(hdr[iproto.SYNC_KEY], hdr, body)
        return iproto_sm(schema_id)
    end

    error_sm = function(err, msg)
        if reconnect_after then
            if sock then
                imx_configure(imx, { fd = -1 })
                sock:close()
                sock = nil
            end
            sendb:recycle()
            recvb:recycle()
            set_state('error_reconnect', err, msg)
            fiber.sleep(reconnect_after)
            set_state('connecting')
            return protocol_sm()
        end
        set_state('error', err, msg)
    end

    -- GARBAGE COLLECTION --
    -- Implicit close on GC depends on self being eventually collected.
    -- Background fiber needs self ref, but fibers are GC roots!
    -- We store self as a weak ref, extracted with get_self().
    -- Hook often has a strong ref to self, tread with care!
    -- We store a hook in self, prevents hook GC while self lives.
    local weak_refs

    function get_self()
        local self = weak_refs[1]
        if not self then
            return {}
        end
        return self
    end

    function hook(...)
        local hook = get_self()._hook
        if hook then
            return hook(...)
        end
    end

    local self    = {}
    local hook    = opts and opts.hook
    weak_refs     = weak_value_table(self)
    opts          = nil -- release opts.hook ref

    return setmetatable(
        self,
        {
            __metatable = false,
            -- Lua 5.2 supports __gc for tables; but LuaJIT doesn't
            __gc0 = ffi.gc(ffi.new('char[1]'), close),
            __index = {
                close          = close,
                connect        = connect,
                wait_state     = wait_state,
                begin_request  = begin_request,
                new_request_id = new_request_id,
                wait_response  = wait_response,
                _inject_error  = inject_error,
                _hook          = hook
            }
        }
    )
end

local function console_eval(c, code)
    local err, buf = c.begin_request()
    if err then
        return err, buf
    end
    local p = buf:reserve(#code)
    ffi.copy(p, code, #code)
    buf.wpos = buf.wpos + #code
    return c.wait_response(c.new_request_id())
end

local function iproto_eval(c, code, ...)
    local err, buf = c.begin_request()
    if err then
        return err, buf
    end
    local request_id = c.new_request_id()
    encode_eval(buf, request_id, nil, code, {...})
    return c.wait_response(request_id)
end

return {
    greeting_decode  = greeting_decode,
    connect          = connect,
    console_eval     = console_eval,
    iproto_eval      = iproto_eval,
    iproto           = iproto
}
