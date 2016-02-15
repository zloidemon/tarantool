local log             = require('log')
local fiber           = require('fiber')
local urilib          = require('uri')
local nc              = require('net_connector')
local internal        = require('net.box.lib')
local box_internal    = require('box.internal')

local T               = type
local format          = string.format
local band            = bit.band
local fiber_self      = fiber.self
local fiber_time      = fiber.time
local check_iter_type = box.internal.check_iterator_type

local encode_ping     = internal.encode_ping
local encode_call     = internal.encode_call
local encode_eval     = internal.encode_eval
local encode_insert   = internal.encode_insert
local encode_replace  = internal.encode_replace
local encode_delete   = internal.encode_delete
local encode_update   = internal.encode_update
local encode_upsert   = internal.encode_upsert
local _encode_select  = internal.encode_select
local encode_select   = function(buf, request_id, schema_id,
                                 spaceno, indexno, key, opts)
    if opts == nil then
        opts = {}
    end

    if spaceno == nil or T(spaceno) ~= 'number' then
        box.error(box.error.NO_SUCH_SPACE, '#'..tostring(spaceno))
    end

    if indexno == nil or T(indexno) ~= 'number' then
        box.error(box.error.NO_SUCH_INDEX, indexno, '#'..tostring(spaceno))
    end

    local limit, offset
    if opts.limit ~= nil then
        limit = tonumber(opts.limit)
    else
        limit = 0xFFFFFFFF
    end

    if opts.offset ~= nil then
        offset = tonumber(opts.offset)
    else
        offset = 0
    end

    local iterator = check_iter_type(opts,
        key == nil or (T(key) == 'table' and #key == 0))

    _encode_select(buf, request_id, schema_id,
                   spaceno, indexno, iterator, offset, limit, key)
end

local sequence_mt = { __serialize = 'sequence' }

local remote_methods = {}
local install_new_schema
local space_metatable
local index_metatable

local function parse_params(host, port, opts, _)
    if type(host) == 'table' then
        -- self in remote:new()
        host = port; port = opts; opts = _
    end

    if T(port) == 'table' then
        -- uri, opts
        opts = port; port = nil
    end

    if opts == nil then
        opts = {}
    end

    local user = opts.user
    local password = opts.password

    if port == nil then
        local address = urilib.parse(tostring(host))
        if address == nil or address.service == nil then
            box.error(box.error.PROC_LUA,
                "usage: remote:new(uri[, opts] | host, port[, opts])")
        end

        host = address.host
        port = address.service
        user = address.login or user
        password = address.password or password
    end

    if user ~= nil and password == nil then
        box.error(box.error.PROC_LUA,
            "net.box: password is not defined")
    end

    if user == nil and password ~= nil then
        box.error(box.error.PROC_LUA,
            "net.box: user is not defined")
    end

    if host == nil then
        host = 'localhost'
    end

    -- do not change opts, belongs to the user
    local opts_copy = {}
    opts_copy.wait_connected = true
    for k,v in pairs(opts) do
        opts_copy[k] = v
    end
    opts_copy.user = user
    opts_copy.password = password

    return host, port, opts_copy
end

local function connect(...)

    local host, port, opts = parse_params(...)

    local new_request_id
    local begin_request
    local wait_response

    local remote
    local conn
    local current_schema_id
    local deadlines = setmetatable({}, { __mode = 'k' })

    -- the whole widget with error reporting, timeouts and retries
    local function request(encoder, ...)
        local deadline = deadlines[ fiber_self() ]
        repeat
            local err, buf = begin_request(deadline)
            if err then
                box.error({ code = err, reason = buf })
            end
            local request_id = new_request_id()
            encoder(buf, request_id, current_schema_id, ...)
            local err, hdr, body = wait_response(request_id, deadline)
            if err then
                box.error({ code = err, reason = hdr })
            end
            local status = hdr[0]
            if status == 0 then
                local res = body[0x30]
                if res then
                    setmetatable(res, sequence_mt)
                    if encoder ~= encode_eval and rawget(box, 'tuple') then
                        local tnew = box.tuple.new
                        for i, v in pairs(res) do
                            res[i] = tnew(v)
                        end
                    end
                end
                return res
            end
            local err = band(0x7FFF, status)
            if err ~= box.error.WRONG_SCHEMA_VERSION then
                if err == box.error.TIMEOUT then
                    self._deadlines[fiber_self()] = nil
                end
                box.error({ code = err, reason = body[0x31] })
            end
            -- retry request
        until false
    end

    -- net_connector invokes this func to process async events
    local function hook(event, a1, a2, a3)
        if event == 'handshake' then
            -- 'handshake', parsed_greeting, greeting
            if a1.protocol ~= 'Binary' then
                return box.error.NO_CONNECTION,
                       format("Unsupported protocol: %s", a1.protocol)
            end
        elseif event == 'schema_updated' then
            -- 'schema_updated', schema_id, spaces, indexes
            current_schema_id = a1
            return install_new_schema(remote, a1, a2, a3)
        elseif event == 'state_changed' then
            -- 'state_changed', new_state, opt_error, opt_message
            remote.error = a3
            if a1 == 'error' or a1 == 'error_reconnect' then
                log.warn("%s:%s: %s", host or "", port or "", tostring(a3))
                a1 = 'closed' -- compatibility
            end
            remote.state = a1
        end
    end

    conn = nc.connect(host, port, {
        user            = opts.user,
        password        = opts.password,
        reconnect_after = opts.reconnect_after,
        enable_schema   = true,
        hook            = hook
    })

    new_request_id = conn.new_request_id
    begin_request  = conn.begin_request
    wait_response  = conn.wait_response

    remote = {
        host  = host,
        port  = port,
        opts  = opts,
        state = 'init'
    }

    -- these are considered private, hide them when dumping object
    local attrs = {
        _request   = request,
        _conn      = conn,
        _deadlines = deadlines,
        _space_mt  = space_metatable(request),
        _index_mt  = index_metatable(request),
    }

    -- copy methods
    for k, v in pairs(remote_methods) do
        attrs[k] = v
    end

    setmetatable(remote, { __index = attrs, __metatable = false })

    -- the object must be fully initialized when connect is attempted
    conn.connect()
    return remote
end

function remote_methods.close(self)
    if type(self) ~= 'table' then
        box.error(box.error.PROC_LUA, "usage: remote:close()")
    end
    self._conn.close()
end

function remote_methods.is_connected(self)
    if type(self) ~= 'table' then
        box.error(box.error.PROC_LUA, "usage: remote:is_connected()")
    end
    local state = self._conn.state
    return state ~= '' and state ~= 'closed' and
           state ~= 'error' and state ~= 'error_reconnect'
end

function remote_methods.wait_connected(self, timeout)
    if type(self) ~= 'table' then
        box.error(box.error.PROC_LUA, "usage: remote:wait_connected()")
    end
    local deadline = (timeout and timeout + fiber_time())
    return self._conn.wait_state('active', deadline)
end

function remote_methods.ping(self)
    if type(self) ~= 'table' then
        box.error(box.error.PROC_LUA, "usage: remote:ping()")
    end
    local conn = self._conn
    -- Don't ping if the connection isn't ready (deadline == now)
    local err, buf = conn.begin_request(fiber_time())
    if err then
        return self:is_connected()
    end
    local request_id = conn.new_request_id()
    encode_ping(buf, request_id, nil)
    local err, hdr, body = conn.wait_response(
        request_id, self._deadlines[fiber_self()])
    if err and err == box.error.TIMEOUT then
        self._deadlines[fiber_self()] = nil
    end
    return not err and hdr[0] == 0
end

function remote_methods.reload_schema(self)
    if type(self) ~= 'table' then
        box.error(box.error.PROC_LUA, "usage: remote:reload_schema()")
    end
    -- If the schema is outdated, the request will fail, and then the
    -- background fiber will fetch a new schema version.
    self._request(encode_ping)
end

function remote_methods.call(self, proc_name, ...)
    if type(self) ~= 'table' then
        box.error(box.error.PROC_LUA, "usage: remote:call(proc_name, ...)")
    end
    return self._request(encode_call, tostring(proc_name), {...})
end

function remote_methods.eval(self, expr, ...)
    if type(self) ~= 'table' then
        box.error(box.error.PROC_LUA, "usage: remote:eval(expr, ...)")
    end
    local res = self._request(encode_eval, tostring(expr), {...})
    local len = #res
    if len == 0 then
        return
    elseif len == 1 then
        return res[1]
    else
        return unpack(res)
    end
end

function remote_methods._select(self, space_id, idx_id, key, opts)
    -- used in test
    return self._request(encode_select, space_id, idx_id, key, opts)
end

function remote_methods._fatal(self)
    -- used in test
    self._conn._inject_error()
    fiber.sleep(0)
end

function remote_methods.timeout(self, timeout)
    if type(self) ~= 'table' then
        box.error(box.error.PROC_LUA, "usage: remote:timeout(timeout)")
    end
    self._deadlines[ fiber_self() ] = (timeout and fiber_time() + timeout)
    return self
end

--
-- schema, space, index
--

install_new_schema = function(remote, schema_id, spaces, indexes)

    local index_mt = remote._index_mt
    local space_mt = remote._space_mt

    local sl = {}

    for _, space in pairs(spaces) do
        local name = space[3]
        local id = space[1]
        local engine = space[4]
        local field_count = space[5]

        local s = {
            id              = id,
            name            = name,
            engine          = engine,
            field_count     = field_count,
            enabled         = true,
            index           = {},
            temporary       = false
        }
        if #space > 5 then
            local opts = space[6]
            if T(opts) == 'table' then
                -- Tarantool >= 1.7.0
                s.temporary = not not opts.temporary
            elseif T(opts) == 'string' then
                -- Tarantool < 1.7.0
                s.temporary = string.match(opts, 'temporary') ~= nil
            end
        end

        setmetatable(s, space_mt)

        sl[id] = s
        sl[name] = s
    end

    for _, index in pairs(indexes) do
        local idx = {
            space   = index[1],
            id      = index[2],
            name    = index[3],
            type    = string.upper(index[4]),
            parts   = {},
        }
        local OPTS = 5
        local PARTS = 6

        if T(index[OPTS]) == 'number' then
            idx.unique = index[OPTS] == 1 and true or false

            for k = 0, index[PARTS] - 1 do
                local pktype = index[7 + k * 2 + 1]
                local pkfield = index[7 + k * 2]

                local pk = {
                    type = string.upper(pktype),
                    fieldno = pkfield
                }
                idx.parts[k] = pk
            end
        else
            for k = 1, #index[PARTS] do
                local pktype = index[PARTS][k][2]
                local pkfield = index[PARTS][k][1]

                local pk = {
                    type = string.upper(pktype),
                    fieldno = pkfield
                }
                idx.parts[k - 1] = pk
            end
            idx.unique = index[OPTS].is_unique and true or false
        end

        if sl[idx.space] ~= nil then
            sl[idx.space].index[idx.id] = idx
            sl[idx.space].index[idx.name] = idx
            idx.space = sl[idx.space]
            setmetatable(idx, index_mt)
        end
    end

    remote._schema_id = schema_id
    remote.space = sl
end

local function one_tuple(tab)
    if tab and tab[1] ~= nil then
        return tab[1]
    end
end

local function check_if_space(space, method)
    if T(space) ~= 'table' or not space.id then
        local template = 'Use space:%s(...) instead of space.%s(...)'
        box.error(box.error.PROC_LUA, format(template, method, method))
    end
end

space_metatable = function(request)
    local methods = {}

    function methods.insert(space, tuple)
        check_if_space(space, 'insert')
        return one_tuple(request(encode_insert, space.id, tuple))
    end

    function methods.replace(space, tuple)
        check_if_space(space, 'replace')
        return one_tuple(request(encode_replace, space.id, tuple))
    end

    function methods.select(space, key, opts)
        check_if_space(space, 'select')
        return request(encode_select, space.id, 0, key, opts)
    end

    function methods.delete(space, key)
        check_if_space(space, 'delete')
        return one_tuple(request(encode_delete, space.id, 0, key))
    end

    function methods.update(space, key, oplist)
        check_if_space(space, 'update')
        return one_tuple(request(encode_update, space.id, 0, key, oplist))
    end

    function methods.upsert(space, key, oplist)
        check_if_space(space, 'upsert')
        return one_tuple(request(encode_upsert, space.id, 0, key, oplist))
    end

    function methods.get(space, key)
        check_if_space(space, 'get')
        local res = request(encode_select, space.id, 0, key,
                            { limit = 2, iterator = 'EQ' })
        if res and res[2] ~= nil then
            box.error(box.error.MORE_THAN_ONE_TUPLE)
        end
        if res and res[1] ~= nil then
            return res[1]
        end
    end

    return { __index = methods }
end

local function check_if_index(idx, method)
    if T(idx) ~= 'table' or not idx.id or T(idx.space) ~= 'table' then
        local template = 'Use index:%s(...) instead of index.%s(...)'
        box.error(box.error.PROC_LUA, format(template, method, method))
    end
end

index_metatable = function(request)
    local methods = {}

    function methods.select(idx, key, opts)
        check_if_index(idx, 'select')
        return request(encode_select, idx.space.id, idx.id, key, opts)
    end

    function methods.get(idx, key)
        check_if_index(idx, 'get')
        local res = request(encode_select, idx.space.id, idx.id, key,
                            { limit = 2, iterator = 'EQ' })
        if res and res[2] ~= nil then
            box.error(box.error.MORE_THAN_ONE_TUPLE)
        end
        if res and res[1] ~= nil then
            return res[1]
        end
    end

    function methods.min(idx, key)
        check_if_index(idx, 'min')
        return one_tuple(request(encode_select, idx.space.id, idx.id, key,
                                 { limit = 1, iterator = 'GE' }))
    end

    function methods.max(idx, key)
        check_if_index(idx, 'max')
        return one_tuple(request(encode_select, idx.space.id, idx.id, key,
                                 { limit = 1, iterator = 'LE' }))
    end

    function methods.count(idx, key)
        check_if_index(idx, 'count')
        local code = format('box.space.%s.index.%s:count',
                            idx.space.name, idx.name)
        local res = request(encode_call, code, { key })
        if res and res[1] ~= nil then
            return res[1][1]
        end
    end

    function methods.delete(idx, key)
        check_if_index(idx, 'delete')
        return one_tuple(request(encode_delete, idx.space.id, idx.id, key))
    end

    function methods.update(idx, key, oplist)
        check_if_index(idx, 'update')
        return one_tuple(request(
                encode_update, idx.space.id, idx.id, key, oplist))
    end

    function methods.upsert(idx, key, oplist)
        check_if_index(idx, 'upsert')
        return one_tuple(request(
                encode_upsert, idx.space.id, idx.id, key, oplist))
    end

    return { __index = methods }
end

--
-- net.box.self
--

local function rollback()
    if rawget(box, 'rollback') ~= nil then
        -- roll back local transaction on error
        box.rollback()
    end
end

local function handle_call_result(status, ...)
    if not status then
        rollback()
        return box.error(box.error.PROC_LUA, (...))
    end
    if select('#', ...) == 1 and type((...)) == 'table' then
        local result = (...)
        for i, v in pairs(result) do
            result[i] = box.tuple.new(v)
        end
        return result
    else
        local result = {}
        for i=1,select('#', ...), 1 do
            result[i] = box.tuple.new((select(i, ...)))
        end
        return result
    end
end

local function handle_eval_result(status, ...)
    if not status then
        rollback()
        return box.error(box.error.PROC_LUA, (...))
    end
    return ...
end

local net_box_self_methods = {
    ping = function() return true end,
    reload_schema = function() end,
    close = function() end,
    timeout = function(self) return self end,
    wait_connected = function(self) return true end,
    is_connected = function(self) return true end,
    call = function(_box, proc_name, ...)
        if type(_box) ~= 'table' then
            box.error(box.error.PROC_LUA, "usage: remote:call(proc_name, ...)")
        end
        proc_name = tostring(proc_name)
        local status, proc, obj = pcall(package.loaded['box.internal'].
            call_loadproc, proc_name)
        if not status then
            rollback()
            return box.error() -- re-throw
        end
        local result
        if obj ~= nil then
            return handle_call_result(pcall(proc, obj, ...))
        else
            return handle_call_result(pcall(proc, ...))
        end
    end,
    eval = function(_box, expr, ...)
        if type(_box) ~= 'table' then
            box.error(box.error.PROC_LUA, "usage: remote:eval(expr, ...)")
        end
        local proc, errmsg = loadstring(expr)
        if not proc then
            proc, errmsg = loadstring("return "..expr)
        end
        if not proc then
            rollback()
            return box.error(box.error.PROC_LUA, errmsg)
        end
        return handle_eval_result(pcall(proc, ...))
    end
}

local function new(...)
    local remote = connect(...)
    if remote.opts.wait_connected then
        remote:wait_connected()
    end
    return remote
end

local function timeout(self, t)
    return {
        new = function(...)
            local remote = connect(...)
            if remote.opts.wait_connected then
                local st = remote:wait_connected(t)
                if not st and remote.state ~= 'closed' then
                    remote:close()
                    box.error(box.error.TIMEOUT)
                end
            end
        end
    }
end

local this_module = setmetatable(
    { new = new, timeout = timeout },
    {
        __index = function(self, key)
            -- lazy init net.box.self
            if key ~= 'self' then
                return nil
            end
            local net_box_self = setmetatable(
                { space = require('box').space },
                { __index = net_box_self_methods }
            )
            self.self = net_box_self
            return net_box_self
        end
    }
)

package.loaded['net.box'] = this_module
return this_module
