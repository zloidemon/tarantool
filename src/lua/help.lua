local doc = require('help.en_US')
local yaml = require('yaml')

help = {
    {
        'Usage',
        {
            ['\\h [topic]'] = 'help',
            ['\\d[S][+] [space name]'] = 'show spaces list',
            ['\\i[+] space'] = 'show indexes in space',
            ['\\m[+]'] = 'show memory information',
            ['\\s'] = 'show request statistics',
            ['\\c'] = 'show cluster information',
            ['\\f [pattern]'] = 'show fiber information',
        }
    },
    { ['Help topics'] = { "Basics", "Administration" } },
    "To get help on a topic, type help('topic') (with quotes)",
    "To get help on a function/object, type help(function) (without quotes)",
    "To start tutorial, type tutorial()"
    
--     return [[Usage:
-- \h [topic]              - the helpscreen
-- ]]
}

tutorial = {}
tutorial[1] = help[4]

local help_function_data = {};
help_function_data["Administration"] = {}
help_function_data["Administration"]["Server administrative commands"] =
{		"box.snapshot()",
		"box.info()",
		"box.stat()",
		"box.slab.info()",
		"box.slab.check()",
		"box.fiber.info()",
		"box.plugin.info()",
		"box.cfg()",
		"box.coredump()"
}
help_function_data["Basics"] = "First thing to be done before any database object can be created, is calling box.cfg() to configure and bootstrap the database instance. Once this is done, define database objects using box.schema, for example type box.schema.space.create('test') to create space 'test'. To add an index on a space, type box.space.test:create_index(). With an index, the space can accept tuples. Insert a tuple with box.space.test:insert{1, 'First tuple'}"

local help_object_data = {}

local function help_call(table, param)
    if type(param) == 'string' then
        if help_function_data[param] ~= nil then
            return help_function_data[param]
        end
    end
    if type(param) == 'table' then
        if help_object_data[param] ~= nil then
            return help_object_data[param]
        end
    end
    if param ~= nil then
        return string.format("Help object '%s' not found", tostring(param))
    end
    return table
end

setmetatable(help, { __call = help_call })

local screen_id = 1;

local function tutorial_call(table, action)
    if action == 'start' then
        screen_id = 1;
    elseif action == 'next' or action == 'more' then
        screen_id = screen_id + 1
    elseif action == 'prev' then
        screen_id = screen_id - 1
    elseif type(action) == 'number' and action % 1 == 0 then
        screen_id = tonumber(action)
    elseif action ~= nil then
        error('Usage: tutorial("start" | "next" | "prev" | 1 .. '..
            #doc.tutorial..')')
    end
    if screen_id < 1 then
        screen_id = 1
    elseif screen_id > #doc.tutorial then
        screen_id = #doc.tutorial
    end
    return doc.tutorial[screen_id]
end

setmetatable(tutorial, { __call = tutorial_call })



local function help_spaces(mods, extended, arg)

    if rawget(box, 'space') == nil then
        return { error = 'Box is not configured' }
    end

    local list = {}


    if arg ~= nil then
        if box.space[arg] == nil then
            return { error = 'No such space: ' .. arg }
        end
    end


    local show_system = string.match(mods, 'S') ~= nil


    for no, space in pairs(box.space) do
        if arg ~= nil then
            if space.name == arg then
                table.insert(list, space)
                break
            end
        else
            if no ~= space.id then  -- skip space[No]
                if string.match(space.name, '^_') then
                    if show_system then
                        table.insert(list, space)
                    end
                else
                    table.insert(list, space)
                end
            end
        end
    end

    local res = {}

    for _, space in pairs(list) do
        local info = {
            id          = space.id,
            engine      = space.engine,
            temporary   = space.temporary,
        }


        if extended then
            info.records = space:len()

            info.indexes = {}
            for i = 0, 100 do
                if space.index[i] == nil then
                    break
                end
                table.insert(info.indexes, space.index[i].name)
            end
            setmetatable(info.indexes, { __serialize = 'seq' })
        end

        res[space.name] = info
    end

    return res
end

function help_indexes(mods, extended, arg)
    if rawget(box, 'space') == nil then
        return { error = 'Box is not configured' }
    end

    if arg == nil then
        return { error = 'Usage: \\i[+] space_name' }
    end

    local space = box.space[arg]
    if space == nil then
        return { error = 'No such space: ' .. arg }
    end

    local res = {}
    for i = 0, 100 do
        if space.index[i] == nil then
            break
        end
        local info = {
            [space.index[i].name] = {
                type = space.index[i].type,
                parts = space.index[i].parts
            }
        }
        table.insert(res, info)
    end

    return { [space.name .. ' indexes'] = res }

end



local function help_cluster(mods, extended, arg)
    if rawget(box, 'space') == nil then
        return { error = 'Box is not configured' }
    end

    local list = box.space._cluster:select{}

    local res = {}


    local lsn = 0

    for _, host in pairs(list) do

        local host_lsn = box.info.vclock[ host[1] ]

        table.insert(res, {
            uuid = host[2],
            id   = host[1],
            lsn  = host_lsn
        })

        lsn = lsn + host_lsn


    end

    local cluster = box.space._schema:get{'cluster'}

    if cluster == nil then
        cluster = 'the cluster'
    else
        cluster = cluster[2]
    end

    res = {
        [cluster] = {
            lsn   = lsn,
            nodes = res
        }
    }

    return res
end


local function help_stat(mods, extended, arg)
    if rawget(box, 'space') == nil then
        return { error = 'Box is not configured' }
    end

    return box.stat()
end

local function help_memory(mods, extended, arg)
    if rawget(box, 'space') == nil then
        return { error = 'Box is not configured' }
    end

    local info = box.slab.info()
    if extended then
        for _, slab in pairs(info.slabs) do
            slab.item_size = nil
        end
        setmetatable(info.slabs, nil)
    else
        info.slabs = nil
    end

    info = { arena = info }
    info.lua = {
        collectgarbage = collectgarbage('count')
    }
    return info
end


local function help_fibers(mods, extended, arg)
    local fiber = require 'fiber'

    local info = fiber.info()
    local res = {}

    if arg == nil then
        arg = ''
    end

    for fid, f in pairs(info) do

        if string.sub(f.name, 1, #arg) == arg then
            
            local fi = {
                fid = f.fid,
                status = fiber.find(f.fid).status()

            }

            if extended then
                fi.backtrace = f.backtrace
            end

            table.insert(res, { [f.name]  = fi })
        end
    end
    return res
end

function help_topics(mods, extended, arg)
    if arg ~= nil then
        return help(arg)
    end

    return help
end

local help_commands = {
    d  = help_spaces,
    c  = help_cluster,
    i  = help_indexes,
    s  = help_stat,
    m  = help_memory,
    h  = help_topics,
    f  = help_fibers,
}


-- return cmd, output
-- if output is defined, console will show it
-- if not console does eval(cmd)
local function process(cmd)


    if string.match(cmd, '^%s*\\') == nil then
        return cmd
    end
    local hc, arg = string.match(cmd, '^%s*\\([a-zA-Z][a-zA-Z0-9+]*)%s+(.+)')
    if hc == nil then
        hc = string.match(cmd, '^%s*\\([a-zA-Z][a-zA-Z0-9+]*)%s*$')
    end
    if hc == nil then
        hc = ''
    end

    if arg ~= nil then
        arg = string.gsub(arg, '%s+$', '')
    end


    local mods = string.match(hc, '^.(.+)')
    if mods == nil then
        mods = ''
    end

    local extended = string.match(mods, '[+]') ~= nil
    if extended then
        mods = string.gsub(mods, '[+]', '')
    end

    if #hc > 0 then
        hc = string.match(hc, '^(.)')
    end

    if help_commands[hc] ~= nil then
        local s, r = pcall(help_commands[hc], mods, extended, arg)
        if s then
            return nil, r
        else
            return nil, { error = r }
        end
    end

    return nil, {
        error = string.format('Unknown command "%s"', cmd),
        hint  = 'Type \\h for help'
    }
end



return {
    help = help;
    tutorial= tutorial;
    process = process;
}
