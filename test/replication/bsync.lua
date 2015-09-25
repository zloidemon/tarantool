#!/usr/bin/env tarantool

local CLUSTER = {
    "rpl:rpl@127.0.0.1:4445",
    "rpl:rpl@127.0.0.1:4446",
    "rpl:rpl@127.0.0.1:4447"
}
-- get a number from the script name
local INSTANCE_ID = tonumber(arg[0]:match("%S+(%d+)%.lua"))
print('LISTEN', CLUSTER[INSTANCE_ID])

function make_scheme()
    box.schema.user.grant('guest','read,write,execute','universe')
    box.schema.user.create('rpl', {password = 'rpl'})
    box.schema.user.grant('rpl', 'read,write,execute','universe')
    box.schema.space.create('test')
    box.space.test:create_index('primary', {parts = {1, 'STR'}})
end

require('console').listen(os.getenv('ADMIN'))

box.cfg({
    log_level   = 6;
    listen      = CLUSTER[INSTANCE_ID];
    replication = {
        bsync   = 1;
        election_timeout = 30;
        source  = CLUSTER,
        read_timeout = 15;
        write_timeout = 15;
        max_host_queue = 10;
    }
})
