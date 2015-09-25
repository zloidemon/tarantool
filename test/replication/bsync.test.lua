--# set connection default

--# create server bsync1 with script='replication/bsync1.lua', wait_load = 0
--# create server bsync2 with script='replication/bsync2.lua', wait_load = 0
--# create server bsync3 with script='replication/bsync3.lua', wait_load = 0
--# start server bsync1
--# start server bsync2
--# start server bsync3

--# set connection bsync1, bsync2, bsync3
while box.info.status ~= 'running' do require('fiber').sleep(0) end

--# set connection bsync1
box.space.test:insert {'hello', 'bsync1'}

--# set connection bsync1, bsync2, bsync3
box.space.test:get('hello')

--# set connection bsync1
box.space.test:drop()

--# stop server bsync1
--# stop server bsync2
--# stop server bsync3
--# cleanup server bsync1
--# cleanup server bsync2
--# cleanup server bsync3
--# set connection default
