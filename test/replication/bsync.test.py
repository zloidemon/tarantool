import os
from lib.tarantool_server import TarantoolServer

servers = []
for i in range(3):
    server = TarantoolServer(server.ini)
    server.n = i + 1
    server.script = 'replication/bsync{}.lua'.format(i + 1)
    server.vardir = os.path.join(server.vardir, 'bsync{}'.format(i + 1))
    print 'starting {}...'.format(server.n),
    server.deploy(wait_load = False)
    print 'ok'
    servers.append(server)

for server in servers:
    print 'waiting {} to start...'.format(server.n),
    server.admin("while box.info.status ~= 'running' do require('fiber').sleep(0) end", silent = True)
    print 'ok'

leader = None
for server in servers:
    server.local_id = server.get_param("bsync.local_id")
    leader_id = server.get_param("bsync.leader_id")
    if server.local_id == leader_id:
        leader = server
        break
if leader is None:
    raise 'failed to find leader'

for server in servers:
    server.admin("box.space.test:insert{'key_%d', 0}" % server.n)

for server in servers:
    server.admin("box.space.test:select{}")

# Fails
#servers[0].admin("box.space.test:update('key_1', {{ '+', 2, 1}})")

for server in servers:
    print 'stopping {}...'.format(server.n),
    server.stop()
    server.cleanup()
    print 'ok'
