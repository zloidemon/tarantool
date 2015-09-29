import os
import random
from lib.tarantool_server import TarantoolServer

servers = []
def wait_ready():
    for server in servers:
        print 'waiting {} to start...'.format(server.n),
        server.admin("while box.info.bsync.status ~= 'ready' do require('fiber').sleep(0) end", silent = True)
        print 'ok'

def find_leader():
    leader = None
    for server in servers:
        server.local_id = server.get_param("bsync.local_id")
        leader_id = server.get_param("bsync.leader_id")
        if server.local_id == leader_id:
            assert leader is None, "single leader"
            leader = server
    return leader

print '-------------------------------------------------------------'
print ' Bootstrap'
print '-------------------------------------------------------------'

for i in range(3):
    server = TarantoolServer(server.ini)
    server.n = i + 1
    server.script = 'replication/bsync{}.lua'.format(i + 1)
    server.vardir = os.path.join(server.vardir, 'bsync{}'.format(i + 1))
    print 'starting {}...'.format(server.n),
    server.deploy(wait_load = False)
    print 'ok'
    servers.append(server)

wait_ready()
leader = find_leader()
assert leader is not None, "found leader"

print '-------------------------------------------------------------'
print ' Modifications on clusters'
print '-------------------------------------------------------------'

for server in servers:
    server.admin("box.space.test:insert{'key_%d', 0}" % server.n)

for server in servers:
    server.admin("box.space.test:select{}")

print '-------------------------------------------------------------'
print ' Kill slave'
print '-------------------------------------------------------------'

leader = find_leader()
killed_slave = random.choice([ server for server in servers if server != leader ])
killed_slave.stop()

for server in servers:
    if server == killed_slave:
        continue
    server.admin("box.space.test:upsert('cnt', {{'+', 2, 1}}, {'cnt', 1})")
    server.admin("box.space.test:get('cnt')")

killed_slave.start()
leader = find_leader()
assert leader != killed_slave, "restarted slave is not leader"

killed_slave.admin("box.space.test:upsert('cnt', {{'+', 2, 1}}, {'cnt', 1})")
for server in servers:
    server.admin("box.space.test:get('cnt')")



killed_leader = find_leader()
## Fails: Assertion `task->complete' failed.
killed_leader.stop()
#for server in servers:
#    if server == killed_leader:
#        continue
#    server.admin("box.space.test:upsert('cnt', {{'+', 2, 1}}, {'cnt', 1})")
#killed_leader.start()
#wait_ready()
#for server in servers:
#    server.admin("box.space.test:get('cnt')")

## Fails: blocks infinity
#servers[0].admin("box.space.test:update('key_1', {{ '+', 2, 1}})")

for server in servers:
    print 'stopping {}...'.format(server.n),
    server.stop()
    server.cleanup()
    print 'ok'
