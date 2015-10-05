import os
import random
from lib.tarantool_server import TarantoolServer

servers = []
def wait_ready():
    print 'waiting servers to be ready...',
    i = 0
    for server in servers:
        i = i + 1
        if server.status != 'started':
            continue
        server.admin("while box.info.bsync.status ~= 'ready' do require('fiber').sleep(0) end", silent = True)
        print ' x ',
    print 'ok'

def find_leader():
    leader = None
    for server in servers:
        if server.status != 'started':
            continue
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
    server.local_id = server.get_param("bsync.local_id")
    print 'ok'
    servers.append(server)


wait_ready()
leader = find_leader()
assert leader is not None, "found leader"


print '-------------------------------------------------------------'
print ' Modifications on clusters'
print '-------------------------------------------------------------'

# INSERT
for server in servers:
    server.admin("box.space.test:insert{'key_%d', 0}" % server.n)
for server in servers:
    server.admin("box.space.test:select{}")

# REPLACE
for server in servers:
    server.admin("box.space.test:replace{'key_%d', 1}" % server.n)
for server in servers:
    server.admin("box.space.test:select{}")

# UPDATE
for server in servers:
    server.admin("box.space.test:update('key_%d', {{ '+', 2, 1 }})" % server.n)
for server in servers:
    server.admin("box.space.test:select{}")

# DELETE
for server in servers:
    server.admin("box.space.test:delete{'key_%d'}" % server.n)
for server in servers:
    server.admin("box.space.test:select{}")

# TODO: UPSERT tests


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
new_leader = find_leader()
if new_leader == leader:
    print 'killed slave did not cause re-election'

killed_slave.start()
new_leader = find_leader()
if new_leader != killed_slave:
    print 'restarted slave is not leader'

killed_slave.admin("box.space.test:upsert('cnt', {{'+', 2, 1}}, {'cnt', 1})")
for server in servers:
    server.admin("box.space.test:get('cnt')")


print '-------------------------------------------------------------'
print ' Kill master'
print '-------------------------------------------------------------'

killed_leader = find_leader()
killed_leader.stop()
print 'killed leader'
wait_ready()

new_leader = find_leader()
if new_leader != killed_leader:
    print('new leader elected')

killed_leader.start()
wait_ready()

new_leader2 = find_leader()
if new_leader == new_leader2:
    print 'a former leader did not cause re-election after restore'


print '-------------------------------------------------------------'
print ' Random kills'
print '-------------------------------------------------------------'

for i in range(1):
    print 'kill random server...',
    killed_slave = random.choice(servers)
    killed_slave.stop()
    print 'ok'
    wait_ready()
    print 'start killed server...',
    killed_slave.start(wait_load = False)
    print 'ok'
    wait_ready()

print '-------------------------------------------------------------'
print ' Cleanup'
print '-------------------------------------------------------------'

for server in servers:
    print 'stopping {}...'.format(server.n),
    server.stop()
    server.cleanup()
    print 'ok'
