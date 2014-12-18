--
-- Access control tests which require a binary protocol
-- connection to the server
--
box.schema.user.grant('guest','read,write,execute','universe')
session = box.session
remote = require('net.box')
c = remote:new(box.cfg.listen)
c:call("dostring", "session.su('admin')")
c:call("dostring", "return session.user()")
c:close()
box.schema.user.revoke('guest', 'read,write,execute', 'universe')

-- gh-488 suid functions
--
setuid_space = box.schema.space.create('setuid_space')
index = setuid_space:create_index('primary')
setuid_func = function() return box.space.setuid_space:auto_increment{} end
box.schema.func.create('setuid_func')
box.schema.user.grant('guest', 'execute', 'function', 'setuid_func')
c = remote:new(box.cfg.listen)
c:call("setuid_func")
session.su('guest')
setuid_func()
session.su('admin')
box.schema.func.drop('setuid_func')
box.schema.func.create('setuid_func', { setuid = true })
box.schema.user.grant('guest', 'execute', 'function', 'setuid_func')
c:call("setuid_func")
session.su('guest')
setuid_func()
session.su('admin')
c:close()
-- OPENTAR-84: crash in on_replace_dd_func during recovery
-- _func space recovered after _user space, so setuid option can be
-- handled incorrectly
box.snapshot()
--# stop server default
--# start server default
remote = require('net.box')
session = box.session
setuid_func = function() return box.space.setuid_space:auto_increment{} end
c = remote:new(box.cfg.listen)
c:call("setuid_func")
session.su('guest')
setuid_func()
session.su('admin')
c:close()
box.schema.func.drop('setuid_func')
box.space.setuid_space:drop()
--
-- gh-530 "assertion failed"
-- If a user is dropped, its session should not be usable
-- any more
--
test = box.schema.space.create('test')
index = test:create_index('primary')
box.schema.user.create('test', {password='test'})
box.schema.user.grant('test', 'read,write', 'space','test')
box.schema.user.grant('test', 'read', 'space', '_space')
box.schema.user.grant('test', 'read', 'space', '_index')
net = require('net.box')
c = net.new('test:test@'..box.cfg.listen)
c.space.test:insert{1}
box.schema.user.drop('test')
c.space.test:insert{1}
c:close()
test:drop()

--
-- gh-575: User loses 'universe' grants after alter
--

box.space._priv:get{1}
u = box.space._user:get{1}
box.session.su('admin')
box.schema.user.passwd('Gx5!')
c = require('net.box').new('admin:Gx5!@'..box.cfg.listen)
c:call('dostring', 'return 2 + 2')
c:close()
box.space._user:replace(u)
--
-- Roles: test that universal access of an authenticated
-- session is not updated if grant is made from another
-- session
--
test = box.schema.space.create('test')
_ = test:create_index('primary')
test:insert{1}
box.schema.user.create('test', {password='test'})
box.schema.user.grant('test', 'read', 'space', '_space')
box.schema.user.grant('test', 'read', 'space', '_index')
net = require('net.box')
c = net.new('test:test@'..box.cfg.listen)
c.space.test:select{}
box.schema.role.grant('public', 'read', 'universe')
c.space.test:select{}
c:close()
c = net.new('test:test@'..box.cfg.listen)
c.space.test:select{}
box.schema.role.revoke('public', 'read', 'universe')
c.space.test:select{}
box.session.su('test')
test:select{}
box.session.su('admin')
c:close()
box.schema.user.drop('test')
test:drop()
