box.cfg{slab_alloc_arena = 8}
box.schema.space.create('repl1', {if_not_exists = true})
box.space.repl1:create_index('pk', {if_not_exists = true})

f = require('fiber')

function f1()
end

function f2()
end

f.create(f1)
f.create(f2)

