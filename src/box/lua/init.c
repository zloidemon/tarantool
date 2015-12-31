/*
 * Copyright 2010-2015, Tarantool AUTHORS, please see AUTHORS file.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */
#include "box/lua/init.h"

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include "lua/utils.h" /* lbox_error() */

#include "box/box.h"
#include "box/txn.h"

#include "box/lua/error.h"
#include "box/lua/tuple.h"
#include "box/lua/call.h"
#include "box/lua/slab.h"
#include "box/lua/index.h"
#include "box/lua/space.h"
#include "box/lua/misc.h"
#include "box/lua/stat.h"
#include "box/lua/sophia.h"
#include "box/lua/info.h"
#include "box/lua/session.h"
#include "box/lua/net_box.h"
#include "box/lua/cfg.h"

/* compiled lua modules */
#include "box/lua/session.lua.h"
#include "box/lua/schema.lua.h"
#include "box/lua/tuple.lua.h"
#include "box/lua/snapshot_daemon.lua.h"
#include "box/lua/load_cfg.lua.h"
#include "box/lua/net_box.lua.h"

#define BC(name) (name), sizeof(name)

static const struct module {
	const char *code;
	size_t code_size;

} lua_modules[] = {
	{BC(luaJIT_BC_session)},
	{BC(luaJIT_BC_schema)},
	{BC(luaJIT_BC_tuple)},
	{BC(luaJIT_BC_snapshot_daemon)},
	{BC(luaJIT_BC_load_cfg)},
	{BC(luaJIT_BC_net_box)},
	{0, 0}
};

static int
lbox_commit(lua_State *L)
{
	if (box_txn_commit() != 0)
		return lbox_error(L);
	return 0;
}

static int
lbox_snapshot(struct lua_State *L)
{
	int ret = box_snapshot();
	if (ret == 0) {
		lua_pushstring(L, "ok");
		return 1;
	}
	luaL_error(L, "can't save snapshot, errno %d (%s)",
		   ret, strerror(ret));
	return 1;
}

static const struct luaL_reg boxlib[] = {
	{"snapshot", lbox_snapshot},
	{"commit", lbox_commit},
	{NULL, NULL}
};

#include "say.h"

void
box_lua_init(struct lua_State *L)
{
	/* Use luaL_register() to set _G.box */
	luaL_register(L, "box", boxlib);
	lua_pop(L, 1);

	box_lua_error_init(L);
	box_lua_tuple_init(L);
	box_lua_call_init(L);
	box_lua_cfg_init(L);
	box_lua_slab_init(L);
	box_lua_index_init(L);
	box_lua_space_init(L);
	box_lua_misc_init(L);
	box_lua_info_init(L);
	box_lua_stat_init(L);
	box_lua_sophia_init(L);
	box_lua_session_init(L);
	luaopen_net_box(L);
	lua_pop(L, 1);

	/* Load Lua extension */
	for (const struct module *mod = lua_modules; mod->code; mod++) {
		if (luaL_loadbuffer(L, mod->code, mod->code_size, NULL))
			panic("Error loading Lua module: %s",
			      lua_tostring(L, -1));
		lua_call(L, 0, 0);
	}

	assert(lua_gettop(L) == 0);
}
