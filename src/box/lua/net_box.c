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
#include "net_box.h"

#include <small/ibuf.h>
#include <msgpuck.h> /* mp_store_u32() */
#include "scramble.h"

#include "box/box.h"
#include "box/iproto_constants.h"
#include "box/tuple.h"
#include "box/lua/tuple.h" /* luamp_convert_tuple() / luamp_convert_key() */

#include "lua/msgpack.h"
#include "third_party/base64.h"

#define cfg luaL_msgpack_default

static inline size_t
netbox_prepare_request(lua_State *L, struct mpstream *stream, uint32_t r_type)
{
	struct ibuf *ibuf = (struct ibuf *) lua_topointer(L, 1);
	uint64_t sync = luaL_touint64(L, 2);
	uint64_t schema_id = luaL_touint64(L, 3);

	mpstream_init(stream, ibuf, ibuf_reserve_cb, ibuf_alloc_cb,
		      luamp_error, L);

	/* Remember initial size of ibuf (see netbox_encode_request()) */
	size_t used = ibuf_used(ibuf);

	/* Reserve and skip space for fixheader */
	size_t fixheader_size = mp_sizeof_uint(UINT32_MAX);
	mpstream_reserve(stream, fixheader_size);
	mpstream_advance(stream, fixheader_size);

	/* encode header */
	luamp_encode_map(cfg, stream, 3);

	luamp_encode_uint(cfg, stream, IPROTO_SYNC);
	luamp_encode_uint(cfg, stream, sync);

	luamp_encode_uint(cfg, stream, IPROTO_SCHEMA_ID);
	luamp_encode_uint(cfg, stream, schema_id);

	luamp_encode_uint(cfg, stream, IPROTO_REQUEST_TYPE);
	luamp_encode_uint(cfg, stream, r_type);

	/* Caller should remember how many bytes was used in ibuf */
	return used;
}

static inline void
netbox_encode_request(struct mpstream *stream, size_t initial_size)
{
	mpstream_flush(stream);

	struct ibuf *ibuf = (struct ibuf *) stream->ctx;

	/*
	 * Calculation the start position in ibuf by getting current size
	 * and then substracting initial size. Since we don't touch
	 * ibuf->rpos during encoding this approach should always work
	 * even on realloc or memmove inside ibuf.
	 */
	size_t fixheader_size = mp_sizeof_uint(UINT32_MAX);
	size_t used = ibuf_used(ibuf);
	assert(initial_size + fixheader_size <= used);
	size_t total_size = used - initial_size;
	char *fixheader = ibuf->wpos - total_size;
	assert(fixheader >= ibuf->rpos);

	/* patch skipped len */
	*(fixheader++) = 0xce;
	/* fixheader size is not included */
	mp_store_u32(fixheader, total_size - fixheader_size);
}

static int
netbox_encode_ping(lua_State *L)
{
	if (lua_gettop(L) < 3)
		return luaL_error(L, "Usage: netbox.encode_ping(ibuf, sync, "
				"schema_id)");

	struct mpstream stream;
	size_t svp = netbox_prepare_request(L, &stream, IPROTO_PING);
	netbox_encode_request(&stream, svp);
	return 0;
}

static int
netbox_encode_auth(lua_State *L)
{
	if (lua_gettop(L) < 6)
		return luaL_error(L, "Usage: netbox.encode_update(ibuf, sync, "
		       "schema_id, user, password, greeting)");

	struct mpstream stream;
	size_t svp = netbox_prepare_request(L, &stream, IPROTO_AUTH);

	size_t user_len;
	const char *user = lua_tolstring(L, 4, &user_len);
	size_t password_len;
	const char *password = lua_tolstring(L, 5, &password_len);
	size_t salt_len;
	const char *salt = lua_tolstring(L, 6, &salt_len);
	if (salt_len < SCRAMBLE_SIZE)
		return luaL_error(L, "Invalid salt");

	/* Adapted from xrow_encode_auth() */
	luamp_encode_map(cfg, &stream, password != NULL ? 2 : 1);
	luamp_encode_uint(cfg, &stream, IPROTO_USER_NAME);
	luamp_encode_str(cfg, &stream, user, user_len);
	if (password != NULL) { /* password can be omitted */
		char scramble[SCRAMBLE_SIZE];
		scramble_prepare(scramble, salt, password, password_len);
		luamp_encode_uint(cfg, &stream, IPROTO_TUPLE);
		luamp_encode_array(cfg, &stream, 2);
		luamp_encode_str(cfg, &stream, "chap-sha1", strlen("chap-sha1"));
		luamp_encode_str(cfg, &stream, scramble, SCRAMBLE_SIZE);
	}

	netbox_encode_request(&stream, svp);
	return 0;
}

static int
netbox_encode_call(lua_State *L)
{
	if (lua_gettop(L) < 5)
		return luaL_error(L, "Usage: netbox.encode_call(ibuf, sync, "
		       "schema_id, function_name, args)");

	struct mpstream stream;
	size_t svp = netbox_prepare_request(L, &stream, IPROTO_CALL);

	luamp_encode_map(cfg, &stream, 2);

	/* encode proc name */
	size_t name_len;
	const char *name = lua_tolstring(L, 4, &name_len);
	luamp_encode_uint(cfg, &stream, IPROTO_FUNCTION_NAME);
	luamp_encode_str(cfg, &stream, name, name_len);

	/* encode args */
	luamp_encode_uint(cfg, &stream, IPROTO_TUPLE);
	luamp_encode_tuple(L, cfg, &stream, 5);

	netbox_encode_request(&stream, svp);
	return 0;
}

static int
netbox_encode_eval(lua_State *L)
{
	if (lua_gettop(L) < 5)
		return luaL_error(L, "Usage: netbox.encode_eval(ibuf, sync, "
		       "schema_id, expr, args)");

	struct mpstream stream;
	size_t svp = netbox_prepare_request(L, &stream, IPROTO_EVAL);

	luamp_encode_map(cfg, &stream, 2);

	/* encode expr */
	size_t expr_len;
	const char *expr = lua_tolstring(L, 4, &expr_len);
	luamp_encode_uint(cfg, &stream, IPROTO_EXPR);
	luamp_encode_str(cfg, &stream, expr, expr_len);

	/* encode args */
	luamp_encode_uint(cfg, &stream, IPROTO_TUPLE);
	luamp_encode_tuple(L, cfg, &stream, 5);

	netbox_encode_request(&stream, svp);
	return 0;
}

static int
netbox_encode_select(lua_State *L)
{
	if (lua_gettop(L) < 9)
		return luaL_error(L, "Usage netbox.encode_select(ibuf, sync, "
				  "schema_id, space_id, index_id, iterator, "
				  "offset, limit, key)");

	struct mpstream stream;
	size_t svp = netbox_prepare_request(L, &stream, IPROTO_SELECT);

	luamp_encode_map(cfg, &stream, 6);

	uint32_t space_id = lua_tointeger(L, 4);
	uint32_t index_id = lua_tointeger(L, 5);
	int iterator = lua_tointeger(L, 6);
	uint32_t offset = lua_tointeger(L, 7);
	uint32_t limit = lua_tointeger(L, 8);

	/* encode space_id */
	luamp_encode_uint(cfg, &stream, IPROTO_SPACE_ID);
	luamp_encode_uint(cfg, &stream, space_id);

	/* encode index_id */
	luamp_encode_uint(cfg, &stream, IPROTO_INDEX_ID);
	luamp_encode_uint(cfg, &stream, index_id);

	/* encode iterator */
	luamp_encode_uint(cfg, &stream, IPROTO_ITERATOR);
	luamp_encode_uint(cfg, &stream, iterator);

	/* encode offset */
	luamp_encode_uint(cfg, &stream, IPROTO_OFFSET);
	luamp_encode_uint(cfg, &stream, offset);

	/* encode limit */
	luamp_encode_uint(cfg, &stream, IPROTO_LIMIT);
	luamp_encode_uint(cfg, &stream, limit);

	/* encode key */
	luamp_encode_uint(cfg, &stream, IPROTO_KEY);
	luamp_convert_key(L, cfg, &stream, 9);

	netbox_encode_request(&stream, svp);
	return 0;
}

static inline int
netbox_encode_insert_or_replace(lua_State *L, uint32_t reqtype)
{
	if (lua_gettop(L) < 5)
		return luaL_error(L, "Usage: netbox.encode_insert(ibuf, sync, "
		       "schema_id, space_id, tuple)");
	lua_settop(L, 5);

	struct mpstream stream;
	size_t svp = netbox_prepare_request(L, &stream, reqtype);

	luamp_encode_map(cfg, &stream, 2);

	/* encode space_id */
	uint32_t space_id = lua_tointeger(L, 4);
	luamp_encode_uint(cfg, &stream, IPROTO_SPACE_ID);
	luamp_encode_uint(cfg, &stream, space_id);

	/* encode args */
	luamp_encode_uint(cfg, &stream, IPROTO_TUPLE);
	luamp_encode_tuple(L, cfg, &stream, 5);

	netbox_encode_request(&stream, svp);
	return 0;
}

static int
netbox_encode_insert(lua_State *L)
{
	return netbox_encode_insert_or_replace(L, IPROTO_INSERT);
}

static int
netbox_encode_replace(lua_State *L)
{
	return netbox_encode_insert_or_replace(L, IPROTO_REPLACE);
}

static int
netbox_encode_delete(lua_State *L)
{
	if (lua_gettop(L) < 6)
		return luaL_error(L, "Usage: netbox.encode_delete(ibuf, sync, "
		       "schema_id, space_id, index_id, key)");

	struct mpstream stream;
	size_t svp = netbox_prepare_request(L, &stream, IPROTO_DELETE);

	luamp_encode_map(cfg, &stream, 3);

	/* encode space_id */
	uint32_t space_id = lua_tointeger(L, 4);
	luamp_encode_uint(cfg, &stream, IPROTO_SPACE_ID);
	luamp_encode_uint(cfg, &stream, space_id);

	/* encode space_id */
	uint32_t index_id = lua_tointeger(L, 5);
	luamp_encode_uint(cfg, &stream, IPROTO_INDEX_ID);
	luamp_encode_uint(cfg, &stream, index_id);

	/* encode key */
	luamp_encode_uint(cfg, &stream, IPROTO_KEY);
	luamp_convert_key(L, cfg, &stream, 6);

	netbox_encode_request(&stream, svp);
	return 0;
}

static int
netbox_encode_update(lua_State *L)
{
	if (lua_gettop(L) < 7)
		return luaL_error(L, "Usage: netbox.encode_update(ibuf, sync, "
		       "schema_id, space_id, index_id, key, ops)");

	struct mpstream stream;
	size_t svp = netbox_prepare_request(L, &stream, IPROTO_UPDATE);

	luamp_encode_map(cfg, &stream, 5);

	/* encode space_id */
	uint32_t space_id = lua_tointeger(L, 4);
	luamp_encode_uint(cfg, &stream, IPROTO_SPACE_ID);
	luamp_encode_uint(cfg, &stream, space_id);

	/* encode index_id */
	uint32_t index_id = lua_tointeger(L, 5);
	luamp_encode_uint(cfg, &stream, IPROTO_INDEX_ID);
	luamp_encode_uint(cfg, &stream, index_id);

	/* encode index_id */
	luamp_encode_uint(cfg, &stream, IPROTO_INDEX_BASE);
	luamp_encode_uint(cfg, &stream, 1);

	/* encode in reverse order for speedup - see luamp_encode() code */
	/* encode ops */
	luamp_encode_uint(cfg, &stream, IPROTO_TUPLE);
	luamp_encode_tuple(L, cfg, &stream, 7);
	lua_pop(L, 1); /* ops */

	/* encode key */
	luamp_encode_uint(cfg, &stream, IPROTO_KEY);
	luamp_convert_key(L, cfg, &stream, 6);

	netbox_encode_request(&stream, svp);
	return 0;
}

static int
netbox_encode_upsert(lua_State *L)
{
	if (lua_gettop(L) != 7)
		return luaL_error(L, "Usage: netbox.encode_update(ibuf, sync, "
			"schema_id, space_id, index_id, tuple, ops)");

	struct mpstream stream;
	size_t svp = netbox_prepare_request(L, &stream, IPROTO_UPSERT);

	luamp_encode_map(cfg, &stream, 6);

	/* encode space_id */
	uint32_t space_id = lua_tointeger(L, 4);
	luamp_encode_uint(cfg, &stream, IPROTO_SPACE_ID);
	luamp_encode_uint(cfg, &stream, space_id);

	/* encode index_id */
	uint32_t index_id = lua_tointeger(L, 5);
	luamp_encode_uint(cfg, &stream, IPROTO_INDEX_ID);
	luamp_encode_uint(cfg, &stream, index_id);

	/* encode index_id */
	luamp_encode_uint(cfg, &stream, IPROTO_INDEX_BASE);
	luamp_encode_uint(cfg, &stream, 1);

	/* encode in reverse order for speedup - see luamp_encode() code */
	/* encode ops */
	luamp_encode_uint(cfg, &stream, IPROTO_OPS);
	luamp_encode_tuple(L, cfg, &stream, 7);
	lua_pop(L, 1); /* ops */

	/* encode tuple */
	luamp_encode_uint(cfg, &stream, IPROTO_TUPLE);
	luamp_encode_tuple(L, cfg, &stream, 6);

	netbox_encode_request(&stream, svp);
	return 0;
}

static int
parse_response_body(lua_State *L, const char **pi, const char *end)
{
	const char         *i = *pi;
	uint32_t            num_cells, cur_cell;
	bool                box_inited;
	box_tuple_format_t *fmt;

	box_inited = box_is_initialized();
	if (box_inited)
		fmt = box_tuple_format_default();

	if (i == end || mp_typeof(*i) != MP_ARRAY ||
		mp_check_array(i, end) > 0) {

		return -1; /* parse error */
	}

	num_cells = mp_decode_array(&i);
	lua_createtable(L, num_cells, 0);
	for (cur_cell = 0; cur_cell < num_cells; cur_cell++) {
		const char *object = i;
		if (mp_check(&i, end) > 0) {
			return -1; /* parse error */
		}
		if (box_inited && mp_typeof(*object) == MP_ARRAY) {
			/*
			 * XXX it would be great if we could do that even if the
			 * box was not inited yet
			 */
			lbox_pushtuple(L, box_tuple_new(fmt, object, i));
		} else {
			luamp_decode(L, cfg, &object);
		}
		lua_rawseti(L, -2, cur_cell + 1);
	}

	*pi = i;
	return 0;
}

static int
netbox_parse_response(lua_State *L)
{
	/* packet_size, hdr, body - success
	 * packet_size            - not enough data
	 * (none)                 - parse error
	 */
	struct ibuf *ibuf = (struct ibuf *) lua_topointer(L, 1);
	const char  *i;
	const char  *end;
	uint32_t     num_fields, cur_field;

	if (ibuf_used(ibuf) == 0) {
		lua_pushinteger(L, 1);
		return 1;
	}

	if (mp_typeof(*ibuf->rpos) != MP_UINT) {
		return 0; /* parse error */
	} else {
		ptrdiff_t res = mp_check_uint(ibuf->rpos, ibuf->wpos);
		uint64_t  payload_len;
		if (res > 0) {
			lua_pushinteger(L, ibuf_used(ibuf) + res);
			return 1;
		}
		i = ibuf->rpos;
		payload_len = mp_decode_uint(&i);
		if (payload_len == 0) {
			/* XXX max packet size? */
			return 0; /* parse error */
		}
		end = i + payload_len;
		lua_pushinteger(L, end - ibuf->rpos);
		if (end > ibuf->wpos) {
			return 1;
		}
	}
	/* have enough bytes, stack:[packet_size] */

	/* parse header */
	if (mp_typeof(*i) != MP_MAP || mp_check_map(i, end) > 0) {
		return 0; /* parse error */
	}
	num_fields = mp_decode_map(&i);
	lua_createtable(L, 0, num_fields);
	for (cur_field = 0; cur_field < num_fields; cur_field++) {
		uint64_t code;
		if (i == end) {
			return 0; /* parse error */
		}
		if (mp_typeof(*i) != MP_UINT || mp_check_uint(i, end) > 0) {
			/* skip unexpected data */
			if (mp_check(&i, end) != 0 || mp_check(&i, end) > 0) {
				return 0; /* parse error */
			}
			continue;
		}
		code = mp_decode_uint(&i);
		lua_pushinteger(L, (int)code); /* range check later */
		if (i == end) {
			return 0; /* parse error */
		}
		if (code > 0xffff ||
			mp_typeof(*i) != MP_UINT || mp_check_uint(i, end) > 0) {

			/* skip unexpected data */
			if (mp_check(&i, end) > 0) {
				return 0; /* parse error */
			}
			lua_pop(L, 1);
			continue;
		}
		luaL_pushuint64(L, mp_decode_uint(&i));
		lua_settable(L, -3);
	}

	/* parse optional body, stack:[packet_size, hdr] */
	if (i == end) {
		return 2;
	}
	if (mp_typeof(*i) != MP_MAP || mp_check_map(i, end) > 0) {
		return 0; /* parse error */
	}
	num_fields = mp_decode_map(&i);
	lua_createtable(L, 0, num_fields);
	for (cur_field = 0; cur_field < num_fields; cur_field++) {
		uint64_t code;
		if (i == end) {
			return 0; /* parse error */
		}
		if (mp_typeof(*i) != MP_UINT || mp_check_uint(i, end) > 0) {
			/* skip unexpected data */
			if (mp_check(&i, end) > 0 || mp_check(&i, end) > 0) {
				return 0; /* parse error */
			}
			continue;
		}
		code = mp_decode_uint(&i);
		lua_pushinteger(L, (int)code); /* range check later */
		if (code == 0x30) {
			if (parse_response_body(L, &i, end) != 0) {
				return 0; /* parse error */
			}
		} else {
			const char *object = i;
			if (mp_check(&i, end) > 0) {
				return 0; /* parse error */
			}
			if (code > 0xffff) {
				lua_pop(L, 1);
				continue;
			}
			luamp_decode(L, cfg, &object);
		}
		lua_settable(L, -3);
	}

	return 3;
}

int
luaopen_net_box(struct lua_State *L)
{
	const luaL_reg net_box_lib[] = {
		{ "encode_ping",    netbox_encode_ping },
		{ "encode_call",    netbox_encode_call },
		{ "encode_eval",    netbox_encode_eval },
		{ "encode_select",  netbox_encode_select },
		{ "encode_insert",  netbox_encode_insert },
		{ "encode_replace", netbox_encode_replace },
		{ "encode_delete",  netbox_encode_delete },
		{ "encode_update",  netbox_encode_update },
		{ "encode_upsert",  netbox_encode_upsert },
		{ "encode_auth",    netbox_encode_auth },
		{ "parse_response", netbox_parse_response },
		{ NULL, NULL}
	};
	luaL_register(L, "net.box.lib", net_box_lib);
	return 1;
}
