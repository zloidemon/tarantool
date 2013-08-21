/*
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

#include "lua.h"

#include "say.h"

#include "lua/init.h"
#include <scoped_guard.h>
#include <exception.h>

extern "C" {
#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>
} /* extern "C" */

namespace js {
namespace lua {

namespace {  /* (anonymous) */

v8::Local<v8::Value>
LuaToV8(struct lua_State *L, int idx);

void
V8ToLua(v8::Local<v8::Value> value, struct lua_State *L);

v8::Local<v8::Object>
CatchLuaException(struct lua_State *L)
{
	assert(L != NULL);

	v8::HandleScope handle_scope;

	size_t len;
	const char *msg = lua_tolstring(L, -1, &len);
	v8::Local<v8::Object> e = v8::Exception::Error(
		v8::String::New(msg, len))->ToObject();
	e->Set(v8::String::NewSymbol("code"),
	       v8::Uint32::New(ER_PROC_LUA));
	lua_settop(L, 0);
	return handle_scope.Close(e);
}

#define LUA_BEGIN(L) \
	struct lua_State *L = NULL;						\
	int thread_ref_##L = 0;							\
	try {									\
		L = lua_newthread(tarantool_L);					\
		thread_ref_##L = luaL_ref(tarantool_L, LUA_REGISTRYINDEX);	\
	} catch(...) {								\
		v8::Local<v8::Object> e = CatchLuaException(tarantool_L);	\
		v8::ThrowException(e);						\
		return;								\
	}									\
										\
	auto scope_guard_for_##L = make_scoped_guard([=]{			\
		lua_settop(L, 0);						\
		luaL_unref(tarantool_L, LUA_REGISTRYINDEX, thread_ref_##L);	\
	});									\
										\
	try {

#define LUA_END(L)\
	} catch (const ClientError& e) {\
		v8::Local<v8::Object> ex = js::CatchNativeException(e);\
		v8::ThrowException(ex);\
		return;\
	} catch (const Exception &e) {\
		e.log();\
		panic("Unhandled C++ exception in JS bindings");\
	} catch (...) {\
		v8::Local<v8::Object> e = CatchLuaException(L);\
		v8::ThrowException(e);\
		return;\
	}

namespace Base {

void
GC(v8::Isolate* isolate, v8::Persistent<v8::Object>* value, void *)
{
	assert(value->IsNearDeath());

	v8::Local<v8::Object> obj = v8::Local<v8::Object>::New(isolate, *value);
	assert(obj->InternalFieldCount() > 0);
	int ref = obj->GetInternalField(0)->Int32Value();
	obj.Clear();

	if (ref != LUA_GLOBALSINDEX)
		lua_unref(tarantool_L, ref);

	value->Dispose();
	value->Clear();
}

void
Call(const v8::FunctionCallbackInfo<v8::Value>& args)
{
	v8::HandleScope handle_scope;

	if (!args.IsConstructCall()) {
		v8::ThrowException(v8::Exception::Error(
			v8::String::New("Use new")));
		return;
	}

	v8::Local<v8::Object> thiz = args.Holder();

	if (args.Length() != 1 || !args[0]->IsInt32()) {
		v8::ThrowException(v8::Exception::Error(
			v8::String::New("Illegal arguments")));
		return;
	}

	int ref = args[0]->Int32Value();

	v8::Persistent<v8::Object> handle(args.GetIsolate(), args.This());

	handle.MakeWeak((void *) NULL, GC);
	handle.MarkIndependent();

	assert(thiz->InternalFieldCount() > 0);
	thiz->SetInternalField(0, v8::Integer::New(ref));
}

void
ExtractObject(struct lua_State *L, v8::Local<v8::Object> thiz)
{
	assert (thiz->InternalFieldCount() > 0);
	int ref = thiz->GetInternalField(0)->Int32Value();

	if (ref != LUA_GLOBALSINDEX) {
		lua_rawgeti(tarantool_L, LUA_REGISTRYINDEX, ref);
		lua_xmove(tarantool_L, L, 1);
	} else {
		lua_pushvalue(L, LUA_GLOBALSINDEX);
	}
}

} /* namespace Base */

namespace Table {
using namespace Base;

void
ToString(const v8::FunctionCallbackInfo<v8::Value>& args)
{
	v8::HandleScope handleScope;

	LUA_BEGIN(L);

	ExtractObject(L, args.Holder());
	assert(lua_istable(L, 1) || lua_isuserdata(L, 1));

	const char *str = tarantool_lua_tostring(L, 1);

	args.GetReturnValue().Set(v8::String::New(str));

	lua_pop(L, 1);

	LUA_END(L);
}

void
Get(v8::Local<v8::String> property,
       const v8::PropertyCallbackInfo<v8::Value>& args)
{
	v8::HandleScope handleScope;

	v8::String::Utf8Value propertyUtf8(property);

	/* Do not override these methods! */
	if (strcmp(*propertyUtf8, "toString") == 0 ||
	    strcmp(*propertyUtf8, "__proto__") == 0 ||
	    strcmp(*propertyUtf8, "valueOf") == 0)
		return;

	LUA_BEGIN(L);

	ExtractObject(L, args.Holder());
	assert(lua_istable(L, 1) || lua_isuserdata(L, 1));

	V8ToLua(property, L);
	lua_gettable(L, 1);

	args.GetReturnValue().Set(LuaToV8(L, 2));

	LUA_END(L);
}

void
Get(uint32_t index, const v8::PropertyCallbackInfo<v8::Value>& args)
{
	v8::HandleScope handleScope;

	LUA_BEGIN(L);

	ExtractObject(L, args.Holder());
	assert(lua_istable(L, 1) || lua_isuserdata(L, 1));

	lua_pushinteger(L, index);
	lua_gettable(L, 1);

	args.GetReturnValue().Set(LuaToV8(L, 2));

	LUA_END(L);
}

void
Set(v8::Local<v8::String> property, v8::Local<v8::Value> value,
       const v8::PropertyCallbackInfo<v8::Value>& args)
{
	v8::String::Utf8Value propertyUtf8(property);

	/* Do not override these methods! */
	if (strcmp(*propertyUtf8, "toString") == 0 ||
	    strcmp(*propertyUtf8, "__proto__") == 0 ||
	    strcmp(*propertyUtf8, "valueOf") == 0)
		return;

	LUA_BEGIN(L);

	ExtractObject(L, args.Holder());
	assert(lua_istable(L, 1) || lua_isuserdata(L, 1));

	V8ToLua(property, L);
	V8ToLua(value, L);
	lua_settable(L, 1);

	args.GetReturnValue().Set(value);
	LUA_END(L);
}

void
Set(uint32_t index, v8::Local<v8::Value> value,
    const v8::PropertyCallbackInfo<v8::Value>& args)
{
	LUA_BEGIN(L);

	ExtractObject(L, args.Holder());
	assert(lua_istable(L, 1) || lua_isuserdata(L, 1));

	lua_pushinteger(L, index);
	V8ToLua(value, L);
	lua_settable(L, 1);

	LUA_END(L);
}

void
Delete(v8::Local<v8::String> property,
       const v8::PropertyCallbackInfo<v8::Boolean>& args)
{
	LUA_BEGIN(L);

	ExtractObject(L, args.Holder());
	assert(lua_istable(L, 1) || lua_isuserdata(L, 1));

	V8ToLua(property, L);
	lua_pushnil(L);
	lua_settable(L, 1);

	args.GetReturnValue().Set(true);

	LUA_END(L);
}

void
Delete(uint32_t index, const v8::PropertyCallbackInfo<v8::Boolean>& args)
{
	LUA_BEGIN(L);

	ExtractObject(L, args.Holder());
	assert(lua_istable(L, 1) || lua_isuserdata(L, 1));

	lua_pushinteger(L, index);
	lua_pushnil(L);
	lua_settable(L, 1);

	args.GetReturnValue().Set(true);

	LUA_END(L);
}

void
Enumerate(const v8::PropertyCallbackInfo<v8::Array>& args)
{
	v8::HandleScope handle_scope;

	v8::Local<v8::Array> names = v8::Array::New();

	LUA_BEGIN(L);

	ExtractObject(L, args.Holder());
	assert(lua_istable(L, 1) || lua_isuserdata(L, 1));

	if (!lua_istable(L, 1))
		/* Is not supported for user data */
		return;

	lua_pushnil(L);
	while (lua_next(L, 1) != 0) {
		v8::HandleScope handle_scope;
		v8::Local<v8::Value> key = LuaToV8(L, -2);
		names->Set(names->Length(), key);
		lua_pop(L, 1);
	}
	lua_pop(L, 1); /* Pop table */

	args.GetReturnValue().Set(names);

	LUA_END(L);
}

v8::Local<v8::FunctionTemplate>
GetTemplate()
{
	v8::HandleScope handle_scope;

	intptr_t tmpl_key = (intptr_t) &GetTemplate;
	js::JS *js = js::JS::GetCurrent();
	v8::Local<v8::FunctionTemplate> tmpl = js->TemplateCacheGet(tmpl_key);
	if (!tmpl.IsEmpty())
		return handle_scope.Close(tmpl);

	tmpl = v8::FunctionTemplate::New(Call);

	tmpl->InstanceTemplate()->SetInternalFieldCount(1);
	tmpl->InstanceTemplate()->SetNamedPropertyHandler(
		Get, Set, 0, Delete, Enumerate);
	tmpl->InstanceTemplate()->SetIndexedPropertyHandler(
		Get, Set, 0, Delete);

	tmpl->InstanceTemplate()->Set(v8::String::NewSymbol("toString"),
				      v8::FunctionTemplate::New(ToString));
	tmpl->SetClassName(v8::String::NewSymbol("Lua.Table"));

	js->TemplateCacheSet(tmpl_key, tmpl);

	return handle_scope.Close(tmpl);
}

v8::Local<v8::Object>
New(int ref)
{
	v8::HandleScope handle_scope;
	v8::Local<v8::Value> arg = v8::Integer::New(ref);
	return handle_scope.Close(GetTemplate()->GetFunction()->
				  NewInstance(1, &arg));
}

} /* namespace Table */

namespace Function {
using namespace Base;

void
Invoke(const v8::FunctionCallbackInfo<v8::Value>& args)
{
	v8::HandleScope handle_scope;

	LUA_BEGIN(L);

	ExtractObject(L, args.Holder());
	assert(lua_isfunction(L, 1));

	for (uint32_t i = 0; i < args.Length(); i++) {
		V8ToLua(args[i], L);
	}

	lua_call(L, args.Length(), LUA_MULTRET);

	int count = lua_gettop(L);
	if (count == 0) {
		args.GetReturnValue().SetUndefined();
	} else if (count == 1) {
		args.GetReturnValue().Set(LuaToV8(L, 1));
	} else {
		v8::Local<v8::Array> array = v8::Array::New(count);
		for (int i = 0; i < count; i++) {
			array->Set(i, LuaToV8(L, i + 1));
		}
		args.GetReturnValue().Set(array);
	}

	LUA_END(L);
}

v8::Local<v8::FunctionTemplate>
GetTemplate()
{
	v8::HandleScope handle_scope;

	intptr_t tmpl_key = (intptr_t) &GetTemplate;
	js::JS *js = js::JS::GetCurrent();
	v8::Local<v8::FunctionTemplate> tmpl = js->TemplateCacheGet(tmpl_key);
	if (!tmpl.IsEmpty())
		return handle_scope.Close(tmpl);

	tmpl = v8::FunctionTemplate::New(Call);

	tmpl->InstanceTemplate()->SetInternalFieldCount(1);
	tmpl->InstanceTemplate()->SetCallAsFunctionHandler(Invoke);

	tmpl->SetClassName(v8::String::NewSymbol("Lua.Function"));

	js->TemplateCacheSet(tmpl_key, tmpl);

	return handle_scope.Close(tmpl);
}

v8::Local<v8::Object>
New(int ref)
{
	v8::HandleScope handleScope;
	v8::Local<v8::Value> arg = v8::Integer::New(ref);
	return handleScope.Close(GetTemplate()->GetFunction()->
				 NewInstance(1, &arg));
}

} /* namespace Function */

void
V8ToLua(v8::Local<v8::Value> value, struct lua_State *L)
{
	if (value->IsInt32() || value->IsUint32()) {
		lua_pushinteger(L, value->Int32Value());
	} else if (value->IsNumber() || value->IsNumberObject()) {
		lua_pushnumber(L, value->NumberValue());;
	} else if (value->IsString() || value->IsStringObject()) {
		v8::String::Utf8Value valueUtf8(value);
		lua_pushlstring(L, *valueUtf8, valueUtf8.length());
	} else if (value->IsBoolean() || value->IsBooleanObject()) {
		lua_pushboolean(L, value->BooleanValue());
	} else if (value->IsNull() || value->IsUndefined()) {
		lua_pushnil(L);
	} else if (value->IsExternal()) {
		lua_pushlightuserdata(L, value.As<v8::External>()->Value());
	} else if (value->IsArray() || value->IsObject()) {
		v8::Local<v8::Object> obj = value->ToObject();
		if (obj->GetConstructorName()->Equals(
				v8::String::New("Lua.Table"))) {
			Base::ExtractObject(L, obj);
			return;
		}

		lua_newtable(L);
		v8::Local<v8::Array> names = obj->GetPropertyNames();
		for (uint32_t i = 0; i < names->Length(); i++) {
			v8::Local<v8::Value> key = names->Get(i);
			v8::Local<v8::Value> value = obj->Get(key);
			V8ToLua(key, L);
			V8ToLua(value, L);
			lua_settable(L, -3);
		}
	} else {
		v8::String::Utf8Value value_utf8(value);
		say_warn("Unsupported JS to Lua conversion: %s", *value_utf8);
		lua_pushnil(L);
	}
}

v8::Local<v8::Value>
LuaToV8(struct lua_State *L, int idx)
{
	v8::HandleScope handle_scope;

	v8::Local<v8::Value> ret;

	switch(lua_type(L, idx)) {
	case LUA_TNUMBER:
		ret = v8::Number::New(lua_tonumber(L, idx));
		break;
	case LUA_TUSERDATA:
	case LUA_TTABLE:
	{
		lua_pushvalue(L, idx);
		int ref = luaL_ref(L, LUA_REGISTRYINDEX);
		ret = Table::New(ref);
		break;
	}
	case LUA_TFUNCTION:
	{
		lua_pushvalue(L, idx);
		int ref = luaL_ref(L, LUA_REGISTRYINDEX);
		ret = Function::New(ref);
		break;
	}
	case LUA_TSTRING:
	{
		size_t len;
		const char *data = lua_tolstring(L, idx, &len);
		ret = v8::String::New(data, len);
		break;
	}
	case LUA_TBOOLEAN:
		ret = v8::Boolean::New(lua_toboolean(L, idx));
		break;
	case LUA_TNIL:
		ret = v8::Undefined();
		break;
	default:
		say_warn("Unsuppoted Lua to JS conversion: %d",
			 lua_type(L, idx));
		ret = v8::Undefined();
		break;
	}

	return handle_scope.Close(ret);
}

} /* namespace (anonymous) */

v8::Local<v8::Object>
Exports()
{
	v8::HandleScope handle_scope;
	return handle_scope.Close(Table::New(LUA_GLOBALSINDEX));
}

} /* namespace lua */
} /* namespace js */
