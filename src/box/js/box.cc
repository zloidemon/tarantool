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

#include "box.h"
#include "box_tuple.h"

#include "pickle.h"
#include "fiber.h"
#include "scoped_guard.h"

#include "../request.h"
#include "../port.h"
#include "../tuple.h"

#include <box/box.h>

extern char box_js[];

namespace js {
namespace box {

uint32_t
BerSizeOf(v8::Local<v8::Array> tuple)
{
	uint32_t tuple_len = 0;
	for (uint32_t i = 0; i < tuple->Length(); i++) {
		uint32_t field_len = 0;
		v8::Local<v8::Value> field = tuple->Get(i);
		if (field->IsInt32()) {
			field_len = sizeof(uint32_t);
		} else if (field->IsString() || field->IsStringObject()) {
			field_len = field.As<v8::String>()->Length();
		} else if (field->IsArrayBuffer()) {
			field_len = field.As<v8::ArrayBuffer>()->ByteLength();
		} else {
			v8::ThrowException(v8::Exception::TypeError(
				v8::String::New(
				"Expected Number, String, ArrayBuffer "
				"as a key/tuple field")));
			return UINT32_MAX;
		}

		tuple_len += varint32_sizeof(field_len) + field_len;
	}

	return tuple_len;
}

char *
BerPack(char *data, v8::Local<v8::Array> tuple)
{
	char *b = data;
	for (uint32_t i = 0; i < tuple->Length(); i++) {
		v8::Local<v8::Value> field = tuple->Get(i);
		if (field->IsInt32()) {
			b = pack_varint32(b, sizeof(uint32_t));
			b = pack_u32(b, field->Int32Value());
		} else if (field->IsString() || field->IsStringObject()) {
			v8::String::Utf8Value field_utf8(field.As<v8::String>());
			b = pack_lstr(b, *field_utf8, field_utf8.length());
		} else if (field->IsArrayBuffer()) {
			v8::Local<v8::ArrayBuffer> field_buffer =
					field.As<v8::ArrayBuffer>();
			b = pack_lstr(b, field_buffer->BaseAddress(),
				      field_buffer->ByteLength());
		} else {
			assert(false);
		}
	}

	return b;
}

namespace { /* anonymous */

struct PortArray
{
	struct port_vtab *vtab;
	v8::Local<v8::Function> tuple_fun;
	v8::Local<v8::Array> ret;

	PortArray(v8::Local<v8::Function> tuple_fun_arg,
		  v8::Local<v8::Array> ret_arg)
		: vtab(&port_multi_vtab),
		  tuple_fun(tuple_fun_arg), ret(ret_arg)
	{

	}

	static void
	AddTuple(struct port *port, struct tuple *tuple,
			    uint32_t flags)
	{
		(void) flags;
		struct PortArray *port_multi = (struct PortArray *) port;

		port_multi->ret->Set(port_multi->ret->Length(),
			tuple::NewInstance(port_multi->tuple_fun, tuple));
	}

	static struct port_vtab port_multi_vtab;
};

struct port_vtab PortArray::port_multi_vtab = {
	AddTuple,
	null_port_eof
};

struct PortCounter
{
	struct port_vtab *vtab;

	PortCounter()
		: vtab(&port_multi_vtab),
		  cnt(0)
	{

	}

	static void
	AddTuple(struct port *port, struct tuple *tuple,
		 uint32_t flags)
	{
		(void) flags;
		(void) tuple;
		struct PortCounter *port_multi = (struct PortCounter *) port;

		port_multi->cnt++;
	}

	uint32_t cnt;
	static struct port_vtab port_multi_vtab;
};

struct port_vtab PortCounter::port_multi_vtab = {
	AddTuple,
	null_port_eof
};

void
Call(const v8::FunctionCallbackInfo<v8::Value>& args)
{
	assert(args.IsConstructCall());

	/* Get a constructor instance for creating tuples */
	args.Holder()->SetInternalField(0, tuple::GetConstructor());
}

void
RequestExecute(const v8::FunctionCallbackInfo<v8::Value>& args,
	       struct request *request)
{
	v8::HandleScope handle_scope;

	v8::Local<v8::Function> tuple_fun = args.Holder()->
		GetInternalField(0).As<v8::Function>();
	assert(!tuple_fun.IsEmpty());

	if (request->flags & BOX_RETURN_TUPLE) {
		struct PortArray port(tuple_fun, v8::Array::New());
		box_process((struct port *) &port, request);
		args.GetReturnValue().Set(port.ret);
	} else {
		struct PortCounter port;
		box_process((struct port *) &port, request);
		args.GetReturnValue().Set(port.cnt);
	}
}

void
SelectMethod(const v8::FunctionCallbackInfo<v8::Value>& args)
{
	v8::HandleScope handle_scope;

	if (args.Length() < 3 || !args[0]->IsUint32() || !args[1]->IsUint32() ||
	    !args[2]->IsArray()) {
		v8::ThrowException(v8::Exception::Error(
			v8::String::New("Usage: select(space_no, index_no, "
					"key, [offset,  [limit]])")));
		return;
	}

	JS_BEGIN();

	size_t allocated_size = palloc_allocated(fiber->gc_pool);

	v8::Local<v8::Array> key_arg = args[2].As<v8::Array>();
	assert(!key_arg.IsEmpty());
	uint32_t key_len = BerSizeOf(key_arg);
	if (key_len == UINT32_MAX)
		return;

	uint32_t body_len = sizeof(uint32_t) + key_len;

	struct request *request = (struct request *) palloc(fiber->gc_pool,
		sizeof(*request) + body_len);
	memset(request, 0, sizeof(*request) + body_len);

	char *key = (char *) (request + 1);
	char *key_end = key + sizeof(uint32_t) + key_len;
	char *b = key;

	b = pack_u32(b, key_arg->Length()); /* Key part count */
	b = BerPack(b, key_arg);  /* Key */
	assert(b == key_end);

	request->type = SELECT;
	request->flags = BOX_RETURN_TUPLE;
	request->s.space_no = args[0]->Uint32Value();
	request->s.index_no = args[1]->Uint32Value();
	request->s.offset = args[3]->IsUint32() ? args[3]->Uint32Value() : 0;
	request->s.limit =  args[4]->IsUint32() ? args[4]->Uint32Value() :
			UINT32_MAX;
	request->s.key_count = 1;
	request->s.keys = key;
	request->s.keys_end = key_end;
	request->execute = execute_select;

	RequestExecute(args, request);

	ptruncate(fiber->gc_pool, allocated_size);

	JS_END();
}

void
Replace(const v8::FunctionCallbackInfo<v8::Value>& args, int flags)
{
	v8::HandleScope handle_scope;

	if (args.Length() < 2 || !args[0]->IsUint32() || !args[1]->IsArray()) {
		v8::ThrowException(v8::Exception::Error(
			v8::String::New("Usage: replace(space_no, tuple, "
					"[return_tuple])")));
		return;
	}

	JS_BEGIN();

	size_t allocated_size = palloc_allocated(fiber->gc_pool);

	v8::Local<v8::Array> key_arg = args[1].As<v8::Array>();
	assert(!key_arg.IsEmpty());
	uint32_t tuple_len = BerSizeOf(key_arg);
	if (tuple_len == UINT32_MAX)
		return;

	uint32_t body_len = 3 * sizeof(uint32_t) + tuple_len;

	struct request *request = (struct request *) palloc(fiber->gc_pool,
		sizeof(*request) + body_len);
	memset(request, 0, sizeof(*request) + body_len);

	request->type = REPLACE;

	char *b = (char *) (request + 1);
	request->data = b;
	request->len = body_len;

	request->r.space_no = args[0]->Uint32Value();
	b = pack_u32(b, request->r.space_no);
	request->flags = flags | BOX_RETURN_TUPLE;
	if (args.Length() > 2 && args[2]->IsBoolean() &&
	    !args[2]->BooleanValue()) {
		request->flags &= ~BOX_RETURN_TUPLE;
	}

	b = pack_u32(b, request->flags);
	request->r.tuple = b;
	b = pack_u32(b, key_arg->Length());
	b = BerPack(b, key_arg);
	assert(b == request->r.tuple + sizeof(uint32_t) + tuple_len);
	request->r.tuple_end = b;
	assert(request->data + request->len == request->r.tuple_end);

	request->execute = execute_replace;

	RequestExecute(args, request);

	ptruncate(fiber->gc_pool, allocated_size);

	JS_END();
}

void
InsertMethod(const v8::FunctionCallbackInfo<v8::Value>& args)
{
	Replace(args, BOX_ADD);
}

void
StoreMethod(const v8::FunctionCallbackInfo<v8::Value>& args)
{
	Replace(args, 0);
}

void
ReplaceMethod(const v8::FunctionCallbackInfo<v8::Value>& args)
{
	Replace(args, BOX_REPLACE);
}

void
DeleteMethod(const v8::FunctionCallbackInfo<v8::Value>& args)
{
	v8::HandleScope handle_scope;

	if (args.Length() < 2 || !args[0]->IsUint32() || !args[1]->IsArray()) {
		v8::ThrowException(v8::Exception::Error(
			v8::String::New("Usage: delete(space_no, key, "
					"[return_tuple])")));
		return;
	}

	JS_BEGIN();

	size_t allocated_size = palloc_allocated(fiber->gc_pool);

	v8::Local<v8::Array> key_arg = args[1].As<v8::Array>();
	assert(!key_arg.IsEmpty());
	uint32_t key_len = BerSizeOf(key_arg);
	if (key_len == UINT32_MAX)
		return;

	uint32_t body_len = 3 * sizeof(uint32_t) + key_len;

	struct request *request = (struct request *) palloc(fiber->gc_pool,
		sizeof(*request) + body_len);

	request->type = DELETE;

	char *b = (char *) (request + 1);
	request->data = b;
	request->len = body_len;

	/* Space Id */
	request->d.space_no = args[0]->Uint32Value();
	b = pack_u32(b, request->r.space_no);
	/* Flags */
	request->flags = BOX_RETURN_TUPLE;
	if (args.Length() > 2 && args[2]->IsBoolean() &&
	    !args[2]->BooleanValue()) {
		request->flags &= ~BOX_RETURN_TUPLE;
	}
	b = pack_u32(b, request->flags);
	/* Key part count */
	request->d.key_part_count = key_arg->Length();
	b = pack_u32(b, request->d.key_part_count);
	/* Key */
	request->d.key = b;
	b = BerPack(b, key_arg);
	assert(b == request->d.key + key_len);
	request->d.key_end = b;
	assert(request->data + request->len == request->d.key_end);

	request->execute = execute_delete;

	RequestExecute(args, request);

	ptruncate(fiber->gc_pool, allocated_size);

	JS_END();
}

} /* namespace (anonymous) */

v8::Handle<v8::FunctionTemplate>
GetTemplate()
{
	v8::HandleScope handle_scope;

	intptr_t tmpl_key = (intptr_t) &GetTemplate;
	JS *js = JS::GetCurrent();
	v8::Local<v8::FunctionTemplate> tmpl = js->TemplateCacheGet(tmpl_key);
	if (!tmpl.IsEmpty())
		return handle_scope.Close(tmpl);

	tmpl = v8::FunctionTemplate::New(Call);

	tmpl->SetClassName(v8::String::NewSymbol("Box"));

	tmpl->InstanceTemplate()->SetInternalFieldCount(1);
	tmpl->InstanceTemplate()->Set("Tuple", tuple::Exports());
	tmpl->InstanceTemplate()->Set("select",
		v8::FunctionTemplate::New(SelectMethod));
	tmpl->InstanceTemplate()->Set("insert",
		v8::FunctionTemplate::New(InsertMethod));
	tmpl->InstanceTemplate()->Set("store",
		v8::FunctionTemplate::New(StoreMethod));
	tmpl->InstanceTemplate()->Set("replace",
		v8::FunctionTemplate::New(ReplaceMethod));
	tmpl->InstanceTemplate()->Set("delete",
		v8::FunctionTemplate::New(DeleteMethod));

	js->TemplateCacheSet(tmpl_key, tmpl);

	return handle_scope.Close(tmpl);
}

v8::Local<v8::Object>
Exports()
{
	v8::HandleScope handle_scope;
	/* Load JavaScript source to alter native object */
	v8::Local<v8::String> exports_key = v8::String::NewSymbol("exports");
	v8::Local<v8::Object> globals = v8::Object::New();
	globals->Set(exports_key, GetTemplate()->GetFunction()->NewInstance());
	v8::Local<v8::String> source = v8::String::New(box_js);
	v8::Local<v8::String> filename = v8::String::New("box.js");
	EvalInNewContext(source, filename, globals);
	return handle_scope.Close(globals->Get(exports_key)->ToObject());
}

} /* namespace box */
} /* namespace js */
