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

#include <say.h>
#include "../tuple.h"

namespace js {
namespace box {
namespace tuple {

namespace { /* anonymous */

inline struct tuple *
Unwrap(v8::Local<v8::Object> thiz)
{
	assert(thiz->InternalFieldCount() > 0);
	struct tuple *tuple = (struct tuple *)
		thiz-> GetAlignedPointerFromInternalField(0);
	assert(tuple != NULL);
	return tuple;
}

void
GC(v8::Isolate* isolate, v8::Persistent<v8::Object>* value, struct tuple *tuple)
{
	(void) isolate;

	/* Destroy v8 persistent handle */
	assert(value->IsNearDeath());
	value->Dispose();
	value->Clear();

	/* Hint v8 GC that actually sizeof(*obj) of memory is freeing */
	v8::V8::AdjustAmountOfExternalAllocatedMemory(
		-(sizeof(*tuple) + tuple->bsize));

	/* Destroy our native object */
	tuple_ref(tuple, -1);
}

void
Create(v8::Local<v8::Object> thiz, struct tuple *tuple)
{
	assert(thiz->InternalFieldCount() > 0);
	thiz->SetAlignedPointerInInternalField(0, tuple);
	tuple_ref(tuple, 1);

	/* Create a ArrayBuffer for fields */
	v8::Local<v8::ArrayBuffer> buf = v8::ArrayBuffer::New(
				tuple->data, tuple->bsize);
	thiz->SetInternalField(1, buf);

	/* Hint v8 GC about the fact that a litle bit more memory is used. */
	v8::V8::AdjustAmountOfExternalAllocatedMemory(
		+sizeof(*tuple) + tuple->bsize);

	/* Create a new v8 persistent handle */
	v8::Persistent<v8::Object> handle(v8::Isolate::GetCurrent(), thiz);

	/* Set v8 GC callback */
	handle.MakeWeak(tuple, GC);
	handle.MarkIndependent();
}

void
Call(const v8::FunctionCallbackInfo<v8::Value>& args)
{
	if (!args.IsConstructCall()) {
		v8::ThrowException(v8::Exception::Error(
			v8::String::New("Please use 'new'!")));
	}

	JS_BEGIN();

	v8::Local<v8::Array> tuple_arg;
	if (args.Length() == 1 && args[0]->IsArray()) {
		/* Create tuple from an array */
		tuple_arg = args[0].As<v8::Array>();
	} else if (args.Length() == 1 &&
		   Inherits(args.Holder()->GetConstructor(), args[0])) {
		/* Create a tuple from another tuple */
		struct tuple *tuple = Unwrap(args[0]->ToObject());
		Create(args.Holder(), tuple);
		return;
	} else {
		/* Create tuple from scalars */
		tuple_arg = v8::Array::New(args.Length());
		for (uint32_t i = 0; i < args.Length(); i++) {
			tuple_arg->Set(i, args[i]);
		}
	}

	assert(!tuple_arg.IsEmpty());
	uint32_t tuple_len = BerSizeOf(tuple_arg);
	if (tuple_len == UINT32_MAX)
		return;

	struct tuple *tuple = NULL;
	tuple = tuple_alloc(tuple_format_ber, tuple_len);

	tuple->field_count = tuple_arg->Length();
	char *b = tuple->data;
	b = BerPack(tuple->data, tuple_arg);
	assert(b == tuple->data + tuple->bsize);
	(void) b;

	Create(args.Holder(), tuple);

	JS_END();
}

void
ArityPropertyGet(v8::Local<v8::String> property,
		 const v8::PropertyCallbackInfo<v8::Value>& args)
{
	(void) property;

	struct tuple *tuple = Unwrap(args.Holder());
	args.GetReturnValue().Set(v8::Uint32::New(tuple->field_count));
}

void
IndexedEnumerate(const v8::PropertyCallbackInfo<v8::Array>& args)
{
	v8::HandleScope handle_scope;

	struct tuple *tuple = Unwrap(args.Holder());
	v8::Local<v8::Array> indexes = v8::Array::New(tuple->field_count);
	for (uint32_t i = 0; i < tuple->field_count; i++) {
		indexes->Set(i, v8::Uint32::New(i));
	}

	args.GetReturnValue().Set(indexes);
}

void
Get(uint32_t index, const v8::PropertyCallbackInfo<v8::Value>& args)
{
	v8::HandleScope handle_scope;
	struct tuple *tuple = Unwrap(args.Holder());

	if (index >= tuple->field_count) {
		args.GetReturnValue().SetUndefined();
		return;
	}

	assert(args.Holder()->InternalFieldCount() > 1);
	v8::Local<v8::ArrayBuffer> buf = args.Holder()->GetInternalField(1).
			As<v8::ArrayBuffer>();
	assert(!buf.IsEmpty());

	uint32_t field_len = 0;
	const char *field = tuple_field(tuple, index, &field_len);

	size_t offset = field - tuple->data;

#if 0
	v8::Local<v8::DataView> view =
			v8::DataView::New(buf, offset, field_len);
#endif

	/* Return Int8Array for this field */
	v8::Local<v8::Int8Array> view =
			v8::Int8Array::New(buf, offset, field_len);
	args.GetReturnValue().Set(view);
}

void
CreateTemplate(v8::Local<v8::FunctionTemplate> tmpl)
{
	tmpl->InstanceTemplate()->SetInternalFieldCount(2);

	tmpl->SetClassName(v8::String::NewSymbol("Box.Tuple"));
	tmpl->InstanceTemplate()->SetAccessor(v8::String::NewSymbol("arity"),
		ArityPropertyGet);

	tmpl->InstanceTemplate()->SetIndexedPropertyHandler(Get,
		0, 0, 0, IndexedEnumerate);
}

v8::Local<v8::FunctionTemplate>
GetTemplate()
{
	v8::HandleScope handle_scope;

	intptr_t tmpl_key = (intptr_t) &GetTemplate;
	JS *js = JS::GetCurrent();
	v8::Local<v8::FunctionTemplate> tmpl = js->TemplateCacheGet(tmpl_key);
	if (!tmpl.IsEmpty())
		return handle_scope.Close(tmpl);

	tmpl = v8::FunctionTemplate::New(Call);

	CreateTemplate(tmpl);

	js->TemplateCacheSet(tmpl_key, tmpl);
	return handle_scope.Close(tmpl);
}

v8::Local<v8::FunctionTemplate>
GetTemplateLite()
{
	v8::HandleScope handle_scope;

	intptr_t tmpl_key = (intptr_t) &GetTemplateLite;
	JS *js = JS::GetCurrent();
	v8::Local<v8::FunctionTemplate> tmpl = js->TemplateCacheGet(tmpl_key);
	if (!tmpl.IsEmpty())
		return handle_scope.Close(tmpl);

	tmpl = v8::FunctionTemplate::New();
	tmpl->Inherit(GetTemplate());

	CreateTemplate(tmpl);

	js->TemplateCacheSet(tmpl_key, tmpl);
	return handle_scope.Close(tmpl);
}

} /* namespace (anonymous) */

v8::Local<v8::Object>
Exports()
{
	return GetTemplate()->GetFunction();
}

v8::Local<v8::Object>
GetConstructor()
{
	return GetTemplateLite()->GetFunction();
}

v8::Local<v8::Object>
NewInstance(v8::Local<v8::Function> constructor, struct tuple *tuple)
{
	v8::HandleScope handle_scope;

	v8::Local<v8::Object> thiz = constructor->NewInstance();
	Create(thiz, tuple);
	return handle_scope.Close(thiz);
}

} /* namespace tuple */
} /* namespace box */
} /* namespace js */
