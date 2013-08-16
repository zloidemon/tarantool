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

#include "stub.h"

#include <say.h>
#include <exception.h>

/**
 * @brief A native object
 */
struct stub {
	int test;
};

/* Use an anonymous namespace to hide all internal symbols (like 'static') */
namespace { /* anonymous */

/**
 * @brief This is a GC callback. V8 will invoke it then your object is not more
 * needed.
 * @param value v8 handle
 * @param data is your native object
 */
void
GC(v8::Isolate* isolate, v8::Persistent<v8::Object>* value, struct stub *obj)
{
	(void) isolate;
	/*
	 * Check that v8 GC is not joking and handle is really going to
	 * be destroyed. Please check just to be sure.
	 */
	assert(value->IsNearDeath());
	/* Destroy v8 handle */
	value->Dispose();
	value->Clear();

	/* Hint v8 GC that actually sizeof(*obj) of memory is freeing */
	v8::V8::AdjustAmountOfExternalAllocatedMemory(-(intptr_t) sizeof(*obj));

	/* Destroy our native object */
	delete obj;
}

/**
 * @brief Invoked by v8 when an user calls you function.
 * You should create a new object with native user data only on 'new'
 * constructor call. On a regular call you can return anything you want.
 * @param args
 * @return
 */
void
Call(const v8::FunctionCallbackInfo<v8::Value>& args)
{
	if (!args.IsConstructCall()) {
		v8::ThrowException(v8::Exception::Error(
			v8::String::New("Use the 'new', Luke!")));
		return;
	}

	v8::Isolate *isolate = v8::Isolate::GetCurrent();
	/* A handle scope. All v8::Local will die on the destructor call */
	v8::HandleScope handle_scope;

	/* Create a new native object */
	struct stub *stub = new struct stub;
	/* Initialize the object */
	stub->test = 0;

	/* Set the native object in the handle */
	assert(args.Holder()->InternalFieldCount() > 0);
	args.Holder()->SetInternalField(0, v8::External::New(stub));

	/* Hint v8 GC about the fact that a litle bit more memory is used. */
	v8::V8::AdjustAmountOfExternalAllocatedMemory(+1000* sizeof(*stub));

	/* Create a new v8 handle */
	v8::Persistent<v8::Object> handle(isolate, args.Holder());

	/* Set v8 GC callback */
	handle.MakeWeak(stub, GC);
	handle.MarkIndependent();

	/* 'this' returned by default */
}

/**
 * @brief An attached callback example.
 */
void
AddMethod(const v8::FunctionCallbackInfo<v8::Value>& args)
{
	v8::HandleScope handle_scope;

	/* Check that method is attached to native object */
	if (args.This() != args.Holder()) {
		v8::ThrowException(v8::Exception::Error(
			v8::String::New("Method was deattached")));
		return;
	}

	/* Check arguments */
	if (args.Length() != 1 || !args[0]->IsInt32()) {
		v8::ThrowException(v8::Exception::Error(
			v8::String::New("Invalid arguments")));
		return;
	}

	/* Get your native object from 'this' */
	assert(args.Holder()->InternalFieldCount() > 0);
	v8::Local<v8::External> ext = args.Holder()->GetInternalField(0).
			As<v8::External>();
	struct stub *wrapper = static_cast<struct stub *>(ext->Value());

	/* Do work */
	wrapper->test += args[0]->Int32Value();

	/* Return result.*/
	args.GetReturnValue().Set(v8::Integer::New(wrapper->test));
}

/**
 * @abstract Constructor method. Returns a new function template that is used
 * for construction new functions. First time when an user call
 * require('modname') the constructor is called and a new function is created.
 * The resulting function can be called in the usual way or can be used
 * in a constructor call (with 'new' keyword).
 */
v8::Local<v8::FunctionTemplate>
GetTemplate()
{
	/* A new handle scope (again) */
	v8::HandleScope handle_scope;

	/* A new functional template to return */
	v8::Local<v8::FunctionTemplate> tmpl = v8::FunctionTemplate::New(Call);

	/*
	 * Instance template is a template of the object that would be
	 * create on 'new' constructor call
	 */

	/* Initialize the user data */
	tmpl->InstanceTemplate()->SetInternalFieldCount(1);

	/* Give a name for it */
	tmpl->SetClassName(v8::String::NewSymbol("Stub"));

	/* Add a method to object prototype */
	tmpl->PrototypeTemplate()->Set(v8::String::NewSymbol("add"),
				       v8::FunctionTemplate::New(AddMethod));

	/* Return the new template to the upper scope  */
	return handle_scope.Close(tmpl);
}

} /* namespace (anonymous) */

v8::Local<v8::Object>
js::stub::Exports()
{
	v8::HandleScope handle_scope;
	return handle_scope.Close(GetTemplate()->GetFunction());
}
