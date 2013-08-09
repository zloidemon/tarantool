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

/* Namespaces */
namespace js {
namespace stub {

/* Define all field names in one place */

static const char CLAZZ_NAME[] = "stub";
static const char FUN_ADD_NAME[] = "add";

/**
 * @brief An our native object
 */
struct stub {
	int test;
};

/**
 * @brief This is a GC callback. V8 will invoke it then your object is not more
 * needed.
 * @param value v8 handle
 * @param data is your native object
 */
static void stub_js_gc(v8::Isolate* isolate,
		       v8::Persistent<v8::Object>* value, struct stub *obj)
{
	say_warn("stub gc: %p", obj);
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

/* Part of the call_cb */
static v8::Handle<v8::Value>
ctor(v8::Handle<v8::Object> thiz)
{
	v8::Isolate *isolate = v8::Isolate::GetCurrent();
	/* A handle scope. All v8::Local will die on the destructor call */
	v8::HandleScope handle_scope;

	/* Create a new native object */
	struct stub *stub = new struct stub;
	/* Initialize the object */
	stub->test = 0;

	say_warn("new: %p", stub);

	/* Set newly create userdata */
	userdata_set(thiz, stub);

	/* Hint v8 GC about the fact that a litle bit more memory is used. */
	v8::V8::AdjustAmountOfExternalAllocatedMemory(+1000* sizeof(*stub));

	/* Create a new v8 handle */
	v8::Persistent<v8::Object> handle(isolate, thiz);

	/* Set v8 GC callback */
	handle.MakeWeak(stub, stub_js_gc);
	handle.MarkIndependent();


	/* Return the new handle to the user */
	return handle_scope.Close(thiz);
}

/**
 * @brief Invoked by v8 when an user calls you function.
 * You should create a new object with native user data only on 'new'
 * constructor call. On a regular call you can return anything you want.
 * @param args
 * @return
 */
static void
call_cb(const v8::FunctionCallbackInfo<v8::Value>& args)
{
	/* If it is a contructor call then construct a new object */
	if (args.IsConstructCall()) {
		/* Parse arguments and call the constructor */
		/* args.This() here! */
		v8::Handle<v8::Value> ret = ctor(args.This());
		args.GetReturnValue().Set(ret);
		return;
	}

	/* Otherwise return something else */
	v8::ThrowException(v8::Exception::Error(
		v8::String::New("Use the 'new', Luke!")));
}

/**
 * @brief Native method. An actual implementation that can be called somethere
 * from the C++ code.
 * @param thiz JS 'this' handle
 * @param a parsed arguments
 * @return a result to return to the caller
 */
v8::Handle<v8::Value> /* v8::Handle, not v8::Local */
add(v8::Handle<v8::Object> thiz, v8::Handle<v8::Integer> a)
{

	v8::HandleScope handle_scope;

	/* Some kind of magic. Get your native object from 'this' */
	struct stub *wrapper = userdata_get<struct stub *>(thiz);

	/* Do work */
	wrapper->test += a->Int32Value();

	/* Return result. Close argument will be place to upper scope.*/
	return handle_scope.Close(v8::Integer::New(wrapper->test));
}

/**
 * @brief An attached callback example.
 */
static void
add_cb(const v8::FunctionCallbackInfo<v8::Value>& args)
{
	v8::HandleScope handle_scope;

	/* Check arguments */
	if (args.Length() != 1 || !args[0]->IsInt32()) {
		v8::ThrowException(v8::Exception::Error(
			v8::String::New("Invalid arguments")));
		return;
	}

	/* An additional check needed to check if method was
	 * attached to another object */
	if (args.This() != args.Holder()) {
		v8::ThrowException(v8::Exception::Error(
			v8::String::New("Method is deattached")));
		return;
	}

	/* Convert arguments and call the actual implementation */
	v8::Handle<v8::Value> ret = add(args.This(), args[0].As<v8::Integer>());
	args.GetReturnValue().Set(ret);
}

/**
 * @abstract Constructor method. Returns a new function template that is used
 * for construction new functions. First time when an user call
 * require('modname') the constructor is called and a new function is created.
 * The resulting function can be called in the usual way or can be used
 * in a constructor call (with 'new' keyword).
 */
v8::Handle<v8::FunctionTemplate>
constructor()
{
	/* A new handle scope (again) */
	v8::HandleScope handle_scope;

	/* A new functional template to return */
	v8::Local<v8::FunctionTemplate> tmpl =
			v8::FunctionTemplate::New(call_cb);

	/*
	 * Instance template is a template of the object that would be
	 * create on 'new' constructor call
	 */

	/* Initialize the user data */
	userdata_init_template<struct stub *>(tmpl);

	/* Name it */
	tmpl->SetClassName(v8::String::NewSymbol(CLAZZ_NAME));

	/* Add a method to object prototype */
	tmpl->PrototypeTemplate()->Set(v8::String::NewSymbol(FUN_ADD_NAME),
				       v8::FunctionTemplate::New(add_cb));

	/* Return the new template to the upper levels */
	return handle_scope.Close(tmpl);
}

} /* namespace stub */
} /* namespace js */
