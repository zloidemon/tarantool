#include "fiber.h"

#include <say.h>
#include <exception.h>
#include <fiber.h>

#include "init.h"

namespace js {
namespace fiber {

static void
fiber_js_wrapper(va_list ap)
{
	js::JS *js = va_arg(ap, js::JS *);
	js->FiberOnStart();

	v8::HandleScope handle_scope;

	const v8::FunctionCallbackInfo<v8::Value>& args =
			*va_arg(ap, const v8::FunctionCallbackInfo<v8::Value> *);

	v8::Isolate *isolate = v8::Isolate::GetCurrent();
	v8::Persistent<v8::Object> thiz(isolate, args.This());
	v8::Persistent<v8::Function> fun(isolate, args[0].As<v8::Function>());
	v8::Local<v8::Array> fun_argv_jsarr;

	uint32_t fun_argc = args.Length() - 1;
	if (fun_argc > 0) {
		fun_argv_jsarr = v8::Array::New(fun_argc);

		for (uint32_t i = 0; i < fun_argc; i++) {
			fun_argv_jsarr->Set(i, args[i + 1]);
		}
	}

	assert(!thiz.IsEmpty());
	assert(!fun.IsEmpty());

	/* Finish with initialization and return control to the caller  */
	fiber_sleep(0.0);

	/* Run callback */
	auto fun2 = v8::Local<v8::Function>::New(isolate, fun);
	auto thiz2 = v8::Local<v8::Object>::New(isolate, thiz);
	v8::Handle<v8::Value> *fun_argv_arr = NULL;
	if (fun_argc > 0) {
		fun_argv_arr = new v8::Local<v8::Value>[fun_argc];

		for (uint32_t i = 0; i < fun_argc; i++) {
			fun_argv_arr[i] = v8::Local<v8::Value>::New(
				fun_argv_jsarr->Get(i));
		}
	}

	(void) fun2->CallAsFunction(thiz2, fun_argc, fun_argv_arr);
	fun.Dispose();
	thiz.Dispose();
}

static void
call_cb(const v8::FunctionCallbackInfo<v8::Value>& args)
{
	v8::HandleScope handle_scope;

	if (!args.IsConstructCall() ||
		   args.Length() < 1 || !args[0]->IsFunction()) {
		v8::ThrowException(v8::Exception::TypeError(
			v8::String::New("Usage: new fiber(fun, [arg])")));
		return;
	}

	v8::Local<v8::Object> thiz = args.This();

	/* Call from the user */
	struct fiber *f = fiber_new("js", fiber_js_wrapper);

	/* Save fiber id as a public member of the object */
	thiz->Set(v8::String::NewSymbol("id"),
		  v8::Integer::NewFromUnsigned(f->fid));

	fiber_call(f, js::JS::GetCurrent(), &args);

	args.GetReturnValue().Set(thiz);
}

static v8::Local<v8::Value>
sleep(v8::Handle<v8::Object> thiz, v8::Handle<v8::Number> timeout)
{
	(void) thiz;
	fiber_sleep(timeout->NumberValue());
	return v8::Local<v8::Value>();
}

static void
sleep_cb(const v8::FunctionCallbackInfo<v8::Value>& args)
{
	v8::HandleScope handle_scope;

	if (!args.IsConstructCall() && args.Length() == 0) {
		auto ret = sleep(args.This(),
				 v8::Number::New(TIMEOUT_INFINITY));
		args.GetReturnValue().Set(ret);
		return;
	} else if (!args.IsConstructCall() && args.Length() == 1 &&
		   args[0]->IsNumber() && args[0]->NumberValue() >= 0.0) {
		auto ret = sleep(args.This(), args[0]->ToNumber());
		args.GetReturnValue().Set(ret);
		return;
	}

	v8::ThrowException(
		v8::String::New("Usage: fiber.sleep([timeout >= 0])"));
}

void
name_getter_cb(v8::Local<v8::String> property,
	       const v8::PropertyCallbackInfo<v8::Value>& args)
{
	(void) property;

	v8::HandleScope handle_scope;
	uint32_t fid = args.This()->Get(v8::String::NewSymbol("id"))->
		       Int32Value();
	struct fiber *f = fiber_find(fid);
	if (!f) {
		v8::ThrowException(v8::Exception::Error(
			v8::String::New("Can not find the fiber")));
		return;
	}

	args.GetReturnValue().Set(v8::String::New(fiber_name(f)));
}

void
name_setter_cb(v8::Local<v8::String> property, v8::Local<v8::Value> value,
	       const v8::PropertyCallbackInfo<void>& args)
{
	(void) property;

	v8::HandleScope handle_scope;
	uint32_t fid = args.This()->Get(v8::String::NewSymbol("id"))->
		       Int32Value();
	struct fiber *f = fiber_find(fid);
	if (!f) {
		v8::ThrowException(v8::Exception::Error(
			v8::String::New("Can not find the fiber")));
		return;
	}

	v8::String::Utf8Value name(value);
	fiber_set_name(f, *name);
}

void
state_getter_cb(v8::Local<v8::String> property,
	       const v8::PropertyCallbackInfo<v8::Value>& args)
{
	(void) property;

	v8::HandleScope handle_scope;
	uint32_t fid = args.This()->Get(v8::String::NewSymbol("id"))->
		       Int32Value();

	v8::Local<v8::String> ret;
	if (fid == fiber_self()->fid) {
		ret = v8::String::New("RESUMED");
	} else {
		struct fiber *f = fiber_find(fid);
		if (f != NULL) {
			ret = v8::String::New("PAUSED");
		} else {
			ret = v8::String::New("STOPPED");
		}
	}

	args.GetReturnValue().Set(ret);
}

v8::Handle<v8::FunctionTemplate>
constructor()
{
	v8::HandleScope handle_scope;

	v8::Local<v8::FunctionTemplate> tmpl =
			v8::FunctionTemplate::New(call_cb);

	tmpl->SetClassName(v8::String::NewSymbol("fiber"));

	tmpl->Set(v8::String::NewSymbol("sleep"),
		   v8::FunctionTemplate::New(sleep_cb));
	tmpl->Set(v8::String::NewSymbol("TIMEOUT_INFINITY"),
		  v8::Number::New(TIMEOUT_INFINITY));

	tmpl->InstanceTemplate()->SetAccessor(v8::String::NewSymbol("name"),
		name_getter_cb, name_setter_cb);
	tmpl->InstanceTemplate()->SetAccessor(v8::String::NewSymbol("state"),
		state_getter_cb);

	return handle_scope.Close(tmpl);
}

} /* namespace fiber */
} /* namespace js */
