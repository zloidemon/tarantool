#include "fiber.h"

#include <say.h>
#include <exception.h>
#include <fiber.h>
#include <scoped_guard.h>

#include "init.h"

namespace js {
namespace fiber {

namespace { /* anonymous */

void
FiberWrapper(va_list ap)
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

	auto scope_guard = make_scoped_guard([&]{
		fun.Dispose();
		thiz.Dispose();
	});

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

	v8::TryCatch try_catch;
	(void) fun2->CallAsFunction(thiz2, fun_argc, fun_argv_arr);
	if (try_catch.HasCaught()) {
		v8::Local<v8::Object> e = js::FillException(&try_catch);
		js::LogException(e);
	}
}

void
Call(const v8::FunctionCallbackInfo<v8::Value>& args)
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
	struct fiber *f = fiber_new("js", FiberWrapper);

	/* Save fiber id as a public member of the object */
	thiz->Set(v8::String::NewSymbol("id"),
		  v8::Integer::NewFromUnsigned(f->fid));

	fiber_call(f, js::JS::GetCurrent(), &args);

	args.GetReturnValue().Set(thiz);
}

void
SleepMethod(const v8::FunctionCallbackInfo<v8::Value>& args)
{
	v8::HandleScope handle_scope;

	if (!args.IsConstructCall() && args.Length() == 0) {
		fiber_sleep(TIMEOUT_INFINITY);
		return;
	} else if (!args.IsConstructCall() && args.Length() == 1 &&
		   args[0]->IsNumber() && args[0]->NumberValue() >= 0.0) {
		fiber_sleep(args[0]->NumberValue());
		return;
	}

	v8::ThrowException(
		v8::String::New("Usage: fiber.sleep([timeout >= 0])"));
}

void
NameGetter(v8::Local<v8::String> property,
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
NameSetter(v8::Local<v8::String> property, v8::Local<v8::Value> value,
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
StateGetter(v8::Local<v8::String> property,
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
GetTemplate()
{
	v8::HandleScope handle_scope;

	v8::Local<v8::FunctionTemplate> tmpl =
			v8::FunctionTemplate::New(Call);

	tmpl->SetClassName(v8::String::NewSymbol("fiber"));

	tmpl->Set(v8::String::NewSymbol("sleep"),
		   v8::FunctionTemplate::New(SleepMethod));
	tmpl->Set(v8::String::NewSymbol("TIMEOUT_INFINITY"),
		  v8::Number::New(TIMEOUT_INFINITY));

	tmpl->InstanceTemplate()->SetAccessor(v8::String::NewSymbol("name"),
		NameGetter, NameSetter);
	tmpl->InstanceTemplate()->SetAccessor(v8::String::NewSymbol("state"),
		StateGetter);

	return handle_scope.Close(tmpl);
}

} /* namespace (anonymous) */

v8::Local<v8::Object>
Exports()
{
	v8::HandleScope handle_scope;
	return handle_scope.Close(GetTemplate()->GetFunction());
}

} /* namespace fiber */
} /* namespace js */
