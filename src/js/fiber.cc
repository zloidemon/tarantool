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
	struct tarantool_js *js = va_arg(ap, struct tarantool_js *);
	fiber_enable_js(js);

	v8::HandleScope handle_scope;

	v8::Persistent<v8::Object> thiz =
			va_arg(ap, v8::Persistent<v8::Object>);
	v8::Persistent<v8::Function> fun =
			va_arg(ap, v8::Persistent<v8::Function>);
	uint32_t fun_argc = va_arg(ap, uint32_t);
	v8::Persistent<v8::Value> *fun_argv =
			va_arg(ap, v8::Persistent<v8::Value> *);
	assert(!thiz.IsEmpty());
	assert(!fun.IsEmpty());

	/* Finish with initialization and return control to the caller  */
	fiber_sleep(0.0);

	/* Run callback */
	(void) fun->CallAsFunction(thiz, fun_argc, fun_argv);

	thiz.Dispose();
	fun.Dispose();
	if (fun_argc > 0) {
		for (uint32_t i = 0; i < fun_argc; i++) {
			fun_argv[i].Dispose();
		}
		delete fun_argv;
	}
}

static v8::Handle<v8::Value>
call_cb(const v8::Arguments& args)
{
	v8::HandleScope handle_scope;

	if (!args.IsConstructCall() ||
		   args.Length() < 1 || !args[0]->IsFunction()) {
		return v8::ThrowException(v8::Exception::TypeError(
			v8::String::New("Usage: new fiber(fun, [arg])")));
	}

	/* Call from the user */
	struct fiber *f = fiber_new("js", fiber_js_wrapper);

	v8::Persistent<v8::Object> thiz =
			v8::Persistent<v8::Object>::New(args.This());
	/* Save fiber id as a public member of the object */
	thiz->Set(v8::String::NewSymbol("id"),
		  v8::Integer::NewFromUnsigned(f->fid));

	v8::Persistent<v8::Function> fun = v8::Persistent<v8::Function>::New(
		args[0].As<v8::Function>());
	v8::Persistent<v8::Value> *fun_argv;

	uint32_t fun_argc = args.Length() - 1;
	if (fun_argc > 0) {
		fun_argv = new v8::Persistent<v8::Value>[fun_argc];
		for (uint32_t i = 0; i < fun_argc; i++) {
			fun_argv[i] = v8::Persistent<v8::Value>::New(
			args[i + 1]);
		}
	}

	fiber_call(f, fiber_self()->js, thiz, fun,
		   fun_argc, fun_argv);

	return handle_scope.Close(thiz);
}

static v8::Local<v8::Value>
sleep(v8::Handle<v8::Object> thiz, v8::Handle<v8::Number> timeout)
{
	(void) thiz;
	fiber_sleep(timeout->NumberValue());
	return v8::Local<v8::Value>();
}

v8::Handle<v8::Value>
sleep_cb(const v8::Arguments& args)
{
	v8::HandleScope handle_scope;

	if (!args.IsConstructCall() && args.Length() == 0) {
		auto ret = sleep(args.This(),
				 v8::Number::New(TIMEOUT_INFINITY));
		return handle_scope.Close(ret);
	} else if (!args.IsConstructCall() && args.Length() == 1 &&
		   args[0]->IsNumber() && args[0]->NumberValue() >= 0.0) {
		auto ret = sleep(args.This(), args[0]->ToNumber());
		return handle_scope.Close(ret);
	}

	return v8::ThrowException(
		v8::String::New("Usage: fiber.sleep([timeout >= 0])"));
}

v8::Handle<v8::Value>
name_getter_cb(v8::Local<v8::String> property, const v8::AccessorInfo& info)
{
	(void) property;

	v8::HandleScope handle_scope;
	uint32_t fid = info.This()->Get(v8::String::NewSymbol("id"))->
		       Int32Value();
	struct fiber *f = fiber_find(fid);
	if (!f) {
		return v8::ThrowException(v8::Exception::Error(
			v8::String::New("Can not find the fiber")));
	}
	return handle_scope.Close(v8::String::New(fiber_name(f)));
}

void
name_setter_cb(v8::Local<v8::String> property, v8::Local<v8::Value> value,
	       const v8::AccessorInfo& info)
{
	(void) property;

	v8::HandleScope handle_scope;
	uint32_t fid = info.This()->Get(v8::String::NewSymbol("id"))->
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

v8::Handle<v8::Value>
state_getter_cb(v8::Local<v8::String> property, const v8::AccessorInfo& info)
{
	(void) property;

	v8::HandleScope handle_scope;
	uint32_t fid = info.This()->Get(v8::String::NewSymbol("id"))->
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
	return handle_scope.Close(ret);
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
