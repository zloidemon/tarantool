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

#include "platform.h"

#include <tarantool.h>
#include <cfg/tarantool_box_cfg.h>
#include <say.h>

namespace js {
namespace platform {

static void
say_cb(const v8::FunctionCallbackInfo<v8::Value>& args)
{
	v8::HandleScope handle_scope;

	int level = -1;
	if (args.Length() == 2 && args[0]->IsInt32()) {
		level = args[0].As<v8::Integer>()->Int32Value();
	}

	if (level < S_FATAL || level > S_DEBUG) {
		v8::ThrowException(v8::Exception::RangeError(
			v8::String::New("Invalid log level")));
		return;
	}

	v8::String::Utf8Value str(args[1]);
	_say(level, "js", 0, NULL, "%.*s", str.length(), *str);
	if (level == S_FATAL) {
		panic("js say(PANIC) was called");
	}

	return;
}

v8::Handle<v8::Value>
gc(void)
{
	while (!v8::V8::IdleNotification()) {
		say_info("js idle");
	}

	return v8::Handle<v8::Value>();
}

static void
gc_cb(const v8::FunctionCallbackInfo<v8::Value>& args)
{
	args.GetReturnValue().Set(gc());
}

v8::Handle<v8::Value>
eval_in_context(v8::Handle<v8::String> source,
		v8::Handle<v8::String> filename,
		v8::Handle<v8::Context> context)
{
	v8::HandleScope handle_scope;

	v8::Context::Scope context_scope(context);

	v8::Local<v8::Script> script = v8::Script::Compile(source, filename);
	if (script.IsEmpty()) {
		return v8::Handle<v8::Value>();
	}

	v8::Handle<v8::Value> ret = script->Run();
	if (ret.IsEmpty()) {
		return v8::Handle<v8::Value>();
	}

	return handle_scope.Close(ret);
}

v8::Handle<v8::Value>
eval_in_new_context(v8::Handle<v8::String> source,
		    v8::Handle<v8::String> filename,
		    v8::Handle<v8::Object> global)
{
	v8::HandleScope handle_scope;

	v8::Isolate *isolate = v8::Isolate::GetCurrent();
	v8::Local<v8::Context> context = v8::Context::New(isolate);

	v8::Local<v8::Object> global_proto = context->Global()->
					     GetPrototype()->ToObject();
	assert(!global_proto.IsEmpty());
	copy_object(global_proto, global);
	init_global(global_proto);

	v8::Handle<v8::Value> ret = eval_in_context(source, filename, context);
	if (ret.IsEmpty()) {
		return v8::Handle<v8::Value>();
	}

	copy_object(global, global_proto);

	return handle_scope.Close(ret);
}

static void
call_cb(const v8::FunctionCallbackInfo<v8::Value>& args)
{
	if (args.IsConstructCall()) {
		v8::ThrowException(v8::Exception::Error(
			v8::String::New("Constructor call")));
		return;
	}
}

v8::Handle<v8::FunctionTemplate>
constructor()
{
	v8::HandleScope handle_scope;
	v8::Local<v8::FunctionTemplate> tmpl =
			v8::FunctionTemplate::New(call_cb);
	tmpl->InstanceTemplate()->SetInternalFieldCount(1);
	tmpl->SetClassName(v8::String::NewSymbol("platform"));

	v8::Local<v8::FunctionTemplate> log_tmpl =
			v8::FunctionTemplate::New(say_cb);
	log_tmpl->Set(v8::String::NewSymbol("FATAL"),
		      v8::Integer::New((uint32_t) S_FATAL));
	log_tmpl->Set(v8::String::NewSymbol("ERROR"),
		      v8::Integer::New((uint32_t) S_ERROR));
	log_tmpl->Set(v8::String::NewSymbol("CRIT"),
		      v8::Integer::New((uint32_t) S_CRIT));
	log_tmpl->Set(v8::String::NewSymbol("WARN"),
		      v8::Integer::New((uint32_t) S_WARN));
	log_tmpl->Set(v8::String::NewSymbol("INFO"),
		      v8::Integer::New((uint32_t) S_INFO));
	log_tmpl->Set(v8::String::NewSymbol("DEBUG"),
		      v8::Integer::New((uint32_t) S_DEBUG));

	tmpl->Set(v8::String::NewSymbol("say"),
				log_tmpl);

	tmpl->Set(v8::String::NewSymbol("gc"),
				v8::FunctionTemplate::New(gc_cb));

	return handle_scope.Close(tmpl);
}

} /* namespace platform */
} /* namespace js */
