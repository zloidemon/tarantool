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

namespace { /* anonymous */

void
SayMethod(const v8::FunctionCallbackInfo<v8::Value>& args)
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

void
Call(const v8::FunctionCallbackInfo<v8::Value>& args)
{
	if (args.IsConstructCall()) {
		v8::ThrowException(v8::Exception::Error(
			v8::String::New("Constructor call")));
		return;
	}
}

v8::Handle<v8::FunctionTemplate>
GetTemplate()
{
	v8::HandleScope handle_scope;
	v8::Local<v8::FunctionTemplate> tmpl =
			v8::FunctionTemplate::New(Call);
	tmpl->InstanceTemplate()->SetInternalFieldCount(1);
	tmpl->SetClassName(v8::String::NewSymbol("platform"));

	v8::Local<v8::FunctionTemplate> say_tmpl =
			v8::FunctionTemplate::New(SayMethod);
	say_tmpl->Set(v8::String::NewSymbol("FATAL"),
		      v8::Integer::New((uint32_t) S_FATAL));
	say_tmpl->Set(v8::String::NewSymbol("ERROR"),
		      v8::Integer::New((uint32_t) S_ERROR));
	say_tmpl->Set(v8::String::NewSymbol("CRIT"),
		      v8::Integer::New((uint32_t) S_CRIT));
	say_tmpl->Set(v8::String::NewSymbol("WARN"),
		      v8::Integer::New((uint32_t) S_WARN));
	say_tmpl->Set(v8::String::NewSymbol("INFO"),
		      v8::Integer::New((uint32_t) S_INFO));
	say_tmpl->Set(v8::String::NewSymbol("DEBUG"),
		      v8::Integer::New((uint32_t) S_DEBUG));

	tmpl->Set(v8::String::NewSymbol("say"), say_tmpl);


	return handle_scope.Close(tmpl);
}

} /* namespace (anonymous) */

v8::Handle<v8::Object>
js::platform::Exports()
{
	v8::HandleScope handle_scope;
	return handle_scope.Close(GetTemplate()->GetFunction());
}
