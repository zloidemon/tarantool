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

#include "console.h"

#include <tarantool.h>
#include <cfg/tarantool_box_cfg.h>
#include <say.h>

extern char console_js[];

namespace js {
namespace console {
namespace { /* anonymous */

void
SayMethod(const v8::FunctionCallbackInfo<v8::Value>& args)
{
	v8::HandleScope handle_scope;

	if (args.Length() < 3 || !args[0]->IsInt32() || !args[1]->IsString() ||
			!args[2]->IsUint32()) {
		v8::ThrowException(v8::Exception::Error(
			v8::String::New("Usage: say(level, "
					"fileName, lineNumber)")));
		return;
	}

	int level = args[0]->Int32Value();
	if (level < S_FATAL || level > S_DEBUG) {
		v8::ThrowException(v8::Exception::RangeError(
			v8::String::New("Invalid log level")));
		return;
	}

	v8::String::Utf8Value filename_utf8(args[1]);
	uint32_t linenumber = args[2]->Uint32Value();

	v8::String::Utf8Value message_utf8(args[3]);
	_say(level, *filename_utf8, linenumber, NULL, "%s", *message_utf8);
	if (level == S_FATAL) {
		panic("panic() was called from JS: %s", *message_utf8);
	}

	args.GetReturnValue().Set(args[3]);
}

v8::Handle<v8::FunctionTemplate>
GetTemplate()
{
	v8::HandleScope handle_scope;
	v8::Local<v8::FunctionTemplate> tmpl = v8::FunctionTemplate::New();

	tmpl->SetClassName(v8::String::NewSymbol("Console"));

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

	tmpl->PrototypeTemplate()->Set(v8::String::NewSymbol("say"), say_tmpl);

	return handle_scope.Close(tmpl);
}

} /* namespace (anonymous) */

v8::Handle<v8::Object>
Exports()
{
	v8::HandleScope handle_scope;
	/* Load JavaScript source to alter native object */
	v8::Local<v8::String> exports_key = v8::String::NewSymbol("exports");
	v8::Local<v8::Object> globals = v8::Object::New();
	globals->Set(exports_key, GetTemplate()->GetFunction()->NewInstance());
	v8::Local<v8::String> source = v8::String::New(console_js);
	v8::Local<v8::String> filename = v8::String::New("console.js");
	EvalInNewContext(source, filename, globals);
	return handle_scope.Close(globals->Get(exports_key)->ToObject());
}

} /* namespace console */
} /* namespace js */
