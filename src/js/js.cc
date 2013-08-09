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

#include "js.h"

#include <say.h>

#include "require.h"
#include "platform.h"
#include "stub.h"
#include "fiber.h"

namespace js {

v8::Handle<v8::Value>
init_global(v8::Handle<v8::Object> global)
{
	v8::Local<v8::Function> require = js::require::constructor()->
					 GetFunction();
	global->Set(v8::String::NewSymbol("require"), require);

	/* Put built-in modules to the 'require' cache */
	js::require::cache_put(require, v8::String::NewSymbol("platform"),
			       js::platform::constructor()->GetFunction());
	js::require::cache_put(require, v8::String::NewSymbol("stub"),
			       js::stub::constructor()->GetFunction());
	js::require::cache_put(require, v8::String::NewSymbol("fiber"),
			       js::fiber::constructor()->GetFunction());
	return v8::Handle<v8::Value>();
}

v8::Handle<v8::Value>
copy_object(v8::Handle<v8::Object> dst, v8::Handle<v8::Object> src)
{
	v8::HandleScope handle_scope;

	v8::Local<v8::Array> props = src->GetPropertyNames();
	const uint32_t length = props->Length();
	for (uint32_t i = 0; i < length; i++) {
		v8::Local<v8::Value> key = props->Get(i);
		v8::Local<v8::Value> value = src->Get(key);
		dst->Set(key, value);
	}

	return v8::Handle<v8::Value>();
}

v8::Handle<v8::Value>
dump_object(v8::Handle<v8::Object> src)
{
	v8::HandleScope handle_scope;

	v8::String::Utf8Value name_utf8(src->GetConstructorName());
	say_debug("Object '%.*s' dump", name_utf8.length(), *name_utf8);

	v8::Local<v8::Array> props = src->GetPropertyNames();
	const uint32_t length = props->Length();
	for (uint32_t i = 0; i < length; i++) {
		v8::Local<v8::Value> key = props->Get(i);
		v8::Local<v8::Value> value = src->Get(key);
		v8::String::Utf8Value key_utf8(key);
		v8::String::Utf8Value value_utf8(value);

		say_debug("%.*s => %.*s", key_utf8.length(), *key_utf8,
			  value_utf8.length(), *value_utf8);
	}

	return v8::Handle<v8::Value>();
}

}  /* namespace js */
