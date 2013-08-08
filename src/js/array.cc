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

#include "array.h"

#include <string.h>
#include <assert.h>

#define ASSERT assert
#define V8_SHARED 1
#pragma GCC visibility push(hidden)
#include "third_party/js-lib/array.h"
#include "third_party/js-lib/array.cc"
#pragma GCC visibility pop

namespace js {
namespace array {

static v8::Handle<v8::Value>
call_cb(const v8::Arguments& args)
{
	(void) args;

	return v8::Local<v8::Value>();
}

v8::Handle<v8::FunctionTemplate>
constructor()
{
	/* A new handle scope (again) */
	v8::HandleScope handle_scope;

	/* A new functional template to return */
	v8::Local<v8::FunctionTemplate> tmpl =
			v8::FunctionTemplate::New(call_cb);

	v8::Shell::InitializeTemplate(tmpl);
	return handle_scope.Close(tmpl);
}

} /* namespace stub */
} /* namespace js */
