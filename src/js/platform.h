#ifndef TARANTOOL_JS_LIB_PLATFORM_H_INCLUDED
#define TARANTOOL_JS_LIB_PLATFORM_H_INCLUDED

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

namespace js {
namespace platform {

v8::Handle<v8::FunctionTemplate>
constructor();

v8::Handle<v8::Value>
gc(void);

v8::Handle<v8::Value>
eval_in_context(v8::Handle<v8::String> source,
		v8::Handle<v8::String> filename,
		v8::Handle<v8::Context> context);

v8::Handle<v8::Value>
eval_in_new_context(v8::Handle<v8::String> source,
		    v8::Handle<v8::String> filename,
		    v8::Handle<v8::Object> global = v8::Handle<v8::Object>());

} /* namespace platform */
} /* namespace js */

#endif /* TARANTOOL_JS_LIB_PLATFORM_H_INCLUDED */
