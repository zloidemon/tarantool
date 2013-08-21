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

#include "init.h"

#include <stdlib.h>
#include <stdbool.h>
#include <memory.h>
#include <sys/time.h>
#include <sys/types.h>
#include <pthread.h>
#include <limits.h>

#include <tarantool.h>
#include <cfg/tarantool_box_cfg.h>
#include <fio.h>
#include <fiber.h>
#include <tbuf.h>
#include <errcode.h>
#include <exception.h>
#include <say.h>
#include <scoped_guard.h>

#include "js.h"
#include "require.h"

/**
 *  tarantool start-up module
 */
const char *TARANTOOL_JS_INIT_MODULE = "init";

/**
 * @brief Initialize JS library and load the init module
 */
static void
init_library_fiber(va_list ap)
{
	js::JS *js = va_arg(ap, js::JS *);

	/* Enable JS in the fiber */
	js->FiberEnsure();

	v8::HandleScope handle_scope;
	v8::TryCatch try_catch;

	js::LoadModules();
	if (unlikely(try_catch.HasCaught())) {
		v8::Local<v8::Object> e = js::FillException(&try_catch);
		js::LogException(e);
	}

	v8::Local<v8::Object> require = js->GetRequire();
	v8::Local<v8::String> root_module =
			v8::String::New(TARANTOOL_JS_INIT_MODULE);
	v8::String::Utf8Value root_module_path_utf8(js::require::Resolve(
		require, root_module));
	if (*root_module_path_utf8 == NULL ||
	    access(*root_module_path_utf8, F_OK ) != 0) {
		/* a root module was not found */
		return;
	}

	v8::Handle<v8::Object> ret = js::require::Call(require, root_module,
						       false);

	if (unlikely(try_catch.HasCaught())) {
		v8::Local<v8::Object> e = js::FillException(&try_catch);
		js::LogException(e, false);
		panic("Cannot load library");
	}

	/* Display the list of modules in 'export' */
	say_info("List of exported methods:");
	size_t count = 0;
	v8::Local<v8::Array> props = ret->GetPropertyNames();
	const uint32_t length = props->Length();
	for (uint32_t i = 0; i < length; i++) {
		v8::Local<v8::Value> key = props->Get(i);
		v8::Local<v8::Value> value = ret->Get(key);
		if (!value->IsFunction()) {
			continue;
		}

		v8::String::Utf8Value key_utf8(key);
		say_info("\t%.*s", key_utf8.length(), *key_utf8);
		count++;
	}
	say_info("End of the list.");

	if (count == 0) {
		say_info("HINT: use 'exports' property in your JS startup "
			"module to export methods into iproto");
	}
}

void
tarantool_js_init_library(js::JS *js)
{
	struct fiber *loader = fiber_new("js-init-library", init_library_fiber);
	fiber_call(loader, js);
}

void
tarantool_js_admin(struct tbuf *out, const void *source, size_t source_size,
		   const char *source_origin)
{
	/* it does not work in sched thread */
	assert(fiber->fid > 1);

	js::JS *js = js::JS::GetCurrent();
	assert(js != NULL);

	v8::HandleScope handle_scope;
	v8::TryCatch try_catch;

	assert(source_size <= INT_MAX);
	v8::Local<v8::String> js_source =
			v8::String::New((const char *) source, (int) source_size);
	v8::Local<v8::String> js_source_origin;
	if (source_origin != NULL) {
		js_source_origin = v8::String::New(source_origin);
	}

	v8::Handle<v8::Value> ret = js::EvalInContext(js_source,
		js_source_origin, v8::Context::GetCurrent());
	if (unlikely(try_catch.HasCaught())) {
		v8::Local<v8::Object> ret2 = v8::Object::New();
		ret2->Set(v8::String::NewSymbol("error"),
			  js::FillException(&try_catch));
		ret = ret2;
	}

	/* TODO: replace JSON with YAML here */
	v8::Local<v8::Value> output = js::FormatJSON(ret);

	v8::String::Utf8Value output_utf8(output);
	tbuf_append(out, *output_utf8, output_utf8.length());
	tbuf_append(out, "\n", 1);
}
