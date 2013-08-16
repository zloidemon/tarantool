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

#include "js.h"
#include "require.h"

/**
 *  tarantool start-up module
 */
const char *TARANTOOL_JS_INIT_MODULE = "init";

using namespace js;

void
tarantool_js_load_cfg(js::JS *js,
		      struct tarantool_cfg *cfg)
{
	(void) js;
	(void) cfg;
}

static void
js_raise(v8::TryCatch *try_catch)
{
	v8::HandleScope handle_scope;

	assert(try_catch->HasCaught());

	v8::Local<v8::Message> message = try_catch->Message();
	v8::String::Utf8Value e_str(try_catch->Exception());
	const char *e_cstr = (*e_str != NULL) ? *e_str : "<empty>";

	if (message.IsEmpty()) {
		/* V8 didn't provide any extra information about this error */
		try_catch->Reset();
		tnt_raise(ClientError, ER_PROC_JS, e_cstr);
	}

	v8::String::Utf8Value filename_str(message->GetScriptResourceName());
	const char *filename_cstr = (*filename_str != NULL)
				    ? *filename_str : "unknown";
	int linenum = message->GetLineNumber();
#if 0
	_say(S_ERROR, filename_cstr, linenum, NULL, "JS %s", e_cstr);

	v8::String::Utf8Value source_str(message->GetSourceLine());
	const char *source_cstr = (*source_str != NULL)
					? *source_str
					: "<unknown source>";
	printf("%s", source_cstr);
	int start = message->GetStartColumn();
	for (int i = 0; i < start; i++) {
		printf(" ");
	}
	int end = message->GetEndColumn();
	for (int i = start; i < end; i++) {
		printf("^");
	}
	printf("\n");
#endif

	v8::String::Utf8Value trace_str(try_catch->StackTrace());
	assert(*trace_str != NULL);
	const char *trace_cstr = *trace_str;

	/* Extract the first line from the stack trace */
	enum { LINE_BUF_SIZE = 1024 };
	char line[LINE_BUF_SIZE];
	size_t line_len = strcspn(trace_cstr, "\r\n");
	if (line_len == 0) {
		strcpy(line, "UnknownError");
	} else if (line_len < LINE_BUF_SIZE) {
		strncpy(line, trace_cstr, line_len);
		line[line_len] = 0;
	} else {
		line_len = LINE_BUF_SIZE - 4;
		strncpy(line, trace_cstr, line_len);
		line[line_len] = 0;
		strcat(line, "...");
	}

	/* Log error */
	_say(S_ERROR, filename_cstr, linenum, NULL, "%s", line);
	/* Log JS stack trace */
	_say(S_ERROR, filename_cstr, linenum, NULL, "JS stack trace:\n%s",
	     trace_cstr);

	/* Raise tnt_Exception to upper levels */
	tnt_raise(ClientError, ER_PROC_JS, line);
}

/**
 * @brief Initialize JS library and load the init module
 */
static void
init_library_fiber(va_list ap)
{
	JS *js = va_arg(ap, JS *);

	/* Enable JS in the fiber */
	js->FiberEnsure();

	v8::HandleScope handle_scope;
	v8::TryCatch try_catch;
	try_catch.SetVerbose(true);

	v8::Local<v8::Object> ret = js::JS::GetCurrent()->LoadLibrary(
			v8::String::New(TARANTOOL_JS_INIT_MODULE));

	if (unlikely(try_catch.HasCaught())) {
		js_raise(&try_catch);
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
tarantool_js_eval(struct tbuf *out, const void *source, size_t source_size,
		  const char *source_origin)
{
	/* it does not work in sched thread */
	assert(fiber->fid > 1);

	JS *js = JS::GetCurrent();
	assert(js != NULL);

	v8::HandleScope handle_scope;
	v8::TryCatch try_catch;
	try_catch.SetVerbose(true);

	assert(source_size <= INT_MAX);
	v8::Local<v8::String> js_source =
			v8::String::New((const char *) source, (int) source_size);
	v8::Local<v8::String> js_source_origin;
	if (source_origin != NULL) {
		js_source_origin = v8::String::New(source_origin);
	}

	v8::Handle<v8::Value> result = EvalInContext(js_source,
		js_source_origin, v8::Context::GetCurrent());
	if (unlikely(try_catch.HasCaught())) {
		js_raise(&try_catch);
	}

	v8::String::Utf8Value output(result);
	tbuf_printf(out, "%.*s" CRLF, output.length(), *output);
}

