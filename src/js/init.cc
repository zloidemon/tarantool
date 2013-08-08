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

#include <v8.h>

#include "js.h"
#include "platform.h"
#include "require.h"

/**
 *  tarantool start-up module
 */
const char *TARANTOOL_JS_INIT_MODULE = "init";

static struct ev_idle idle_watcher;


struct tarantool_js {
	v8::Persistent<v8::Context> context;
	v8::Isolate *isolate;
};

static void
tarantool_js_idle(EV_P_ struct ev_idle *w, int revents);

/*
 * The v8 TLS hack
 * The hack replaces pthread's local storage with our fiber local storage
 */

#if defined(ENABLE_V8_HACK_LINK_TIME)
/* Use link-time version of the hack */

/* It is known that the link-time version does not work if v8 was compiled with
 * visibility=hidden */

static volatile bool V8_THREAD_LOCAL_HACK_WORKS = false;

/* Overload some v8 internal symbols in this compilation unit */
#include "js_tls_hack_link_time.mm"

static void
js_tls_hack_init(void)
{
	if (!v8::V8::Initialize())
		goto error_1;

	if (V8_THREAD_LOCAL_HACK_WORKS) {
		say_debug("v8 TLS link-time hack works");
		return;
	}

error_1:
	panic("v8 TLS link-time hack does not work - "
	      "try to recompile without -DENABLE_V8_HACK_LINK_TIME=1");

	return;
}

#else /* !defined(ENABLE_V8_HACK_LINK_TIME) */

/* See fiber.h for V8_TLS_SIZE */
static pthread_key_t js_tls_pthread_keys[V8_TLS_SIZE];

static void
js_tls_hack_init(void)
{
	pthread_key_t last_key;
	size_t js_tls_size = 0;
	v8::Isolate *isolate;

	if (pthread_key_create(&last_key, NULL) != 0)
		goto error_1;

	enum { FLS_CHECK_MAX = 1024 };

	if (last_key > FLS_CHECK_MAX)
		goto error_2;

	void *fls[FLS_CHECK_MAX];
	for (pthread_key_t k = 0; k < last_key; k++) {
		fls[k] = pthread_getspecific(k);
	}

	/* Initialize v8 and see what is changed */
	if (!v8::V8::Initialize())
		goto error_2;

	isolate = v8::Isolate::GetCurrent();
	if (isolate == NULL)
		goto error_2;

	for (pthread_key_t k = 0; k < last_key; k++) {
		void *cur = pthread_getspecific(k);
		/*
		 * Check if the key was changed or if the key contains
		 * a pointer to the current v8::Isolate which could be created
		 * before our hack
		 */
		if (cur != fls[k] || cur == isolate) {
			js_tls_pthread_keys[js_tls_size++] = k;
		}
	}

	/* Check if we found enough number of keys */
	if (js_tls_size != V8_TLS_SIZE)
		goto error_2;

	say_debug("v8 TLS run-time hack works - "
		  "all %zu keys have been detected", js_tls_size);
	say_debug("v8 pthread's TLS <-> fiber's FLS mapping:");
	for (size_t t = 0; t < V8_TLS_SIZE; t++) {
		say_debug(" %u => %zu",
			  (uint32_t) js_tls_pthread_keys[t], t);
	}

	pthread_key_delete(last_key);

	return;

error_2:
	pthread_key_delete(last_key);
error_1:
	panic("Can not detect v8 TLS keys - the hack does not work :(");
}

/**
 * @brief Clear all v8 pthread's TLS data
 */
static void
js_tls_hack_pthread_clear(void)
{
	for (size_t t = 0; t < V8_TLS_SIZE; t++) {
		pthread_setspecific(js_tls_pthread_keys[t], NULL);
	}
}

/**
 * @brief Copy v8 pthread's TLS data to fiber's FLS
 */
static void
js_tls_hack_pthread_to_fiber(void)
{
	for (size_t t = 0; t < V8_TLS_SIZE; t++) {
		void *var = pthread_getspecific(js_tls_pthread_keys[t]);
		fiber->js_tls[t] = var;
	}
}

/**
 * @brief Copy v8 fiber's FLS data to pthread's TLS
 */
static void
js_tls_hack_fiber_to_pthread(void)
{
	for (size_t t = 0; t < V8_TLS_SIZE; t++) {
		pthread_setspecific(js_tls_pthread_keys[t], fiber->js_tls[t]);
	}
}

#endif /* defined(ENABLE_V8_HACK_LINK_TIME) */

void
tarantool_js_init()
{
	js_tls_hack_init();

	ev_idle_init(&idle_watcher, tarantool_js_idle);

	return;
}

void
tarantool_js_free()
{
	ev_idle_stop(&idle_watcher);
}

struct tarantool_js *
tarantool_js_new()
{
	assert(fiber->fid == 1);
	struct tarantool_js *js = new struct tarantool_js;

#if defined(JS_CUSTOM_ISOLATE)
	/* An experimental support of working in non-default isolate. */
	js->isolate = v8::Isolate::New();
	assert(js->isolate != NULL);

	v8::Locker locker(js->isolate);
	v8::Isolate::Scope isolate_scope(js->isolate);
#endif /* defined(JS_CUSTOM_ISOLATE) */

	js->context = v8::Context::New();
	assert(!js->context.IsEmpty());

	v8::Context::Scope context_scope(js->context);

	js->isolate = v8::Isolate::GetCurrent();

	return js;
}

void
tarantool_js_delete(struct tarantool_js *js)
{
	assert(fiber->fid == 1);

	if (v8::Isolate::GetCurrent() != NULL) {
		v8::Isolate::GetCurrent()->Exit();
	}

#if defined(JS_CUSTOM_ISOLATE)
	js->isolate->Dispose();
	delete js->global_locker;
#endif /* defined(JS_CUSTOM_ISOLATE) */

	delete js;
}

static void
tarantool_js_idle(EV_P_ struct ev_idle *w, int revents)
{
	(void) revents;
	(void) w;

	say_debug("js idle");
	if (v8::V8::IdleNotification()) {
		ev_idle_stop(EV_A_ &idle_watcher);
	}
}

static void
tarantool_js_fiber_on_start(void)
{
	say_debug("js enable");

	struct tarantool_js *js = fiber->js;
	assert(js != NULL);
#if !defined(ENABLE_V8_HACK_LINK_TIME)
	js_tls_hack_pthread_clear();
#endif /* !defined(ENABLE_V8_HACK_LINK_TIME) */

	v8::Locker *locker = new v8::Locker(fiber->js->isolate);
	fiber->js_locker = locker;
	say_debug("js create locker");

#if 0
	js->isolate->Enter();
#endif

	v8::ResourceConstraints constraints;
	constraints.set_stack_limit((uint32_t *) fiber->coro.stack);
	v8::SetResourceConstraints(&constraints);

	js->context->Enter();

	v8::HandleScope handle_scope;

	/*
	 * Workaround for v8 issue #1180
	 * http://code.google.com/p/v8/issues/detail?id=1180
	 */
	v8::Script::Compile(v8::String::New("void 0;"));

#if !defined(ENABLE_V8_HACK_LINK_TIME)
	js_tls_hack_pthread_to_fiber();
#endif /* !defined(ENABLE_V8_HACK_LINK_TIME) */
}

void
tarantool_js_fiber_on_resume(void)
{
	assert(fiber->js != NULL);

	say_debug("js fiber resume hook");

#if !defined(ENABLE_V8_HACK_LINK_TIME)
	js_tls_hack_fiber_to_pthread();
#endif /* !defined(ENABLE_V8_HACK_LINK_TIME) */

	v8::Unlocker *unlocker = static_cast<v8::Unlocker *>
			(fiber->js_unlocker);
	if (unlocker != NULL) {
		fiber->js_unlocker = NULL;
		say_debug("js delete unlocker");
		delete unlocker;
	}
}

void
tarantool_js_fiber_on_pause(void)
{
	struct tarantool_js *js = fiber->js;
	assert(js != NULL);

	say_debug("js fiber pause hook");

#if !defined(ENABLE_V8_HACK_LINK_TIME)
	js_tls_hack_pthread_to_fiber();
#endif /* !defined(ENABLE_V8_HACK_LINK_TIME) */

	assert(fiber->js_unlocker == NULL);
	say_debug("js create unlocker");
	v8::Unlocker *unlocker = new v8::Unlocker(js->isolate);
	fiber->js_unlocker = unlocker;

	/* Invoke V8::IdleNotificaiton in subsequents event loops */
	ev_idle_start(&idle_watcher);
}

void
tarantool_js_fiber_on_stop(void)
{
	struct tarantool_js *js = fiber->js;
	assert(js != NULL);

	say_debug("js fiber stop hook");

	js->context->Exit();
#if defined(JS_CUSTOM_ISOLATE)
	js->isolate->Exit();
#endif /* defined(JS_CUSTOM_ISOLATE) */

	v8::Unlocker *unlocker = static_cast<v8::Unlocker *>
			(fiber->js_unlocker);

	if (unlocker != NULL) {
		fiber->js_unlocker = NULL;
		say_debug("js delete unlocker");
		delete unlocker;
	}

	v8::Locker *locker = static_cast<v8::Locker *>(fiber->js_locker);
	fiber->js_locker = NULL;
	assert(locker != NULL);
	say_debug("js delete locker");
	delete locker;
}

void
fiber_enable_js(struct tarantool_js *js)
{
	assert(js != NULL);

	if (likely(fiber->js != NULL)) {
		assert(fiber->js == js);
		return;
	}

	fiber->js = js;
	tarantool_js_fiber_on_start();
}

void
tarantool_js_load_cfg(struct tarantool_js *js,
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
	struct tarantool_js *js = va_arg(ap, struct tarantool_js *);

	/* Enable JS in the fiber */
	fiber_enable_js(js);

	v8::HandleScope handle_scope;
	v8::TryCatch try_catch;
	try_catch.SetVerbose(true);

	/* Init global with built-in modules */
	v8::Local<v8::Object> global = v8::Context::GetCurrent()->Global();
	js::init_global(global);

	/* Eval require(TARANTOOL_JS_INIT_MODULE, false) */
	v8::Handle<v8::Object> require = global->Get(
			v8::String::NewSymbol("require")).As<v8::Object>();
	assert (!require.IsEmpty());
	v8::Local<v8::String> what = v8::String::New(TARANTOOL_JS_INIT_MODULE);
	v8::Handle<v8::Object> ret = js::require::call(require, what, false);
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
tarantool_js_init_library(struct tarantool_js *js)
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

	struct tarantool_js *js = fiber->js;
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

	v8::Handle<v8::Value> result = js::platform::eval_in_context(
			js_source, js_source_origin, v8::Context::GetCurrent());
	if (unlikely(try_catch.HasCaught())) {
		js_raise(&try_catch);
	}

	v8::String::Utf8Value output(result);
	tbuf_printf(out, "%.*s" CRLF, output.length(), *output);
}
