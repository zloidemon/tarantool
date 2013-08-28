#ifndef TARANTOOL_JS_JS_H_INCLUDED
#define TARANTOOL_JS_JS_H_INCLUDED
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

/**
 * @file
 * @brief JS core utils
 */

#define V8_ALLOW_ACCESS_TO_RAW_HANDLE_CONSTRUCTOR 1 /* Needed for mhash */
#include <v8.h>
#include <assert.h>

class ClientError;
class Exception;

namespace js {

void
OnLoad();

void
OnUnload();

class JS {
public:
	v8::Isolate *
	GetIsolate() const {
		return _isolate;
	}

	v8::Local<v8::Context>
	GetPrimaryContext() {
		return v8::Local<v8::Context>::New(_isolate, _context);
	}

	static JS *
	New();

	void
	Dispose();

	v8::Local<v8::Object>
	GetRequire()
	{
		return v8::Local<v8::Object>::New(_isolate, _require_handle);
	}

	v8::Local<v8::FunctionTemplate>
	TemplateCacheGet(intptr_t key) const;

	void
	TemplateCacheSet(intptr_t key, v8::Local<v8::FunctionTemplate> tmpl);

	static JS *
	GetCurrent();

	void
	FiberEnsure();

	void
	FiberOnStart();

	void
	FiberOnResume();

	void
	FiberOnPause();

	void
	FiberOnStop();

private:
	JS();
	~JS();

	JS(JS const&) = delete;
	JS& operator=(JS const&) = delete;

	v8::Persistent<v8::Context> _context;
	v8::Persistent<v8::Object> _require_handle;
	v8::Isolate *_isolate;
	void *_tmplcache;
};

void
LoadModules();

v8::Handle<v8::Value>
EvalInContext(v8::Handle<v8::String> source,
		v8::Handle<v8::String> filename,
		v8::Handle<v8::Context> context);

v8::Handle<v8::Value>
EvalInNewContext(v8::Handle<v8::String> source,
		 v8::Handle<v8::String> filename,
		 v8::Handle<v8::Object> global = v8::Handle<v8::Object>());

void
CopyObject(v8::Handle<v8::Object> dst, v8::Handle<v8::Object> src);

void
DumpObject(v8::Handle<v8::Object> src);

inline bool
Inherits(v8::Local<v8::Value> constructor, v8::Local<v8::Value> obj)
{
	if (!constructor->IsObject() || !obj->IsObject())
		return false;
	return constructor->ToObject()->Equals(obj->ToObject()->GetConstructor());
}

v8::Local<v8::Object>
CatchNativeException(const ClientError &e);

void
LogException(v8::Local<v8::Object> e, bool rethrow_native = false);


/* Exception wrappers */
#define JS_BEGIN()								\
try {

#define JS_END() \
} catch (const ClientError& e) {						\
	v8::Local<v8::Object> ex = CatchNativeException(e);			\
	v8::ThrowException(ex);							\
	return;									\
} catch (const Exception& e) {							\
	e.log();								\
	panic("Unhandled C++ exception in JS bindings");			\
} catch (...) {									\
	panic("Unhandled C++ exception in JS bindings");			\
}

/* Used from admin console */
v8::Local<v8::Object>
FillException(v8::TryCatch *try_catch);

v8::Local<v8::Value>
FormatJSON(v8::Local<v8::Value> obj);

} /* namespace js */

#endif /* TARANTOOL_JS_JS_H_INCLUDED */
