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

#define V8_ALLOW_ACCESS_TO_RAW_HANDLE_CONSTRUCTOR 1
#include <v8.h>
#include <assert.h>

namespace js {

void
OnLoad();

void
OnUnload();

class JS {
public:
	v8::Isolate *
	GetIsolate() const {
		return isolate;
	}

	v8::Local<v8::Context>
	GetPrimaryContext() {
		return v8::Local<v8::Context>::New(isolate, context);
	}

	static JS *
	New();

	void
	Dispose();

	v8::Local<v8::Object>
	LoadLibrary(v8::Local<v8::String> rootModule);

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

	v8::Persistent<v8::Context> context;
	v8::Isolate *isolate;
	void *tmplcache;
};

v8::Handle<v8::Value>
InitGlobal(v8::Handle<v8::Object> global);

v8::Handle<v8::Value>
EvalInContext(v8::Handle<v8::String> source,
		v8::Handle<v8::String> filename,
		v8::Handle<v8::Context> context);

v8::Handle<v8::Value>
EvalInNewContext(v8::Handle<v8::String> source,
		 v8::Handle<v8::String> filename,
		 v8::Handle<v8::Object> global = v8::Handle<v8::Object>());

v8::Handle<v8::Value>
CopyObject(v8::Handle<v8::Object> dst, v8::Handle<v8::Object> src);

v8::Handle<v8::Value>
DumpObject(v8::Handle<v8::Object> src);

/**
 * @breif Initialize \a tmpl to work with userdata \a T
 */
template<typename T> void
userdata_init_template(v8::Handle<v8::FunctionTemplate> tmpl)
{
	/* One internal field is used for storing our native object */
	tmpl->InstanceTemplate()->SetInternalFieldCount(1);
}

/**
 * @brief Set \a userdata in object \a handle
 */
template<typename T> void
userdata_set(v8::Local<v8::Object> handle, T userdata)
{
	/* Set the native object in the handle */
	assert(handle->InternalFieldCount() > 0);
	handle->SetInternalField(0, v8::External::New(userdata));
}

/**
 * @brief Get from object \a handle
 */
template<typename T> T
userdata_get(v8::Local<v8::Object> handle)
{
	/* Get the native object from the handle */
	v8::HandleScope handle_scope;

	assert(!handle.IsEmpty());
	assert(handle->InternalFieldCount() > 0);

	v8::Local<v8::External> ext = handle->GetInternalField(0).
				      As<v8::External>();
	assert(!ext.IsEmpty());
	assert(ext->Value() != NULL);

	T object = static_cast<T>(ext->Value());
	return object;
}

} /* namespace js */

#endif /* TARANTOOL_JS_JS_H_INCLUDED */
