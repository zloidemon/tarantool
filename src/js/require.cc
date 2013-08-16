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

#include <stdio.h>
#include <limits.h>

#include <tarantool.h>
#include <cfg/tarantool_box_cfg.h>
#include <say.h>

namespace js {
namespace require {

v8::Local<v8::Object>
CacheGet(v8::Local<v8::Object> thiz, v8::Local<v8::String> what)
{
	v8::HandleScope handle_scope;

	v8::Local<v8::Array> cache = thiz->Get(
		v8::String::NewSymbol("cache")).As<v8::Array>();

	if (cache.IsEmpty()) {
		say_warn("Require cache is disabled");
		return v8::Local<v8::Object>();
	}

	/* Lookup cache */
	return handle_scope.Close(cache->Get(what).As<v8::Object>());
}

void
CacheSet(v8::Local<v8::Object> thiz, v8::Local<v8::String> what,
	 v8::Local<v8::Object> object)
{
	v8::HandleScope handle_scope;

	v8::Local<v8::Array> cache = thiz->Get(
		v8::String::NewSymbol("cache")).As<v8::Array>();

	if (cache.IsEmpty()) {
		say_warn("Require cache is disabled");
		return;
	}

	/* Update cache */
	cache->Set(what, object);

	return;
}

v8::Local<v8::String>
Resolve(v8::Local<v8::Object> thiz, v8::Local<v8::String> what)
{
	(void) thiz;

	v8::HandleScope handle_scope;

	char path[PATH_MAX + 1];

	v8::String::Utf8Value what_utf8(what);
	snprintf(path, PATH_MAX, "%s/%s.js", cfg.script_dir,
		 basename(*what_utf8));
	v8::Local<v8::String> filename = v8::String::New(path, strlen(path));

	return handle_scope.Close(filename);
}

v8::Local<v8::Object>
Call(v8::Local<v8::Object> thiz, v8::Local<v8::String> what, bool sandbox)
{
	v8::HandleScope handle_scope;

	v8::Local<v8::Object> ret = CacheGet(thiz, what);
	if (!ret.IsEmpty() && !ret->IsUndefined()) {
		return ret;
	}

	v8::String::Utf8Value what_utf8(what);
	v8::Local<v8::String> filename = Resolve(thiz, what).As<v8::String>();
	if (filename.IsEmpty()) {
		say_warn("Module is not found: %.*s",
			 what_utf8.length(), *what_utf8);
	}

	v8::String::Utf8Value filename_utf8(filename);
	say_info("Loading new JS module '%.*s' from '%.*s'",
		 what_utf8.length(), *what_utf8,
		 filename_utf8.length(), *filename_utf8);

	FILE *f = fopen(*filename_utf8, "r");
	if (f == NULL) {
		v8::ThrowException(v8::Exception::Error(
				v8::String::New("Can not open module file")));
		return v8::Local<v8::Object>();
	}

	int rc = fseek(f, 0L, SEEK_END);
	long size = ftell(f);
	rewind(f);
	if (rc != 0 || size <= 0 || size > INT_MAX) {
		v8::ThrowException(v8::Exception::Error(
				v8::String::New("Can not read module file 1")));
		return v8::Local<v8::Object>();
	}

	char *buf = (char *) malloc(size);
	if (fread(buf, size, 1, f) != 1) {
		fclose(f);
		free(buf);
		v8::ThrowException(v8::Exception::Error(
				v8::String::New("Can not read module file 2")));
		return v8::Local<v8::Object>();
	}
	fclose(f);

	v8::Local<v8::String> source = v8::String::New(buf, (int) size - 1);

	/* Ignore return value and grab entire globals object */
	v8::Local<v8::Object> globals;
	if (sandbox) {
		globals = v8::Object::New();
		globals->Set(v8::String::NewSymbol("exports"), v8::Object::New());
		js::EvalInNewContext(source, filename, globals).As<v8::Object>();
	} else {
		v8::Local<v8::Context> context = v8::Context::GetCurrent();
		globals = context->Global();
		globals->Set(v8::String::NewSymbol("exports"), v8::Object::New());
		js::EvalInContext(source, filename, context).As<v8::Object>();
	}

	if (ret.IsEmpty()) {
		say_warn("Can not compile module %.*s",
			 what_utf8.length(), *what_utf8);
		return v8::Local<v8::Object>();
	}

	ret = globals->Get(v8::String::NewSymbol("exports")).As<v8::Object>();
	if (ret.IsEmpty()) {
		say_error("module %.*s: 'exports' is empty",
			  what_utf8.length(), *what_utf8);
		return v8::Local<v8::Object>();
	}

	CacheSet(thiz, what, ret);

	return handle_scope.Close(ret);
}

namespace { /* anonymous */

void
ResolveMethod(const v8::FunctionCallbackInfo<v8::Value>& args)
{
	if (args.Length() != 1 || !args[0]->IsString()) {
		v8::ThrowException(v8::Exception::Error(
			v8::String::New("Invalid arguments")));
		return;
	}

	args.GetReturnValue().Set(Resolve(args.This(), args[0]->ToString()));
}


void
CallMethod(const v8::FunctionCallbackInfo<v8::Value>& args)
{
	v8::HandleScope handle_scope;

	if (args.IsConstructCall()) {
		v8::ThrowException(v8::Exception::Error(
			v8::String::New("Constructor call")));
		return;
	}

	if (args.Length() != 1 || !args[0]->IsString()) {
		v8::ThrowException(v8::Exception::Error(
			v8::String::New("Invalid arguments")));
		return;
	}

	args.GetReturnValue().Set(Call(args.This(),
				       args[0]->ToString(), true));
}

v8::Local<v8::FunctionTemplate>
GetTemplate()
{
	v8::HandleScope handle_scope;
	v8::Local<v8::FunctionTemplate> tmpl = v8::FunctionTemplate::New();
	tmpl->SetClassName(v8::String::NewSymbol("Require"));

	tmpl->InstanceTemplate()->SetCallAsFunctionHandler(CallMethod);
	tmpl->InstanceTemplate()->Set(v8::String::NewSymbol("cache"),
				      v8::Array::New());
	tmpl->InstanceTemplate()->Set(v8::String::NewSymbol("extensions"),
				      v8::Object::New());
	tmpl->InstanceTemplate()->Set(v8::String::NewSymbol("resolve"),
				      v8::FunctionTemplate::New(ResolveMethod));

	return handle_scope.Close(tmpl);
}

} /* anonymous */

v8::Local<v8::Object>
Exports()
{
	v8::HandleScope handle_scope;

	v8::Local<v8::FunctionTemplate> tmpl = GetTemplate();
	v8::Local<v8::Object> thiz = tmpl->GetFunction()->NewInstance();
	CacheSet(thiz, v8::String::NewSymbol("require"), thiz);

	return handle_scope.Close(thiz);
}

} /* namespace require */
} /* namespace js */
