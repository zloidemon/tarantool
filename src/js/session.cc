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

#include "session.h"

#include <session.h>
#include <fiber.h>
#include <sio.h>

namespace js {
namespace session {

namespace { /* anonymous */

void
IdGetter(v8::Local<v8::String> property,
	 const v8::PropertyCallbackInfo<v8::Value>& args)
{
	(void) property;
	(void) args;

	args.GetReturnValue().Set(fiber_self()->sid);
}

void
FdGetter(v8::Local<v8::String> property,
	   const v8::PropertyCallbackInfo<v8::Value>& args)
{
	(void) property;
	(void) args;

	int fd = session_fd(fiber_self()->sid);
	args.GetReturnValue().Set(fd);
}

void
PeerGetter(v8::Local<v8::String> property,
	   const v8::PropertyCallbackInfo<v8::Value>& args)
{
	(void) property;
	(void) args;

	int fd = session_fd(fiber_self()->sid);
	struct sockaddr_in addr;
	sio_getpeername(fd, &addr);

	args.GetReturnValue().Set(v8::String::New(sio_strfaddr(&addr)));
}

void
ExistsMethod(const v8::FunctionCallbackInfo<v8::Value>& args)
{
	if (args.Length() < 1 || !args[0]->IsUint32()) {
		v8::ThrowException(v8::Exception::Error(
			v8::String::New("Usage: exists(sid)")));
		return;
	}

	args.GetReturnValue().Set(session_exists(args[0]->Uint32Value()));
}

} /* namespace (anonymous) */

v8::Handle<v8::FunctionTemplate>
GetTemplate()
{
	v8::HandleScope handle_scope;

	intptr_t tmpl_key = (intptr_t) &GetTemplate;
	JS *js = JS::GetCurrent();
	v8::Local<v8::FunctionTemplate> tmpl = js->TemplateCacheGet(tmpl_key);
	if (!tmpl.IsEmpty())
		return handle_scope.Close(tmpl);

	tmpl = v8::FunctionTemplate::New();

	tmpl->SetClassName(v8::String::NewSymbol("Session"));

	tmpl->InstanceTemplate()->Set("exists",
		v8::FunctionTemplate::New(ExistsMethod));
	tmpl->InstanceTemplate()->SetAccessor(v8::String::NewSymbol("id"),
		IdGetter);
	tmpl->InstanceTemplate()->SetAccessor(v8::String::NewSymbol("fd"),
		FdGetter);
	tmpl->InstanceTemplate()->SetAccessor(v8::String::NewSymbol("peer"),
		PeerGetter);

	js->TemplateCacheSet(tmpl_key, tmpl);

	return handle_scope.Close(tmpl);
}

v8::Local<v8::Object>
Exports()
{
	v8::HandleScope handle_scope;
	return handle_scope.Close(GetTemplate()->GetFunction()->NewInstance());
}

} /* namespace session */
} /* namespace js */
