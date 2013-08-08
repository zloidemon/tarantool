// Copyright 2012 the V8 project authors. All rights reserved.
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
//       notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
//       copyright notice, this list of conditions and the following
//       disclaimer in the documentation and/or other materials provided
//       with the distribution.
//     * Neither the name of Google Inc. nor the names of its
//       contributors may be used to endorse or promote products derived
//       from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

/* Source origin: v8/src/d8.h */

namespace v8 {

#ifdef V8_SHARED
class Shell {
#else
class Shell : public i::AllStatic {
#endif  // V8_SHARED
 public:
  static Handle<Value> ArrayBuffer(const Arguments& args);
  static Handle<Value> Int8Array(const Arguments& args);
  static Handle<Value> Uint8Array(const Arguments& args);
  static Handle<Value> Int16Array(const Arguments& args);
  static Handle<Value> Uint16Array(const Arguments& args);
  static Handle<Value> Int32Array(const Arguments& args);
  static Handle<Value> Uint32Array(const Arguments& args);
  static Handle<Value> Float32Array(const Arguments& args);
  static Handle<Value> Float64Array(const Arguments& args);
  static Handle<Value> Uint8ClampedArray(const Arguments& args);
  static Handle<Value> ArrayBufferSlice(const Arguments& args);
  static Handle<Value> ArraySubArray(const Arguments& args);
  static Handle<Value> ArraySet(const Arguments& args);
  static void InitializeTemplate(Handle<FunctionTemplate> tmpl);
 private:
  static Handle<FunctionTemplate> CreateArrayBufferTemplate(InvocationCallback);
  static Handle<FunctionTemplate> CreateArrayTemplate(InvocationCallback);
  static Handle<Value> CreateExternalArrayBuffer(Handle<Object> buffer,
						 int32_t size);
  static Handle<Object> CreateExternalArray(Handle<Object> array,
					    Handle<Object> buffer,
					    ExternalArrayType type,
					    int32_t length,
					    int32_t byteLength,
					    int32_t byteOffset,
					    int32_t element_size);
  static Handle<Value> CreateExternalArray(const Arguments& args,
					   ExternalArrayType type,
					   int32_t element_size);
  static void ExternalArrayWeakCallback(Persistent<Value> object, void* data);

};

}  // namespace v8
