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

/* Source origin: v8/src/d8.cc */

namespace v8 {

static int32_t convertToInt(Local<Value> value_in, TryCatch* try_catch) {
  if (value_in->IsInt32()) {
    return value_in->Int32Value();
  }

  Local<Value> number = value_in->ToNumber();
  if (try_catch->HasCaught()) return 0;

  ASSERT(number->IsNumber());
  Local<Int32> int32 = number->ToInt32();
  if (try_catch->HasCaught() || int32.IsEmpty()) return 0;

  int32_t value = int32->Int32Value();
  if (try_catch->HasCaught()) return 0;

  return value;
}


static int32_t convertToUint(Local<Value> value_in, TryCatch* try_catch) {
  int32_t raw_value = convertToInt(value_in, try_catch);
  if (try_catch->HasCaught()) return 0;

  if (raw_value < 0) {
    ThrowException(String::New("Array length must not be negative."));
    return 0;
  }

  static const int kMaxLength = 0x3fffffff;
#ifndef V8_SHARED
  ASSERT(kMaxLength == i::ExternalArray::kMaxLength);
#endif  // V8_SHARED
  if (raw_value > static_cast<int32_t>(kMaxLength)) {
    ThrowException(
        String::New("Array length exceeds maximum length."));
  }
  return raw_value;
}


// TODO(rossberg): should replace these by proper uses of HasInstance,
// once we figure out a good way to make the templates global.
const char kArrayBufferMarkerPropName[] = "d8::_is_array_buffer_";
const char kArrayMarkerPropName[] = "d8::_is_typed_array_";


Handle<Value> Shell::CreateExternalArrayBuffer(Handle<Object> buffer,
                                               int32_t length) {
  static const int32_t kMaxSize = 0x7fffffff;
  // Make sure the total size fits into a (signed) int.
  if (length < 0 || length > kMaxSize) {
    return ThrowException(String::New("ArrayBuffer exceeds maximum size (2G)"));
  }
  uint8_t* data = new uint8_t[length];
  if (data == NULL) {
    return ThrowException(String::New("Memory allocation failed"));
  }
  memset(data, 0, length);

  buffer->SetHiddenValue(String::New(kArrayBufferMarkerPropName), True());
  Persistent<Object> persistent_array = Persistent<Object>::New(buffer);
  persistent_array.MakeWeak(data, ExternalArrayWeakCallback);
  persistent_array.MarkIndependent();
  V8::AdjustAmountOfExternalAllocatedMemory(length);

  buffer->SetIndexedPropertiesToExternalArrayData(
      data, v8::kExternalByteArray, length);
  buffer->Set(String::New("byteLength"), Int32::New(length), ReadOnly);

  return buffer;
}


Handle<Value> Shell::ArrayBuffer(const Arguments& args) {
  if (!args.IsConstructCall()) {
    Handle<Value>* rec_args = new Handle<Value>[args.Length()];
    for (int i = 0; i < args.Length(); ++i) rec_args[i] = args[i];
    Handle<Value> result = args.Callee()->NewInstance(args.Length(), rec_args);
    delete[] rec_args;
    return result;
  }

  if (args.Length() == 0) {
    return ThrowException(
        String::New("ArrayBuffer constructor must have one argument"));
  }
  TryCatch try_catch;
  int32_t length = convertToUint(args[0], &try_catch);
  if (try_catch.HasCaught()) return try_catch.ReThrow();

  return CreateExternalArrayBuffer(args.This(), length);
}


Handle<Object> Shell::CreateExternalArray(Handle<Object> array,
                                          Handle<Object> buffer,
                                          ExternalArrayType type,
                                          int32_t length,
                                          int32_t byteLength,
                                          int32_t byteOffset,
                                          int32_t element_size) {
  ASSERT(element_size == 1 || element_size == 2 ||
         element_size == 4 || element_size == 8);
  ASSERT(byteLength == length * element_size);

  void* data = buffer->GetIndexedPropertiesExternalArrayData();
  ASSERT(data != NULL);

  array->SetIndexedPropertiesToExternalArrayData(
      static_cast<uint8_t*>(data) + byteOffset, type, length);
  array->SetHiddenValue(String::New(kArrayMarkerPropName), Int32::New(type));
  array->Set(String::New("byteLength"), Int32::New(byteLength), ReadOnly);
  array->Set(String::New("byteOffset"), Int32::New(byteOffset), ReadOnly);
  array->Set(String::New("length"), Int32::New(length), ReadOnly);
  array->Set(String::New("BYTES_PER_ELEMENT"), Int32::New(element_size));
  array->Set(String::New("buffer"), buffer, ReadOnly);

  return array;
}


Handle<Value> Shell::CreateExternalArray(const Arguments& args,
                                         ExternalArrayType type,
                                         int32_t element_size) {
  if (!args.IsConstructCall()) {
    Handle<Value>* rec_args = new Handle<Value>[args.Length()];
    for (int i = 0; i < args.Length(); ++i) rec_args[i] = args[i];
    Handle<Value> result = args.Callee()->NewInstance(args.Length(), rec_args);
    delete[] rec_args;
    return result;
  }

  TryCatch try_catch;
  ASSERT(element_size == 1 || element_size == 2 ||
         element_size == 4 || element_size == 8);

  // All of the following constructors are supported:
  //   TypedArray(unsigned long length)
  //   TypedArray(type[] array)
  //   TypedArray(TypedArray array)
  //   TypedArray(ArrayBuffer buffer,
  //              optional unsigned long byteOffset,
  //              optional unsigned long length)
  Handle<Object> buffer;
  int32_t length;
  int32_t byteLength;
  int32_t byteOffset;
  bool init_from_array = false;
  if (args.Length() == 0) {
    return ThrowException(
        String::New("Array constructor must have at least one argument"));
  }
  if (args[0]->IsObject() &&
      !args[0]->ToObject()->GetHiddenValue(
          String::New(kArrayBufferMarkerPropName)).IsEmpty()) {
    // Construct from ArrayBuffer.
    buffer = args[0]->ToObject();
    int32_t bufferLength =
        convertToUint(buffer->Get(String::New("byteLength")), &try_catch);
    if (try_catch.HasCaught()) return try_catch.ReThrow();

    if (args.Length() < 2 || args[1]->IsUndefined()) {
      byteOffset = 0;
    } else {
      byteOffset = convertToUint(args[1], &try_catch);
      if (try_catch.HasCaught()) return try_catch.ReThrow();
      if (byteOffset > bufferLength) {
        return ThrowException(String::New("byteOffset out of bounds"));
      }
      if (byteOffset % element_size != 0) {
        return ThrowException(
            String::New("byteOffset must be multiple of element size"));
      }
    }

    if (args.Length() < 3 || args[2]->IsUndefined()) {
      byteLength = bufferLength - byteOffset;
      length = byteLength / element_size;
      if (byteLength % element_size != 0) {
        return ThrowException(
            String::New("buffer size must be multiple of element size"));
      }
    } else {
      length = convertToUint(args[2], &try_catch);
      if (try_catch.HasCaught()) return try_catch.ReThrow();
      byteLength = length * element_size;
      if (byteOffset + byteLength > bufferLength) {
        return ThrowException(String::New("length out of bounds"));
      }
    }
  } else {
    if (args[0]->IsObject() &&
        args[0]->ToObject()->Has(String::New("length"))) {
      // Construct from array.
      length = convertToUint(
          args[0]->ToObject()->Get(String::New("length")), &try_catch);
      if (try_catch.HasCaught()) return try_catch.ReThrow();
      init_from_array = true;
    } else {
      // Construct from size.
      length = convertToUint(args[0], &try_catch);
      if (try_catch.HasCaught()) return try_catch.ReThrow();
    }
    byteLength = length * element_size;
    byteOffset = 0;

    Handle<Value> array_buffer = args.This()->Get(String::New("__ArrayBuffer"));

    ASSERT(!try_catch.HasCaught());
    ASSERT(array_buffer->IsFunction());
    ASSERT(!try_catch.HasCaught() && array_buffer->IsFunction());
    Handle<Value> buffer_args[] = { Uint32::New(byteLength) };
    Handle<Value> result = Handle<Function>::Cast(array_buffer)->NewInstance(
        1, buffer_args);
    if (try_catch.HasCaught()) return result;
    buffer = result->ToObject();
  }

  Handle<Object> array = CreateExternalArray(
      args.This(), buffer, type, length, byteLength, byteOffset, element_size);
  if (init_from_array) {
    Handle<Object> init = args[0]->ToObject();
    for (int i = 0; i < length; ++i) array->Set(i, init->Get(i));
  }

  return array;
}


Handle<Value> Shell::ArrayBufferSlice(const Arguments& args) {
  TryCatch try_catch;

  if (!args.This()->IsObject()) {
    return ThrowException(
        String::New("'slice' invoked on non-object receiver"));
  }

  Local<Object> self = args.This();
  Local<Value> marker =
      self->GetHiddenValue(String::New(kArrayBufferMarkerPropName));
  if (marker.IsEmpty()) {
    return ThrowException(
        String::New("'slice' invoked on wrong receiver type"));
  }

  int32_t length =
      convertToUint(self->Get(String::New("byteLength")), &try_catch);
  if (try_catch.HasCaught()) return try_catch.ReThrow();

  if (args.Length() == 0) {
    return ThrowException(
        String::New("'slice' must have at least one argument"));
  }
  int32_t begin = convertToInt(args[0], &try_catch);
  if (try_catch.HasCaught()) return try_catch.ReThrow();
  if (begin < 0) begin += length;
  if (begin < 0) begin = 0;
  if (begin > length) begin = length;

  int32_t end;
  if (args.Length() < 2 || args[1]->IsUndefined()) {
    end = length;
  } else {
    end = convertToInt(args[1], &try_catch);
    if (try_catch.HasCaught()) return try_catch.ReThrow();
    if (end < 0) end += length;
    if (end < 0) end = 0;
    if (end > length) end = length;
    if (end < begin) end = begin;
  }

  Local<Function> constructor = Local<Function>::Cast(self->GetConstructor());
  Handle<Value> new_args[] = { Uint32::New(end - begin) };
  Handle<Value> result = constructor->NewInstance(1, new_args);
  if (try_catch.HasCaught()) return result;
  Handle<Object> buffer = result->ToObject();
  uint8_t* dest =
      static_cast<uint8_t*>(buffer->GetIndexedPropertiesExternalArrayData());
  uint8_t* src = begin + static_cast<uint8_t*>(
      self->GetIndexedPropertiesExternalArrayData());
  memcpy(dest, src, end - begin);

  return buffer;
}


Handle<Value> Shell::ArraySubArray(const Arguments& args) {
  TryCatch try_catch;

  if (!args.This()->IsObject()) {
    return ThrowException(
        String::New("'subarray' invoked on non-object receiver"));
  }

  Local<Object> self = args.This();
  Local<Value> marker = self->GetHiddenValue(String::New(kArrayMarkerPropName));
  if (marker.IsEmpty()) {
    return ThrowException(
        String::New("'subarray' invoked on wrong receiver type"));
  }

  Handle<Object> buffer = self->Get(String::New("buffer"))->ToObject();
  if (try_catch.HasCaught()) return try_catch.ReThrow();
  int32_t length =
      convertToUint(self->Get(String::New("length")), &try_catch);
  if (try_catch.HasCaught()) return try_catch.ReThrow();
  int32_t byteOffset =
      convertToUint(self->Get(String::New("byteOffset")), &try_catch);
  if (try_catch.HasCaught()) return try_catch.ReThrow();
  int32_t element_size =
      convertToUint(self->Get(String::New("BYTES_PER_ELEMENT")), &try_catch);
  if (try_catch.HasCaught()) return try_catch.ReThrow();

  if (args.Length() == 0) {
    return ThrowException(
        String::New("'subarray' must have at least one argument"));
  }
  int32_t begin = convertToInt(args[0], &try_catch);
  if (try_catch.HasCaught()) return try_catch.ReThrow();
  if (begin < 0) begin += length;
  if (begin < 0) begin = 0;
  if (begin > length) begin = length;

  int32_t end;
  if (args.Length() < 2 || args[1]->IsUndefined()) {
    end = length;
  } else {
    end = convertToInt(args[1], &try_catch);
    if (try_catch.HasCaught()) return try_catch.ReThrow();
    if (end < 0) end += length;
    if (end < 0) end = 0;
    if (end > length) end = length;
    if (end < begin) end = begin;
  }

  length = end - begin;
  byteOffset += begin * element_size;

  Local<Function> constructor = Local<Function>::Cast(self->GetConstructor());
  Handle<Value> construct_args[] = {
    buffer, Uint32::New(byteOffset), Uint32::New(length)
  };
  return constructor->NewInstance(3, construct_args);
}


Handle<Value> Shell::ArraySet(const Arguments& args) {
  TryCatch try_catch;

  if (!args.This()->IsObject()) {
    return ThrowException(
        String::New("'set' invoked on non-object receiver"));
  }

  Local<Object> self = args.This();
  Local<Value> marker = self->GetHiddenValue(String::New(kArrayMarkerPropName));
  if (marker.IsEmpty()) {
    return ThrowException(
        String::New("'set' invoked on wrong receiver type"));
  }
  int32_t length =
      convertToUint(self->Get(String::New("length")), &try_catch);
  if (try_catch.HasCaught()) return try_catch.ReThrow();
  int32_t element_size =
      convertToUint(self->Get(String::New("BYTES_PER_ELEMENT")), &try_catch);
  if (try_catch.HasCaught()) return try_catch.ReThrow();

  if (args.Length() == 0) {
    return ThrowException(
        String::New("'set' must have at least one argument"));
  }
  if (!args[0]->IsObject() ||
      !args[0]->ToObject()->Has(String::New("length"))) {
    return ThrowException(
        String::New("'set' invoked with non-array argument"));
  }
  Handle<Object> source = args[0]->ToObject();
  int32_t source_length =
      convertToUint(source->Get(String::New("length")), &try_catch);
  if (try_catch.HasCaught()) return try_catch.ReThrow();

  int32_t offset;
  if (args.Length() < 2 || args[1]->IsUndefined()) {
    offset = 0;
  } else {
    offset = convertToUint(args[1], &try_catch);
    if (try_catch.HasCaught()) return try_catch.ReThrow();
  }
  if (offset + source_length > length) {
    return ThrowException(String::New("offset or source length out of bounds"));
  }

  int32_t source_element_size;
  if (source->GetHiddenValue(String::New(kArrayMarkerPropName)).IsEmpty()) {
    source_element_size = 0;
  } else {
    source_element_size =
       convertToUint(source->Get(String::New("BYTES_PER_ELEMENT")), &try_catch);
    if (try_catch.HasCaught()) return try_catch.ReThrow();
  }

  if (element_size == source_element_size &&
      self->GetConstructor()->StrictEquals(source->GetConstructor())) {
    // Use memmove on the array buffers.
    Handle<Object> buffer = self->Get(String::New("buffer"))->ToObject();
    if (try_catch.HasCaught()) return try_catch.ReThrow();
    Handle<Object> source_buffer =
        source->Get(String::New("buffer"))->ToObject();
    if (try_catch.HasCaught()) return try_catch.ReThrow();
    int32_t byteOffset =
        convertToUint(self->Get(String::New("byteOffset")), &try_catch);
    if (try_catch.HasCaught()) return try_catch.ReThrow();
    int32_t source_byteOffset =
        convertToUint(source->Get(String::New("byteOffset")), &try_catch);
    if (try_catch.HasCaught()) return try_catch.ReThrow();

    uint8_t* dest = byteOffset + offset * element_size + static_cast<uint8_t*>(
        buffer->GetIndexedPropertiesExternalArrayData());
    uint8_t* src = source_byteOffset + static_cast<uint8_t*>(
        source_buffer->GetIndexedPropertiesExternalArrayData());
    memmove(dest, src, source_length * element_size);
  } else if (source_element_size == 0) {
    // Source is not a typed array, copy element-wise sequentially.
    for (int i = 0; i < source_length; ++i) {
      self->Set(offset + i, source->Get(i));
      if (try_catch.HasCaught()) return try_catch.ReThrow();
    }
  } else {
    // Need to copy element-wise to make the right conversions.
    Handle<Object> buffer = self->Get(String::New("buffer"))->ToObject();
    if (try_catch.HasCaught()) return try_catch.ReThrow();
    Handle<Object> source_buffer =
        source->Get(String::New("buffer"))->ToObject();
    if (try_catch.HasCaught()) return try_catch.ReThrow();

    if (buffer->StrictEquals(source_buffer)) {
      // Same backing store, need to handle overlap correctly.
      // This gets a bit tricky in the case of different element sizes
      // (which, of course, is extremely unlikely to ever occur in practice).
      int32_t byteOffset =
          convertToUint(self->Get(String::New("byteOffset")), &try_catch);
      if (try_catch.HasCaught()) return try_catch.ReThrow();
      int32_t source_byteOffset =
          convertToUint(source->Get(String::New("byteOffset")), &try_catch);
      if (try_catch.HasCaught()) return try_catch.ReThrow();

      // Copy as much as we can from left to right.
      int i = 0;
      int32_t next_dest_offset = byteOffset + (offset + 1) * element_size;
      int32_t next_src_offset = source_byteOffset + source_element_size;
      while (i < length && next_dest_offset <= next_src_offset) {
        self->Set(offset + i, source->Get(i));
        ++i;
        next_dest_offset += element_size;
        next_src_offset += source_element_size;
      }
      // Of what's left, copy as much as we can from right to left.
      int j = length - 1;
      int32_t dest_offset = byteOffset + (offset + j) * element_size;
      int32_t src_offset = source_byteOffset + j * source_element_size;
      while (j >= i && dest_offset >= src_offset) {
        self->Set(offset + j, source->Get(j));
        --j;
        dest_offset -= element_size;
        src_offset -= source_element_size;
      }
      // There can be at most 8 entries left in the middle that need buffering
      // (because the largest element_size is 8 times the smallest).
      ASSERT(j+1 - i <= 8);
      Handle<Value> temp[8];
      for (int k = i; k <= j; ++k) {
        temp[k - i] = source->Get(k);
      }
      for (int k = i; k <= j; ++k) {
        self->Set(offset + k, temp[k - i]);
      }
    } else {
      // Different backing stores, safe to copy element-wise sequentially.
      for (int i = 0; i < source_length; ++i)
        self->Set(offset + i, source->Get(i));
    }
  }

  return Undefined();
}


void Shell::ExternalArrayWeakCallback(Persistent<Value> object, void* data) {
  HandleScope scope;
  int32_t length =
      object->ToObject()->Get(String::New("byteLength"))->Uint32Value();
  V8::AdjustAmountOfExternalAllocatedMemory(-length);
  delete[] static_cast<uint8_t*>(data);
  object.Dispose();
}


Handle<Value> Shell::Int8Array(const Arguments& args) {
  return CreateExternalArray(args, v8::kExternalByteArray, sizeof(int8_t));
}


Handle<Value> Shell::Uint8Array(const Arguments& args) {
  return CreateExternalArray(args, kExternalUnsignedByteArray, sizeof(uint8_t));
}


Handle<Value> Shell::Int16Array(const Arguments& args) {
  return CreateExternalArray(args, kExternalShortArray, sizeof(int16_t));
}


Handle<Value> Shell::Uint16Array(const Arguments& args) {
  return CreateExternalArray(
      args, kExternalUnsignedShortArray, sizeof(uint16_t));
}


Handle<Value> Shell::Int32Array(const Arguments& args) {
  return CreateExternalArray(args, kExternalIntArray, sizeof(int32_t));
}


Handle<Value> Shell::Uint32Array(const Arguments& args) {
  return CreateExternalArray(args, kExternalUnsignedIntArray, sizeof(uint32_t));
}


Handle<Value> Shell::Float32Array(const Arguments& args) {
  return CreateExternalArray(
      args, kExternalFloatArray, sizeof(float));  // NOLINT
}


Handle<Value> Shell::Float64Array(const Arguments& args) {
  return CreateExternalArray(
      args, kExternalDoubleArray, sizeof(double));  // NOLINT
}


Handle<Value> Shell::Uint8ClampedArray(const Arguments& args) {
  return CreateExternalArray(args, kExternalPixelArray, sizeof(uint8_t));
}

Handle<FunctionTemplate> Shell::CreateArrayBufferTemplate(
    InvocationCallback fun) {
  Handle<FunctionTemplate> buffer_template = FunctionTemplate::New(fun);
  Local<Template> proto_template = buffer_template->PrototypeTemplate();
  proto_template->Set(String::New("slice"),
                      FunctionTemplate::New(ArrayBufferSlice));
  return buffer_template;
}


Handle<FunctionTemplate> Shell::CreateArrayTemplate(InvocationCallback fun) {
  Handle<FunctionTemplate> array_template = FunctionTemplate::New(fun);
  Local<Template> proto_template = array_template->PrototypeTemplate();
  proto_template->Set(String::New("__ArrayBuffer"),
        CreateArrayBufferTemplate(ArrayBuffer),
        static_cast<PropertyAttribute>(ReadOnly  | DontEnum | DontDelete));
  proto_template->Set(String::New("set"), FunctionTemplate::New(ArraySet));
  proto_template->Set(String::New("subarray"),
                      FunctionTemplate::New(ArraySubArray));
  return array_template;
}

void
Shell::InitializeTemplate(Handle<FunctionTemplate> global_template) {
  // Bind the handlers for external arrays.
  PropertyAttribute attr =
      static_cast<PropertyAttribute>(ReadOnly | DontDelete);
  global_template->Set(String::New("ArrayBuffer"),
                       CreateArrayBufferTemplate(ArrayBuffer), attr);
  global_template->Set(String::New("Int8Array"),
                       CreateArrayTemplate(Int8Array), attr);
  global_template->Set(String::New("Uint8Array"),
                       CreateArrayTemplate(Uint8Array), attr);
  global_template->Set(String::New("Int16Array"),
                       CreateArrayTemplate(Int16Array), attr);
  global_template->Set(String::New("Uint16Array"),
                       CreateArrayTemplate(Uint16Array), attr);
  global_template->Set(String::New("Int32Array"),
                       CreateArrayTemplate(Int32Array), attr);
  global_template->Set(String::New("Uint32Array"),
                       CreateArrayTemplate(Uint32Array), attr);
  global_template->Set(String::New("Float32Array"),
                       CreateArrayTemplate(Float32Array), attr);
  global_template->Set(String::New("Float64Array"),
                       CreateArrayTemplate(Float64Array), attr);
  global_template->Set(String::New("Uint8ClampedArray"),
                       CreateArrayTemplate(Uint8ClampedArray), attr);
}

}
