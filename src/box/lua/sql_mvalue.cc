/*
 * Copyright 2010-2015, Tarantool AUTHORS, please see AUTHORS file.
 *
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

#include "sql_mvalue.h"

MValue::MValue() : type(MP_NIL), data(NULL), data_len(0), error(false) { }

MValue::MValue(const MValue &ob) : type(-1), data(NULL), data_len(0), error(false) {
	*this = ob;
}

MValue::MValue(i64 n) : type(-1), data(NULL), data_len(0), error(false) {
	data = malloc(sizeof(i64));
	memcpy(data, &n, sizeof(i64));
	data_len = sizeof(i64);
	if (n > 0) {
		type = MP_UINT;
	} else {
		type = MP_INT;
	}
}

MValue::MValue(u64 n) {
	data = malloc(sizeof(u64));
	memcpy(data, &n, sizeof(u64));
	data_len = sizeof(u64);
	type = MP_UINT;
}

MValue::MValue(const char *src, int len) {
	if (len < 0) {
		len = strlen(src);
	}
	data = malloc(len + 1);
	((char *)data)[len] = 0;
	memcpy(data, src, len);
	type = MP_STR;
	data_len = len;
}

MValue::MValue(double num) {
	data = malloc(sizeof(double));
	memcpy(data, &num, sizeof(double));
	data_len = sizeof(double);
	type = MP_DOUBLE;
}

MValue::MValue(const void *src, int len) {
	data = malloc(len);
	memcpy(data, src, len);
	data_len = len;
	type = MP_BIN;
}

MValue &MValue::operator=(const MValue &ob) {
	Clear();
	type = ob.type;
	data_len = ob.data_len;
	error = ob.error;
	if (type == MP_ARRAY) {
		data = malloc(sizeof(MValue) * data_len);
		memset(data, 0, sizeof(MValue) * data_len);
		for (int i = 0; i < data_len; ++i) {
			*((MValue *)data + i) = *ob[i];
		}
	} else {
		data = malloc(data_len);
		memcpy(data, ob.data, data_len);
	}
	return *this;
}

double MValue::GetDouble() const {
	error = false;
	if (type != MP_DOUBLE) {
		error = true;
		return 0;
	}
	double res;
	memcpy(&res, data, sizeof(double));
	return res;
}

u64 MValue::GetUint64() const {
	error = false;
	if (type != MP_UINT) {
		error = true;
		return 0;
	}
	u64 res;
	memcpy(&res, data, sizeof(u64));
	return res;
}

i64 MValue::GetInt64() const {
	error = false;
	if (type != MP_INT) {
		error = true;
		return 0;
	}
	i64 res;
	memcpy(&res, data, sizeof(i64));
	return res;
}

int MValue::Size() const {
	return data_len;
}

const char *MValue::GetStr(size_t *len) const {
	error = false;
	if (type != MP_STR) {
		error = true;
		return NULL;
	}
	if (len) {
		*len = data_len;
	}
	return (const char *)(data);
}

const char *MValue::GetBin(size_t *len) const {
	error = false;
	if (type != MP_BIN) {
		error = true;
		return NULL;
	}
	if (len) *len = data_len;
	return (const char *)data;
}

bool MValue::GetBool() const {
	error = false;
	if (type != MP_BOOL) {
		error = true;
		return false;
	}
	return !!(*((uint8_t *)data));
}

bool MValue::IsError() const { return error; }

bool MValue::IsEmpty() const {
	return ((!data) || (!data_len));
}

int MValue::GetType() const { return type; }

MValue *MValue::operator[](int index) {
	error = false;
	if ((type != MP_ARRAY) || (index >= data_len)) {
		error = true;
		return NULL;
	}
	return ((MValue *)(data)) + index;
}

const MValue *MValue::operator[](int index) const {
	error = false;
	if ((type != MP_ARRAY) || (index >= data_len)) {
		error = true;
		return NULL;
	}
	return ((const MValue *)(data)) + index;
}

MValue::~MValue() {
	Clear();
}

void MValue::Clear() {
	error = false;
	switch(type) {
		case MP_UINT: case MP_INT: case MP_STR: case MP_BIN: case MP_BOOL:
		case MP_FLOAT: case MP_DOUBLE: if (data) free(data); data = NULL; data_len = 0; break;
		case MP_ARRAY: {
			for (int i = 0; i < data_len; ++i) {
				((MValue *)(data) + i)->Clear();
			}
			if (data) {
				free(data);
				data = NULL;
				data_len = 0;
			}
			break;
		}
		default: return;
	}
}

MValue MValue::FromMSGPuck(const char **data) {
	MValue res;
	MValue::do_mvalue_from_msgpuck(&res, data);
	return res;
}

MValue MValue::FromBtreeCell(const char *data, int serial_type, int &size) {
	Mem mem;
	memset(&mem, 0, sizeof(mem));
	size = sqlite3VdbeSerialGet((const unsigned char *)data, serial_type, &mem);

	switch(mem.flags) {
		case MEM_Null: return MValue();
		case MEM_Int: return MValue((i64)mem.u.i);
		case MEM_Real: return MValue(mem.u.r);
		default: {
			if (mem.flags & MEM_Blob)
				return MValue((const void *)mem.z, mem.n);
			else if (mem.flags & MEM_Str)
				return MValue((const char *)mem.z, mem.n);
			return MValue();
		}
	}
}

int MValue::do_mvalue_from_msgpuck(MValue *res, const char **data) {
	const char *tuple = *data;
	mp_type type = mp_typeof(*tuple);
	switch(type) {
		case MP_NIL:
			mp_decode_nil(&tuple);
			*data = tuple;
			res->type = type;
			res->data = NULL;
			res->data_len = 0;
			return 0;
		case MP_UINT: {
			u64 tmp = mp_decode_uint(&tuple);
			*data = tuple;
			res->type = type;
			res->data_len = sizeof(u64);
			res->data = malloc(res->data_len);
			memcpy(res->data, &tmp, res->data_len);
			return 0;
		}
		case MP_INT: {
			i64 tmp = mp_decode_int(&tuple);
			*data = tuple;
			res->type = type;
			res->data_len = sizeof(i64);
			res->data = malloc(res->data_len);
			memcpy(res->data, &tmp, res->data_len);
			return 0;
		}
		case MP_STR: {
			uint32_t len;
			const char *tmp = mp_decode_str(&tuple, &len);
			*data = tuple;
			res->type = type;
			res->data_len = len;
			res->data = malloc((res->data_len + 1) * sizeof(char));
			memset(res->data, 0, res->data_len + 1);
			memcpy(res->data, tmp, res->data_len * sizeof(char));
			return 0;
		}
		case MP_BIN: {
			uint32_t len;
			const char *tmp = mp_decode_bin(&tuple, &len);
			*data = tuple;
			res->type = type;
			res->data_len = len;
			res->data = malloc(res->data_len * sizeof(char));
			memcpy(res->data, tmp, res->data_len);
			return 0;
		}
		case MP_BOOL: {
			uint8_t tmp = mp_decode_bool(&tuple);
			*data = tuple;
			res->type = type;
			res->data_len = sizeof(uint8_t);
			res->data = malloc(sizeof(uint8_t));
			memcpy(res->data, &tmp, res->data_len);
			return 0;
		}
		case MP_FLOAT: {
			float tmp = mp_decode_float(&tuple);
			*data = tuple;
			res->type = type;
			res->data_len = sizeof(float);
			res->data = malloc(res->data_len);
			memcpy(res->data, &tmp, res->data_len);
			return 0;
		}
		case MP_DOUBLE: {
			double tmp = mp_decode_double(&tuple);
			*data = tuple;
			res->type = type;
			res->data_len = sizeof(double);
			res->data = malloc(res->data_len);
			memcpy(res->data, &tmp, res->data_len);
			return 0;
		}
		case MP_ARRAY: {
			uint32_t size = mp_decode_array(&tuple);
			res->type = type;
			res->data_len = size;
			MValue *tmp = (MValue *)malloc(sizeof(MValue) * size);
			memset(tmp, 0, sizeof(MValue) * size);
			res->data = tmp;
			for (uint32_t i = 0; i < size; ++i) {
				MValue::do_mvalue_from_msgpuck(tmp + i, &tuple);
			}
			*data = tuple;
			return 0;
		}
		default: return -1;
	}
}

// uint32_t get_hash_mvalue(mvalue *) {
// 	static uint32_t hash_cntr = 0;
// 	return hash_cntr++;
// }
