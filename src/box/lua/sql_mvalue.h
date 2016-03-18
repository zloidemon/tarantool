#ifndef SQL_MVALUE_H
#define SQL_MVALUE_H

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

extern "C" {
#include "sqliteInt.h"
#include "sqlite3.h"
#include "vdbeInt.h"
}
#undef likely
#undef unlikely

#include "box/index.h"

class MValue {
private:
	int type;
	void *data;
	int data_len;
	mutable bool error;

static int do_mvalue_from_msgpuck(MValue *res, const char **data);

public:
	MValue();
	MValue(const MValue &ob);
	MValue(int64_t n);
	MValue(uint64_t n);
	MValue(const char *src, int len = -1);
	MValue(double num);
	MValue(const void *src, int len);

	bool IsError() const;
	bool IsEmpty() const;
	int GetType() const;

	double GetDouble() const;
	uint64_t GetUint64() const;
	int64_t GetInt64() const;
	const char *GetStr(size_t *len = NULL) const;
	const char *GetBin(size_t *len = NULL) const;
	bool GetBool() const;
	MValue *operator[](int index);
	const MValue *operator[](int index) const;
	MValue &operator=(const MValue &ob);
	int Size() const;

	static MValue FromMSGPuck(const char **data);
	static MValue FromBtreeCell(const char *data, int serial_type, int &size);

	void Clear();

	~MValue();
};

#endif