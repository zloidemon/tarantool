#ifndef INCLUDES_SQL_TARANTOOL_CURSOR_H
#define INCLUDES_SQL_TARANTOOL_CURSOR_H
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
#include "sqlite3.h"
#include "sqliteInt.h"
#include "vdbeInt.h"
#include "sql.h"
#include <string.h>
}

#undef likely
#undef unlikely

#include "say.h"
#include "box/index.h"
#include "box/schema.h"
#include "box/txn.h"
#include "box/tuple.h"
#include "box/box.h"
#include "sql_mvalue.h"

class TarantoolCursor {
private:
	uint32_t space_id;
	uint32_t index_id;
	int type;
	const char *key;
	const char *key_end;
	box_iterator_t *it;
	box_tuple_t *tpl;
	SIndex *sql_index;
	int wrFlag;

	sqlite3 *db;

	void *data;
	uint32_t size;

	bool make_btree_cell_from_tuple();
	bool make_btree_key_from_tuple();
	bool make_msgpuck_from_btree_cell(const char *dt, int sz);

public:
	TarantoolCursor();
	TarantoolCursor(sqlite3 *db_, uint32_t space_id_, uint32_t index_id_, int type_,
               const char *key_, const char *key_end_, SIndex *sql_index_, int wrFlag);
	TarantoolCursor(const TarantoolCursor &ob);
	TarantoolCursor &operator=(const TarantoolCursor &ob);
	int MoveToFirst(int *pRes);
	int MoveToLast(int *pRes);
	int DataSize(u32 *pSize) const;
	const void *DataFetch(u32 *pAmt) const;
	int KeySize(i64 *pSize);
	const void *KeyFetch(u32 *pAmt);
	int Next(int *pRes);
	int MoveToUnpacked(UnpackedRecord *pIdxKey, i64 intKey, int *pRes, RecordCompare xRecordCompare);
	int Insert(const void *pKey,
		i64 nKey, const void *pData, int nData, int nZero, int appendBias,
		int seekResult);
	int DeleteCurrent();
	~TarantoolCursor();
};

#endif
