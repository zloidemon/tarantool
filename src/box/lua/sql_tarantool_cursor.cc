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

#include "sql_tarantool_cursor.h"
#include "space_iterator.h"

int GetSerialTypeNum(u64 number) {
	if (number == 0) return 8;
	if (number == 1) return 9;
	Mem mem;
	memset(&mem, 0, sizeof(Mem));
	mem.u.i = number;
	mem.flags = MEM_Int;
	return sqlite3VdbeSerialType(&mem, 1);
}

int GetSerialTypeNum(double) {
	return 7;
}

int GetSerialTypeNum(i64 number) {
	u64 tmp;
	memcpy(&tmp, &number, sizeof(u64));
	return GetSerialTypeNum(tmp);
}

int GetSerialTypeStr(size_t len) {
	return 2 * len + 13;
}

int GetSerialTypeBin(size_t len) {
	return 2 * len + 12;
}

int PutVarintDataNum(unsigned char *data, u64 n) {
	int st = GetSerialTypeNum(n);
	if ((st == 8) || (st == 9)) {
		return 0;
	}
	Mem mem;
	memset(&mem, 0, sizeof(Mem));
	memcpy((void *)&mem.u.i, (const void *)&n, sizeof(i64));
	mem.flags = MEM_Int;
	return sqlite3VdbeSerialPut(data, &mem, st);
}

int PutVarintDataNum(unsigned char *data, i64 n) {
	u64 tmp;
	memcpy(&tmp, &n, sizeof(u64));
	return PutVarintDataNum(data, tmp);
}

int PutVarintDataNum(unsigned char *data, double n) {
	Mem tmp;
	memset(&tmp, 0, sizeof(Mem));
	tmp.u.r = n;
	tmp.flags = MEM_Real;
	return sqlite3VdbeSerialPut(data, &tmp, GetSerialTypeNum(n));
}

int DataVarintLenNum(u64 number) {
	if ((number == 0) || (number == 1)) return 0;
	int st = GetSerialTypeNum(number);
	return sqlite3VdbeSerialTypeLen(st);
}

int DataVarintLenNum(i64 number) {
	u64 tmp;
	memcpy(&tmp, &number, sizeof(u64));
	return DataVarintLenNum(tmp);
}

int DataVarintLenNum(double) {
	return 8;
}

int CalculateHeaderSize(size_t h) {
	int l1 = sqlite3VarintLen(h);
	int l2 = sqlite3VarintLen(h + l1);
	return l2 + h;
}

//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ T A R A N T O O L   C U R S O R ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

bool TarantoolCursor::make_btree_cell_from_tuple() {
	if (tpl == NULL) return false;
	size = 0;
	int cnt = box_tuple_field_count(tpl);
	MValue *fields = new MValue[cnt];
	int *serial_types = new int[cnt];
	for (int i = 0; i < cnt; ++i) {
		const char *tmp = box_tuple_field(tpl, i);
		fields[i] = MValue::FromMSGPuck(&tmp);
		if (fields[i].GetType() == -1) {
			delete[] fields;
			delete[] serial_types;
			return false;
		}
	}

	int header_size = 0;
	int data_size = 0;

	for (int i = 0; i < cnt; ++i) {
		MValue *val = fields + i;
		serial_types[i] = 0;
		switch(val->GetType()) {
			case MP_NIL: {
				serial_types[i] = 0;
				header_size += 1;
				break;
			}
			case MP_UINT: {
				serial_types[i] = GetSerialTypeNum(val->GetUint64());
				header_size += sqlite3VarintLen(serial_types[i]);
				data_size += DataVarintLenNum(val->GetUint64());
				break;
			}
			case MP_STR: {
				size_t len;
				val->GetStr(&len);
				serial_types[i] = GetSerialTypeStr(len);
				header_size += sqlite3VarintLen(serial_types[i]);
				data_size += len;
				break;
			}
			case MP_INT: {
				serial_types[i] = GetSerialTypeNum(val->GetInt64());
				header_size += sqlite3VarintLen(serial_types[i]);
				data_size += DataVarintLenNum(val->GetInt64());
				break;
			}
			case MP_BIN: {
				size_t len;
				val->GetBin(&len);
				serial_types[i] = GetSerialTypeStr(len);
				header_size += sqlite3VarintLen(serial_types[i]);
				data_size += len;
				break;
			}
			case MP_BOOL: {
				u64 tmp = (val->GetBool()) ? 1 : 0;
				serial_types[i] = GetSerialTypeNum(tmp);
				header_size += sqlite3VarintLen(serial_types[i]);
				data_size += DataVarintLenNum(tmp);
				break;
			}
			case MP_DOUBLE: {
				double tmp = val->GetDouble();
				serial_types[i] = GetSerialTypeNum(tmp);
				header_size += sqlite3VarintLen(serial_types[i]);
				data_size += DataVarintLenNum(tmp);
				break;
			}
			default: {
				serial_types[i] = 0;
				header_size += 1;
				break;
			}
		}
	}

	header_size = CalculateHeaderSize(header_size);
	data = sqlite3DbMallocZero(db, header_size + data_size);
	int offset = 0;
	offset += sqlite3PutVarint((unsigned char *)data + offset, header_size);
	for (int i = 0; i < cnt; ++i) {
		offset += sqlite3PutVarint((unsigned char *)data + offset, serial_types[i]);
	}

	for (int i = 0; i < cnt; ++i) {
		MValue *val = fields + i;
		switch(val->GetType()) {
			case MP_UINT: {
				offset += PutVarintDataNum((unsigned char *)data + offset, val->GetUint64());
				break;
			}
			case MP_INT: {
				offset += PutVarintDataNum((unsigned char *)data + offset, val->GetInt64());
				break;
			}
			case MP_STR: {
				size_t len;
				const char *tmp = val->GetStr(&len);
				memcpy((char *)data + offset, tmp, len);
				offset += len;
				break;
			}
			case MP_BIN: {
				size_t len;
				const char *tmp = val->GetBin(&len);
				memcpy((char *)data + offset, tmp, len);
				offset += len;
				break;
			}
			case MP_BOOL: {
				u64 tmp = (val->GetBool()) ? 1 : 0;
				offset += PutVarintDataNum((unsigned char *)data + offset, tmp);
				break;
			}
			case MP_DOUBLE: {
				double tmp = val->GetDouble();
				offset += PutVarintDataNum((unsigned char *)data + offset, tmp);
				break;
			}
			default: break;
		}		
	}
	size = header_size + data_size;
	return true;
}

bool TarantoolCursor::make_btree_key_from_tuple() {
	if (sql_index == NULL) return false;
	if (tpl == NULL) return false;
	size = 0;
	int cnt = box_tuple_field_count(tpl);
	MValue *fields = new MValue[cnt];
	int *serial_types = new int[cnt];
	int *another_cols = new int[sql_index->nColumn - sql_index->nKeyCol];
	for (int i = 0, k = 0; i < sql_index->nColumn; ++i) {
		bool found = false;
		for (int j = 0; j < sql_index->nKeyCol; ++j) {
			if (i == sql_index->aiColumn[j]) {
				found = true;
				break;
			}
		}
		if (!found) another_cols[k++] = i;
	}
	for (int i = 0; i < sql_index->nKeyCol; ++i) {
		const char *tmp = box_tuple_field(tpl, sql_index->aiColumn[i]);
		fields[size] = MValue::FromMSGPuck(&tmp);
		if (fields[size].GetType() == -1) {
			delete[] fields;
			delete[] serial_types;
			return false;
		}
		++size;
	}
	for (int i = 0, cnt2 = sql_index->nColumn - sql_index->nKeyCol; i < cnt2; ++i) {
		const char *tmp = box_tuple_field(tpl, another_cols[i]);
		fields[size] = MValue::FromMSGPuck(&tmp);
		if (fields[size].GetType() == -1) {
			delete[] fields;
			delete[] serial_types;
			return false;
		}
		++size;
	}
	delete[] another_cols;
	size = 0;

	int header_size = 0;
	int data_size = 0;

	for (int i = 0; i < cnt; ++i) {
		MValue *val = fields + i;
		serial_types[i] = 0;
		switch(val->GetType()) {
			case MP_NIL: {
				serial_types[i] = 0;
				header_size += 1;
				break;
			}
			case MP_UINT: {
				serial_types[i] = GetSerialTypeNum(val->GetUint64());
				header_size += sqlite3VarintLen(serial_types[i]);
				data_size += DataVarintLenNum(val->GetUint64());
				break;
			}
			case MP_STR: {
				size_t len;
				val->GetStr(&len);
				serial_types[i] = GetSerialTypeStr(len);
				header_size += sqlite3VarintLen(serial_types[i]);
				data_size += len;
				break;
			}
			case MP_INT: {
				serial_types[i] = GetSerialTypeNum(val->GetInt64());
				header_size += sqlite3VarintLen(serial_types[i]);
				data_size += DataVarintLenNum(val->GetInt64());
				break;
			}
			case MP_BIN: {
				size_t len;
				val->GetBin(&len);
				serial_types[i] = GetSerialTypeStr(len);
				header_size += sqlite3VarintLen(serial_types[i]);
				data_size += len;
				break;
			}
			case MP_BOOL: {
				u64 tmp = (val->GetBool()) ? 1 : 0;
				serial_types[i] = GetSerialTypeNum(tmp);
				header_size += sqlite3VarintLen(serial_types[i]);
				data_size += DataVarintLenNum(tmp);
				break;
			}
			case MP_DOUBLE: {
				double tmp = val->GetDouble();
				serial_types[i] = GetSerialTypeNum(tmp);
				header_size += sqlite3VarintLen(serial_types[i]);
				data_size += DataVarintLenNum(tmp);
				break;
			}
			default: {
				serial_types[i] = 0;
				header_size += 1;
				break;
			}
		}
	}

	header_size = CalculateHeaderSize(header_size);
	data = sqlite3DbMallocZero(db, header_size + data_size);
	int offset = 0;
	offset += sqlite3PutVarint((unsigned char *)data + offset, header_size);
	for (int i = 0; i < cnt; ++i) {
		offset += sqlite3PutVarint((unsigned char *)data + offset, serial_types[i]);
	}

	for (int i = 0; i < cnt; ++i) {
		MValue *val = fields + i;
		switch(val->GetType()) {
			case MP_UINT: {
				offset += PutVarintDataNum((unsigned char *)data + offset, val->GetUint64());
				break;
			}
			case MP_INT: {
				offset += PutVarintDataNum((unsigned char *)data + offset, val->GetInt64());
				break;
			}
			case MP_STR: {
				size_t len;
				const char *tmp = val->GetStr(&len);
				memcpy((char *)data + offset, tmp, len);
				offset += len;
				break;
			}
			case MP_BIN: {
				size_t len;
				const char *tmp = val->GetBin(&len);
				memcpy((char *)data + offset, tmp, len);
				offset += len;
				break;
			}
			case MP_BOOL: {
				u64 tmp = (val->GetBool()) ? 1 : 0;
				offset += PutVarintDataNum((unsigned char *)data + offset, tmp);
				break;
			}
			case MP_DOUBLE: {
				double tmp = val->GetDouble();
				offset += PutVarintDataNum((unsigned char *)data + offset, tmp);
				break;
			}
			default: break;
		}		
	}
	delete[] fields;
	delete[] serial_types;
	size = header_size + data_size;
	return true;
}

bool TarantoolCursor::make_msgpuck_from_btree_cell(const char *dt, int sz) {
	static const char *__func_name = "TarantoolCursor::make_msgpuck_from_btree_cell";
	u64 header_size;
	int bytes = sqlite3GetVarint((const unsigned char *)dt, &header_size);
	(void)sz;
	const char *orig = dt;
	const char *iterator = dt + bytes;
	int nCol = sql_index->nColumn;
	u64 *serial_types = new u64[nCol];
	int i, j;
	for (i = 0; i < nCol; ++i) {
		bytes = sqlite3GetVarint((const unsigned char *)iterator, serial_types + i);
		iterator += bytes;
	}
	if (i < nCol - 1) {
		say_debug("%s(): cols number in btree cell less than cols number in index\n", __func_name);
		delete[] serial_types;
		return false;
	}
	MValue *vals = new MValue[nCol];
	iterator = orig + header_size;
	int msg_size = 5;
	for (i = 0; i < nCol; ++i) {
		int step;
		vals[i] = MValue::FromBtreeCell(iterator, serial_types[i], step);
		iterator += step;
		switch(vals[i].GetType()) {
			case MP_NIL: msg_size += mp_sizeof_nil(); break;
			case MP_UINT: msg_size += mp_sizeof_uint(vals[i].GetUint64()); break;
			case MP_INT: {
				if (vals[i].GetInt64() >= 0) {
					msg_size += mp_sizeof_uint(vals[i].GetInt64());
				} else {
					msg_size += mp_sizeof_int(vals[i].GetInt64());
				}
				break;
			}
			case MP_STR: msg_size += mp_sizeof_str(vals[i].Size()); break;
			case MP_BIN: msg_size += mp_sizeof_bin(vals[i].Size()); break;
			case MP_DOUBLE: msg_size += mp_sizeof_double(vals[i].GetDouble()); break;
			default: {
				say_debug("%s(): unsupported mvalue type\n", __func_name);
				delete[] serial_types;
				delete[] vals;
				return false;
			}
		}
	}
	delete[] serial_types;
	char *msg_pack = new char[msg_size];
	char *it = msg_pack;
	int *cols_in_index = new int[nCol];
	int *cols_in_msg = new int[nCol];
	for (i = 0; i < sql_index->nKeyCol; ++i) {
		cols_in_index[i] = sql_index->aiColumn[i];
	}
	for (i = 0, j = sql_index->nKeyCol; i < nCol; ++i) {
		bool found = false;
		for (int k = 0; k < j; ++k) {
			if (cols_in_index[k] == i) {
				found = true;
				break;
			}
		}
		if (found) continue;
		cols_in_index[j++] = i;
	}
	for (i = 0; i < nCol; ++i) {
		for (j = 0; j < nCol; ++j) {
			if (cols_in_index[j] == i) {
				cols_in_msg[i] = j;
				break;
			}
		}
	}
	delete[] cols_in_index;
	it = mp_encode_array(it, nCol);
	for (i = 0; i < nCol; ++i) {
		j = cols_in_msg[i];
		switch(vals[j].GetType()) {
			case MP_NIL: it = mp_encode_nil(it); break;
			case MP_UINT: it = mp_encode_uint(it, vals[j].GetUint64()); break;
			case MP_INT: {
				if (vals[i].GetInt64() >= 0) {
					it = mp_encode_uint(it, vals[j].GetInt64());
				} else {
					it = mp_encode_int(it, vals[j].GetInt64());
				}
				break;
			}
			case MP_DOUBLE: it = mp_encode_double(it, vals[j].GetDouble()); break;
			case MP_STR: it = mp_encode_str(it, vals[j].GetStr(), vals[j].Size()); break;
			case MP_BIN: it = mp_encode_bin(it, vals[j].GetBin(), vals[j].Size()); break;
			default: {
				say_debug("%s(): unsupported mvalue type\n", __func_name);
				delete[] msg_pack;
				delete[] cols_in_msg;
				return false;
			}
		}
	}
	delete[] cols_in_msg;
	delete[] vals;
	data = msg_pack;
	size = it - msg_pack;
	return true;
}

TarantoolCursor::TarantoolCursor() : space_id(0), index_id(0), type(-1), key(NULL),
	key_end(NULL), it(NULL), tpl(NULL), sql_index(NULL), wrFlag(-1), original(NULL),
	db(NULL), data(NULL), size(0) { }

TarantoolCursor::TarantoolCursor(sqlite3 *db_, uint32_t space_id_, uint32_t index_id_, int type_,
               const char *key_, const char *key_end_, SIndex *sql_index_, int wrFlag_, BtCursor *cursor_)
: space_id(space_id_), index_id(index_id_), type(type_), key(key_), key_end(key_end_), it(NULL),
	tpl(NULL), sql_index(sql_index_), wrFlag(wrFlag_), original(cursor_), db(db_), data(NULL), size(0) {
	if (!wrFlag)
		it = box_index_iterator(space_id, index_id, type, key, key_end);
}

int TarantoolCursor::MoveToFirst(int *pRes) {
	static const char *__func_name = "TarantoolCursor::MoveToFirst";
	if (it) box_iterator_free(it);
	it = box_index_iterator(space_id, index_id, type, key, key_end);
	int rc = box_iterator_next(it, &tpl);
	if (rc) {
		say_debug("%s(): box_iterator_next return rc = %d <> 0\n", __func_name, rc);
		*pRes = 1;
		tpl = NULL;
	} else {
		*pRes = 0;
	}
	if (tpl == NULL) {
		*pRes = 1;
		return SQLITE_OK;
	}
	rc = this->make_btree_cell_from_tuple();
	return SQLITE_OK;
}

int TarantoolCursor::MoveToLast(int *pRes) {
	if (it) box_iterator_free(it);
	if (type != ITER_LE) {
		say_debug("%s(): change iterator type to ITER_LE", __FUNCTION__);
	}
	type = ITER_LE;
	it = box_index_iterator(space_id, index_id, type, key, key_end);
	int rc = box_iterator_next(it, &tpl);
	if (rc) {
		say_debug("%s(): box_iterator_next return rc = %d <> 0\n", __FUNCTION__, rc);
		tpl = NULL;
	}
	if (tpl == NULL) {
		*pRes = 1;
		return SQLITE_OK;
	}
	*pRes = 0;
	rc = this->make_btree_cell_from_tuple();
	return SQLITE_OK;
}

int TarantoolCursor::DataSize(u32 *pSize) const {
	*pSize = size;
	return SQLITE_OK;
}

const void *TarantoolCursor::DataFetch(u32 *pAmt) const {
	*pAmt = size;
	return data;
}

int TarantoolCursor::KeySize(i64 *pSize) {
	this->make_btree_key_from_tuple();
	*pSize = size;
	return SQLITE_OK;
}

const void *TarantoolCursor::KeyFetch(u32 *pAmt) {
	this->make_btree_key_from_tuple();
	*pAmt = size;
	return data;
}

int TarantoolCursor::Next(int *pRes) {
	static const char *__func_name = "TarantoolCursor::Next";
	if (type == ITER_LE) {
		*pRes = 1;
		return SQLITE_OK;
	}
	int rc = box_iterator_next(it, &tpl);
	if (rc) {
		say_debug("%s(): box_iterator_next return rc = %d <> 0\n", __func_name, rc);
		*pRes = 1;
		return SQLITE_OK;
	}
	if (!tpl) {
		*pRes = 1;
		return SQLITE_OK;
	} else {
		*pRes = 0;
	}
	rc = this->make_btree_cell_from_tuple();
	return SQLITE_OK;
}

int TarantoolCursor::Previous(int *pRes) {
	if (type != ITER_LE) {
		*pRes = 1;
		return SQLITE_OK;
	}
	int rc = box_iterator_next(it, &tpl);
	if (rc) {
		say_debug("%s(): box_iterator_next return rc = %d <> 0\n", __FUNCTION__, rc);
		*pRes = 1;
		return SQLITE_OK;
	}
	if (!tpl) {
		*pRes = 1;
		return SQLITE_OK;
	} else {
		*pRes = 0;
	}
	rc = this->make_btree_cell_from_tuple();
	return SQLITE_OK;
}

int TarantoolCursor::Insert(const void *pKey,
	i64 nKey, const void *pData, int nData, int nZero, int appendBias,
	int seekResult) {
	static const char *__func_name = "TarantoolCursor::Insert";
	(void)pData;
	(void)nData;
	(void)nZero;
	(void)appendBias;
	(void)seekResult;
	if (!make_msgpuck_from_btree_cell((const char *)pKey, nKey)) {
		say_debug("%s(): error while inserting record\n", __func_name);
		return SQLITE_ERROR;
	}
	int rc = box_insert(space_id, (const char *)data, (const char *)data + size, NULL);
	sql_tarantool_api *trn_api = &db->trn_api;
	if (rc) {
		trn_api->log_debug(box_error_message(box_error_last()));
	}
	return rc;
}

int TarantoolCursor::DeleteCurrent() {
	static const char *__func_name = "TarantoolCursor::DeleteCurrent";
	(void)__func_name;
	/*TODO: написать функцию, которая получает текущий ключ потом по этому ключу удлить
			ожидаемая проблема, что итератор после этого упадет, подумать как это решить*/
	int nKeyCol = sql_index->nKeyCol;
	int msg_size = 5;
	char *msg_end, *msg_begin;
	int cSize;
	int rc;
	const char *tmp, *tmp_next;
	for (int i = 0; i < nKeyCol; ++i) {
		tmp_next = tmp = box_tuple_field(tpl, sql_index->aiColumn[i]);
		mp_next(&tmp_next);
		msg_size += tmp_next - tmp;
	}
	msg_begin = msg_end = new char[msg_size];
	msg_end = mp_encode_array(msg_end, nKeyCol);
	for (int i = 0; i < nKeyCol; ++i) {
		tmp_next = tmp = box_tuple_field(tpl, sql_index->aiColumn[i]);
		mp_next(&tmp_next);
		cSize = tmp_next - tmp;
		memcpy((void *)msg_end, (void *)tmp, cSize);
		msg_end += cSize;
	}
	rc = box_delete(space_id, index_id, msg_begin, msg_end, NULL);
	assert(rc == 0);
	box_iterator_free(it);
	type = ITER_GE;
	it = box_index_iterator(space_id, index_id, type, msg_begin, msg_end);
	return rc;
}

int TarantoolCursor::Count(i64 *pnEntry) {
	*pnEntry = box_index_len(space_id, index_id);
	return SQLITE_OK;
}

int TarantoolCursor::MoveToUnpacked(UnpackedRecord *pIdxKey, i64 intKey, int *pRes, RecordCompare xRecordCompare) {
	static const char *__func_name = "TarantoolCursor::MoveToUnpacked";
	int rc = SQLITE_OK;
	if (!xRecordCompare) {
		say_debug("%s(): intKey not supported, intKey = %lld\n", __func_name, intKey);
		return SQLITE_ERROR;
	} else {
		bool reversed = *pRes < 0;
		if (reversed) rc = this->MoveToLast(pRes);
		else rc = this->MoveToFirst(pRes);
		if (!tpl) {
			if (original) original->eState = CURSOR_INVALID;
			*pRes = -1;
			rc = SQLITE_OK;
			say_debug("%s(): space is empty\n", __func_name);
			return rc;
		}
		if (rc) {
			say_debug("%s(): MoveToFirst return rc = %d <> 0\n", __func_name, rc);
			return rc;
		}
		*pRes = 0;
		while(!*pRes) {
			i64 data_size;
			u32 tmp_;
			this->make_btree_key_from_tuple();
			if ((rc = this->KeySize(&data_size))) {
				say_debug("%s(): DataSize return rc = %d <> 0\n", __func_name, rc);
				return rc;
			}
			const void *data = this->KeyFetch(&tmp_);
			data_size = tmp_;
			int c = xRecordCompare(data_size, data, pIdxKey);
			if ((reversed && (c == pIdxKey->r1)) || ((!reversed) && (c == pIdxKey->r2)) ||
				((pIdxKey->default_rc == 0) && (c == 0))) {
				if (c == 0) *pRes = 0;
				else if (!reversed) *pRes = 1;
				else *pRes = -1;
				rc = SQLITE_OK;
				say_debug("%s(): end of search\n", __func_name);
				return rc;
			}
			if (reversed) rc = this->Previous(pRes);
			else rc = this->Next(pRes);
			if (rc) {
				say_debug("%s(): Next/Prev return rc = %d <> 0\n", __func_name, rc);
				return rc;
			}
		}
		if (!reversed)
			*pRes = -1;
		else
			*pRes = 1;
		rc = SQLITE_OK;
	}
	return rc;
}

TarantoolCursor::TarantoolCursor(const TarantoolCursor &ob) : data(NULL), size(0) {
	*this = ob;
}

TarantoolCursor &TarantoolCursor::operator=(const TarantoolCursor &ob) {
	space_id = ob.space_id;
	index_id = ob.index_id;
	type = ob.type;
	key = ob.key;
	key_end = ob.key_end;
	tpl = ob.tpl;
	db = ob.db;
	sql_index = ob.sql_index;
	wrFlag = ob.wrFlag;
	original = ob.original;
	it = NULL;

	if (ob.size) {
		size = ob.size;
		data = sqlite3DbMallocZero(db, size);
		memcpy(data, ob.data, size);
	}
	return *this;
}

TarantoolCursor::~TarantoolCursor() {
	if (it) box_iterator_free(it);
	box_txn_commit();
}