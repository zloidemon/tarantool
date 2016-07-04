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
#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>
#include "hash.h"
#include "sql.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "sqlite3.h"
#include "sqliteInt.h"
#include "btreeInt.h"
}

#undef likely
#undef unlikely

#include "say.h"
#include "box/index.h"
#include "box/schema.h"
#include "box/txn.h"
#include "box/tuple.h"
#include "box/session.h"
#include "box/key_def.h"
#include "trigger.h"
#include "small/rlist.h"
#include "sql_mvalue.h"
#include "smart_ptr.h"
#include "trivia/util.h"

#include "lua/utils.h"
#include <string>
#include "sql_tarantool_cursor.h"
#include "space_iterator.h"

#define GetLastErrMsg box_error_message(box_error_last())

static const char *sqlitelib_name = "sqlite";
static bool triggers_connected = false;
static trigger *on_index_replace_trigger;
static trigger *on_space_replace_trigger;
static trigger *on_trigger_replace_trigger;


/**
 * Structure for linking BtCursor (sqlite) with
 * his tarantool backend - TarantoolCursor
 */
struct TrntlCursor {
	BtCursor *brother; /* BtCursor for TarantoolCursor */
	TarantoolCursor cursor;
	char *key; /* Key for creating box_index_iterator */
};

/**
 * Structure that contains objects needed by API functions.
 * API see below.
 */
typedef struct sql_trntl_self {
	TrntlCursor **cursors; /* All cursors, opened now */
	int cnt_cursors; /* Size of cursors array */
	SIndex **indices; /* All tarantool indices */
	int cnt_indices; /* Size of indices */
} sql_trntl_self;

//~~~~~~~~~~~~~~~~~~~~~~~~ G L O B A L   O P E R A T I O N S   (C++) ~~~~~~~~~~~~~~~~~~~~~~~~

/**
 * Converts struct Table object into msgpuck representation
 */
char *
make_msgpuck_from(const Table *table, int &size, const char *crt_stmt);

/**
 * Converts struct SIndex object into msgpuck representation
 */
char *
make_msgpuck_from(const SIndex *index, int &size, const char *crt_stmt);

/**
 * True if space with name "name" exists in _space.
 * If id != NULL then in it will be space_id.
 */
bool
space_with_name_exists(const char *name, int *id);

/**
 * Get maximal ID of all records in space with id cspace_id.
 */
int
get_max_id_of_space(int cspace_id=BOX_SPACE_ID);

/**
 * Get maximal ID of all records in _index where space id = space_id
 */
int
get_max_id_of_index(int space_id);

/**
 * Get maximal trigger ID of all records in _trigger where space id = space_id
 */
int
get_max_id_of_trigger(int space_id);

/**
 * Insert new struct Table object into _space after converting it
 * into msgpuck.
 */
int
insert_new_table_as_space(Table *table, const char *crt_stmt);

/**
 * Insert new struct Table object into _space as view after converting it
 * into msgpuck.
 */
int
insert_new_view_as_space(Table *table, const char *crt_stmt);

/**
 * Insert new struct SIndex object into _space after converting it
 * into msgpuck.
 */
int
insert_new_sindex_as_index(SIndex *index, const char *crt_stmt);

/**
 * Insert new struct Trigger object into _trigger after converting it
 * into msgpuck.
 */
int
insert_trigger(Trigger *trigger, char *crt_stmt);


/**
 * This function converts space from msgpuck tuple to
 * sqlite3 Table structure.
 */
Table *
get_trntl_table_from_tuple(box_tuple_t *tpl,sqlite3 *db,
	Schema *pSchema, bool *is_temp = NULL, bool *is_view = NULL,
	bool is_delete = false);

/**
 * This function converts index from msgpuck tuple to
 * sqlite3 SIndex structure.
 */
SIndex *
get_trntl_index_from_tuple(box_tuple_t *index_tpl, sqlite3 *db,
	Table *table, bool &ok);

/**
 * Drop index of space space_id with id = index_id. Also removes index
 * from sqlite structures.
 */
int
drop_index(int space_id, int index_id);

/**
 * Remove all indices of space with id = space_id
 */
int
drop_all_indices(int space_id);

field_type
get_tarantool_type_from_sql_aff(int affinity) {
	switch(affinity) {
		case SQLITE_AFF_REAL:
		case SQLITE_AFF_NUMERIC: return NUMBER;
		case SQLITE_AFF_INTEGER: return INT;
		case SQLITE_AFF_TEXT: return STRING;
		case SQLITE_AFF_BLOB: return SCALAR;
		default: return UNKNOWN;
	}
}

int get_maximum_field_type_len() {
	int res = 0;
	for (int i = 0, cur_len = strlen(field_type_strs[0]); cur_len > 0; ++i)
	{
		res = MAX(res, cur_len);
		if (cur_len > 0) cur_len = strlen(field_type_strs[i + 1]);
	}
	return res;
}

extern "C" {

//~~~~~~~~~~~~~~~~~~~~~~~~ G L O B A L   O P E R A T I O N S   (C) ~~~~~~~~~~~~~~~~~~~~~~~~

sqlite3 *global_db = NULL; /* Global descriptor for sqlite connection */

sqlite3 *get_global_db() { return global_db; }

void set_global_db(sqlite3 *db) { global_db = db; }

/**
 * Constructor for sql_tarantool_api structure.
 */
void
sql_tarantool_api_init(sql_tarantool_api *ob);

/**
 * Function for joining sqlite schema and tarantool schema.
 */
void
get_trntl_spaces(void *self_, sqlite3 *db, char **pzErrMsg,
	Schema *pSchema, Hash *idxHash, Hash *tblHash);

/**
 * Add triggers from _trigger to sqlite inmemory representation.
 */
void
get_sql_triggers(void *self_, sqlite3 *db, Schema *pSchema,
	Hash *tblHash, Hash *trigHash);

/**
 * Check if number of root page - num - is container for
 * tarantool space and index numbers.
 */
char
check_num_on_tarantool_id(void *self, u32 num);

/**
 * Create fictive root page number from space_id and index_number.
 */
u32
make_index_id(u32 space_id, u32 index_number) {
	u32 res = 0;
	u32 first = 1 << 30;
	u32 second = (index_number << 28) >> 2;
	u32 third = (space_id << 15) >> 6;
	res = first | second | third;
	return res;
}

/**
 * Create fictive root page number from space_id. Index id in that
 * case is 15.
 */
u32
make_space_id(u32 space_id) {
	return make_index_id(space_id, 15);
}

/**
 * Get space id from root page number.
 */
u32
get_space_id_from(u32 num) {
	return (num << 6) >> 15;
}

/**
 * Get index id from root page number.
 */
u32
get_index_id_from(u32 num) {
	return ((num << 2) >> 28);
}

/**
 * Function for adding new SIndex to array of all indices in global self.
 */
void
add_new_index_to_self(sql_trntl_self *self, SIndex *new_index);

/**
 * Function for removing old SIndex from array of all indices in global self.
 */
void
remove_old_index_from_self(sql_trntl_self *self, SIndex *olf_index);

/**
 * Remove index from self->indices array
 */
void
remove_and_free_sindex(void *self, SIndex *index);

/**
 * Function for logging into tarantool from sqlite.
 */
void
log_debug(const char *msg);

/**
 * Clear tarantool space with given sqlite id.
 */
void
space_truncate_by_id(int space_id);

//~~~~~~~~~~~~~~~~~~~~~~~~ T A R A N T O O L   C U R S O R   A P I ~~~~~~~~~~~~~~~~~~~~~~~~

/**
 * Constructor for TarantoolCursor inside pCur. Cursor will be
 * opened on index specified in iTable.
 *
 * @param self_ Pointer to sql_trntl_self object.
 * @param iTable Sqlite3 root page number for opening cursor
 * 		,but in tarantool it is used for containing
 * 		index and space id.
 * @param pCur Sqlite3 cursor that will send all operations
 * 		to its TarantoolCursor.
 * return SQLITE_OK if success.
 */
int
trntl_cursor_create(void *self_, Btree *p, int iTable,
	int wrFlag, struct KeyInfo *pKeyInfo, BtCursor *pCur);

/**
 * Move TarantoolCursor in pCur on first record in index.
 *
 * @param pRes Set pRes to 1 if space is empty.
 * return SQLITE_OK if success.
 */
int
trntl_cursor_first(void *self_, BtCursor *pCur, int *pRes);

/**
 * Move TarantoolCursor in pCur on last record in index.
 *
 * @param pRes Set pRes to 1 if space is empty.
 * return SQLITE_OK if success.
 */
int
trntl_cursor_last(void *self, BtCursor *pCur, int *pRes);

/**
 * Size of data in current record in bytes.
 *
 * @param pSize In that parameter actual size will be saved.
 * returns always SQLITE_OK
 */
int
trntl_cursor_data_size(void *self_, BtCursor *pCur, u32 *pSize);

/**
 * Get data of current record in sqlite3 btree cell format.
 *
 * @param pAmt Actual size of data will be saved here.
 * returns pointer to record in sqlite btree cell format.
 */
const void *
trntl_cursor_data_fetch(void *self_, BtCursor *pCur, u32 *pAmt);

/**
 * Same as trntl_cursor_data_size - for compatibility with
 * sqlite.
 */
int
trntl_cursor_key_size(void *self, BtCursor *pCur, i64 *pSize);

/**
 * Same as trntl_cursor_data_fetch - for compatibility with
 * sqlite.
 */
const void *
trntl_cursor_key_fetch(void *self, BtCursor *pCur, u32 *pAmt);

/**
 * Move TarantoolCursor in pCur on next record in index.
 *
 * @param pRes This will be set to 0 if success and 1 if current record
 * 		already is last in index.
 * returns SQLITE_OK if success
 */
int
trntl_cursor_next(void *self, BtCursor *pCur, int *pRes);

/**
 * Move TarantoolCursor in pCur on previous record in index.
 *
 * @param pRes This will be set to 0 if success and 1 if current record
 * 		already is first in index.
 * returns SQLITE_OK if success
 */
int
trntl_cursor_prev(void * /*self_*/, BtCursor *pCur, int *pRes);

/**
 * Insert data in pKey into space on index of which is pointed Tarantool
 * Cursor in pCur.
 *
 * @param pKey Date in sqlite btree cell format that must be inserted.
 * @param nKey Size of pKey.
 * @param pData Data for inserting directly in table - not used for tarantool.
 * @param nData Size of pData.
 * Other params is not used now.
 */
int
trntl_cursor_insert(void *self, BtCursor *pCur, const void *pKey,
	i64 nKey, const void *pData, int nData, int nZero, int appendBias,
	int seekResult);

/**
 * Delete tuple pointed by pCur.
 * @param bPreserve If this parameter is zero, then the cursor is left pointing at an
 * 		arbitrary location after the delete. If it is non-zero, then the cursor
 * 		is left in a state such that the next call to Next() or Prev()
 * 		moves it to the same row as it would if the call to DeleteCurrent() had
 * 		been omitted.
 */
int
trntl_cursor_delete_current (void *self, BtCursor *pCur, int bPreserve);

/**
 * Remove TarantoolCursor from global array of opened cursors and
 * release resources of BtCursor.
 */
void
remove_cursor_from_global(sql_trntl_self *self, BtCursor *cursor);

/**
 * Destructor for TarantoolCursor in pCur. Also removes
 * this cursor from global sql_trntl_self.
 */
int
trntl_cursor_close(void *self, BtCursor *pCur);

/**
 * Count of tuples in index on that pCur is pointing.
 */
int
trntl_cursor_count(void *self, BtCursor *pCur, i64 *pnEntry);

/**
 * Move TarantoolCursor in pCur to first record that <= than pIdxKey -
 * unpacked sqlite btree cell with some data.
 *
 * @param pIdxKey Structure that contains data in sqlite btree cell
 * 		format and to that index must be moved.
 * @param intKey Contains key if it is integer.
 * @param pRes Here results will be stored. If *pRes < 0 then
 * 		current record either is smaller than pIdxKey/intKey or
 *		index is empty. If *pRes == 0 then pIdxKey/intKey equal to
 *		current record. If *pRes > 0 then current record is bigger than
 *		pIdxKey/intKey.
 */
int
trntl_cursor_move_to_unpacked(void *self, BtCursor *pCur,
	UnpackedRecord *pIdxKey, i64 intKey, int biasRight, int *pRes,
	RecordCompare xRecordCompare);

/**
 * Drop table or index with id coded in iTable.
 * Set *piMoved in zero for sqlite compatibility.
 */
int
trntl_drop_table(Btree *p, int iTable, int *piMoved);

/**
 * Remove given trigger from _trigger.
 */
void
trntl_drop_trigger(Trigger *pTrigger);

//~~~~~~~~~~~~~~~~~~~~~~~~ T A R A N T O O L   N E S T E D   F U N C S ~~~~~~~~~~~~~~~~~~~~~~~~

/**
 * Function for inserting into space.
 * sql_trntl_self = argv[0], char *name = argv[1], struct Table = argv[2].
 */
int
trntl_nested_insert_into_space(int argc, void *argv);

}

/**
 * Function for getting sqlite connection object from lua stack
*/
static inline
sqlite3 *lua_check_sqliteconn(struct lua_State *L, int index)
{
	sqlite3 *conn = *(sqlite3 **) luaL_checkudata(L, index, sqlitelib_name);
	if (conn == NULL)
		luaL_error(L, "Attempt to use closed connection");
	return conn;
}

extern "C" {

/**
 * Callback function for sqlite3_exec function. It fill
 * sql_result structure with data, received from database.
 *
 * @param data Pointer to struct sql_result
 * @param cols Number of columns in result
 * @param values One concrete row from result
 * @param names Array of column names
*/
int
sql_callback(void *data, int cols, char **values, char **names) {
	sql_result *res = (sql_result *)data;
	if (res->names == NULL) {
		res->names = (char **)malloc(sizeof(char *) * cols);
		for (int i = 0; i < cols; ++i) {
			int tmp = (strlen(names[i]) + 1) * sizeof(char);
			res->names[i] = (char *)malloc(tmp);
			memset(res->names[i], 0, tmp);
			memcpy(res->names[i], names[i], tmp);
		}
		res->cols = cols;
	}
	res->rows++;
	if (res->values == NULL) {
		res->values = (char ***)malloc(sizeof(char **) * 1);
	} else {
		res->values = (char ***)realloc((void *)res->values, sizeof(char **) * res->rows);
	}
	int cur = res->rows - 1;
	res->values[cur] = (char **)malloc(sizeof(char *) * cols);
	for (int i = 0; i < cols; ++i) {
		int tmp = 0;
		if (values[i] == NULL) {
			tmp = sizeof(char) * strlen("NULL");
		} else tmp = sizeof(char) * strlen(values[i]);
		++tmp;
		res->values[cur][i] = (char *)malloc(tmp);
		memset(res->values[cur][i], 0, tmp);
		if (values[i] == NULL) {
			memcpy(res->values[cur][i], "NULL", tmp);
		} else memcpy(res->values[cur][i], values[i], tmp);
	}
	return 0;
}

/**
 * Calls every time when _space is updated and this
 * changes are commited. It applies commited changes to
 * sqlite schema.
 */
void
on_commit_space(struct trigger * /*trigger*/, void * event) {
	static const char *__func_name = "on_commit_space";
	say_debug("%s():\n", __func_name);
	struct txn *txn = (struct txn *) event;
	struct txn_stmt *stmt = txn_last_stmt(txn);
	struct tuple *old_tuple = stmt->old_tuple;
	struct tuple *new_tuple = stmt->new_tuple;
	sqlite3 *db = get_global_db();
	Hash *tblHash = &db->aDb[0].pSchema->tblHash;
	Schema *pSchema = db->aDb[0].pSchema;
	bool is_temp, is_view;
	bool is_delete = false;
	if (old_tuple != NULL) {
		is_delete = (new_tuple == NULL);
		say_debug("%s(): old_tuple != NULL\n", __func_name);
		auto table_deleter = [](Table *ptr){
			if (!ptr) return;
			sqlite3DbFree(get_global_db(), ptr->zName);
			for (int i = 0; i < ptr->nCol; ++i) {
				sqlite3DbFree(get_global_db(), ptr->aCol[i].zName);
				sqlite3DbFree(get_global_db(), ptr->aCol[i].zType);
			}
			sqlite3DbFree(get_global_db(), ptr->aCol);
			sqlite3DbFree(get_global_db(), ptr);
		};
		SmartPtr<Table> table(get_trntl_table_from_tuple(old_tuple, db,
					pSchema, &is_temp, &is_view, is_delete),
			table_deleter);
		if (!table.get()) {
			say_debug("%s(): error while getting table\n", __func_name);
			return;
		}
		if (is_temp) {
			tblHash = &db->aDb[1].pSchema->tblHash;
			pSchema = db->aDb[1].pSchema;
			table->pSchema = pSchema;
		}
		SmartPtr<Table> schema_table((Table *)sqlite3HashFind(tblHash, table->zName),
			table_deleter);
		if (!schema_table.get()) {
			say_debug("%s(): table was not found\n", __func_name);
			return;
		}

		sqlite3HashInsert(tblHash, table->zName, NULL);
	}
	if (new_tuple != NULL) {
		say_debug("%s(): new_tuple != NULL\n", __func_name);
		Table *table = get_trntl_table_from_tuple(new_tuple, db,
		pSchema, &is_temp, &is_view);
		if (is_temp) {
			tblHash = &db->aDb[1].pSchema->tblHash;
			pSchema = db->aDb[1].pSchema;
			table->pSchema = pSchema;
		}
		sqlite3HashInsert(tblHash, table->zName, table);
	}
}

/**
 * Call every time when _space is modified. This function
 * doesn't do any updates but creating new trigger on commiting
 * this _space updates.
 */
void
on_replace_space(struct trigger * /*trigger*/, void * event) {
	static const char *__func_name = "on_replace_space";
	say_debug("%s():\n", __func_name);
	struct txn *txn = (struct txn *) event;
	struct trigger *on_commit = (struct trigger *)
		region_calloc_object_xc(&fiber()->gc, struct trigger);
	trigger_create(on_commit, on_commit_space, NULL, NULL);
	txn_on_commit(txn, on_commit);
}

/**
 * Calls every time when _index is updated and this
 * changes are commited. It applies commited changes to
 * sqlite schema.
 */
void
on_commit_index(struct trigger * /*trigger*/, void * event) {
	static const char *__func_name = "on_commit_index";
	say_debug("%s():\n", __func_name);
	struct txn *txn = (struct txn *) event;
	struct txn_stmt *stmt = txn_last_stmt(txn);
	struct tuple *old_tuple = stmt->old_tuple;
	struct tuple *new_tuple = stmt->new_tuple;
	sqlite3 *db = get_global_db();
	Hash *idxHash = &db->aDb[0].pSchema->idxHash;
	sql_trntl_self *self = (sql_trntl_self *)db->trn_api.self;
	if (old_tuple != NULL) {
		say_debug("%s(): old_tuple != NULL\n", __func_name);
		bool ok;
		SIndex *index = get_trntl_index_from_tuple(old_tuple, db, NULL, ok);
		if (index == NULL) {
			say_debug("%s(): index is null\n", __func_name);
			return;
		}
		if (sqlite3SchemaToIndex(db, index->pSchema)) {
			idxHash = &db->aDb[1].pSchema->idxHash;
		}
		Table *table = index->pTable;
		SIndex *prev = NULL, *cur;
		ok = false;
		for (cur = table->pIndex; cur != NULL; prev = cur, cur = cur->pNext) {
			if (cur->tnum == index->tnum) {
				ok = true;
				if (!prev) {
					table->pIndex = cur->pNext;
					break;
				}
				if (!cur->pNext) {
					prev->pNext = NULL;
					break;
				}
				prev->pNext = cur->pNext;
				break;
			}
		}
		remove_old_index_from_self(self, cur);
		for (int i = 0; i < table->nCol; ++i) {
			sqlite3DbFree(db, index->azColl[i]);
		}
		sqlite3DbFree(db, index);
		if (!ok) {
			say_debug("%s(): index was not found in sql schema\n", __func_name);
			return;
		}
		sqlite3HashInsert(idxHash, cur->zName, NULL);
		for (int i = 0; i < table->nCol; ++i) {
			sqlite3DbFree(db, cur->azColl[i]);
		}
		sqlite3DbFree(db, cur);
	}
	if (new_tuple != NULL) {
		say_debug("%s(): new_tuple != NULL\n", __func_name);
		bool ok;
		SIndex *index = get_trntl_index_from_tuple(new_tuple, db, NULL, ok);
		if (!index) {
			say_debug("%s(): error while getting index from tuple\n", __func_name);
			return;
		}
		if (sqlite3SchemaToIndex(db, index->pSchema)) {
			idxHash = &db->aDb[1].pSchema->idxHash;
		}
		Table *table = index->pTable;
		if (table->pIndex == NULL) {
			table->pIndex = index;
		} else {
			SIndex *last = table->pIndex;
			for (; last->pNext; last = last->pNext) {}
			last->pNext = index;
		}
		sqlite3HashInsert(idxHash, index->zName, index);
		add_new_index_to_self(self, index);
	}
}

/**
 * Call every time when _index is modified. This function
 * doesn't do any updates but creating new trigger on commiting
 * this _index updates.
 */
void
on_replace_index(struct trigger * /*trigger*/, void * event) {
	static const char *__func_name = "on_replace_index";
	say_debug("%s():\n", __func_name);
	struct txn *txn = (struct txn *) event;
	struct trigger *on_commit = (struct trigger *)
		region_calloc_object_xc(&fiber()->gc, struct trigger);
	trigger_create(on_commit, on_commit_index, NULL, NULL);
	txn_on_commit(txn, on_commit);
}

/**
 * Calls every time when _trigger is updated and this
 * changes are commited. It applies commited changes to
 * sqlite schema.
 */
void
on_commit_trigger(struct trigger * /*trigger*/, void * event) {
	static const char *__func_name = "on_commit_index";
	say_debug("%s(): _trigger commited\n", __func_name);
	struct txn *txn = (struct txn *) event;
	struct txn_stmt *stmt = txn_last_stmt(txn);
	struct tuple *old_tuple = stmt->old_tuple;
	struct tuple *new_tuple = stmt->new_tuple;
	if (old_tuple != NULL && new_tuple == NULL) {
		say_debug("%s(): _trigger old_tuple != NULL and new_tuple == NULL\n", __func_name);
		return;
	}
	else if (new_tuple != NULL) {
		say_debug("%s(): _trigger new_tuple != NULL && old_tuple == NULL\n", __func_name);

		const char *sql_field = (char *) tuple_field(new_tuple, 5);
		uint32_t stmt_len;
		const char *z_sql = mp_decode_str(&sql_field, &stmt_len);

		sqlite3_stmt *sqlite3_stmt;
		sqlite3 *db = get_global_db();
		Trigger *pTrigger;
		Vdbe *v;
		const char *pTail;
		int rc = sqlite3_prepare_v2(db, z_sql, stmt_len, &sqlite3_stmt, &pTail);
		if (rc) {
			say_debug("%s(): error while parsing create statement for trigger\n", __func_name);
			return;
		}

		const char *tid_opt = tuple_field(new_tuple, 1);

		if (!db->init.busy) {
			v = (Vdbe *)sqlite3_stmt;
			(void) rc;
			pTrigger = sqlite3TriggerDup(db, v->pParse->pNewTrigger, 0);
			Trigger *pLink = pTrigger;
			Hash *trigHash = &pTrigger->pSchema->trigHash;
			pTrigger->tid = mp_decode_uint(&tid_opt);
			pTrigger = (Trigger *) sqlite3HashInsert(trigHash, pTrigger->zName, (void *)pTrigger);
			if(pTrigger){
				db->mallocFailed = 1;
			} else if(pLink->pSchema == pLink->pTabSchema) {
			  Table *pTab;
			  pTab = (Table *)sqlite3HashFind(&pLink->pTabSchema->tblHash, pLink->table);
			  assert( pTab!=0 );
			  pLink->pNext = pTab->pTrigger;
			  pTab->pTrigger = pLink;
			}

		}
		sqlite3_finalize(sqlite3_stmt);
	}

}


/**
 * Call every time when _index is modified. This function
 * doesn't do any updates but creating new trigger on commiting
 * this _index updates.
 */
void
on_replace_trigger(struct trigger * /*trigger*/, void * event) {
	static const char *__func_name = "on_replace_trigger";
	say_debug("%s():\n", __func_name);
	struct txn *txn = (struct txn *) event;
	struct trigger *on_commit = (struct trigger *)
		region_calloc_object_xc(&fiber()->gc, struct trigger);
	trigger_create(on_commit, on_commit_trigger, NULL, NULL);
	txn_on_commit(txn, on_commit);
}


/**
 * Set global sql_tarantool_api and ready flag.
 * When sqlite will make initialization this flag
 * will be used for detection if global sql_tarantool_api is
 * initialized. And when flag is set then any sqlite3 object
 * will save this API object to self.
 */
void
prepare_to_open_db() {
	if (global_trn_api_is_ready == 1)
		return;
	sql_tarantool_api_init(&global_trn_api);
	global_trn_api_is_ready = 1;
}

/**
 * Connect triggers on creating, dropping or updating spaces and indices.
*/
void
connect_triggers() {
	if (triggers_connected)
		return;
	/* _space */
	struct space *space = space_cache_find(BOX_SPACE_ID);
	struct trigger *alter_space_on_replace_space = (struct trigger *)malloc(sizeof(struct trigger));
	memset(alter_space_on_replace_space, 0, sizeof(struct trigger));
	*alter_space_on_replace_space = {
		RLIST_LINK_INITIALIZER, on_replace_space, NULL, NULL
	};
	on_space_replace_trigger = alter_space_on_replace_space;
	rlist_add_tail_entry(&space->on_replace, alter_space_on_replace_space, link);

	/* _index */
	space = space_cache_find(BOX_INDEX_ID);
	struct trigger *alter_space_on_replace_index = (struct trigger *)malloc(sizeof(struct trigger));
	memset(alter_space_on_replace_index, 0, sizeof(struct trigger));
	*alter_space_on_replace_index = {
		RLIST_LINK_INITIALIZER, on_replace_index, NULL, NULL
	};
	on_index_replace_trigger = alter_space_on_replace_index;
	rlist_add_tail_entry(&space->on_replace, alter_space_on_replace_index, link);

	/* _trigger */
	space = space_cache_find(BOX_TRIGGER_ID);
	struct trigger *alter_space_on_replace_trigger = (struct trigger *)malloc(sizeof(struct trigger));
	memset(alter_space_on_replace_trigger, 0, sizeof(struct trigger));
	*alter_space_on_replace_trigger = {
		RLIST_LINK_INITIALIZER, on_replace_trigger, NULL, NULL
	};
	on_trigger_replace_trigger = alter_space_on_replace_trigger;
	rlist_add_tail_entry(&space->on_replace, alter_space_on_replace_trigger, link);
	triggers_connected = true;
}

void
disconnect_trriggers() {
	rlist_del((rlist *) on_space_replace_trigger);
	rlist_del((rlist *) on_index_replace_trigger);
	rlist_del((rlist *) on_trigger_replace_trigger);

	triggers_connected = false;
}

/**
 * Create connection to sqlite with preparing sql_tarantool_api and
 * with triggers connection.
 */
int
make_connect_sqlite_db(const char *db_name, struct sqlite3 **db) {
	static const char *__func_name = "make_connect_sqlite_db";
	prepare_to_open_db();
	int rc = sqlite3_open(db_name, db);
	if (rc != SQLITE_OK) {
		return rc;
	}
	set_global_db(*db);
	char *errMsg = NULL;
	sqlite3Init(*db, &errMsg);
	if (errMsg != NULL) {
		say_debug("%s(): error while initializing db, msg: %s\n", __func_name, errMsg);
		return SQLITE_ERROR;
	}
	connect_triggers();
	return rc;
}

/**
 * Function for creating connection to sqlite database.
 * Pointer to connection object is put on lua stack with
 * name 'sqlite'.
 *
 * Params needed on stack:
 * - (char *) - name of database
*/
static int
lua_sql_connect(struct lua_State *L)
{
	if (lua_gettop(L) < 1) {
		return luaL_error(L, "Usage: sql.connect(<database_name>)");
	}
	const char *db_name = luaL_checkstring(L, 1);
	sqlite3 *db = NULL;

	int rc = make_connect_sqlite_db(db_name, &db);
	if (rc != SQLITE_OK) {
		luaL_error(L, "Error: error during opening database <%s>", db_name);
	}
	global_db = db;
	sqlite3 **ptr = (sqlite3 **)lua_newuserdata(L, sizeof(sqlite3 *));
	*ptr = db;
	luaL_getmetatable(L, sqlitelib_name);
	lua_setmetatable(L, -2);
	return 1;
}
}

/**
 * Function for pushing content of sql_result structure
 * on lua stack. It will be table with first row as names
 * and others as values. It push lua stack true and table of
 * results if all is ok and false else.
 *
 * @param L lua_State object on stack of which result must be pushed
 * @param res Object of struct sql_result from which data must be pushed
*/
static int
lua_sqlite_pushresult(struct lua_State *L, sql_result res)
{

	lua_createtable(L, 1, 1);
	int tmp = lua_gettop(L);
	lua_pushstring(L, "__serialize");
	lua_pushstring(L, "seq");
	lua_settable(L, tmp);


	//lua_setmetatable(L, tid);
	lua_createtable(L, 0, 1 + res.rows);
	int tid = lua_gettop(L);
	//adding array of names
	lua_createtable(L, res.cols, 0);
	lua_pushvalue(L, tmp);
	lua_setmetatable(L, -2);
	int names_id = lua_gettop(L);
	for (int i = 0; i < res.cols; ++i) {
		lua_pushstring(L, res.names[i]);
		lua_rawseti(L, names_id, i + 1);
	}
	lua_rawseti(L, tid, 1);
	for (int i = 0; i < res.rows; ++i) {
		lua_createtable(L, res.cols, 0);
		lua_pushvalue(L, tmp);
		lua_setmetatable(L, -2);
		int vals_id = lua_gettop(L);
		for (int j = 0; j < res.cols; ++j) {
			lua_pushstring(L, res.values[i][j]);
			lua_rawseti(L, vals_id, j + 1);
		}
		lua_rawseti(L, tid, i + 2);
	}
	return 1;
}

/**
 * Close connection to sqlite - release all allocated memory
 * and close all opened files.
 */
static int
lua_sqlite_close(struct lua_State *L)
{
	sqlite3 *db = lua_check_sqliteconn(L, 1);
	sqlite3_close(db);
	disconnect_trriggers();
	return 0;
}

/**
 * Function for executing SQL query.
 *
 * Params needed on stack:
 * - (char *) - SQL query
*/
static int
lua_sqlite_execute(struct lua_State *L)
{
	sqlite3 *db = lua_check_sqliteconn(L, 1);
	size_t len;
	const char *sql = lua_tolstring(L, 2, &len);

	char *errMsg = NULL;
	sql_result res;
	sql_result_init(&res);
	int rc = sqlite3_exec(db, sql, sql_callback, (void *)&res, &errMsg);
	if (rc != SQLITE_OK) {
		std::string tmp;
		try {
			if (errMsg != NULL) tmp = std::string(errMsg);
		} catch(...) { tmp = "Error during reading error message"; }
		sqlite3_free(errMsg);
		luaL_error(L, "Error: error while executing query `%s`\nError message: %s", sql, tmp.c_str());
	}
	int ret = lua_sqlite_pushresult(L, res);
	sql_result_free(&res);
	return ret;
}

/**
 * Function for initializing sqlite submodule
*/
void
box_lua_sqlite_init(struct lua_State *L)
{
	static const struct luaL_reg sqlite_methods [] = {
		{"execute",	lua_sqlite_execute},
		{"close",	lua_sqlite_close},
		{NULL, NULL}
	};
	luaL_newmetatable(L, sqlitelib_name);
	lua_pushvalue(L, -1);
	luaL_register(L, NULL, sqlite_methods);
	lua_setfield(L, -2, "__index");
	lua_pushstring(L, sqlitelib_name);
	lua_setfield(L, -2, "__metatable");
	lua_pop(L, 1);

	static const struct luaL_reg module_funcs [] = {
		{"connect", lua_sql_connect},
		{NULL, NULL}
	};
	luaL_register_module(L, "sql", module_funcs);
	lua_pop(L, 1);
}

//~~~~~~~~~~~~~~~~~~~~~~~~ G L O B A L   O P E R A T I O N S   (C++) ~~~~~~~~~~~~~~~~~~~~~~~~

char *
make_msgpuck_from(const Table *table, int &size, const char *crt_stmt) {
	char *msg_data, *it;
	int space_id = get_space_id_from(table->tnum);
	int msg_size = 5 + mp_sizeof_uint(space_id);
	struct credentials *cred = current_user();
	const char *engine = "memtx";
	int name_len = strlen("name");
	int type_len = strlen("type");
	int engine_len = strlen(engine);
	int temporary_len = strlen("temporary");
	int view_len = strlen("view");
	int table_name_len = strlen(table->zName);
	int crt_stmt_len = crt_stmt ? strlen(crt_stmt) : 0;
	int stmt_len = strlen("crt_stmt");
	int has_crt_stmt = !!crt_stmt;
	int max_type_len = get_maximum_field_type_len();
	Column *cur;
	int i;
	bool is_temp = sqlite3SchemaToIndex(get_global_db(), table->pSchema);
	bool is_view = (table->pSelect != 0);
	int flags_cnt = (int)is_temp + is_view + has_crt_stmt;
	msg_size += mp_sizeof_uint(cred->uid);
	msg_size += mp_sizeof_str(table_name_len);
	msg_size += mp_sizeof_str(engine_len);
	msg_size += mp_sizeof_uint(0);
	msg_size += mp_sizeof_map(flags_cnt);
	//size of flags
	if (is_temp) {
		msg_size += mp_sizeof_str(temporary_len)
			+ mp_sizeof_bool(true);
	}
	if (is_view) {
		msg_size += mp_sizeof_str(view_len)
			+ mp_sizeof_bool(true);
	}
	if (crt_stmt) {
		msg_size += mp_sizeof_str(stmt_len)
				+ mp_sizeof_str(crt_stmt_len);
	}
	//sizeof parts
	msg_size += 5;
	msg_size += 5; // array of maps
	msg_size += (mp_sizeof_map(2) + mp_sizeof_str(name_len) +\
		mp_sizeof_str(type_len)) * table->nCol;
	for (i = 0; i < table->nCol; ++i) {
		cur = table->aCol + i;
		msg_size += mp_sizeof_str(strlen(cur->zName));
		msg_size += mp_sizeof_str(max_type_len);
	}
	msg_data = new char[msg_size];
	it = msg_data;
	it = mp_encode_array(it, 7);
	it = mp_encode_uint(it, space_id); // space id
	it = mp_encode_uint(it, cred->uid); // owner id
	it = mp_encode_str(it, table->zName, table_name_len); // space name
	it = mp_encode_str(it, engine, engine_len); // space engine
	it = mp_encode_uint(it, 0); // field count

	// flags
	it = mp_encode_map(it, flags_cnt);
	if (is_temp) {
		it = mp_encode_str(it, "temporary", temporary_len);
		it = mp_encode_bool(it, true);
	}
	if (is_view) {
		it = mp_encode_str(it, "view", view_len);
		it = mp_encode_bool(it, true);
	}
	if (crt_stmt) {
		it = mp_encode_str(it, "crt_stmt", stmt_len);
		it = mp_encode_str(it, crt_stmt, crt_stmt_len);
	}
	it = mp_encode_array(it, table->nCol);
	for (i = 0; i < table->nCol; ++i) {
		cur = table->aCol + i;
		it = mp_encode_map(it, 2);
			it = mp_encode_str(it, "name", name_len);
			it = mp_encode_str(it, cur->zName, strlen(cur->zName));
			it = mp_encode_str(it, "type", type_len);
			field_type ftp = get_tarantool_type_from_sql_aff(cur->affinity);
			if (ftp == UNKNOWN) {
				size = 0;
				delete[] msg_data;
				return NULL;
			}
			it = mp_encode_str(it, field_type_strs[ftp],
				strlen(field_type_strs[ftp]));
	}
	size = it - msg_data;
	return msg_data;
}

char *
make_msgpuck_from(const SIndex *index, int &size, const char *crt_stmt) {
	char *msg_data = NULL;
	int msg_size = 5;
	int space_id = get_space_id_from(index->tnum);
	int index_id = get_index_id_from(index->tnum) % 15;
	int name_len = strlen(index->zName);
	int crt_stmt_len = 0;
	int opts_size = 1; //'unique' opt always here
	int max_type_len = 10;
	if (crt_stmt) {
		crt_stmt_len = strlen(crt_stmt);
		opts_size++;
	}
	const char *type = "TREE";
	int type_len = strlen(type);
	msg_size += mp_sizeof_uint(space_id);
	msg_size += mp_sizeof_uint(index_id);
	msg_size += mp_sizeof_str(name_len);
	msg_size += mp_sizeof_str(type_len);
	msg_size += mp_sizeof_map(opts_size);
	msg_size += mp_sizeof_str(6); //strlen('unique')
	msg_size += mp_sizeof_bool(true); //sizes of true and false are equal
	if (crt_stmt) {
		msg_size += mp_sizeof_str(8); //len of 'crt_stmt'
		msg_size += mp_sizeof_str(crt_stmt_len);
	}
	msg_size += mp_sizeof_array(index->nKeyCol);
	for (int i = 0; i < index->nKeyCol; ++i) {
		msg_size += mp_sizeof_array(2);
			msg_size += mp_sizeof_uint(index->aiColumn[i]);
			msg_size += mp_sizeof_str(max_type_len);
	}

	msg_data = new char[msg_size];
	char *it = msg_data;
	it = mp_encode_array(it, 6);
	it = mp_encode_uint(it, space_id);
	it = mp_encode_uint(it, index_id);
	it = mp_encode_str(it, index->zName, name_len);
	it = mp_encode_str(it, type, type_len);
	it = mp_encode_map(it, opts_size);
		it = mp_encode_str(it, "unique", 6);
	if (index->idxType == 0) {
		it = mp_encode_bool(it, false);
	} else {
		it = mp_encode_bool(it, true);
	}
	if (crt_stmt) {
		it = mp_encode_str(it, "crt_stmt", 8);
		it = mp_encode_str(it, crt_stmt, crt_stmt_len);
	}
	it = mp_encode_array(it, index->nKeyCol);
	for (int i = 0; i < index->nKeyCol; ++i) {
		int col = index->aiColumn[i];
		int affinity = index->pTable->aCol[col].affinity;
		it = mp_encode_array(it, 2);
		it = mp_encode_uint(it, col);
		field_type ftp = get_tarantool_type_from_sql_aff(affinity);
		if (ftp == UNKNOWN) {
			size = 0;
			delete[] msg_data;
			return NULL;
		}
		it = mp_encode_str(it, field_type_strs[ftp],
			strlen(field_type_strs[ftp]));
	}
	size = it - msg_data;
	return msg_data;
}

bool
space_with_name_exists(const char *name, int *id) {
	//static const char *__func_name = "space_with_name_exists";
	char key[2], *key_end = mp_encode_array(key, 0);
	char exists = false;
	void *params[3];
	params[0] = (void *)name;
	params[1] = (void *)id;
	params[2] = (void *)&exists;

	SpaceIterator::SIteratorCallback callback =\
		[](box_tuple_t *tpl, int, void **argv) -> int {
			const char *data = box_tuple_field(tpl, 2);
			MValue space_name = MValue::FromMSGPuck(&data), space_id;
			if (!strcmp(space_name.GetStr(), (const char *)(argv[0]))) {
				if (argv[1]) {
					data = box_tuple_field(tpl, 0);
					space_id = MValue::FromMSGPuck(&data);
					*(int *)(argv[1]) = space_id.GetUint64();
				}
				*(char *)(argv[2]) = 1;
				return 1;
			}
			return 0;
		};

	SpaceIterator space_iterator(3, params, callback, BOX_SPACE_ID, 0, key, key_end);
	space_iterator.IterateOver();
	return !!exists;
}

int
get_max_id_of_space(int cspace_id) {
	int id_max = -1;
	char key[2], *key_end = mp_encode_array(key, 0);
	void *argv[1];
	argv[0] = (void *)&id_max;
	SpaceIterator::SIteratorCallback callback =\
		[](box_tuple_t *tpl, int, void **argv) -> int {
			const char *data = box_tuple_field(tpl, 0);
			MValue space_id = MValue::FromMSGPuck(&data);
			int *id_max = (int *)argv[0];
			if ((int64_t)space_id.GetUint64() > *id_max)
				*id_max = space_id.GetUint64();
			return 0;
		};
	SpaceIterator space_iterator(1, argv, callback, cspace_id, 0, key, key_end);
	space_iterator.IterateOver();
	return id_max;
}

int get_max_id_of_index(int space_id) {
	int id_max = -2;
	char key[128], *key_end = mp_encode_array(key, 1);
	key_end = mp_encode_uint(key_end, space_id);
	void *argv[1];
	argv[0] = (void *)&id_max;
	SpaceIterator::SIteratorCallback callback=\
		[](box_tuple_t *tpl, int, void **argv) -> int {
			const char *data = box_tuple_field(tpl, 1);
			MValue index_id = MValue::FromMSGPuck(&data);
			int *id_max = (int *)argv[0];
			if ((int64_t)index_id.GetUint64() > *id_max)
				*id_max = index_id.GetUint64();
			return 0;
		};
	SpaceIterator space_iterator(1, argv, callback, BOX_INDEX_ID, 0, key, key_end, ITER_EQ);
	space_iterator.IterateOver();
	if (space_iterator.IsOpen() && space_iterator.IsEnd() && (id_max < 0)) id_max = -1;
	return id_max;
}

int get_max_id_of_trigger(int space_id) {
	int id_max = -2;
	char key[128], *key_end = mp_encode_array(key, 1);
	key_end = mp_encode_uint(key_end, space_id);
	void *argv[1];
	argv[0] = (void *)&id_max;
	SpaceIterator::SIteratorCallback callback=\
		[](box_tuple_t *tpl, int, void **argv) -> int {
			const char *data = box_tuple_field(tpl, 1);
			MValue index_id = MValue::FromMSGPuck(&data);
			int *id_max = (int *)argv[0];
			if ((int64_t)index_id.GetUint64() > *id_max)
				*id_max = index_id.GetUint64();
			return 0;
		};
	SpaceIterator space_iterator(1, argv, callback, BOX_TRIGGER_ID, 0, key, key_end, ITER_EQ);
	space_iterator.IterateOver();
	if (space_iterator.IsOpen() && space_iterator.IsEnd() && (id_max < 0)) id_max = -1;
	return id_max;
}

int
insert_new_table_as_space(Table *table, const char *crt_stmt) {
	static const char *__func_name = "insert_new_table_as_space";
	char *msg_data;
	int msg_size;
	int id_max = get_max_id_of_space();
	id_max = MAX(id_max, BOX_SYSTEM_ID_MAX + 1);
	int rc;
	if (id_max < 0) {
		say_debug("%s(): error while getting max id\n", __func_name);
		return -1;
	}
	id_max += 5;
	table->tnum = make_space_id(id_max);
	msg_data = make_msgpuck_from((const Table *)table, msg_size, crt_stmt);
	rc = box_insert(BOX_SPACE_ID, msg_data, msg_data + msg_size, NULL);
	delete[] msg_data;
	return rc;
}

int
insert_new_view_as_space(Table *table, const char *crt_stmt) {
	static const char *__func_name = "insert_new_view_as_view";
	char *msg_data;
	int msg_size;
	int id_max = get_max_id_of_space();
	id_max = MAX(id_max, BOX_SYSTEM_ID_MAX + 1);
	int rc;
	if (id_max < 0) {
		say_debug("%s(): error while getting max id\n", __func_name);
		return -1;
	}
	id_max += 5;
	table->tnum = make_space_id(id_max);
	msg_data = make_msgpuck_from((const Table *)table, msg_size, crt_stmt);
	rc = box_insert(BOX_SPACE_ID, msg_data, msg_data + msg_size, NULL);
	delete[] msg_data;
	return rc;
}

int
insert_new_sindex_as_index(SIndex *index, const char *crt_stmt) {
	static const char *__func_name = "insert_new_sindex_as_index";
	char *msg_data = NULL;
	int msg_size;
	int space_id = get_space_id_from(index->pTable->tnum);
	int id_max = get_max_id_of_index(space_id);
	int rc;
	if (id_max < -1) {
		say_debug("%s(): error while getting max id\n", __func_name);
		return -1;
	}
	id_max++;
	index->tnum = make_index_id(space_id, id_max);
	msg_data = make_msgpuck_from((const SIndex *)index, msg_size, crt_stmt);
	rc = box_insert(BOX_INDEX_ID, msg_data, msg_data + msg_size, NULL);
	delete[] msg_data;
	return rc;
}

int
insert_trigger(Trigger *trigger, char *crt_stmt) {
	static const char *__func_name = "insert_trigger";
	uint32_t iDb = sqlite3SchemaToIndex(get_global_db(), trigger->pSchema);

	Hash *tblHash = &trigger->pSchema->tblHash;
	Table *table = (Table *)sqlite3HashFind(tblHash, trigger->table);

	if (!table) {
		say_debug("%s(): error while getting id of space\n", __func_name);
		return 1;
	}
	uint32_t space_id = get_space_id_from(table->tnum);

	uint32_t max_id = get_max_id_of_trigger(space_id);
	uint32_t new_id = max_id + 1;

	uint32_t create_len = 15; //15 = strlen("CREATE TRIGGER ")
	uint32_t full_stmt_len = create_len + strlen(crt_stmt);
	char *crt_stmt_full = new char[full_stmt_len];
	sprintf(crt_stmt_full, "CREATE TRIGGER %s", crt_stmt);

	bool is_temp = (iDb == 1);

	uint32_t temporary_len = 9; //9 = strlen("temporary")
	uint32_t rec_size = 0;
	rec_size += mp_sizeof_array(8);
	rec_size += mp_sizeof_uint(space_id);
	rec_size += mp_sizeof_uint(new_id);
	rec_size += mp_sizeof_uint(0); // owner id
	rec_size += mp_sizeof_str(strlen(trigger->zName));
	rec_size += mp_sizeof_str(strlen(trigger->table));
	rec_size += mp_sizeof_str(full_stmt_len);
	rec_size += mp_sizeof_uint(0); // setuid
	rec_size += mp_sizeof_map(1);
	rec_size += mp_sizeof_str(temporary_len);
	rec_size += mp_sizeof_bool(is_temp);

	char *new_tuple = new char[rec_size];
	char *it = new_tuple;

	it = mp_encode_array(it, 8);
	it = mp_encode_uint(it, space_id);
	it = mp_encode_uint(it, new_id);
	it = mp_encode_uint(it, 0); // owner id
	it = mp_encode_str(it, trigger->zName, strlen(trigger->zName)); // plus one for ending zero
	it = mp_encode_str(it, trigger->table, strlen(trigger->table));
	it = mp_encode_str(it, crt_stmt_full, full_stmt_len);
	it = mp_encode_uint(it, 0); // setuid
	it = mp_encode_map(it, 1);
	it = mp_encode_str(it, "temporary", temporary_len);
	it = mp_encode_bool(it, is_temp);

	box_insert(BOX_TRIGGER_ID, new_tuple, it, NULL);

	delete[] new_tuple;

	return 0;
}

Table *
get_trntl_table_from_tuple(box_tuple_t *tpl, sqlite3 *db,
	Schema *pSchema, bool *is_temp, bool *is_view, bool is_delete)
{
	int cnt = box_tuple_field_count(tpl);
	if (cnt != 7) {
		say_debug("%s(): box_tuple_field_count not equal 7, but %d\n", __FUNCTION__, cnt);
		return NULL;
	}
	sqlite3 *db_alloc = 0;
	Hash *tblHash = &db->aDb[0].pSchema->tblHash;
	char zName[256];
	memset(zName, 0, sizeof(zName));

	const char *data = box_tuple_field(tpl, 0);
	int type = (int)mp_typeof(*data);
	MValue tbl_id = MValue::FromMSGPuck(&data);
	if (tbl_id.GetType() != MP_UINT) {
		say_debug("%s(): field[0] in tuple in SPACE must be uint, but is %d\n", __FUNCTION__, type);
		return NULL;
	}

	data = box_tuple_field(tpl, 2);
	type = (int)mp_typeof(*data);
	if (type != MP_STR) {
		say_debug("%s(): field[2] in tuple in SPACE must be string, but is %d\n", __FUNCTION__, type);
		return NULL;
	} else {
		size_t len;
		MValue buf = MValue::FromMSGPuck(&data);
		buf.GetStr(&len);
		memcpy(zName, buf.GetStr(), len);
	}

	Table *new_table = (Table *)sqlite3DbMallocZero(db_alloc, sizeof(Table));
	new_table->zName = sqlite3DbStrDup(db_alloc, zName);
	new_table->iPKey = -1;
	new_table->pSchema = pSchema;
	new_table->nRef = 1;
	new_table->nRowLogEst = 200;
	SmartPtr<Table> table(new_table, [](Table *ptr){
		if (!ptr) return;
		sqlite3DbFree(get_global_db(), ptr->zName);
		sqlite3DbFree(get_global_db(), ptr);
	});
	char key[128], *key_end;
	key_end = mp_encode_array(key, 1);
	key_end = mp_encode_uint(key_end, tbl_id.GetUint64());
	int index_len = 200;
	void *argv[1];
	argv[0] = (void *)&index_len;
	SpaceIterator::SIteratorCallback callback =
	[](box_tuple_t *tpl, int, void **argv) -> int
	{
		int *index_len = (int *)argv[0];
		const char *data = box_tuple_field(tpl, 0);
		MValue space_id = MValue::FromMSGPuck(&data);
		data = box_tuple_field(tpl, 1);
		MValue index_id = MValue::FromMSGPuck(&data);
		int tmp = box_index_len(space_id.GetUint64(),
			index_id.GetUint64());
		if (tmp > *index_len) *index_len = tmp;
		return 1;
	};
	SpaceIterator iterator(1, argv, callback, BOX_INDEX_ID,
		0, key, key_end, ITER_EQ);
	iterator.IterateOver();
	table->nRowLogEst = index_len;
	table->szTabRow = ESTIMATED_ROW_SIZE;
	table->tnum = make_space_id(tbl_id.GetUint64());
	if (db->mallocFailed) {
		say_debug("%s(): error while allocating memory for table\n", __FUNCTION__);
		return NULL;
	}
	table->pSchema = pSchema;
	table->iPKey = -1;
	/* View has not got any tableFlags in sqlite */
	if (! is_view) {
		table->tabFlags = TF_WithoutRowid | TF_HasPrimaryKey;
	}
	//Get flags
	if (is_temp) *is_temp = false;
	if (is_view) *is_view = false;
	bool local_is_view = false;

	data = box_tuple_field(tpl, 5);
	uint32_t map_size = mp_decode_map(&data);
	Vdbe *v;
	Table *pstmtTable = NULL;
	for (uint32_t i = 0; i < map_size; ++i) {
		MValue name = MValue::FromMSGPuck(&data);
		MValue value = MValue::FromMSGPuck(&data);
		const char *pname = name.GetStr();
		if (!strcmp(pname, "temporary") && value.GetBool()) {
			if (is_temp) *is_temp = true;
		}
		if (!strcmp(pname, "view") && value.GetBool()) {
			if (is_view) *is_view = true;
			local_is_view = true;
		}
		if (!strcmp(pname, "crt_stmt") && !is_delete) {
			/* we need not to parse crt_stmt_in case of deleting
			 * view
			 **/
			sqlite3_stmt *stmt;
			const char *pTail;
			const char *z_sql = value.GetStr();
			uint32_t len = strlen(z_sql);
			sqlite3 *db = get_global_db();
			int rc;
			if (!local_is_view) {
				//this is TABLE
				rc = sqlite3_prepare_v3_taran(db, z_sql, len, &stmt, &pTail);
				if (rc != SQLITE_OK) {
					say_debug("%s(): error while parsing create statement for table\n", __FUNCTION__);
					return NULL;
				}
				v = (Vdbe *)stmt;
				//all that we need is on callbacks stack
				void **argv;
				rc = sqlite3VdbeNestedCallbackByID(v, "creating space", NULL, NULL, &argv);
				if (rc < 0) {
					say_debug("%s(): creating space callback was not found\n", __FUNCTION__);
					sqlite3_finalize(stmt);
					return NULL;
				}
				pstmtTable = make_deep_copy_Table((Table *)argv[2], db_alloc);
				sqlite3_finalize(stmt);
				pstmtTable->tnum = table->tnum;
				pstmtTable->pSchema = pSchema;
				pstmtTable->iPKey = -1;
				pstmtTable->tabFlags = TF_WithoutRowid | TF_HasPrimaryKey;
				pstmtTable->nRowLogEst = table->nRowLogEst;
				pstmtTable->szTabRow = table->szTabRow;
				continue;
			} else {
				//this is VIEW
				rc = sqlite3_prepare_v2(db, z_sql, len, &stmt, &pTail);
				if (rc != SQLITE_OK) {
					say_debug("%s(): error while parsing create statement for view\n", __FUNCTION__);
					return NULL;
				}
				if (!db->init.busy) {
					v = (Vdbe *)stmt;
					(void) rc;
					Parse *pParse = v->pParse;
					table->pSelect = sqlite3SelectDup(0, pParse->pNewTable->pSelect, 0);
				} else {
					/* In init process we need to update tnum in
					 * inmemory representation.
					 **/
					pstmtTable = (Table *)sqlite3HashFind(tblHash, table->zName);
					pstmtTable->tnum = table->tnum;
				}
				sqlite3_finalize(stmt);
			}
		}
	}
	//we can't return immediately above because we need walk through all space opts
	if (pstmtTable) return pstmtTable;
	//Get space format
	data = box_tuple_field(tpl, 6);
	uint32_t len = mp_decode_array(&data);

	SmartPtr<Column> cols((Column *)sqlite3DbMallocZero(db_alloc, len * sizeof(Column)),
		[](Column *ptr){sqlite3DbFree(0, ptr);});
	memset(cols.get(), 0, sizeof(Column) * len);
	int nCol = 0;
	auto free_cols_content = [&](){
		for (int i = 0; i < nCol; ++i) {
			Column *cur = cols.get() + i;
			sqlite3DbFree(db_alloc, cur->zName);
			sqlite3DbFree(db_alloc, cur->zType);
		}
	};
	for (uint32_t i = 0; i < len; ++i) {
		map_size = mp_decode_map(&data);
		MValue colname, coltype;
		if (map_size != 2) {
			say_debug("%s(): map_size not equal 2, but %u\n", __FUNCTION__, map_size);
			free_cols_content();
			return NULL;
		}
		for (uint32_t j = 0; j < map_size; ++j) {
			MValue key = MValue::FromMSGPuck(&data);
			MValue val = MValue::FromMSGPuck(&data);
			if ((key.GetType() != MP_STR) || (val.GetType() != MP_STR)) {
				say_debug("%s(): unexpected not string format\n", __FUNCTION__);
				free_cols_content();
				return NULL;
			}
			char c = key.GetStr()[0];
			if ((c == 'n') || (c == 'N')) {
				//name
				colname = val;
			} else if ((c == 't') || (c == 'T')) {
				//type
				coltype = val;
			} else {
				say_debug("%s(): unknown string in space_format\n", __FUNCTION__);
				free_cols_content();
				return NULL;
			}
		}
		if (colname.IsEmpty() || coltype.IsEmpty()) {
			say_debug("%s(): both name and type must be init\n", __FUNCTION__);
		}
		char c1 = coltype.GetStr()[0];
		char c2 = coltype.GetStr()[1];
		const char *sql_type;
		int affinity;
		if ((c1 == 'N') || (c1 == 'n')) {
			if (coltype.Size() > 3) {
				affinity = SQLITE_AFF_NUMERIC;
				sql_type = "NUMERIC";
			} else {
				affinity = SQLITE_AFF_INTEGER;
				sql_type = "INT";
			}
		} else if ((c1 == 's') || (c1 == 'S')) {
			if ((c2 == 'T') || (c2 == 't')) {
				affinity = SQLITE_AFF_TEXT;
				sql_type = "TEXT";
			} else {
				affinity = SQLITE_AFF_BLOB;
				sql_type = "BLOB";
			}
		} else if ((c1 == 'A') || (c1 == 'a')) {
			affinity = SQLITE_AFF_BLOB;
			sql_type = "BLOB";
		} else if ((c1 == 'i') || (c1 == 'I')) {
			affinity = SQLITE_AFF_INTEGER;
			sql_type = "INT";
		} else {
			affinity = SQLITE_AFF_BLOB;
			sql_type = "BLOB";
		}
		Column *cur = cols.get() + nCol++;
		size_t len;
		colname.GetStr(&len);
		cur->zName = (char *)sqlite3DbMallocZero(db_alloc, len + 1);
		memset(cur->zName, 0, len + 1);
		memcpy(cur->zName, colname.GetStr(), len);

		len = strlen(sql_type);
		cur->zType = (char *)sqlite3DbMallocZero(db_alloc, len + 1);
		memset(cur->zType, 0, len + 1);
		memcpy(cur->zType, sql_type, len);
		cur->affinity = affinity;
	}
	table->aCol = cols.take_away();
	table->nCol = nCol;
	return table.take_away();
}

SIndex *
get_trntl_index_from_tuple(box_tuple_t *index_tpl, sqlite3 *db, Table *table, bool &ok) {
	static const char *__func_name = "get_trntl_index_from_tuple";
	ok = false;

	int cnt = box_tuple_field_count(index_tpl);
	if (cnt != 6) {
		say_debug("%s(): box_tuple_field_count not equal 6, but %d, for next index\n", __func_name, cnt);
		return NULL;
	}

	//---- SPACE ID ----

	const char *data = box_tuple_field(index_tpl, 0);
	int type = (int)mp_typeof(*data);
	MValue space_id = MValue::FromMSGPuck(&data);
	if (space_id.GetType() != MP_UINT) {
		say_debug("%s(): field[0] in tuple in INDEX must be uint, but is %d\n", __func_name, type);
		return NULL;
	}

	if (!table) {
		Schema *pSchema = db->aDb[0].pSchema;
		char key[256], *key_end;
		key_end = mp_encode_array(key, 1);
		key_end = mp_encode_uint(key_end, space_id.GetUint64());
		bool is_temp = false;
		void *params[2];
		MValue space_name;
		params[0] = (void *)&is_temp;
		params[1] = (void *)&space_name;
		SpaceIterator::SIteratorCallback callback =\
			[](box_tuple_t *tpl, int, void **argv) -> int {
				bool *is_temp = (bool *)(argv[0]);
				MValue *space_name = (MValue *)argv[1];
				const char *data;
				data = box_tuple_field(tpl, 5);
				uint32_t map_size = mp_decode_map(&data);
				for (uint32_t i = 0; i < map_size; ++i) {
					MValue name = MValue::FromMSGPuck(&data);
					MValue value = MValue::FromMSGPuck(&data);
					if (!strcmp(name.GetStr(), "temporary") && value.GetBool()) {
						*is_temp = true;
						break;
					}
				}
				data = box_tuple_field(tpl, 2);
				*space_name = MValue::FromMSGPuck(&data);
				return 0;
			};
		SpaceIterator iterator(2, params, callback, BOX_SPACE_ID, 0, key, key_end, ITER_EQ);
		iterator.IterateOver();
		if (is_temp) {
			pSchema = db->aDb[1].pSchema;
		}
		table = (Table *)sqlite3HashFind(&pSchema->tblHash, space_name.GetStr());
		if (!table) {
			say_debug("%s(): space with id %llu was not found\n", __func_name, space_id.GetUint64());
			return NULL;
		}
	}

	u32 tbl_id = get_space_id_from(table->tnum);
	if (space_id.GetUint64() != tbl_id) {
		ok = true;
		return NULL;
	}

	SmartPtr<SIndex> index;
	char *extra = NULL;
	int extra_sz = 0;
	char zName[256];

	//---- INDEX ID ----

	data = box_tuple_field(index_tpl, 1);
	type = (int)mp_typeof(*data);
	MValue index_id = MValue::FromMSGPuck(&data);
	if (index_id.GetType() != MP_UINT) {
		say_debug("%s(): field[1] in tuple in INDEX must be uint, but is %d\n", __func_name, type);
		return NULL;
	}

	//---- INDEX NAME ----

	data = box_tuple_field(index_tpl, 2);
	type = (int)mp_typeof(*data);
	if (type != MP_STR) {
		say_debug("%s(): field[2] in tuple in INDEX must be string, but is %d\n", __func_name, type);
		return NULL;
	} else {
		memset(zName, 0, 256);
		size_t len = 0;
		MValue buf = MValue::FromMSGPuck(&data);
		buf.GetStr(&len);
		sprintf(zName, "%d_%d_", (int)space_id.GetUint64(), (int)index_id.GetUint64());
		memcpy(zName + strlen(zName), buf.GetStr(), len);
		extra_sz += strlen(zName) + 1;
	}

	index = SmartPtr<SIndex>(sqlite3AllocateIndexObject(db, table->nCol, extra_sz, &extra),
		[](SIndex *ptr){sqlite3DbFree(get_global_db(), ptr);});
	if (db->mallocFailed) {
		say_debug("%s(): error while allocating memory for index\n", __func_name);
		return NULL;
	}
	index->pTable = table;
	index->pSchema = table->pSchema;
	index->isCovering = 1;
	index->noSkipScan = 1;
	if (index_id.GetUint64()) {
		index->idxType = 0;
	} else {
		index->idxType = 2;
	}
	index->tnum = make_index_id(space_id.GetUint64(), index_id.GetUint64());
	index->zName = extra;
	{
		int len = strlen(zName);
		memcpy(index->zName, zName, len);
		index->zName[len] = 0;
	}

	//---- SORT ORDER ----

	index->aSortOrder[0] = 0;
	index->szIdxRow = ESTIMATED_ROW_SIZE;
	index->nColumn = table->nCol;
	index->onError = OE_None;
	for (int j = 0; j < table->nCol; ++j) {
		index->azColl[j] = reinterpret_cast<char *>(sqlite3DbMallocZero(db, sizeof(char) * (strlen("BINARY") + 1)));
		memcpy(index->azColl[j], "BINARY", strlen("BINARY"));
	}
	auto free_index_azColls = [&]() {
		for (int j = 0; j < table->nCol; ++j) {
			sqlite3DbFree(db, index->azColl[j]);
		}
	};

	//---- TYPE ----

	data = box_tuple_field(index_tpl, 3);
	type = (int)mp_typeof(*data);
	if (type != MP_STR) {
		say_debug("%s(): field[3] in tuple in INDEX must be string, but is %d\n", __func_name, type);
		free_index_azColls();
		return NULL;
	} else {
		MValue buf = MValue::FromMSGPuck(&data);
		if ((buf.GetStr()[0] == 'T') || (buf.GetStr()[0] == 't')) {
			index->bUnordered = 0;
		} else {
			index->bUnordered = 1;
		}
	}

	//---- UNIQUE ----

	data = box_tuple_field(index_tpl, 4);
	int map_size = mp_decode_map(&data);
	if (map_size > 2) {
		say_debug("%s(): field[4] map size in INDEX must be <= 2, but is %u\n", __func_name, map_size);
		free_index_azColls();
		return NULL;
	}
	MValue key, value;
	for (int j = 0; j < map_size; ++j) {
		key = MValue::FromMSGPuck(&data);
		value = MValue::FromMSGPuck(&data);
		if (key.GetType() != MP_STR) {
			say_debug("%s(): field[4][%u].key must be string, but type %d\n", __func_name, j, key.GetType());
			free_index_azColls();
			return NULL;
		}
		if (value.GetType() != MP_BOOL) {
			if (value.GetType() != MP_STR) {
				say_debug("%s(): field[4][%u].value must be string 'crt_stmt',"\
					" but type %d\n", __FUNCTION__, j, value.GetType());
				free_index_azColls();
				return NULL;
			}
			say_debug("%s(): found index with crt_stmt: %s\n", __FUNCTION__, value.GetStr());
			continue;
		}
		if ((key.GetStr()[0] == 'u') || (key.GetStr()[0] == 'U')) {
			if (value.GetBool()) {
				if (index->idxType != 2) index->idxType = 1;
			}
		}
	}

	//---- INDEX FORMAT ----

	data = box_tuple_field(index_tpl, 5);
	MValue idx_cols = MValue::FromMSGPuck(&data);
	if (idx_cols.GetType() != MP_ARRAY) {
		say_debug("%s(): field[5] in INDEX must be array, but type is %d\n", __func_name, idx_cols.GetType());
		free_index_azColls();
		return NULL;
	}
	index->nKeyCol = idx_cols.Size();
	for (int j = 0, sz = idx_cols.Size(); j < sz; ++j) {
		i16 num = idx_cols[j][0][0]->GetUint64();
		index->aiColumn[j] = num;
	}
	for (int j = 0, start = idx_cols.Size(); j < table->nCol; ++j) {
		bool used = false;
		for (uint32_t k = 0, sz = idx_cols.Size(); k < sz; ++k) {
			if (index->aiColumn[k] == j) {
				used = true;
				break;
			}
		}
		if (used) continue;
		index->aiColumn[start++] = j;
	}

	for (int i = 0; i < index->nKeyCol; ++i) index->aiRowLogEst[i] = table->nRowLogEst;

	ok = true;
	return index.take_away();
}

int
drop_index(int space_id, int index_id) {
	int rc;
	char key[128], *key_end;
	key_end = mp_encode_array(key, 2);
	key_end = mp_encode_uint(key_end, space_id);
	key_end = mp_encode_uint(key_end, index_id);
	rc = box_delete(BOX_INDEX_ID, 0, key, key_end, NULL);
	if (rc) {
		say_debug("%s(): error = %s\n", __FUNCTION__, GetLastErrMsg);
	}
	return rc;
}

int
drop_all_indices(int space_id) {
	int rc;
	char key[128], *key_end;
	key_end = mp_encode_array(key, 1);
	key_end = mp_encode_uint(key_end, space_id);
	void *argv[2];
	int indices[15];
	int ind_cnt = 0;
	argv[0] = (void *)indices;
	argv[1] = (void *)&ind_cnt;
	SpaceIterator::SIteratorCallback callback = [](box_tuple_t *tpl, int, void **argv)
	-> int {
		int *indices = (int *)argv[0];
		int *ind_cnt = (int *)argv[1];
		const char *data = box_tuple_field(tpl, 1);
		MValue index_id = MValue::FromMSGPuck(&data);
		indices[(*ind_cnt)++] = index_id.GetUint64();
		return 0;
	};
	SpaceIterator iterator(3, argv, callback, BOX_INDEX_ID, 0, key, key_end, ITER_EQ);
	rc = iterator.IterateOver();
	for (--ind_cnt; ind_cnt >= 0; --ind_cnt) {
		rc = drop_index(space_id, indices[ind_cnt]);
		if (rc) return rc;
	}
	return rc;
}

extern "C" {

//~~~~~~~~~~~~~~~~~~~~~~~~ G L O B A L   O P E R A T I O N S ~~~~~~~~~~~~~~~~~~~~~~~~

void
sql_tarantool_api_init(sql_tarantool_api *ob) {
	ob->get_trntl_spaces = &get_trntl_spaces;
	ob->get_sql_triggers = &get_sql_triggers;
	sql_trntl_self *self = new sql_trntl_self;
	ob->self = self;
	self->cursors = NULL;
	self->cnt_cursors = 0;
	self->indices = NULL;
	self->cnt_indices = 0;
	ob->trntl_cursor_create = trntl_cursor_create;
	ob->trntl_cursor_first = trntl_cursor_first;
	ob->trntl_cursor_last = trntl_cursor_last;
	ob->trntl_cursor_data_size = trntl_cursor_data_size;
	ob->trntl_cursor_data_fetch = trntl_cursor_data_fetch;
	ob->trntl_cursor_next = trntl_cursor_next;
	ob->trntl_cursor_prev = trntl_cursor_prev;
	ob->trntl_cursor_close = trntl_cursor_close;
	ob->trntl_cursor_count = trntl_cursor_count;
	ob->check_num_on_tarantool_id = check_num_on_tarantool_id;
	ob->trntl_cursor_move_to_unpacked = trntl_cursor_move_to_unpacked;
	ob->trntl_cursor_key_size = trntl_cursor_key_size;
	ob->trntl_cursor_key_fetch = trntl_cursor_key_fetch;
	ob->trntl_cursor_insert = trntl_cursor_insert;
	ob->trntl_cursor_delete_current = trntl_cursor_delete_current;
	ob->log_debug = log_debug;
	ob->trntl_nested_insert_into_space = trntl_nested_insert_into_space;
	ob->trntl_drop_table = trntl_drop_table;
	ob->get_global_db = get_global_db;
	ob->set_global_db = set_global_db;
	ob->space_truncate_by_id = space_truncate_by_id;
	ob->remove_and_free_sindex = remove_and_free_sindex;
	ob->insert_trigger = insert_trigger;
	ob->trntl_drop_trigger = trntl_drop_trigger;
}

void
get_trntl_spaces(void *self_, sqlite3 *db, char **pzErrMsg, Schema *pSchema,
	Hash *idxHash, Hash *tblHash) {
	static const char *__func_name = "get_trntl_spaces";
	(void)pzErrMsg;
	sql_trntl_self *self = reinterpret_cast<sql_trntl_self *>(self_);
	bool must_be_temp = sqlite3SchemaToIndex(db, pSchema);

	char key[2], *key_end = mp_encode_array(key, 0);
	SpaceIterator space_iterator(WITHOUT_CALLBACK, BOX_SPACE_ID, 0, key, key_end);
	box_tuple_t *tpl = NULL;

	do {
		if (space_iterator.Next()) {
			say_debug("%s(): space_iterator return not 0\n", __func_name);
			return;
		}
		if (space_iterator.IsEnd()) break;
		tpl = space_iterator.GetTuple();
		bool is_temp;
		bool is_view;
		SmartPtr<Table> table(get_trntl_table_from_tuple(tpl, db,
					pSchema, &is_temp, &is_view),
			[](Table *ptr){
				if (!ptr) return;
				sqlite3 *db = get_global_db();
				int i;
				sqlite3DbFree(db, ptr->zName);
				for (i = 0; i < ptr->nCol; ++i) {
					sqlite3DbFree(db, ptr->aCol[i].zName);
					sqlite3DbFree(db, ptr->aCol[i].zType);
				}
				sqlite3DbFree(db, ptr->aCol);
				sqlite3DbFree(db, ptr);
			});
		if (is_temp != must_be_temp) {
			continue;
		}
		if (table.get() == NULL) return;
		if (!strcmp(table->zName, "sqlite_master")) {
			continue;
		}
		if (!strcmp(table->zName, "sqlite_temp_master")) {
			continue;
		}
		if (! (db->init.busy && is_view) ) {
			/**
			  * For view insertion in inmemory representation made
			  * in sqlite3EndTable
			  */
			sqlite3HashInsert(tblHash, table->zName, table.get());
		}

		//----Indices----
		SpaceIterator index_iterator(WITHOUT_CALLBACK, BOX_INDEX_ID, 0, key, key_end);
		box_tuple_t *index_tpl = NULL;
		do {
			MValue key, value, idx_cols, index_id, space_id;
			SIndex *index = NULL;

			if (index_iterator.Next()) {
				say_debug("%s(): index_iterator return not 0 for next index\n", __func_name);
				goto __get_trntl_spaces_index_bad;
			}
			if (index_iterator.IsEnd()) {
				break;
			}
			index_tpl = index_iterator.GetTuple();
			bool ok;
			index = get_trntl_index_from_tuple(index_tpl, db, table.get(), ok);
			if (index == NULL) {
				if (ok) continue;
				say_debug("%s(): error while getting index from tuple\n", __func_name);
				sqlite3HashInsert(tblHash, table->zName, NULL);
				return;
			}

			sqlite3HashInsert(idxHash, index->zName, index);
			if (!table->pIndex) {
				table->pIndex = index;
			} else {
				SIndex *last = table->pIndex;
				for (; last->pNext; last = last->pNext) {}
				last->pNext = index;
			}

			add_new_index_to_self(self, index);

			continue;
__get_trntl_spaces_index_bad:
			sqlite3HashInsert(tblHash, table->zName, NULL);
			sqlite3HashInsert(idxHash, index->zName, NULL);
			if (index->aSortOrder) sqlite3DbFree(db, index->aSortOrder);
			if (index->aiColumn) sqlite3DbFree(db, index->aiColumn);
			if (index->azColl) {
				for (uint32_t j = 0; j < index->nColumn; ++j) {
					sqlite3DbFree(db, index->azColl[j]);
				}
				sqlite3DbFree(db, index->azColl);
			}
			if (index) sqlite3DbFree(db, index);
			return;
		} while (index_iterator.InProcess());
		table.take_away();
	} while (space_iterator.InProcess());
	return;
}

void
get_sql_triggers(void *self_, sqlite3 *db, Schema *pSchema,
	Hash *tblHash, Hash *trigHash) {
	static const char *__func_name = "get_sql_triggers";
	(void)*self_;
	(void)tblHash;
	(void)trigHash;
	//sql_trntl_self *self = reinterpret_cast<sql_trntl_self *>(self_);

	bool must_be_temp = sqlite3SchemaToIndex(db, pSchema);
	bool is_temp;
	char key[2], *key_end = mp_encode_array(key, 0);
	SpaceIterator trigger_iterator(WITHOUT_CALLBACK, BOX_TRIGGER_ID, 0, key, key_end);
	box_tuple_t *trigger_tpl = NULL;

	do {
		if (trigger_iterator.Next()) {
			say_debug("%s(): trigger_iterator return not 0\n", __func_name);
			return;
		}
		if (trigger_iterator.IsEnd()) break;


		trigger_tpl = trigger_iterator.GetTuple();

		/* get create statement */
		const char *tid_opt = tuple_field(trigger_tpl, 1);

		uint32_t name_len, crt_stmt_len;
		const char *trig_name_field = (const char *) tuple_field(trigger_tpl, 3);
		const char *trig_name_tuple = mp_decode_str(&trig_name_field, &name_len);
		const char *crt_stmt_field = (const char *) tuple_field(trigger_tpl, 5);
		const char *crt_stmt_tuple = mp_decode_str(&crt_stmt_field, &crt_stmt_len);
		char *crt_stmt = new char[crt_stmt_len + 1];
		memcpy((void *) crt_stmt, (void *) crt_stmt_tuple, crt_stmt_len);
		crt_stmt[crt_stmt_len] = 0;
		char *trig_name = new char[name_len + 1];
		memcpy((void *) trig_name, (void *) trig_name_tuple, name_len);
		crt_stmt[name_len] = 0;

		const char *options = tuple_field(trigger_tpl, 7);
		static uint32_t temporary_len = 9; // 9 = strlen("temporary")
		uint32_t opt_size = mp_decode_map(&options);

		if (opt_size != 1) {
			say_debug("%s(): expected 1 option other found\n", __func_name);
		}

		uint32_t key_len;
		const char *key = mp_decode_str(&options, &key_len);
		if (key_len == temporary_len && !memcmp(key, "temporary", temporary_len)) {
			if (mp_typeof(*options) != MP_BOOL) {
				say_debug("%s(): temporary option is not BOOL\n", __func_name);
				return;
			}
			is_temp = mp_decode_bool(&options);
		}
		else {
			say_debug("%s(): unexpected option\n", __func_name);
			return;
		}
		if (is_temp == must_be_temp) {
			/*
			 * Insertion in sqlite inmemory representation will be made in
			 * sqlite3FinishTrigger. sqlite3FinishTrigger will be called in
			 * sqlite3_prepare_v2.
			 */

			sqlite3_stmt *sqlite3_stmt;
			uint32_t len = strlen(crt_stmt);
			Trigger *pTrigger;
			Vdbe *v;
			const char *pTail;
			int rc = sqlite3_prepare_v2(db, crt_stmt, len, &sqlite3_stmt, &pTail);
			if (rc) {
				say_debug("%s(): error while parsing create statement for trigger\n", __func_name);
				return;
			}
			v = (Vdbe *)sqlite3_stmt;

			Hash *trigHash = &pSchema->trigHash;
			pTrigger = (Trigger*)sqlite3HashFind(trigHash, trig_name);
			pTrigger->tid = mp_decode_uint(&tid_opt);
			sqlite3_finalize(sqlite3_stmt);
		}

		delete[] trig_name;
		delete[] crt_stmt;
	} while (trigger_iterator.InProcess());
}


char
check_num_on_tarantool_id(void * /*self*/, u32 num) {
	u32 buf;
	buf = (num << 23) >> 23;
	if (buf) return 0;
	return !!(num & (1 << 30));
}

void
add_new_index_to_self(sql_trntl_self *self, SIndex *new_index) {
	SIndex **new_indices = new SIndex*[self->cnt_indices + 1];
	memcpy(new_indices, self->indices, self->cnt_indices * sizeof(SIndex *));
	new_indices[self->cnt_indices] = new_index;
	self->cnt_indices++;
	if (self->indices) delete[] self->indices;
	self->indices = new_indices;
}

void
remove_old_index_from_self(sql_trntl_self *self, SIndex *old_index) {
	SIndex **new_indices = nullptr;
	int new_count = 0;
	for (int i = 0; i < self->cnt_indices; ++i) {
		if ((self->indices[i] != old_index) &&
			(self->indices[i]->tnum != old_index->tnum))
		{
			new_count++;
		}
	}
	new_indices = new SIndex*[new_count];
	for (int i = 0, j = 0; i < self->cnt_indices; ++i) {
		if ((self->indices[i] != old_index) &&
			(self->indices[i]->tnum != old_index->tnum))
		{
			new_indices[j] = self->indices[i];
			j++;
		}
	}
	self->cnt_indices = new_count;
	delete[] self->indices;
	self->indices = new_indices;
}

void
remove_and_free_sindex(void *self, SIndex *index) {
	remove_old_index_from_self((sql_trntl_self *)self, index);
}

void log_debug(const char *msg) {
	say_debug("%s\n", msg);
}

void
space_truncate_by_id(int root_page) {
	box_truncate(get_space_id_from(root_page));
}

//~~~~~~~~~~~~~~~~~~~~~~~~ T A R A N T O O L   C U R S O R   A P I ~~~~~~~~~~~~~~~~~~~~~~~~

int
trntl_cursor_create(void *self_, Btree *p, int iTable, int wrFlag,
							  struct KeyInfo *pKeyInfo, BtCursor *pCur) {
	static const char *__func_name = "trntl_cursor_create";
	sql_trntl_self *self = reinterpret_cast<sql_trntl_self *>(self_);

	for (int i = 0; i < self->cnt_cursors; ++i) {
		if (self->cursors[i]->brother == pCur) {
			say_debug("%s(): trying to reinit existing cursor\n", __func_name);
			return SQLITE_ERROR;
		}
	}
	u32 num;
	memcpy(&num, &iTable, sizeof(u32));
	TrntlCursor *c = new TrntlCursor();
	c->key = new char[2];
	char *key_end = mp_encode_array(c->key, 0);
	int index_id = 0;
	int type = ITER_ALL;
	int space_id = get_space_id_from(num);
	index_id = get_index_id_from(num) % 15;
	u32 tnum = make_index_id(space_id, index_id);
	SIndex *sql_index = NULL;
	for (int i = 0; i < self->cnt_indices; ++i) {
		if ((u32)self->indices[i]->tnum == tnum) {
			sql_index = self->indices[i];
			break;
		}
	}
	if (sql_index == NULL) {
		say_debug("%s(): sql_index not found, space_id = %d, index_id = %d\n", __func_name, space_id, index_id);
		delete c;
		delete[] c->key;
		return SQLITE_ERROR;
	}
	c->cursor = TarantoolCursor(p->db, space_id, index_id, type, c->key, key_end, sql_index, wrFlag, pCur);
	c->brother = pCur;
	pCur->trntl_cursor = (void *)c;
	pCur->pBtree = p;
	pCur->pBt = p->pBt;
	memcpy(&pCur->pgnoRoot, &iTable, sizeof(Pgno));
	pCur->iPage = -1;
	pCur->curFlags = wrFlag;
	pCur->pKeyInfo = pKeyInfo;
	pCur->eState = CURSOR_VALID;
	if (self->cnt_cursors == 0) {
		self->cursors = new TrntlCursor*[1];
	} else {
		TrntlCursor **tmp = new TrntlCursor*[self->cnt_cursors + 1];
		memcpy(tmp, self->cursors, sizeof(TrntlCursor *) * self->cnt_cursors);
		delete[] self->cursors;
		self->cursors = tmp;
	}
	self->cursors[self->cnt_cursors++] = c;
	return SQLITE_OK;
}

int
trntl_cursor_first(void * /*self_*/, BtCursor *pCur, int *pRes) {
	TrntlCursor *c = (TrntlCursor *)(pCur->trntl_cursor);
	return c->cursor.MoveToFirst(pRes);
}

int
trntl_cursor_last(void * /*self_*/, BtCursor *pCur, int *pRes) {
	TrntlCursor *c = (TrntlCursor *)(pCur->trntl_cursor);
	return c->cursor.MoveToLast(pRes);
}

int
trntl_cursor_data_size(void * /*self_*/, BtCursor *pCur, u32 *pSize) {
	TrntlCursor *c = (TrntlCursor *)(pCur->trntl_cursor);
	return c->cursor.DataSize(pSize);
}

const void *
trntl_cursor_data_fetch(void * /*self_*/, BtCursor *pCur, u32 *pAmt) {
	TrntlCursor *c = (TrntlCursor *)(pCur->trntl_cursor);
	return c->cursor.DataFetch(pAmt);
}

int
trntl_cursor_key_size(void * /*self_*/, BtCursor *pCur, i64 *pSize) {
	TrntlCursor *c = (TrntlCursor *)(pCur->trntl_cursor);
	return c->cursor.KeySize(pSize);
}

const void *
trntl_cursor_key_fetch(void * /*self_*/, BtCursor *pCur, u32 *pAmt) {
	TrntlCursor *c = (TrntlCursor *)(pCur->trntl_cursor);
	return c->cursor.KeyFetch(pAmt);
}

int
trntl_cursor_next(void * /*self_*/, BtCursor *pCur, int *pRes) {
	TrntlCursor *c = (TrntlCursor *)(pCur->trntl_cursor);
	return c->cursor.Next(pRes);
}

int
trntl_cursor_prev(void * /*self_*/, BtCursor *pCur, int *pRes) {
	TrntlCursor *c = (TrntlCursor *)(pCur->trntl_cursor);
	return c->cursor.Previous(pRes);
}

int trntl_cursor_insert(void * /*self_*/, BtCursor *pCur, const void *pKey,
	i64 nKey, const void *pData, int nData, int nZero, int appendBias,
	int seekResult) {
	TrntlCursor *c = (TrntlCursor *)(pCur->trntl_cursor);
	return c->cursor.Insert(pKey, nKey, pData, nData, nZero, appendBias,
		seekResult);
}

int trntl_cursor_delete_current(void * /*self_*/, BtCursor *pCur, int bPreserve) {
	(void)bPreserve;
	TrntlCursor *c = (TrntlCursor *)(pCur->trntl_cursor);
	return c->cursor.DeleteCurrent();
}

void
remove_cursor_from_global(sql_trntl_self *self, BtCursor *cursor) {
	TrntlCursor *c = (TrntlCursor *)cursor->trntl_cursor;
	delete[] c->key;
	TrntlCursor **new_cursors = new TrntlCursor*[self->cnt_cursors - 1];
	for (int i = 0, j = 0; i < self->cnt_cursors; ++i) {
		if (self->cursors[i]->brother != cursor) {
			new_cursors[j++] = self->cursors[i];
		}
	}
	delete[] self->cursors;
	self->cnt_cursors--;
	if (self->cnt_cursors)
		self->cursors = new_cursors;
	else
		self->cursors = nullptr;
	delete c;
	sqlite3DbFree(get_global_db(), cursor->pKey);
	cursor->pKey = 0;
	cursor->eState = CURSOR_INVALID;
}

int
trntl_cursor_close(void *self_, BtCursor *pCur) {
	static const char *__func_name = "trntl_cursor_first";
	sql_trntl_self *self = reinterpret_cast<sql_trntl_self *>(self_);
	if (!self) {
		say_debug("%s(): self must not be NULL\n", __func_name);
		return SQLITE_ERROR;
	}
	remove_cursor_from_global(self, pCur);
	return SQLITE_OK;
}

int
trntl_cursor_count(void * /*self_*/, BtCursor *pCur, i64 *pnEntry) {
	TrntlCursor *c = (TrntlCursor *)(pCur->trntl_cursor);
	return c->cursor.Count(pnEntry);
}

int
trntl_cursor_move_to_unpacked(void * /*self_*/, BtCursor *pCur, UnpackedRecord *pIdxKey, i64 intKey, int /*biasRight*/, int *pRes, RecordCompare xRecordCompare) {
	TrntlCursor *c = (TrntlCursor *)pCur->trntl_cursor;
	return c->cursor.MoveToUnpacked(pIdxKey, intKey, pRes, xRecordCompare);
}

int
trntl_drop_table(Btree *p, int iTable, int *piMoved)
{
	int space_id = get_space_id_from(iTable);
	int index_id = get_index_id_from(iTable);
	int rc;
	char key[128], *key_end;
	*piMoved = 0;
	(void)p;
	say_debug("%s(): space_id: %d, index_id: %d\n",
		__FUNCTION__, space_id, index_id);
	if (index_id == 15) {
		rc = drop_all_indices(space_id);
		if (rc) {
			return SQLITE_ERROR;
		}
		//drop space
		key_end = mp_encode_array(key, 1);
		key_end = mp_encode_uint(key_end, space_id);
		rc = box_delete(BOX_SPACE_ID, 0, key, key_end, NULL);
	} else {
		//drop index
		key_end = mp_encode_array(key, 2);
		key_end = mp_encode_uint(key_end, space_id);
		key_end = mp_encode_uint(key_end, index_id);
		rc = box_delete(BOX_INDEX_ID, 0, key, key_end, NULL);
	}
	if (rc) {
		say_debug("%s(): error while droping = %s\n",
			__FUNCTION__, GetLastErrMsg);
		return SQLITE_ERROR;
	}
	return SQLITE_OK;
}

void
trntl_drop_trigger(Trigger *pTrigger) {
	char key[10];
	char *it = key;
	Table *pTab = (Table *)sqlite3HashFind(&pTrigger->pTabSchema->tblHash,
										 pTrigger->table);
	uint32_t id = get_space_id_from(pTab->tnum);
	it = mp_encode_array(it, 2);
	it = mp_encode_uint(it, id);
	it = mp_encode_uint(it, pTrigger->tid);

	box_delete(BOX_TRIGGER_ID, 0, key, it, NULL);
}


//~~~~~~~~~~~~~~~~~~~~~~~~ T A R A N T O O L   N E S T E D   F U N C S ~~~~~~~~~~~~~~~~~~~~~~~~

int
trntl_nested_insert_into_space(int argc, void *argv_) {
	static const char *__func_name = "trntl_nested_insert_into_space";
	void **argv = (void **)argv_;
	sql_trntl_self *self = reinterpret_cast<sql_trntl_self *>(argv[0]);
	int rc;
	(void)argc;
	if (!self) {
		say_debug("%s(): self must not be NULL\n", __func_name);
		return SQLITE_ERROR;
	}
	char *name = (char *)(argv[1]);
	if (!strcmp(name, "_space")) {
		Table *table = (Table *)argv[2];
		rc = insert_new_table_as_space(table, (char *)argv[3]);
		if (rc) {
			say_debug("%s(): error while inserting new table as space\n", __func_name);
			return SQLITE_ERROR;
		}
	} else if (!strcmp(name, "_index")) {
		SIndex *index = (SIndex *)argv[2];
		rc = insert_new_sindex_as_index(index, (char *)argv[3]);
		if (rc) {
			say_debug("%s(): error while insering new sindex as index\n", __func_name);
			return SQLITE_ERROR;
		}
	} else if (!strcmp(name, "_view")) {
		Table *table = (Table *)argv[2];
		char *crt_stmt = (char *)argv[3];
		rc = insert_new_view_as_space(table, crt_stmt);
		if (rc) {
			say_debug("%s(): error while inserting new view as space\n", __func_name);
			return SQLITE_ERROR;
		}
	} else if (!strcmp(name, "_trigger")) {
		Trigger *trigger = (Trigger *)argv[2];
		char *crt_stmt = (char *)argv[3];
		rc = insert_trigger(trigger, crt_stmt);
		if (rc) {
			say_debug("%s(): error while inserting new trigger into _trigger\n", __func_name);
			return SQLITE_ERROR;
		}
	}
	say_debug("%s(): made insert into space %s\n", __func_name, (char *)(argv[1]));
	return SQLITE_OK;
}

}
