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
#include "tuple.h"

#include "small/small.h"
#include "small/quota.h"

#include "fiber.h"

uint32_t snapshot_version;

struct quota memtx_quota;

struct slab_arena memtx_arena;
struct lsall memtx_allocator;
extern struct mempool memtx_index_extent_pool;
static size_t tuple_size_limit = 0;

enum {
	/** Lowest allowed slab_alloc_minimal */
	OBJSIZE_MIN = 16,
	/** Lowest allowed slab_alloc_maximal */
	OBJSIZE_MAX_MIN = 16 * 1024,
	/** Lowest allowed slab size, for mmapped slabs */
	SLAB_SIZE_MIN = 1024 * 1024
};

static struct mempool tuple_iterator_pool;

/**
 * Last tuple returned by public C API
 * \sa tuple_bless()
 */
tuple_id box_tuple_last;

/*
 * Validate a new tuple format and initialize tuple-local
 * format data.
 */
void
tuple_init_field_map(struct tuple_format *format, tuple_id tupid)
{
	assert(format->field_map_size <= UINT16_MAX);
	if (format->field_count == 0)
		return; /* Nothing to initialize */
	struct tuple *tuple = tuple_ptr(tupid);
	uint16_t *field_map = (uint16_t *) tuple;
	const char *data = tuple_ptr_data(tuple, format);
	const char *pos = data;

	/* Check to see if the tuple has a sufficient number of fields. */
	uint32_t field_count = mp_decode_array(&pos);
	if (unlikely(field_count < format->field_count))
		tnt_raise(ClientError, ER_INDEX_FIELD_COUNT,
			  (unsigned) field_count,
			  (unsigned) format->field_count);

	/* first field is simply accessible, so we do not store offset to it */
	enum mp_type mp_type = mp_typeof(*pos);
	key_mp_type_validate(format->fields[0].type, mp_type,
			     ER_FIELD_TYPE, INDEX_OFFSET);
	mp_next(&pos);
	/* other fields...*/
	for (uint32_t i = 1; i < format->field_count; i++) {
		mp_type = mp_typeof(*pos);
		key_mp_type_validate(format->fields[i].type, mp_type,
				     ER_FIELD_TYPE, i + INDEX_OFFSET);
		if (format->fields[i].offset_slot < 0) {
			if (pos - data > UINT16_MAX)
				tnt_raise(ClientError, ER_TUPLE_TOO_BIG_OFFSET,
					  (long)(pos - data));
			field_map[format->fields[i].offset_slot] = pos - data;
		}
		mp_next(&pos);
	}
}


/**
 * Check tuple data correspondence to space format;
 * throw proper exception if smth wrong.
 * data argument expected to be a proper msgpack array
 * Actually checks everything that checks tuple_init_field_map.
 */
void
tuple_validate_raw(struct tuple_format *format, const char *data)
{
	if (format->field_count == 0)
		return; /* Nothing to check */

	/* Check to see if the tuple has a sufficient number of fields. */
	uint32_t field_count = mp_decode_array(&data);
	if (unlikely(field_count < format->field_count))
		tnt_raise(ClientError, ER_INDEX_FIELD_COUNT,
			  (unsigned) field_count,
			  (unsigned) format->field_count);

	/* Check field types */
	for (uint32_t i = 0; i < format->field_count; i++) {
		key_mp_type_validate(format->fields[i].type, mp_typeof(*data),
				     ER_FIELD_TYPE, i + INDEX_OFFSET);
		mp_next(&data);
	}
}

/**
 * Check tuple data correspondence to space format;
 * throw proper exception if smth wrong.
 */
void
tuple_validate(struct tuple_format *format, tuple_id tupid)
{
	tuple_validate_raw(format, tuple_data(tupid));
}

/**
 * Incremented on every snapshot and is used to distinguish tuples
 * which were created after start of a snapshot (these tuples can
 * be freed right away, since they are not used for snapshot) or
 * before start of a snapshot (these tuples can be freed only
 * after the snapshot has finished, otherwise it'll write bad data
 * to the snapshot file).
 */

static tuple_id delayed_free_list = TUPLE_ID_NIL;
static bool is_delayed_free_mode = false;

static void
delayed_free_batch()
{
	if (is_delayed_free_mode)
		return;
	const int BATCH = 100;
	for (int i = 0; delayed_free_list != TUPLE_ID_NIL && i < BATCH; i++) {
		tuple_id id = delayed_free_list;
		void *ptr = lsall_get(&memtx_allocator, id);
		char *ptr_skip_size = (char *)ptr + sizeof(uint16_t);
		delayed_free_list = *(tuple_id *)ptr_skip_size;
		lsfree(&memtx_allocator, id);
	}
}



/**
 * Allocate a tuple
 * It's similar to tuple_new, but does not set tuple data and thus does not
 * initialize field offsets.
 * Sets 'data' pointer to the beginning of msgpack buffer
 *
 * After tuple_alloc and filling tuple data the tuple_init_field_map must be
 * called!
 *
 * @param size  tuple->bsize
 */
tuple_id
tuple_alloc(struct tuple_format *format, size_t size, char **data)
{
	delayed_free_batch();
	size_t total = sizeof(struct tuple) + size + format->field_map_size;
	ERROR_INJECT(ERRINJ_TUPLE_ALLOC,
		     tnt_raise(OutOfMemory, (unsigned) total,
			       "slab allocator", "tuple"));
	uint32_t id;
	if (total > tuple_size_limit) {
		tnt_raise(LoggedError, ER_SLAB_ALLOC_MAX,
			  (unsigned) total);
	}
	char *ptr = (char *)lsalloc(&memtx_allocator, total, &id);
	/**
	 * Use a nothrow version and throw an exception here,
	 * to throw an instance of ClientError. Apart from being
	 * more nice to the user, ClientErrors are ignored in
	 * panic_on_wal_error=false mode, allowing us to start
	 * with lower arena than necessary in the circumstances
	 * of disaster recovery.
	 */
	if (ptr == NULL) {
		tnt_raise(OutOfMemory, (unsigned) total,
			  "slab allocator", "tuple");
	}
	*(uint16_t *)ptr = format->field_map_size;
	struct tuple *tuple = (struct tuple *)(ptr + format->field_map_size);

	tuple->refs = 0;
	tuple->version = snapshot_version;
	tuple->bsize = size;
	tuple->format_id = tuple_format_id(format);
	tuple_format_ref(format, 1);

	say_debug("tuple_alloc(%zu) = %p", size, tuple);
	*data = (char *)tuple_ptr_data(tuple, format);
	return id;
}

/**
 * Free the tuple.
 * @pre tuple->refs  == 0
 */
void
tuple_ptr_delete(struct tuple *tuple, tuple_id tupid)
{
	say_debug("tuple_delete(%p)", tuple);
	assert(tuple->refs == 0);
	struct tuple_format *format = tuple_ptr_format(tuple);
	char *ptr = (char *) tuple - format->field_map_size;
	assert(*(uint16_t *)ptr == format->field_map_size);
	tuple_format_ref(format, -1);
	if (!is_delayed_free_mode || tuple->version == snapshot_version) {
		lsfree(&memtx_allocator, tupid);
	} else {
		char *ptr_skip_size = ptr + sizeof(uint16_t);
		*(tuple_id *)ptr_skip_size = delayed_free_list;
		delayed_free_list = tupid;
	}
}

/**
 * Throw and exception about tuple reference counter overflow.
 */
void
tuple_ref_exception()
{
	tnt_raise(ClientError, ER_TUPLE_REF_OVERFLOW);
}

const char *
tuple_seek(struct tuple_iterator *it, uint32_t i)
{
	struct tuple_format *format = tuple_ptr_format(it->tuple);
	const char *field = tuple_ptr_field(format, it->tuple, i);
	if (likely(field != NULL)) {
		it->pos = field;
		it->fieldno = i;
		return tuple_next(it);
	} else {
		it->pos = it->data_end;
		it->fieldno = it->field_count;
		return NULL;
	}
}

const char *
tuple_next(struct tuple_iterator *it)
{
	if (it->pos < it->data_end) {
		const char *field = it->pos;
		mp_next(&it->pos);
		assert(it->pos <= it->data_end);
		it->fieldno++;
		return field;
	}
	return NULL;
}

extern inline uint32_t
tuple_next_u32(struct tuple_iterator *it);

const char *
tuple_field_to_cstr(const char *field, uint32_t len)
{
	enum { MAX_STR_BUFS = 4, MAX_BUF_LEN = 256 };
	static __thread char bufs[MAX_STR_BUFS][MAX_BUF_LEN];
	static __thread unsigned i = 0;
	char *buf = bufs[i];
	i = (i + 1) % MAX_STR_BUFS;
	len = MIN(len, MAX_BUF_LEN - 1);
	memcpy(buf, field, len);
	buf[len] = '\0';
	return buf;
}

const char *
tuple_next_cstr(struct tuple_iterator *it)
{
	uint32_t fieldno = it->fieldno;
	const char *field = tuple_next(it);
	if (field == NULL)
		tnt_raise(ClientError, ER_NO_SUCH_FIELD, fieldno);
	if (mp_typeof(*field) != MP_STR)
		tnt_raise(ClientError, ER_FIELD_TYPE, fieldno + INDEX_OFFSET,
			  field_type_strs[STRING]);
	uint32_t len = 0;
	const char *str = mp_decode_str(&field, &len);
	return tuple_field_to_cstr(str, len);
}

const char *
tuple_field_cstr(tuple_id tupid, uint32_t i)
{
	const char *field = tuple_field(tupid, i);
	if (field == NULL)
		tnt_raise(ClientError, ER_NO_SUCH_FIELD, i);
	if (mp_typeof(*field) != MP_STR)
		tnt_raise(ClientError, ER_FIELD_TYPE, i + INDEX_OFFSET,
			  field_type_strs[STRING]);
	uint32_t len = 0;
	const char *str = mp_decode_str(&field, &len);
	return tuple_field_to_cstr(str, len);
}

/**
 * Extract msgpacked key parts from tuple data.
 * Write the key to the provided buffer ('key_buf' argument), if the
 * buffer size is big enough ('key_buf_size' argument)
 * Return length of the key (required buffer size for storing it)
 */
uint32_t
key_parts_create_from_tuple(struct key_def *key_def, const char *tuple,
			    char *key_buf, uint32_t key_buf_size)
{
	uint32_t key_len = 0;
	uint32_t part_count = key_def->part_count;
	const char *field0 = tuple;
	mp_decode_array(&field0);
	const char *field0_end = field0;
	mp_next(&field0_end);
	const char *field = field0;
	const char *field_end = field0_end;
	uint32_t current_field_no = 0;
	for (uint32_t i = 0; i < part_count; i++) {
		uint32_t field_no = key_def->parts[i].fieldno;
		if (field_no < current_field_no) {
			/* Rewind. */
			field = field0;
			field_end = field0_end;
			current_field_no = 0;
		}
		while (current_field_no < field_no) {
			field = field_end;
			mp_next(&field_end);
			current_field_no++;
		}
		uint32_t field_len = (uint32_t)(field_end - field);
		key_len += field_len;
		if (field_len <= key_buf_size) {
			memcpy(key_buf, field, field_len);
			key_buf += field_len;
			key_buf_size -= field_len;
		}
	}
	return key_len;
}

/**
 * Extract msgpacked array with key parts from tuple data/
 * Write the key to the provided buffer ('key_buf' argument), if the
 * buffer size is big enough ('key_buf_size' argument)
 * Return length of the key (required buffer size for storing it)
 */
uint32_t
key_create_from_tuple(struct key_def *key_def, const char *tuple,
		      char *key_buf, uint32_t key_buf_size)
{
	uint32_t space_for_arr = mp_sizeof_array(key_def->part_count);
	if (key_buf_size >= space_for_arr) {
		key_buf = mp_encode_array(key_buf, key_def->part_count);
		key_buf_size -= space_for_arr;
	}
	return space_for_arr +
		key_parts_create_from_tuple(key_def, tuple,
					    key_buf, key_buf_size);
}

tuple_id
tuple_update(struct tuple_format *format,
	     tuple_update_alloc_func f, void *alloc_ctx,
	     const tuple_id old_tupid, const char *expr,
	     const char *expr_end, int field_base)
{
	uint32_t old_size;
	const char *old_data = tuple_data_range(old_tupid, &old_size);
	uint32_t new_size = 0;
	const char *new_data =
		tuple_update_execute(f, alloc_ctx,
				     expr, expr_end, old_data,
				     old_data + old_size,
				     &new_size, field_base);

	return tuple_new(format, new_data, new_data + new_size);
}

tuple_id
tuple_upsert(struct tuple_format *format,
	     void *(*region_alloc)(void *, size_t), void *alloc_ctx,
	     const tuple_id old_tupid,
	     const char *expr, const char *expr_end, int field_base)
{
	uint32_t old_size;
	const char *old_data = tuple_data_range(old_tupid, &old_size);
	uint32_t new_size = 0;
	const char *new_data =
		tuple_upsert_execute(region_alloc, alloc_ctx, expr, expr_end,
				     old_data, old_data + old_size,
				     &new_size, field_base);

	return tuple_new(format, new_data, new_data + new_size);
}

tuple_id
tuple_new(struct tuple_format *format, const char *data, const char *end)
{
	size_t tuple_len = end - data;
	assert(mp_typeof(*data) == MP_ARRAY);
	char *new_data;
	tuple_id new_tuple = tuple_alloc(format, tuple_len, &new_data);
	memcpy(new_data, data, tuple_len);
	try {
		tuple_init_field_map(format, new_tuple);
	} catch (Exception *e) {
		tuple_delete(new_tuple);
		throw;
	}
	return new_tuple;
}

enum {
	MEMTX_EXTENT_SIZE = 16 * 1024
};

static void *
mextent_new()
{
	assert(memtx_index_extent_pool.objsize == MEMTX_EXTENT_SIZE);
	return mempool_alloc(&memtx_index_extent_pool);
}

static void
mextent_free(void *ptr)
{
	mempool_free(&memtx_index_extent_pool, ptr);
}

void
tuple_init(float tuple_arena_max_size, uint32_t objsize_min,
	   uint32_t objsize_max, float alloc_factor)
{
	(void)alloc_factor;
	tuple_format_init();

	/* Apply lowest allowed objsize bounds */
	if (objsize_min < OBJSIZE_MIN)
		objsize_min = OBJSIZE_MIN;
	if (objsize_max < OBJSIZE_MAX_MIN)
		objsize_max = OBJSIZE_MAX_MIN;
	tuple_size_limit = objsize_max;

	/* Calculate slab size for tuple arena */
	size_t slab_size = small_round(objsize_max * 4);
	if (slab_size < SLAB_SIZE_MIN)
		slab_size = SLAB_SIZE_MIN;

	/** Preallocate entire quota. */
	size_t prealloc = tuple_arena_max_size * 1024 * 1024 * 1024;
	quota_init(&memtx_quota, prealloc);

	say_info("mapping %zu bytes for tuple arena...", prealloc);

	if (slab_arena_create(&memtx_arena, &memtx_quota,
			      prealloc, slab_size, MAP_PRIVATE)) {
		if (ENOMEM == errno) {
			panic("failed to preallocate %zu bytes: "
			      "Cannot allocate memory, check option "
			      "'slab_alloc_arena' in box.cfg(..)",
			      prealloc);
		} else {
			panic_syserror("failed to preallocate %zu bytes",
				       prealloc);
		}
	}
	lsall_create(&memtx_allocator, &memtx_arena, MEMTX_EXTENT_SIZE,
		     mextent_new, mextent_free);
	mempool_create(&tuple_iterator_pool, &cord()->slabc,
		       sizeof(struct tuple_iterator));

	box_tuple_last = TUPLE_ID_NIL;
}

void
tuple_free()
{
	/* Unref last tuple returned by public C API */
	if (box_tuple_last != TUPLE_ID_NIL) {
		tuple_unref(box_tuple_last);
		box_tuple_last = TUPLE_ID_NIL;
	}

	mempool_destroy(&tuple_iterator_pool);

	tuple_format_free();
}

void
tuple_begin_snapshot()
{
	snapshot_version++;
	is_delayed_free_mode = true;
}

void
tuple_end_snapshot()
{
	is_delayed_free_mode = false;
}

double mp_decode_num(const char **data, uint32_t i)
{
	double val;
	switch (mp_typeof(**data)) {
	case MP_UINT:
		val = mp_decode_uint(data);
		break;
	case MP_INT:
		val = mp_decode_int(data);
		break;
	case MP_FLOAT:
		val = mp_decode_float(data);
		break;
	case MP_DOUBLE:
		val = mp_decode_double(data);
		break;
	default:
		tnt_raise(ClientError, ER_FIELD_TYPE, i + INDEX_OFFSET,
			  field_type_strs[NUM]);
	}
	return val;
}

box_tuple_format_t *
box_tuple_format_default(void)
{
	return tuple_format_default;
}

box_tuple_t
box_tuple_new(box_tuple_format_t *format, const char *data, const char *end)
{
	try {
		return tuple_bless(tuple_new(format, data, end));
	} catch (Exception *e) {
		return TUPLE_ID_NIL;
	}
}

int
box_tuple_ref(box_tuple_t tuple)
{
	assert(tuple != TUPLE_ID_NIL);
	try {
		tuple_ref(tuple);
		return 0;
	} catch (Exception *e) {
		return -1;
	}
}

void
box_tuple_unref(box_tuple_t tuple)
{
	assert(tuple != TUPLE_ID_NIL);
	return tuple_unref(tuple);
}

uint32_t
box_tuple_field_count(const box_tuple_t tuple)
{
	assert(tuple != TUPLE_ID_NIL);
	return tuple_field_count(tuple);
}

size_t
box_tuple_bsize(const box_tuple_t tuple)
{
	assert(tuple != TUPLE_ID_NIL);
	return tuple_ptr(tuple)->bsize;
}

ssize_t
box_tuple_to_buf(const box_tuple_t tuple, char *buf, size_t size)
{
	assert(tuple != TUPLE_ID_NIL);
	return tuple_to_buf(tuple, buf, size);
}

box_tuple_format_t *
box_tuple_format(const box_tuple_t tupid)
{
	assert(tupid != TUPLE_ID_NIL);
	struct tuple *tuple = tuple_ptr(tupid);
	return tuple_ptr_format(tuple);
}

const char *
box_tuple_field(const box_tuple_t tuple, uint32_t i)
{
	assert(tuple != TUPLE_ID_NIL);
	return tuple_field(tuple, i);
}

typedef struct tuple_iterator box_tuple_iterator_t;

box_tuple_iterator_t *
box_tuple_iterator(box_tuple_t tupid)
{
	assert(tupid != TUPLE_ID_NIL);
	struct tuple_iterator *it;
	try {
		it = (struct tuple_iterator *)
			mempool_alloc0_xc(&tuple_iterator_pool);
	} catch (Exception *e) {
		return NULL;
	}

	struct tuple *tuple = tuple_ptr(tupid);
	tuple_ptr_ref(tuple);
	tuple_ptr_iter_init(it, tuple, tupid);
	return it;
}

void
box_tuple_iterator_free(box_tuple_iterator_t *it)
{
	tuple_ptr_unref(it->tuple, it->tupid);
	mempool_free(&tuple_iterator_pool, it);
}

uint32_t
box_tuple_position(box_tuple_iterator_t *it)
{
	return it->fieldno;
}

void
box_tuple_rewind(box_tuple_iterator_t *it)
{
	tuple_rewind(it);
}

const char *
box_tuple_seek(box_tuple_iterator_t *it, uint32_t field_no)
{
	return tuple_seek(it, field_no);
}

const char *
box_tuple_next(box_tuple_iterator_t *it)
{
	return tuple_next(it);
}

box_tuple_t
box_tuple_update(const box_tuple_t tuple, const char *expr, const char *expr_end)
{
	try {
		RegionGuard region_guard(&fiber()->gc);
		tuple_id new_tuple = tuple_update(tuple_format_default,
						  region_aligned_alloc_xc_cb,
						  &fiber()->gc, tuple,
						  expr, expr_end, 1);
		return tuple_bless(new_tuple);
	} catch (ClientError *e) {
		return TUPLE_ID_NIL;
	}
}

box_tuple_t
box_tuple_upsert(const box_tuple_t tuple, const char *expr, const char *expr_end)
{
	try {
		RegionGuard region_guard(&fiber()->gc);
		tuple_id new_tuple = tuple_upsert(tuple_format_default,
						  region_aligned_alloc_xc_cb,
						  &fiber()->gc, tuple,
						  expr, expr_end, 1);
		return tuple_bless(new_tuple);
	} catch (ClientError *e) {
		return TUPLE_ID_NIL;
	}
}
