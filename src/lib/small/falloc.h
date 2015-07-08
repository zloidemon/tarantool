#ifndef TESTALLOC_FALLOC_H
#define TESTALLOC_FALLOC_H

#include <stdlib.h>
#include "heap.h"
#include "matras.h"

enum {
	FALLOC_END = 0xFFFFFFFF,
	FUI32S = (uint32_t)sizeof(uint32_t),
	FALLOC_MAX_DATA_SIZE = UINT32_MAX - 2 * FUI32S
};

struct slab {
};

typedef struct slab * (*falloc_slab_alloc_t)(void *slab_alloc_ctx);
typedef void (*falloc_slab_free_t)(struct slab *slab, void *slab_alloc_ctx);
typedef void * (*falloc_extent_alloc_t)();
typedef void (*falloc_extent_free_t)(void *extent);
typedef size_t (*get_allocation_size_t)(void *data);

struct falloc {
	size_t slab_size;
	uintptr_t fslab_addr_mask;
	void *alloc_ctx;
	falloc_slab_alloc_t slab_alloc_fun;
	falloc_slab_free_t slab_free_fun;
	get_allocation_size_t get_allocation_fun;
	struct heap slabs_by_fragmentaions;
	struct matras mtab;
	struct rlist slabs;
	struct fslab *current_slab;
	size_t currect_slab_pos;
	struct rlist free_list[32];
	uint32_t free_mask;
	void **spare_slot;
	uint32_t spare_id;
};

struct fslab {
	struct slab base;
	struct rlist link;
	size_t used_size;
	size_t fragmented_size;
	size_t allocated_size;
	size_t wasted_size;
	uint32_t fragmentation_factor;
	uint32_t pos_in_heap;
	char data[];
};

struct falloc_free_block {
	uint32_t id;
	struct rlist link;
	uint32_t allocated_size;
} __attribute__((packed));

void
falloc_create(struct falloc *f, size_t slab_size, void *alloc_ctx,
	      falloc_slab_alloc_t slab_alloc_fun,
	      falloc_slab_free_t slab_free_fun,
	      get_allocation_size_t get_allocation_fun,
	      size_t matras_extent_size,
	      falloc_extent_alloc_t extent_alloc_t_fun,
	      falloc_extent_free_t extent_free_fun)
{
	assert(slab_size < UINT32_MAX / 2);
	f->slab_size = slab_size;
	assert((slab_size & (slab_size - 1)) == 0);
	f->fslab_addr_mask = ~((uintptr_t)slab_size - 1);
	f->alloc_ctx = alloc_ctx;
	f->slab_alloc_fun = slab_alloc_fun;
	f->slab_free_fun = slab_free_fun;
	f->get_allocation_fun = get_allocation_fun;
	heap_create(&f->slabs_by_fragmentaions);
	matras_create(&f->mtab, (uint32_t)matras_extent_size, sizeof(void*),
		      extent_alloc_t_fun, extent_free_fun);
	rlist_create(&f->slabs);
	f->current_slab = 0;
	f->currect_slab_pos = 0;
	for (int i = 0; i < 32; i++)
		rlist_create(&f->free_list[i]);
	f->free_mask = 0;
	f->spare_slot = 0;
	f->spare_id = FALLOC_END;
}

void
falloc_destroy(struct falloc *f)
{
	heap_destroy(&f->slabs_by_fragmentaions);
	while (!rlist_empty(&f->slabs)) {
		struct fslab *s = rlist_first_entry(&f->slabs,
		struct fslab,
		link);
		rlist_del_entry(s, link);
		f->slab_free_fun(&s->base, f->alloc_ctx);
	}
	matras_destroy(&f->mtab);
}

static inline struct fslab *
falloc_fslab_by_data(struct falloc *f, void *ptr)
{
	return (struct fslab *)(f->fslab_addr_mask & (uintptr_t) ptr);
}

void *
falloc_data_by_id(struct falloc *f, uint32_t id)
{
	return *(void **)matras_get(&f->mtab, id);
}

uint32_t
falloc_id_by_data(struct falloc *f, void *data)
{
	return ((uint32_t *)data)[-1];
}

struct fslab *
fslab_new(struct falloc *f)
{
	struct fslab *s = (struct fslab *)f->slab_alloc_fun(f->alloc_ctx);
	if (!s)
		return 0;
	rlist_add_entry(&f->slabs, s, link);
	s->used_size = 0;
	s->allocated_size = 0;
	s->wasted_size = 0;
	s->fragmentation_factor = 0;
	s->pos_in_heap = FALLOC_END;
}

void
falloc_mark_block(struct falloc *f, char *block,
		  uint32_t alloc_size, uint32_t id)
{
	*(uint32_t *) block = id;
	*(uint32_t *) (block + alloc_size - FUI32S) = alloc_size;
}

static inline uint32_t
falloc_round_up_data_size(uint32_t size)
{
	const uint32_t roundup_mask = (uint32_t)(sizeof(uint32_t) - 1);
	if (size >= sizeof(struct rlist))
		return (size + roundup_mask) & ~roundup_mask;
	else
		return (uint32_t) sizeof(struct rlist);
}

uint32_t
falloc_block_alloc_size(char *block, uint32_t data_size_rounded_up)
{
	return *(uint32_t *)(block + data_size_rounded_up + FUI32S);
}

void
falloc_reg_used_block(struct falloc *f, char *block,
		      uint32_t size, uint32_t alloc_size, uint32_t id)
{
	falloc_mark_block(f, block, alloc_size, id);
	((uint32_t *) block)[size / sizeof(uint32_t) + 1] = alloc_size;
}

void
falloc_reg_free_block(struct falloc *f, char *block, uint32_t alloc_size)
{
	falloc_mark_block(f, block, alloc_size, FALLOC_END);
	struct falloc_free_block *b = (struct falloc_free_block *) block;
	b->allocated_size = alloc_size;
	uint32_t class_id = __builtin_clz(alloc_size);
	f->free_mask |= 1u << class_id;
	struct rlist *l = &f->free_list[class_id];
	if (!rlist_empty(l)) {
		struct falloc_free_block *a =
		rlist_first_entry(l, struct falloc_free_block, link);
		if (a->allocated_size <= alloc_size)
			rlist_add(&f->free_list[class_id], &b->link);
		else
			rlist_add_tail(&f->free_list[class_id], &b->link);
	} else {
		rlist_add(&f->free_list[class_id], &b->link);
	}
}

void
falloc_detach_free_block(struct falloc *f, char *block)
{
	struct falloc_free_block *b = (struct falloc_free_block *) block;
	rlist_del(&b->link);
	uint32_t class_id = __builtin_clz(b->allocated_size);
	struct rlist *l = &f->free_list[class_id];
	if (rlist_empty(l))
		f->free_mask &= ~(1u << class_id);
}

void
fslab_close(struct falloc *f, struct fslab *s)
{
	const size_t max_size_in_slab = f->slab_size - sizeof(struct fslab);
	size_t block_size = max_size_in_slab - f->currect_slab_pos;
	assert(block_size >= sizeof(struct falloc_free_block));
	assert(block_size <= UINT32_MAX);
	falloc_reg_free_block(f, &s->data[f->currect_slab_pos],
			      (uint32_t)block_size);
	s->fragmentation_factor = (uint32_t)(100 * s->fragmented_size /
					     max_size_in_slab);
	heap_push(&f->slabs_by_fragmentaions, s,
		  s->fragmentation_factor, &s->pos_in_heap);
}


static inline void **
falloc_new_slot(struct falloc *f, uint32_t *id)
{
	if (f->spare_id != FALLOC_END) {
		*id = f->spare_id;
		void **res = f->spare_slot;
		f->spare_id = *(uint32_t *)res;
		if (f->spare_id == FALLOC_END)
			f->spare_slot = 0;
		else
			f->spare_slot = (void **) matras_get(&f->mtab,
							     f->spare_id);
		return res;
	}
	return (void **) matras_alloc(&f->mtab, id);
}

static inline void
falloc_free_slot(struct falloc *f, uint32_t id)
{
	void ** slot = (void **) matras_get(&f->mtab, id);
	*(uint32_t *)slot = f->spare_id;
	f->spare_slot = slot;
	f->spare_id = id;
}

void *
falloc(struct falloc *f, uint32_t size, uint32_t *id)
{
	assert(size < FALLOC_MAX_DATA_SIZE);
	uint32_t rnd_size = falloc_round_up_data_size(size);
	uint32_t alloc_size = rnd_size + (uint32_t)(2 * sizeof(uint32_t));
	size_t max_size_in_slab = f->slab_size - sizeof(struct fslab);
	if (max_size_in_slab < alloc_size)
		return 0;
	if (!f->current_slab || max_size_in_slab - f->currect_slab_pos <
				rnd_size) {
		if (f->current_slab)
			fslab_close(f, f->current_slab);
		f->current_slab = fslab_new(f);
		if (!f->current_slab)
			return 0;
		f->currect_slab_pos = 0;
	}
	size_t left_in_slab = max_size_in_slab - f->currect_slab_pos;
	if (left_in_slab - alloc_size < sizeof(struct falloc_free_block))
		alloc_size = (uint32_t) left_in_slab;
	void **storage = falloc_new_slot(f, id);
	if (!storage)
		return 0;
	char *all = (char *)&f->current_slab->data[f->currect_slab_pos];
	f->currect_slab_pos += alloc_size;
	if (f->currect_slab_pos == max_size_in_slab)
		f->current_slab = 0;
	falloc_reg_used_block(f, all, rnd_size, alloc_size, *id);
	return (void *)((uint32_t *)all + 1);
}


void
ffree(struct falloc *f, void *ptr, uint32_t id, uint32_t size)
{
	falloc_free_slot(f, id);
	uint32_t rnd_size = falloc_round_up_data_size(size);
	char *b = (char *)ptr - sizeof(uint32_t);
	assert(*(uint32_t *)b == id);
	uint32_t alloc_size = falloc_block_alloc_size(b, rnd_size);
	struct fslab *s = falloc_fslab_by_data(f, ptr);
	if (b > s->data) {
		uint32_t prev_block_aszie = ((uint32_t *)b)[-1];
		char *prev_block = b - prev_block_aszie;
		uint32_t prev_block_id = *(uint32_t *)prev_block;
		if (prev_block_id == FALLOC_END) {
			b = prev_block;
			alloc_size += prev_block_aszie;
		}
	}
}

#endif //TESTALLOC_FALLOC_H
