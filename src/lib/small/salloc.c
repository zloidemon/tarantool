#include "salloc.h"
#include <string.h>
#include "slab_cache.h"

struct sslab {
	struct slab base;
	struct rlist link;
	size_t used_size;
	size_t fragmented_size;
	size_t wasted_size;
	size_t system_size;
	uint32_t fragmentation_factor;
	uint32_t pos_in_heap;
	char data[];
};

static inline void **
salloc_new_slot(struct salloc *f, uint32_t *id)
{
	if (f->spare_id != SALLOC_END) {
		*id = f->spare_id;
		void **res = f->spare_slot;
		f->spare_id = *(uint32_t *)res;
		if (f->spare_id == SALLOC_END)
			f->spare_slot = 0;
		else
			f->spare_slot = (void **) matras_get(&f->mtab,
							     f->spare_id);
		return res;
	}
	return (void **) matras_alloc(&f->mtab, id);
}

static inline void
salloc_free_slot(struct salloc *f, uint32_t id)
{
	void ** slot = (void **) matras_get(&f->mtab, id);
	*(uint32_t *)slot = f->spare_id;
	f->spare_slot = slot;
	f->spare_id = id;
}

static inline struct sslab *
sslab_new(struct salloc *f)
{
	struct sslab *s = (struct sslab *)f->slab_alloc_fun(f->alloc_ctx);
	if (!s)
		return 0;
	rlist_add_entry(&f->slabs, s, link);
	s->used_size = 0;
	s->wasted_size = 0;
	s->fragmented_size = 0;
	s->system_size = sizeof(struct sslab);
	s->fragmentation_factor = 0;
	s->pos_in_heap = SALLOC_END;
	f->system_size += sizeof(struct sslab);
	f->reserved_size += f->slab_size - sizeof(struct sslab);
	return s;
}

static inline void
sslab_delete(struct salloc *f, struct sslab *s)
{
	rlist_del_entry(s, link);
	f->system_size -= sizeof(struct sslab);
	f->reserved_size -= f->slab_size - sizeof(struct sslab);
	f->slab_free_fun((struct slab *)s, f->alloc_ctx);
}

static inline void
sslab_close(struct salloc *f, struct sslab *s, uint32_t left_space,
	    size_t free_offset)
{
	const size_t max_size_in_slab = f->slab_size - sizeof(struct sslab);
	f->reserved_size -= left_space;
	f->fragmented_size += left_space;
	s->fragmented_size += left_space;
	if (left_space >= 2 * SUI32S) {
		((uint32_t *)(s->data + free_offset))[0] = SALLOC_END;
		((uint32_t *)(s->data + free_offset))[1] = left_space;
		s->system_size += SUI32S;
		s->fragmented_size -= SUI32S;
		f->system_size += SUI32S;
		f->fragmented_size -= SUI32S;
	}
	s->fragmentation_factor = (uint32_t)(100 * s->fragmented_size /
					     max_size_in_slab);
	heap_push(&f->slabs_by_fragmentaions, s,
		  s->fragmentation_factor, &s->pos_in_heap);
}


static inline char *
salloc_get_block(struct salloc *f, uint32_t alloc_size)
{
	size_t max_size_in_slab = f->slab_size - sizeof(struct sslab);
	size_t left_in_slab = max_size_in_slab - f->currect_slab_pos;
	if (!f->current_slab || left_in_slab < alloc_size) {
		if (f->current_slab)
			sslab_close(f, f->current_slab, (uint32_t)left_in_slab,
				    f->currect_slab_pos);
		f->current_slab = sslab_new(f);
		if (!f->current_slab)
			return 0;
		f->currect_slab_pos = 0;
	}

	char *b = f->current_slab->data + f->currect_slab_pos;
	f->currect_slab_pos += alloc_size;
	return b;
}

static inline uint32_t
salloc_round_up_data_size(uint32_t size)
{
	const uint32_t roundup_mask = SUI32S - 1;
	if (size >= sizeof(struct rlist))
		return (size + roundup_mask) & ~roundup_mask;
	else
		return (uint32_t) sizeof(struct rlist);
}

static inline struct sslab *
salloc_sslab_by_data(struct salloc *f, void *ptr)
{
	return (struct sslab *)(f->fslab_addr_mask & (uintptr_t) ptr);
}

void salloc_create(struct salloc *f, size_t slab_size, void *alloc_ctx,
		   salloc_slab_alloc_t slab_alloc_fun,
		   salloc_slab_free_t slab_free_fun,
		   get_allocation_size_t get_allocation_fun,
		   size_t matras_extent_size,
		   salloc_extent_alloc_t extent_alloc_t_fun,
		   salloc_extent_free_t extent_free_fun)
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
	f->spare_slot = 0;
	f->spare_id = SALLOC_END;

	f->used_size = 0;
	f->fragmented_size = 0;
	f->wasted_size = 0;
	f->system_size = 0;
	f->reserved_size = 0;

	f->allow_memmove = 0;
	f->memmoved = 0;
	f->allow_memmove_factor = 1;
	f->current_defrag_slab = 0;
	f->currect_defrag_slab_pos = 0;

}

void salloc_destroy(struct salloc *f)
{
	heap_destroy(&f->slabs_by_fragmentaions);
	while (!rlist_empty(&f->slabs)) {
		struct sslab *s = rlist_first_entry(&f->slabs,
						    struct sslab,
						    link);
		rlist_del_entry(s, link);
		f->slab_free_fun(&s->base, f->alloc_ctx);
	}
	matras_destroy(&f->mtab);
}

static inline void
salloc_defrag(struct salloc *f)
{
	size_t max_size_in_slab = f->slab_size - sizeof(struct sslab);
	while (f->memmoved < f->allow_memmove) {
		if (!f->current_defrag_slab) {
			if (f->slabs_by_fragmentaions.count == 0)
				return;
			f->current_defrag_slab = (struct sslab *)
				heap_max(&f->slabs_by_fragmentaions);
			heap_pop(&f->slabs_by_fragmentaions);
			f->current_defrag_slab->pos_in_heap = SALLOC_END;
			f->currect_defrag_slab_pos = 0;
		}
		struct sslab *s = f->current_defrag_slab;
		char *b = s->data + f->currect_defrag_slab_pos;
		uint32_t id = ((uint32_t *)b)[0];
		uint32_t alloc_size;
		if (id == SALLOC_END) {
			alloc_size = ((uint32_t *)b)[1];
			f->system_size -= SUI32S;
			f->fragmented_size -= alloc_size - SUI32S;
			f->reserved_size += alloc_size;
		} else {
			char *ptr = b + SUI32S;
			uint32_t size = (uint32_t)f->get_allocation_fun(ptr);
			uint32_t rnd_size = salloc_round_up_data_size(size);
			alloc_size = rnd_size + SUI32S;
			void ** storage = (void **) matras_get(&f->mtab, id);
			assert(*storage == ptr);
			char *new_b = salloc_get_block(f, alloc_size);
			if (!new_b) {
				return;
			}
			memcpy(new_b, b, alloc_size);
			ptr = new_b + SUI32S;
			*storage = ptr;
			f->memmoved += alloc_size;
		}
		f->currect_defrag_slab_pos += alloc_size;
		uint32_t left_space = (uint32_t)(max_size_in_slab -
						 f->currect_defrag_slab_pos);
		if (left_space < 2 * SUI32S) {
			f->reserved_size += left_space;
			f->fragmented_size -= left_space;
			sslab_delete(f, s);
			f->current_defrag_slab = 0;
		}
	}
}

void *
salloc(struct salloc *f, uint32_t size, uint32_t *id)
{
	assert(size < SALLOC_MAX_DATA_SIZE);
	uint32_t rnd_size = salloc_round_up_data_size(size);
	uint32_t alloc_size = rnd_size + SUI32S;
	size_t max_size_in_slab = f->slab_size - sizeof(struct sslab);
	if (max_size_in_slab < alloc_size)
		return 0;
	void **storage = salloc_new_slot(f, id);
	if (!storage)
		return 0;
	char *b = salloc_get_block(f, alloc_size);
	if (!b) {
		salloc_free_slot(f, *id);
		return 0;
	}

	void *res = (void *)(b + SUI32S);
	*storage = res;
	*(uint32_t *)b = *id;

	f->current_slab->used_size += size;
	f->current_slab->system_size += SUI32S;
	f->current_slab->wasted_size += rnd_size - size;

	f->reserved_size -= alloc_size;
	f->used_size += size;
	f->system_size += SUI32S;
	f->wasted_size += rnd_size - size;

	return res;
}

void
sfree(struct salloc *f, void *ptr, uint32_t id, uint32_t size)
{
	salloc_free_slot(f, id);
	uint32_t rnd_size = salloc_round_up_data_size(size);
	uint32_t alloc_size = rnd_size + SUI32S;

	char *b = (char *)ptr - SUI32S;
	assert(*(uint32_t *)b == id);
	((uint32_t *)b)[0] = SALLOC_END;
	((uint32_t *)b)[1] = alloc_size;
	struct sslab *s = salloc_sslab_by_data(f, ptr);

	s->used_size -= size;
	s->wasted_size -= rnd_size - size;
	s->fragmented_size += rnd_size;

	f->used_size -= size;
	f->wasted_size -= rnd_size - size;
	f->fragmented_size += rnd_size;

	if (s->pos_in_heap != SALLOC_END) {
		size_t max_size_in_slab = f->slab_size - sizeof(struct sslab);
		uint32_t new_fragmentation_factor =
			(uint32_t)(100 * s->fragmented_size / max_size_in_slab);
		if (new_fragmentation_factor !=s->fragmentation_factor) {
			heap_remove(&f->slabs_by_fragmentaions, s->pos_in_heap);
			s->fragmentation_factor = new_fragmentation_factor;
			heap_push(&f->slabs_by_fragmentaions, s,
				  s->fragmentation_factor, &s->pos_in_heap);
		}
	}

	f->allow_memmove += alloc_size * f->allow_memmove_factor;
	salloc_defrag(f);
}