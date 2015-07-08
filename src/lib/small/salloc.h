#ifndef INCLUDES_TARANTOOL_SMALL_SALLOC_H
#define INCLUDES_TARANTOOL_SMALL_SALLOC_H

#include <stdlib.h>
#include "salad/heap.h"
#include "matras.h"
#include "salad/rlist.h"

#if defined(__cplusplus)
extern "C" {
#endif /* defined(__cplusplus) */

enum {
	SALLOC_END = 0xFFFFFFFF,
	SUI32S = (uint32_t)sizeof(uint32_t),
	SALLOC_MAX_DATA_SIZE = UINT32_MAX - 2 * SUI32S
};

typedef struct slab * (*salloc_slab_alloc_t)(void *slab_alloc_ctx);
typedef void (*salloc_slab_free_t)(struct slab *slab, void *slab_alloc_ctx);
typedef void * (*salloc_extent_alloc_t)();
typedef void (*salloc_extent_free_t)(void *extent);
typedef size_t (*get_allocation_size_t)(void *data);

struct salloc {
	size_t slab_size;
	uintptr_t fslab_addr_mask;
	void *alloc_ctx;
	salloc_slab_alloc_t slab_alloc_fun;
	salloc_slab_free_t slab_free_fun;
	get_allocation_size_t get_allocation_fun;
	struct heap slabs_by_fragmentaions;
	struct matras mtab;
	struct rlist slabs;
	struct sslab *current_slab;
	size_t currect_slab_pos;
	void **spare_slot;
	uint32_t spare_id;

	size_t used_size;
	size_t fragmented_size;
	size_t wasted_size;
	size_t system_size;
	size_t reserved_size;

	size_t allow_memmove;
	size_t memmoved;
	size_t allow_memmove_factor;
	struct sslab *current_defrag_slab;
	size_t currect_defrag_slab_pos;
};

void salloc_create(struct salloc *f, size_t slab_size, void *alloc_ctx,
	      salloc_slab_alloc_t slab_alloc_fun,
	      salloc_slab_free_t slab_free_fun,
	      get_allocation_size_t get_allocation_fun,
	      size_t matras_extent_size,
	      salloc_extent_alloc_t extent_alloc_t_fun,
	      salloc_extent_free_t extent_free_fun);

void salloc_destroy(struct salloc *f);


void *
salloc(struct salloc *f, uint32_t size, uint32_t *id);


void
sfree(struct salloc *f, void *ptr, uint32_t id, uint32_t size);

static inline void *
salloc_data_by_id(struct salloc *f, uint32_t id)
{
	return *(void **)matras_get(&f->mtab, id);
}

static inline uint32_t
salloc_id_by_data(struct salloc *f, void *data)
{
	(void)f;
	return ((uint32_t *)data)[-1];
}

#if defined(__cplusplus)
}; /* extern "C" */
#endif /* defined(__cplusplus) */

#endif //INCLUDES_TARANTOOL_SMALL_SALLOC_H
