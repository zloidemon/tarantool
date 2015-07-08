#ifndef INCLUDES_TARANTOOL_SALAD_HEAP_H
#define INCLUDES_TARANTOOL_SALAD_HEAP_H

#include <stdint.h>
#include <stdlib.h>
#include <assert.h>
#include <stdbool.h>

#if defined(__cplusplus)
extern "C" {
#endif /* defined(__cplusplus) */

struct heap {
	/* array of records */
	struct heap_record *records;
	/* count of records */
	uint32_t count;
	/* allocated size in records */
	uint32_t size;
};

inline void
heap_create(struct heap *h);

inline void
heap_destroy(struct heap *h);

inline void
heap_clear(struct heap *h);

inline int
heap_bulk_add(struct heap *h, void *data, uint32_t prior, uint32_t *pos);

inline void
heap_build(struct heap *h);

inline int
heap_push(struct heap *h, void *data, uint32_t prior, uint32_t *pos);

inline void
heap_remove(struct heap *h, uint32_t pos);

inline void *
heap_max(const struct heap *h);

inline void *
heap_maxp(const struct heap *h, uint32_t *prior);

inline void
heap_pop(struct heap *h);

inline uint32_t
heap_self_check(const struct heap *h);

struct heap_record {
	/* useful data */
	void *data;
	/* priority of the record */
	uint32_t prior;
	/* pointer to variable that stores positions of the record in array */
	uint32_t *pos;
};

void
heap_create(struct heap *h)
{
	h->records = 0;
	h->count = 0;
	h->size = 0;
}

void
heap_destroy(struct heap *h)
{
	if (h->size)
		free(h->records);
}

void
heap_clear(struct heap *h)
{
	h->count = 0;
}

static inline void
heap_swap(struct heap *h, uint32_t i, uint32_t j)
{
	assert(i == *h->records[i].pos);
	assert(j == *h->records[j].pos);
	{
		void *tmp = h->records[i].data;
		h->records[i].data = h->records[j].data;
		h->records[j].data = tmp;
	}
	{
		uint32_t tmp = h->records[i].prior;
		h->records[i].prior = h->records[j].prior;
		h->records[j].prior = tmp;
	}
	{
		uint32_t *tmp = h->records[i].pos;
		h->records[i].pos = h->records[j].pos;
		h->records[j].pos = tmp;
	}
	*h->records[i].pos = i;
	*h->records[j].pos = j;
}

int
heap_bulk_add(struct heap *h, void *data, uint32_t prior, uint32_t *pos)
{
	if (h->count >= h->size) {
		uint32_t new_size = h->size * 3 / 2 + 1;
		void *new_buf =
			realloc(h->records, new_size * sizeof(*h->records));
		if (!new_buf)
			return -1;
		h->size = new_size;
		h->records = (struct heap_record *)new_buf;
	}
	uint32_t i = h->count++;
	h->records[i].data = data;
	h->records[i].prior = prior;
	h->records[i].pos = pos;
	*pos = i;
	return 0;
}

static inline void
heap_heapify_down(struct heap *h, uint32_t i)
{
	while (true) {
		uint32_t j1 = 2 * i + 1;
		uint32_t j2 = j1 + 1;
		if (j2 >= h->count) {
			if (j1 < h->count)
			if (h->records[j1].prior > h->records[i].prior)
				heap_swap(h, i, j1);
			break;
		}
		uint32_t jm = h->records[j1].prior > h->records[j2].prior ?
			      j1 : j2;
		if (h->records[i].prior >= h->records[jm].prior)
			break;
		heap_swap(h, i, jm);
		i = jm;
	}
}

static inline void
heap_heapify_up(struct heap *h, uint32_t i)
{
	while (i) {
		uint32_t j = (i - 1) / 2;
		if (h->records[j].prior >= h->records[i].prior)
			break;
		heap_swap(h, i, j);
		i = j;
	}
}

int
heap_push(struct heap *h, void *data, uint32_t prior, uint32_t *pos)
{
	uint32_t i = h->count;
	if (heap_bulk_add(h, data, prior, pos))
		return -1;
	heap_heapify_up(h, i);
	return 0;
}

void
heap_remove(struct heap *h, uint32_t pos)
{
	assert(pos < h->count);
	h->count--;
	if (pos == h->count)
		return;
	heap_swap(h, pos, h->count);
	heap_heapify_up(h, pos);
	heap_heapify_down(h, pos);
}

void *
heap_max(const struct heap *h)
{
	assert(h->count);
	return h->records[0].data;
}

void *
heap_maxp(const struct heap *h, uint32_t *prior)
{
	assert(h->count);
	*prior = h->records[0].prior;
	return h->records[0].data;
}

void
heap_pop(struct heap *h)
{
	h->count--;
	if (h->count == 0)
		return;
	heap_swap(h, 0, h->count);
	heap_heapify_down(h, 0);
}

void
heap_build(struct heap *h)
{
	if (h->count <= 1)
		return;
	uint32_t i = h->count / 2 - 1;
	while (true) {
		heap_heapify_down(h, i);
		if (i == 0)
			break;
		i--;
	}
}


uint32_t
heap_self_check(const struct heap *h)
{
	uint32_t res = 0;
	for (uint32_t i = 0; i < h->count; i++) {
		if (i != *h->records[i].pos)
			res |= 1;
		uint32_t j1 = 2 * i + 1;
		uint32_t j2 = j1 + 1;
		for (uint32_t j = j1; j <= j2; j++) {
			if (j >= h->count)
				break;
			if (h->records[i].prior < h->records[j].prior)
				res |= 2;
		}
	}
	return res;
}

#if defined(__cplusplus)
}; /* extern "C" */
#endif /* defined(__cplusplus) */

#endif /* INCLUDES_TARANTOOL_SALAD_HEAP_H */
