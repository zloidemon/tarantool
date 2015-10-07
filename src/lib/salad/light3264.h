/*
 * *No header guard*: the header is allowed to be included twice
 * with different sets of defines.
 */
/*
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

#include "small/matras.h"
#include <string.h>

/**
 * light - Linear probing Incremental Growing Hash Table
 */


/**
 * Additional user defined name that appended to prefix 'light'
 *  for all names of structs and functions in this header file.
 * All names use pattern: light<LIGHT_NAME>_<name of func/struct>
 * May be empty, but still have to be defined (just #define LIGHT_NAME)
 * Example:
 * #define LIGHT_NAME _test
 * ...
 * struct light_test_core hash_table;
 * light_test_create(&hash_table, ...);
 */
#ifndef LIGHT_NAME
#error "LIGHT_NAME must be defined"
#endif

/**
 * Data type that hash table holds. Must be less than 8 bytes.
 */
#ifndef LIGHT_DATA_TYPE
#error "LIGHT_DATA_TYPE must be defined"
#endif

/**
 * Data type that used to for finding values.
 */
#ifndef LIGHT_KEY_TYPE
#error "LIGHT_KEY_TYPE must be defined"
#endif

/**
 * Type of optional third parameter of comparing function.
 * If not needed, simply use #define LIGHT_CMP_ARG_TYPE int
 */
#ifndef LIGHT_CMP_ARG_TYPE
#error "LIGHT_CMP_ARG_TYPE must be defined"
#endif

/**
 * Data comparing function. Takes 3 parameters - value, key and
 * optional value that stored in hash table struct.
 * Third parameter may be simply ignored like that:
 * #define LIGHT_EQUAL(a, b, garbage) a == b
 */
#ifndef LIGHT_EQUAL
#error "LIGHT_EQUAL must be defined"
#endif

/**
 * Tools for name substitution:
 */
#ifndef CONCAT4
#define CONCAT4_R(a, b, c, d) a##b##c##d
#define CONCAT4(a, b, c, d) CONCAT4_R(a, b, c, d)
#endif

#ifdef _
#error '_' must be undefinded!
#endif
#define LIGHT(name) CONCAT4(light, LIGHT_NAME, _, name)

#ifndef WIN32
#include <stdint.h>

#else
#define __builtin_expect(x) (x)
#define __builtin_prefetch(x, y) do {} while(0)
#ifndef WIN_UINT32_DEFINED
#define WIN_UINT32_DEFINED
typedef __int32 uint32_t;
#endif
#ifndef WIN_UINT64_DEFINED
#define WIN_UINT64_DEFINED
typedef __int64 uint64_t;
#endif
#endif

#if !defined __GNUC_MINOR__ || defined __INTEL_COMPILER || \
        defined __SUNPRO_C || defined __SUNPRO_CC
#define LIGHT_GCC_VERSION(major, minor) 0
#else
#define LIGHT_GCC_VERSION(major, minor) (__GNUC__ > (major) || \
        (__GNUC__ == (major) && __GNUC_MINOR__ >= (minor)))
#endif

#if !defined(__has_builtin)
#define __has_builtin(x) 0 /* clang */
#endif

#if LIGHT_GCC_VERSION(2, 9) || __has_builtin(__builtin_expect)
#define LIGHT_LIKELY(x) __builtin_expect(!!(x), 1)
#define LIGHT_UNLIKELY(x) __builtin_expect(!!(x), 0)
#else
#define LIGHT_LIKELY(x) (x)
#define LIGHT_UNLIKELY(x) (x)
#pragma message("__builtin_expect was not found")
#endif

#ifdef WIN32
#include <intrin.h>
#pragma intrinsic (_BitScanForward)
inline int __builtin_ctz(unsigned value)
{
	unsigned long trailing_zero = 0;
	unsigned char nonzero = _BitScanForward(&trailing_zero, value);
	assert(nonzero); (void)nonzero;
	return trailing_zero;
}
#pragma intrinsic (_BitScanForward64)
inline int __builtin_ctzll(unsigned long long value)
{
	unsigned long trailing_zero = 0;
	unsigned char nonzero = _BitScanForward64(&trailing_zero, value);
	assert(nonzero); (void)nonzero;
	return trailing_zero;
}
#endif

/**
 * Main struct for holding hash table
 */
struct LIGHT(core) {
	/* count of values in hash table */
	uint64_t count;
	/* size of hash table in clusters ( equal to mtable.size ) */
	uint32_t table_size;
	/*
	 * let cover is table_size rounded up to power of 2;
	 * cover_mask is (cover - 1)
	 */
	uint32_t cover_mask;
	/* dynamic storage for clusters */
	struct matras mtable;
	/* additional parameter for data comparison */
	LIGHT_CMP_ARG_TYPE arg;
	/* seed for random generator */
	unsigned long rnd_seed;
#ifdef LIGHT_DEBUG
	/* debug counters */
	mutable struct debug_counters {
		uint64_t find_calls;
		uint64_t find_lookups;
		uint64_t ins_calls;
		uint64_t ins_lookups;
		uint64_t del_calls;
		uint64_t del_lookups;
		uint64_t grow_calls;
		uint64_t grow_lookups;
		uint64_t nice_calls;
		uint64_t nice_lookups;
	} debug_counters;
#endif
};

struct LIGHT(iterator) {
	uint32_t slot;
	uint32_t pos;
};

/**
 * Type of functions for memory allocation and deallocation
 */
typedef void *(*LIGHT(extent_alloc_t))();
typedef void (*LIGHT(extent_free_t))(void *);

/**
 * Special slot that means that nothing was found
 */
static const uint32_t LIGHT(end) = 0xFFFFFFFF;

/**
 * @brief Hash table construction. Fills struct light members.
 * @param ht - pointer to a hash table struct
 * @param extent_size - size of allocating memory blocks
 * @param extent_alloc_func - memory blocks allocation function
 * @param extent_free_func - memory blocks allocation function
 * @param arg - optional parameter to save for comparing function
 */
void
LIGHT(create)(struct LIGHT(core) *ht, size_t extent_size,
	      LIGHT(extent_alloc_t) extent_alloc_func,
	      LIGHT(extent_free_t) extent_free_func,
	      LIGHT_CMP_ARG_TYPE arg);

/**
 * @brief Hash table destruction. Frees all allocated memory
 * @param ht - pointer to a hash table struct
 */
void
LIGHT(destroy)(struct LIGHT(core) *ht);

/**
 * @brief Find a record with given hash and value and set the iterator to
 * that record
 * @param ht - pointer to a hash table struct
 * @param hash - hash to find
 * @param data - value to find
 * @parap itr - iterator to set
 * @return true of found record or false if nothing found
 */
bool
LIGHT(find)(const struct LIGHT(core) *ht, uint32_t hash, LIGHT_KEY_TYPE data,
	    struct LIGHT(iterator) *itr);

/**
 * @brief Insert a record with given hash and value
 * @param ht - pointer to a hash table struct
 * @param hash - hash to insert
 * @param data - value to insert
 * @return 0 on success or -1 on memory error
 */
int
LIGHT(insert)(struct LIGHT(core) *ht, uint32_t hash, LIGHT_DATA_TYPE data);

/**
 * @brief Delete a record from a hash table by given record ID
 * @param ht - pointer to a hash table struct
 * @param slotpos - ID of an record. See light_find for details.
 * @return 0 if ok or -1 on memory error
 */
int
LIGHT(delete)(struct LIGHT(core) *ht, struct LIGHT(iterator) *itr);

/**
 *
 */
bool
LIGHT(iterator_is_valid)(struct LIGHT(core) *ht, struct LIGHT(iterator) *itr);

/**
 *
 */
LIGHT_DATA_TYPE
LIGHT(iterator_get_val)(struct LIGHT(core) *ht, struct LIGHT(iterator) *itr);

/**
 *
 */
void
LIGHT(iterator_next)(struct LIGHT(core) *ht, struct LIGHT(iterator) *itr);

/**
 * Tuning
 */

#define LIGHT_GROW_FILLNESS 4
#define LIGHT_NICE_ATTEMPTS 10
#define LIGHT_HASH_TRANSFORM(h) (((h) >> 2) ^ (((h) & 3) << 28))

/**
 * Internal definitions
 */

/**
 * Light hash table can be represented as an array of clusters.
 *
 * A cluster can hold up to 5 values with their 30bit hashes
 *  at positions (pos) 0..4.
 * Every position thus could be occupied (present) or not.
 * A value in cluster could be a true value, if it is in proper slot,
 *  or chain value - if not.
 *
 * Cluster could be in these states and their properties:
 * 1)isolated:
 *  Contains 0..5 true values and has no next cluster.
 *  a) All chain bits are zero;
 *  b) No next cluster - high order next_id bits are 0x1ff
 * 2)chain start:
 *  Contains 5 true values and has next cluster.
 *  a) Contains chain bits are zero;
 *  b) Has next cluster - high order next_id bits are not 0x1ff and 0x1fe
 * 3)chain middle:
 *  Contains 5 values, at least one is chain value;
 *   all chain values has same start slot.
 *  a) At least one chain bit is not zero;
 *  b) Has next cluster - high order next_id bits are not 0x1ff and 0x1fe
 * 4)chain end:
 *  Contains 5 values, at least one is chain value;
 *   all chain values has same start slot.
 *  a) At least one chain bit is not zero;
 *  b) No next cluster - high order next_id bits are 0x1ff
 */
struct LIGHT(cluster) {
	/**
	 * [high-order-bits..........low-order-bits]
	 * bits[0]:
	 * [9bits next_id_1][11bits hash4_1][11bits hash3_1]....[11bits hash0_1]
	 * bits[1]:
	 * [9bits next_id_2][11bits hash4_2][11bits hash3_2]....[11bits hash0_2]
	 * bits[2]:
	 * [10 bits next_id_3][1 bit chain4][1 bit present4][8 bits hash4_3]
	 *  [1 bit next_id_4][1 bit chain3][1 bit present3][8 bits hash3_3]
	 *  [1 bit next_id_5][1 bit chain2][1 bit present2][8 bits hash2_3]
	 *  [1 bit next_id_6][1 bit chain1][1 bit present1][8 bits hash1_3]
	 *  [1 bit next_id_7][1 bit chain0][1 bit present0][8 bits hash0_3]
	 *
	 * 30 bit hashx = [8 bits hashx_3][11 bits hashx_2][11 bits hashx_1]
	 * 32 bit next_id = [9bits next_id_1][9bits next_id_2][10bits next_id_3]
	 *  [1 bit next_id_4][1 bit next_id_5][1 bit next_id_6][1 bit next_id_7]
	 *
	 *  If present bit is not set, other bits (including chain) means
	 *  nothing
	 */
	uint64_t bits[3];
	union {
		LIGHT_DATA_TYPE data[5];
		uint64_t padding[5];
	};
};

/**
 * Support functions
 * TODO: unit test 'em all
 */
inline uint32_t
LIGHT(rand)(struct LIGHT(core) *ht)
{
	ht->rnd_seed = ht->rnd_seed * 1103515245 + 12345;
	return (uint32_t)ht->rnd_seed;
}

inline uint64_t
LIGHT(bit_mask)(uint32_t bit_no)
{
	assert(bit_no < 11);
	const uint64_t mask = 0x100200400801ull; /* 0, 11, 22, 33, 44 */
	return mask << bit_no;
}

inline bool
LIGHT(cluster_has_next)(const struct LIGHT(cluster) *c)
{
	return (c->bits[0] >> 55) != 0x1FFull;
}

inline void
LIGHT(cluster_clear_next)(struct LIGHT(cluster) *c)
{
	c->bits[0] |= 0x1FFull << 55;
}

inline uint32_t
LIGHT(cluster_get_next)(const struct LIGHT(cluster) *c)
{
	uint64_t part1 = c->bits[0] >> 55; /* 9 bits */
	uint64_t part2 = c->bits[1] >> 55; /* 9 bits */
	uint64_t part3 = c->bits[2] >> 54; /* 10 bits */
	const uint64_t mask = 0x80100200400ull; /* 1 << 10, 21, 32, 43 */
	const uint64_t mult = 0x4010040100000ull; /* 1 << 20, 30, 40, 50 */
	uint64_t part4 = ((c->bits[2] & mask) * mult) >> 60; /* 4 bits */
	return (uint32_t) (part4 | (part3 << 4) | (part2 << 14) |
			   (part1 << 23));
	/* 12 operations total */
}

inline void
LIGHT(cluster_set_next)(struct LIGHT(cluster) *c, uint32_t next32bit)
{
	c->bits[0] &= ~(0x1FFull << 55);
	c->bits[1] &= ~(0x1FFull << 55);
	c->bits[2] &= ~((0x1FFull << 55) | LIGHT(bit_mask)(10));
	uint64_t next = (uint64_t) next32bit;
	c->bits[0] |= (next & 0xFF800000ull) << 32; /* 9 bits */
	c->bits[1] |= (next & 0x007FC000ull) << (32 + 9); /* 9 bits */
	c->bits[2] |= (next & 0x00003FF0ull) << (32 + 9 + 9); /* 10 bits */
	uint64_t part4 = ((next & 0xFull) /* 4 bits */
			  * 0x10040100400ull) /* 1 << 10, 20, 30, 40 */
			 & LIGHT(bit_mask)(10);
	c->bits[2] |= part4;
	/* 16 operations total */
}

inline void
LIGHT(cluster_copy_next)(struct LIGHT(cluster) *dst,
			 const struct LIGHT(cluster) *src)
{
	const uint64_t mask1 = 0x1FFull << 55;
	const uint64_t mask2 = (0x3FFull << 54) | LIGHT(bit_mask)(10);
	dst->bits[0] = (dst->bits[0] & ~mask1) | (src->bits[0] & mask1);
	dst->bits[1] = (dst->bits[1] & ~mask1) | (src->bits[1] & mask1);
	dst->bits[2] = (dst->bits[2] & ~mask2) | (src->bits[2] & mask2);
	/* 9 operations total */
}

/**
 * transformed hash
 * chain_bit : 0 or 1
 */
inline void
LIGHT(cluster_set_hash_and_val)(struct LIGHT(cluster) *cluster,
				int pos, int shift,
				uint32_t hash32, LIGHT_DATA_TYPE value,
				uint64_t chain_bit)
{
	assert((hash32 >> 30) == 0);
	uint64_t hash = (uint64_t) hash32;

	uint64_t mask1 = ~(0x7FFull << shift);
	uint64_t mask2 = ~(0x3FFull << shift);
	cluster->bits[0] &= mask1;
	cluster->bits[0] |= (hash & 0x7FF) << shift;
	cluster->bits[1] &= mask1;
	cluster->bits[1] |= ((hash >> 11) & 0x7FF) << shift;
	cluster->bits[2] &= mask2;
	cluster->bits[2] |= ((hash >> 22) | (1ull << 8) | (chain_bit << 9))
			    << shift;
	cluster->data[pos] = value;
}

inline uint32_t
LIGHT(cluster_get_hash)(const struct LIGHT(cluster) *cluster, int offset)
{
	return ((cluster->bits[0] >> offset) & 0x7FFull) |
	       (((cluster->bits[1] >> offset) & 0x7FFull) << 11) |
	       (((cluster->bits[2] >> offset) & 0xFFull) << 22);
}

inline uint64_t
LIGHT(cluster_present_mask)(const struct LIGHT(cluster) *c)
{
	return (c->bits[2] & LIGHT(bit_mask)(8)) >> 8;
}

inline uint64_t
LIGHT(cluster_free_mask)(const struct LIGHT(cluster) *c)
{
	return ((~c->bits[2]) & LIGHT(bit_mask)(8)) >> 8;
}

inline bool
LIGHT(cluster_is_full)(const struct LIGHT(cluster) *c)
{
	return (c->bits[2] & LIGHT(bit_mask)(8)) == LIGHT(bit_mask)(8);
}

inline bool
LIGHT(cluster_is_empty)(const struct LIGHT(cluster) *c)
{
	return (c->bits[2] & LIGHT(bit_mask)(8)) == 0ull;
}

inline uint64_t
LIGHT(cluster_chain_mask)(const struct LIGHT(cluster) *c)
{
	return (c->bits[2] >> 8) & (c->bits[2] >> 9) & LIGHT(bit_mask)(0);
}

inline bool
LIGHT(cluster_full_has_chain)(const struct LIGHT(cluster) *c)
{
	return c->bits[2] & LIGHT(bit_mask)(9);
}

inline uint64_t
LIGHT(cluster_true_mask)(const struct LIGHT(cluster) *c)
{
	return (c->bits[2] >> 8) & (~c->bits[2] >> 9) & LIGHT(bit_mask)(0);
}

inline bool
LIGHT(cluster_has_chain)(const struct LIGHT(cluster) *c)
{
	return c->bits[2] & LIGHT(bit_mask)(9);
}

inline uint64_t
LIGHT(mask_bit_count)(uint64_t sparse_mask)
{
	const uint64_t mult = LIGHT(bit_mask)(0) << 16;
	return (sparse_mask * mult) >> 60;
}

inline uint64_t
LIGHT(cluster_val_count)(const struct LIGHT(cluster) *c)
{
	return ((c->bits[2] & LIGHT(bit_mask)(8)) * LIGHT(bit_mask)(8)) >> 60;
}

inline uint64_t
LIGHT(cluster_hash_bits)(const struct LIGHT(cluster) *c, uint64_t bit_no)
{
	/* bit_no : 0..29 */
	assert(bit_no < 30);
	uint64_t bits_index = (bit_no * 47) >> 9; /* i.e. divide by 11 */
	uint64_t bits_bit = bit_no - bits_index * 11; /* same as % 11 */
	return (c->bits[bits_index] & LIGHT(bit_mask)(bits_bit)) >> bits_bit;
}

/**
 * sparse mask
 */
inline int
LIGHT(grab_bit)(uint64_t mask, int *offset)
{
	assert(mask);
	*offset = __builtin_ctzll(mask); /* 0-10, 11-21, 22-32, ... */
	return (*offset * 47) >> 9; /* divide by 11: 0, 1, 2, 3, 4 */
}

/**
 * sparse mask
 */
inline int
LIGHT(grab_bit_exclude)(uint64_t *mask)
{
	assert(mask);
	int offset = __builtin_ctzll(*mask); /* 0-10, 11-21, 22-32, ... */
	*mask ^= (1ull << offset);
	return (offset * 47) >> 9; /* divide by 11: 0, 1, 2, 3, 4 */
}

inline uint32_t
LIGHT(slot)(const struct LIGHT(core) *ht, uint32_t hash)
{
	assert(ht->table_size);
	uint32_t cover_mask = ht->cover_mask;
	uint32_t res = hash & cover_mask;
	uint32_t probe = (ht->table_size - res - 1) >> 31;
	uint32_t shift = __builtin_ctz(~(cover_mask >> 1));
	res ^= (probe << shift);
	assert(res < ht->table_size);
	return res; /* 9 operations? */
}

/**
 * moves with chain bit
 */
inline void
LIGHT(cluster_move_hash_and_val_symmetric)(struct LIGHT(cluster) *dst,
					   struct LIGHT(cluster) *src,
					   uint64_t mask)
{
	assert((mask & LIGHT(bit_mask)(0)) == mask);
	assert((dst->bits[2] & (mask << 8)) == 0);
	assert((src->bits[2] & (mask << 8)) == (mask << 8));
	uint64_t mask1 = mask * 0x7FFull;
	uint64_t mask2 = mask * 0x3FFull;
	dst->bits[0] = (dst->bits[0] & ~mask1) | (src->bits[0] & mask1);
	dst->bits[1] = (dst->bits[1] & ~mask1) | (src->bits[1] & mask1);
	dst->bits[2] = (dst->bits[2] & ~mask2) | (src->bits[2] & mask2);
	src->bits[2] ^= mask << 8;
	/* 16 operations done ? */
	while (mask) { /* TODO: test avoding the branch */
		int pos = LIGHT(grab_bit_exclude)(&mask);
		dst->data[pos] = src->data[pos];
	}
}

/**
 * moves with chain bit
 */
inline void
LIGHT(cluster_move_hash_and_val_one)(struct LIGHT(cluster) *dst,
				     struct LIGHT(cluster) *src,
				     uint64_t *dst_mask, uint64_t *src_mask)
{
	assert(*dst_mask);
	assert(*src_mask);
	assert((*dst_mask & LIGHT(bit_mask)(0)) == *dst_mask);
	assert((*src_mask & LIGHT(bit_mask)(0)) == *src_mask);
	assert((dst->bits[2] & (*dst_mask << 8)) == 0);
	assert((src->bits[2] & (*src_mask << 8)) == (*src_mask << 8));

	int dst_shift = __builtin_ctzll(*dst_mask); /* 0, 11, 22... */
	*dst_mask ^= (1ull << dst_shift);
	int pos_dst = (dst_shift * 47) >> 9; /* divide by 11 */

	int src_shift = __builtin_ctzll(*src_mask); /* 0, 11, 22... */
	*src_mask ^= (1ull << src_shift);
	int pos_src = (src_shift * 47) >> 9; /* divide by 11 */

	uint64_t mask_dst1 = ~(0x7FFull << dst_shift);
	uint64_t mask_dst2 = ~(0x3FFull << dst_shift);

	dst->bits[0] = (dst->bits[0] & mask_dst1) |
		       (((src->bits[0] >> src_shift) & 0x7FFull) << dst_shift);
	dst->bits[1] = (dst->bits[1] & mask_dst1) |
		       (((src->bits[1] >> src_shift) & 0x7FFull) << dst_shift);
	dst->bits[2] = (dst->bits[2] & mask_dst2) |
		       (((src->bits[2] >> src_shift) & 0x3FFull) << dst_shift);

	src->bits[2] ^= 1ull << (src_shift + 8);

	dst->data[pos_dst] = src->data[pos_src];
	/* 32 operations? */
}

/**
 * moves with chain bit
 */
inline void
LIGHT(cluster_move_hash_and_val)(struct LIGHT(cluster) *dst,
				 struct LIGHT(cluster) *src,
				 uint64_t *dst_mask, uint64_t *src_mask)
{
	assert((*dst_mask & LIGHT(bit_mask)(0)) == *dst_mask);
	assert((*src_mask & LIGHT(bit_mask)(0)) == *src_mask);
	assert((dst->bits[2] & (*dst_mask << 8)) == 0);
	assert((src->bits[2] & (*src_mask << 8)) == (*src_mask << 8));
	uint64_t common_mask = *dst_mask & *src_mask;
	LIGHT(cluster_move_hash_and_val_symmetric)(dst, src, common_mask);
	*dst_mask ^= common_mask;
	*src_mask ^= common_mask;
	while (*dst_mask && *src_mask)
		LIGHT(cluster_move_hash_and_val_one)(dst, src,
						     dst_mask, src_mask);
}

/**
 * Prepare search bits by 30bit (transformed) hash
 */
inline void
LIGHT(prepare_search_bits)(uint64_t *search_bits, uint32_t hash)
{
	uint64_t not_hash64 = ~((uint64_t) hash);
	search_bits[0] = (not_hash64 & 0x7FFull) * LIGHT(bit_mask)(0);
	search_bits[1] = ((not_hash64 >> 11) & 0x7FFull) * LIGHT(bit_mask)(0);
	search_bits[2] = (((not_hash64 >> 22) & 0xFFull) * LIGHT(bit_mask)(0));
}

/**
 * Get sparse mask of found values, shifted left by 10 (!)
 */
inline uint64_t
LIGHT(search_in_cluster)(uint64_t *search_bits, struct LIGHT(cluster) *cluster)
{
	uint64_t match_mask = (cluster->bits[0] ^ search_bits[0]) &
			      (cluster->bits[1] ^ search_bits[1]);
	uint64_t hit_mask1 = match_mask & LIGHT(bit_mask)(10);
	match_mask &= (cluster->bits[2] ^ search_bits[2]);
	const uint64_t mask2 = ~(LIGHT(bit_mask)(10) | LIGHT(bit_mask)(9));
	uint64_t hit_mask2 = (match_mask & mask2) + LIGHT(bit_mask)(0);
	return hit_mask1 & (hit_mask2 << 1);
}

inline struct LIGHT(cluster) *
LIGHT(cluster)(const struct LIGHT(core) *ht, uint32_t slot)
{
	return (struct LIGHT(cluster) *) matras_get(&ht->mtable, slot);
}

inline struct LIGHT(cluster) *
LIGHT(alloc)(struct LIGHT(core) *ht, uint32_t *slot)
{
	return (struct LIGHT(cluster) *) matras_alloc(&ht->mtable, slot);
}

inline struct LIGHT(cluster) *
LIGHT(touch)(struct LIGHT(core) *ht, uint32_t slot)
{
	return (struct LIGHT(cluster) *) matras_touch(&ht->mtable, slot);
}

inline void
LIGHT(dealloc)(struct LIGHT(core) *ht)
{
	matras_dealloc(&ht->mtable);
}

/**
 * Light implementation
 */
inline void
LIGHT(create)(struct LIGHT(core) *ht, size_t extent_size,
	      LIGHT(extent_alloc_t) extent_alloc_func,
	      LIGHT(extent_free_t) extent_free_func,
	      LIGHT_CMP_ARG_TYPE arg)
{
	/* check that values will fit cluster of 64 byte size */
	assert(sizeof(LIGHT_DATA_TYPE) == 8);
	/* additional check that cluster is of 64 byte size */
	assert(sizeof(struct LIGHT(cluster)) == 64);
	/* check that __builtin_ctz is used properly */
	assert(sizeof(unsigned) == sizeof(uint32_t));
	/* check that __builtin_ctzll is used properly */
	assert(sizeof(unsigned long long) == sizeof(uint64_t));
	ht->count = 0;
	ht->table_size = 0;
	ht->cover_mask = 0;
	ht->arg = arg;
	matras_create(&ht->mtable,
		      (matras_id_t) extent_size, (matras_id_t) 64,
		      extent_alloc_func, extent_free_func);
	ht->rnd_seed = 1;
#ifdef LIGHT_DEBUG
	memset(&ht->debug_counters, 0, sizeof(LIGHT(core)::debug_counters));
#endif
}

inline void
LIGHT(destroy)(struct LIGHT(core) *ht)
{
	matras_destroy(&ht->mtable);
}

/**
 * Find not fully occupied cluster, preferably isolated and preferably that has
 *  more unoccupied space.
 */
uint32_t
LIGHT(nice_cluster)(struct LIGHT(core) *ht)
{
#ifdef LIGHT_DEBUG
	ht->debug_counters.nice_calls++;
#endif
	for (int r = 0; r < LIGHT_NICE_ATTEMPTS; r++) {
		uint32_t slot = LIGHT(rand)(ht) % ht->table_size;
		struct LIGHT(cluster) *cluster = LIGHT(cluster)(ht, slot);
#ifdef LIGHT_DEBUG
		ht->debug_counters.nice_lookups++;
#endif
		if (LIGHT(cluster_is_full)(cluster))
			continue;
		if (LIGHT(cluster_has_chain)(cluster))
			continue;
		return slot;
	}
	return LIGHT(end);
}

inline bool
LIGHT(find)(const struct LIGHT(core) *ht, uint32_t hash, LIGHT_KEY_TYPE key,
	    struct LIGHT(iterator) *itr)
{
#ifdef LIGHT_DEBUG
	ht->debug_counters.find_calls++;
#endif
	if (LIGHT_UNLIKELY(ht->table_size == 0))
		return false;

	hash = LIGHT_HASH_TRANSFORM(hash);
	uint32_t slot = LIGHT(slot)(ht, hash);
	struct LIGHT(cluster) *cluster = LIGHT(cluster)(ht, slot);
	__builtin_prefetch(cluster, 0);
	/* 12 operation done? */

	uint64_t search_bits[3];
	LIGHT(prepare_search_bits)(search_bits, hash);
	/* 9 operations done? */

	while (true) {
#ifdef LIGHT_DEBUG
		ht->debug_counters.find_lookups++;
#endif
		uint64_t hit_mask = LIGHT(search_in_cluster)(search_bits,
							     cluster);
		/* another 9 operations done? */
		while (hit_mask) {
			uint32_t pos = LIGHT(grab_bit_exclude)(&hit_mask);
			if (LIGHT_LIKELY(LIGHT_EQUAL((cluster->data[pos]),
						     (key),
						     (ht->arg)))) {
				itr->slot = slot;
				itr->pos = pos;
				return true;
			}
		}
		if (LIGHT_LIKELY(!LIGHT(cluster_has_next)(cluster)))
			return false;
		slot = LIGHT(cluster_get_next)(cluster);
		cluster = LIGHT(cluster)(ht, slot);
	}
	return false; /* unreachable */
}

/**
 * light_grow helper
 */
inline int
LIGHT(compact_chain)(struct LIGHT(core) *ht, struct LIGHT(cluster) *cluster)
{
	/* TODO: try to implement move from tail */
	assert(LIGHT(cluster_free_mask)(cluster));
	assert(LIGHT(cluster_chain_mask)(cluster));

	uint32_t next_slot = LIGHT(cluster_get_next)(cluster);
	struct LIGHT(cluster) *next_cluster = LIGHT(touch)(ht, next_slot);
#ifdef LIGHT_DEBUG
	ht->debug_counters.grow_lookups++;
#endif
	if (LIGHT_UNLIKELY(!next_cluster)) /* memory failure */
		return -1;
	uint64_t mask = LIGHT(cluster_free_mask)(cluster);
	uint64_t next_mask = LIGHT(cluster_chain_mask)(next_cluster);
	const uint64_t mask_save = mask; /* for rollback */
	const uint64_t next_mask_save = next_mask;  /* for rollback */
	LIGHT(cluster_move_hash_and_val)(cluster, next_cluster,
					 &mask, &next_mask);

	struct LIGHT(cluster) *next_compact_cluster = next_cluster;
	if (next_mask == 0) {
		/* next cluster has no chain slots, unlinking it */
		LIGHT(cluster_copy_next)(cluster, next_cluster);
		LIGHT(cluster_clear_next)(next_cluster);
		if (mask == 0)
			return 0;
		next_compact_cluster = cluster;
	}
	if (!LIGHT(cluster_has_next)(next_compact_cluster))
		return 0;
	int result = LIGHT(compact_chain)(ht, next_compact_cluster);
	if (LIGHT_LIKELY(result == 0))
		return 0;

	/* rollback */
	if (next_mask == 0) {
		/* link back */
		LIGHT(cluster_copy_next)(next_cluster, cluster);
		LIGHT(cluster_set_next)(cluster, next_slot);
	}
	mask ^= mask_save;
	next_mask ^= next_mask_save;
	LIGHT(cluster_move_hash_and_val)(next_cluster, cluster,
					 &next_mask, &mask);
	assert(mask == 0 && mask_save == 0);
	return result;
}

/**
 * light_grow helper
 */
struct LIGHT(cluster_list_node) {
	struct LIGHT(cluster) *cluster;
	uint32_t slot;
	bool last_and_dirty;
	uint64_t mask1;
	uint64_t mask2;
	struct LIGHT(cluster_list_node) *next;
};

/**
 * light_split_chain helper
 */
inline int
LIGHT(split_chain_rec)(struct LIGHT(cluster_list_node) *chain_beg,
		       struct LIGHT(cluster_list_node) *chain_end,
		       struct LIGHT(core) *ht, uint32_t split_bit_no,
		       uint32_t start_slot1, uint32_t start_slot2,
		       struct LIGHT(cluster) *cluster1,
		       struct LIGHT(cluster) *cluster2)
{
	uint64_t mask = LIGHT(cluster_chain_mask)(chain_end->cluster);
	uint64_t hash_bit_mask = LIGHT(cluster_hash_bits)(chain_end->cluster,
							  split_bit_no);
	chain_end->mask1 = mask & ~hash_bit_mask;
	chain_end->mask2 = mask & hash_bit_mask;
	chain_end->last_and_dirty = false;

	if (LIGHT(cluster_has_next)(chain_end->cluster)) {
		uint32_t next_slot =
			LIGHT(cluster_get_next)(chain_end->cluster);
		struct LIGHT(cluster) *next_cluster = LIGHT(touch)(ht,
								   next_slot);
#ifdef LIGHT_DEBUG
		ht->debug_counters.grow_lookups++;
#endif
		if (LIGHT_UNLIKELY(!next_cluster)) /* memory failure */
			return -1;

		struct LIGHT(cluster_list_node) node;
		node.cluster = next_cluster;
		node.slot = next_slot;
		chain_end->next = &node;
		return LIGHT(split_chain_rec)(chain_beg, &node,
					      ht, split_bit_no,
					      start_slot1, start_slot2,
					      cluster1, cluster2);
	}
	chain_end->next = 0;

	uint64_t mask1 = LIGHT(cluster_free_mask)(cluster1);
	uint64_t mask2 = LIGHT(cluster_free_mask)(cluster2);
	uint64_t truify1 = ~LIGHT(bit_mask)(9);
	uint64_t truify2 = ~LIGHT(bit_mask)(9);
	struct LIGHT(cluster_list_node) *empty_list = 0;
	do {
		struct LIGHT(cluster_list_node) *next = chain_beg->next;

		LIGHT(cluster_move_hash_and_val)(cluster1, chain_beg->cluster,
						 &mask1, &chain_beg->mask1);
		cluster1->bits[2] &= truify1;
		LIGHT(cluster_move_hash_and_val)(cluster2, chain_beg->cluster,
						 &mask2, &chain_beg->mask2);
		cluster2->bits[2] &= truify2;

		if (chain_beg->mask1 == 0) {
			if (chain_beg->mask2 == 0) {
				/* next cluster is not chain anymore */
				chain_beg->next = empty_list;
				empty_list = chain_beg;
			} else {
				/* cluster2 is full */
				LIGHT(cluster_set_next)(cluster2,
							chain_beg->slot);
				cluster2 = chain_beg->cluster;
				mask2 = LIGHT(cluster_free_mask)(cluster2);
				truify2 = 0xFFFFFFFFFFFFFFFFull;
			}
		} else {
			LIGHT(cluster_set_next)(cluster1,
						chain_beg->slot);
			cluster1 = chain_beg->cluster;
			truify1 = 0xFFFFFFFFFFFFFFFFull;
			while (chain_beg->mask2) {
				/* cluster1 and cluster2 are full */
				assert(empty_list);
				LIGHT(cluster_set_next)(cluster2,
							empty_list->slot);
				cluster2 = empty_list->cluster;
				mask2 = LIGHT(cluster_free_mask)(cluster2);
				truify2 = 0xFFFFFFFFFFFFFFFFull;
				empty_list = empty_list->next;
				LIGHT(cluster_move_hash_and_val)(cluster2,
								 chain_beg->cluster,
								 &mask2,
								 &chain_beg->mask2);
			}
			mask1 = LIGHT(cluster_free_mask)(cluster1);
		}
		chain_beg = next;
	} while (chain_beg);
	LIGHT(cluster_clear_next)(cluster1);
	LIGHT(cluster_clear_next)(cluster2);
	while (empty_list) {
		LIGHT(cluster_clear_next)(empty_list->cluster);
		empty_list = empty_list->next;
	}
	return 0;
}

/**
 * light_grow helper
 */
inline int
LIGHT(split_chain)(struct LIGHT(core) *ht, uint32_t split_bit_no,
		   uint32_t start_slot1, uint32_t start_slot2,
		   struct LIGHT(cluster) *cluster1,
		   struct LIGHT(cluster) *cluster2)
{
	uint32_t next_slot = LIGHT(cluster_get_next)(cluster1);
	struct LIGHT(cluster) *next_cluster = LIGHT(touch)(ht, next_slot);
#ifdef LIGHT_DEBUG
	ht->debug_counters.grow_lookups++;
#endif
	if (LIGHT_UNLIKELY(!next_cluster)) /* memory failure */
		return -1;

	struct LIGHT(cluster_list_node) node;
	node.cluster = next_cluster;
	node.slot = next_slot;
	return LIGHT(split_chain_rec)(&node, &node, ht, split_bit_no,
				      start_slot1, start_slot2,
				      cluster1, cluster2);
}


inline int
LIGHT(grow)(struct LIGHT(core) *ht)
{
#ifdef LIGHT_DEBUG
	ht->debug_counters.grow_calls++;
#endif
	/*
	 * +------------+------------+--------------+------------+
	 * | table_size | split_slot | split_bit_no | cover_mask |
	 * +------------+------------+--------------+------------+
	 * |   1 -> 2   |      0     |       0      |   0 -> 1   |
	 * |   2 -> 3   |      0     |       1      |   1 -> 3   |
	 * |   3 -> 4   |      1     |       1      |      3     |
	 * |   4 -> 5   |      0     |       2      |   3 -> 7   |
	 * |   5 -> 6   |      1     |       2      |      7     |
	 * |   6 -> 7   |      2     |       2      |      7     |
	 * |   7 -> 8   |      3     |       2      |      7     |
	 * |   8 -> 9   |      0     |       3      |   7 -> 15  |
	 * +------------+------------+--------------+------------+
	 */
	uint32_t split_bit_no = 31 - __builtin_clz(ht->table_size);
	uint32_t split_slot = ht->table_size ^(1u << split_bit_no);

	struct LIGHT(cluster) *split_cluster = LIGHT(touch)(ht, split_slot);
	if (LIGHT_UNLIKELY(!split_cluster)) /* memory failure */
		return -1;
	__builtin_prefetch(split_cluster, 1);
#ifdef LIGHT_DEBUG
	ht->debug_counters.grow_lookups++;
#endif

	uint32_t new_slot;
	struct LIGHT(cluster) *new_cluster = LIGHT(alloc)(ht, &new_slot);
	if (LIGHT_UNLIKELY(!new_cluster)) /* memory failure */
		return -1;
	assert(new_slot == ht->table_size);
	new_cluster = LIGHT(touch)(ht, new_slot);
	if (LIGHT_UNLIKELY(!new_cluster)) { /* memory failure */
		LIGHT(dealloc)(ht);
		return -1;
	}
	__builtin_prefetch(new_cluster, 1);
#ifdef LIGHT_DEBUG
	ht->debug_counters.grow_lookups++;
#endif

	if (LIGHT_UNLIKELY(new_slot > ht->cover_mask))
		ht->cover_mask = (ht->cover_mask << 1) | 1;
	ht->table_size++;

	/*
	 * We must move present, but not chained records,
	 * with hash 1 bits at split_bit_no.
	 * Let's copy all and then turn off appropriate present bits
	 */
	*new_cluster = *split_cluster;
	uint64_t move_mask = LIGHT(cluster_true_mask)(split_cluster);
	move_mask &= LIGHT(cluster_hash_bits)(split_cluster, split_bit_no);
	move_mask <<= 8;
	split_cluster->bits[2] ^= move_mask;
	new_cluster->bits[2] &= ~LIGHT(bit_mask)(8);
	new_cluster->bits[2] |= move_mask;

	int result;
	if (LIGHT_UNLIKELY(!LIGHT(cluster_has_next)(split_cluster))) {
		/* That could be isolated or chain end */
		result = 0;
	} else if (LIGHT_UNLIKELY(LIGHT(cluster_has_chain)(split_cluster))) {
		/* Other's chain middle */
		result = LIGHT(cluster_is_full)(split_cluster) ? 0 :
			 LIGHT(compact_chain)(ht, split_cluster);
		LIGHT(cluster_clear_next)(new_cluster);
		/* on undo we just dealloc new_cluster */
	} else {
		/* Chain start */
		result = LIGHT(split_chain)(ht, split_bit_no,
					    split_slot, new_slot,
					    split_cluster, new_cluster);
	}

	if (LIGHT_UNLIKELY(result)) { /* memory failure */
		assert((move_mask >> 8) ==
		       LIGHT(cluster_present_mask)(new_cluster));
		LIGHT(cluster_move_hash_and_val_symmetric)(split_cluster,
							   new_cluster,
							   move_mask >> 8);
		LIGHT(dealloc)(ht);
		if (ht->cover_mask + 1 == new_slot * 2)
			ht->cover_mask >>= 1;
		ht->table_size--;
		return result;
	}

	return 0;
}

inline int
LIGHT(test_grow)(struct LIGHT(core) *ht)
{
	if (ht->table_size == 0) {
		ht->table_size = 1;
		ht->cover_mask = 0;
		uint32_t slot;
		struct LIGHT(cluster) *cluster = LIGHT(alloc)(ht, &slot);
		if (LIGHT_UNLIKELY(!cluster)) {
			ht->table_size = 0;
			return -1;
		}

		cluster->bits[0] = 0xFF80000000000000ull;
		cluster->bits[2] = 0ull;
		return 0;
	}
	if (ht->count >= LIGHT_GROW_FILLNESS * ht->table_size)
		return LIGHT(grow)(ht);
	return 0;
}

/**
 * light_insert helper
 * increments ht->count!
 */
inline int
LIGHT(cluster_insert)(struct LIGHT(core) *ht, struct LIGHT(cluster) *cluster,
		      uint32_t hash, LIGHT_DATA_TYPE data, uint64_t in_chain)
{
	uint64_t mask = LIGHT(cluster_free_mask)(cluster);
	int offset;
	uint32_t pos = LIGHT(grab_bit)(mask, &offset);
	LIGHT(cluster_set_hash_and_val)(cluster, pos, offset,
					hash, data, in_chain);
	ht->count++;
	return 0;
}

/**
 * light_insert helper
 * increments ht->count!
 */
inline int
LIGHT(cluster_touch_insert)(struct LIGHT(core) *ht, uint32_t slot,
			    uint32_t hash, LIGHT_DATA_TYPE data,
			    uint64_t in_chain)
{
	struct LIGHT(cluster) *cluster = LIGHT(touch)(ht, slot);
	if (LIGHT_UNLIKELY(!cluster))
		return -1; /* memory failure */
	return LIGHT(cluster_insert)(ht, cluster, hash, data, in_chain);
}

int
LIGHT(insert_tr)(struct LIGHT(core) *ht, uint32_t hash, LIGHT_DATA_TYPE data);

/**
 * light_insert helper
 */
inline int
LIGHT(insert_other_chain_mid)(struct LIGHT(core) *ht, uint32_t slot,
			      uint32_t hash, LIGHT_DATA_TYPE data)
{
	struct LIGHT(cluster) *cluster = LIGHT(touch)(ht, slot);
	if (LIGHT_UNLIKELY(!cluster))
		return -1; /* memory failure */

	uint32_t last_slot = LIGHT(cluster_get_next)(cluster);
	struct LIGHT(cluster) *last_cluster = LIGHT(cluster)(ht, last_slot);
#ifdef LIGHT_DEBUG
	ht->debug_counters.ins_lookups++;
#endif
	while (LIGHT(cluster_has_next)(last_cluster)) {
		last_slot = LIGHT(cluster_get_next)(last_cluster);
		last_cluster = LIGHT(cluster)(ht, last_slot);
#ifdef LIGHT_DEBUG
		ht->debug_counters.ins_lookups++;
#endif
	}
	last_cluster = LIGHT(touch)(ht, last_slot);
	if (LIGHT_UNLIKELY(!last_cluster))
		return -1; /* memory failure */

	uint32_t nice_slot = LIGHT(end);
	struct LIGHT(cluster) *nice_cluster = 0;
	uint64_t last_mask = LIGHT(cluster_free_mask)(last_cluster);
	if (!last_mask) {
		nice_slot = LIGHT(nice_cluster)(ht);
		if (nice_slot == LIGHT(end)) {
			if (LIGHT(grow)(ht))
				return -1; /* memory failure */
			return LIGHT(insert_tr)(ht, hash, data);
		}
		nice_cluster = LIGHT(touch)(ht, nice_slot);
#ifdef LIGHT_DEBUG
		ht->debug_counters.ins_lookups++;
#endif
		if (LIGHT_UNLIKELY(!nice_cluster))
			return -1; /* memory failure */
	}

	uint64_t chain_mask = LIGHT(cluster_chain_mask)(cluster);
	if (!(chain_mask & (chain_mask - 1))) {
		int offset = __builtin_ctzll(chain_mask);
		uint32_t origin_hash = LIGHT(cluster_get_hash)(cluster, offset);
		uint32_t origin_slot = LIGHT(slot)(ht, origin_hash);
		struct LIGHT(cluster) *origin_cluster;
		origin_cluster = LIGHT(cluster)(ht, origin_slot);
		uint32_t origin_next = LIGHT(cluster_get_next)(origin_cluster);
		while (origin_next != slot) {
			origin_slot = origin_next;
			origin_cluster = LIGHT(cluster)(ht, origin_slot);
#ifdef LIGHT_DEBUG
			ht->debug_counters.ins_lookups++;
#endif
			origin_next = LIGHT(cluster_get_next)(origin_cluster);
		}
		origin_cluster = LIGHT(touch)(ht, origin_slot);
		if (LIGHT_UNLIKELY(!origin_cluster))
			return -1; /* memory failure */
		LIGHT(cluster_copy_next)(origin_cluster, cluster);
		LIGHT(cluster_clear_next)(cluster);
	}
	if (!last_mask) {
		uint64_t free_mask = LIGHT(cluster_free_mask)(nice_cluster);
		LIGHT(cluster_move_hash_and_val_one)(nice_cluster, cluster,
						     &free_mask, &chain_mask);
		LIGHT(cluster_set_next)(last_cluster, nice_slot);
	} else {
		LIGHT(cluster_move_hash_and_val_one)(last_cluster, cluster,
						     &last_mask, &chain_mask);
	}
	return LIGHT(cluster_insert)(ht, cluster, hash, data, 0);
}

/**
 * light_insert helper
 */
inline int
LIGHT(insert_other_chain_end)(struct LIGHT(core) *ht, uint32_t slot,
			      uint32_t hash, LIGHT_DATA_TYPE data)
{
	struct LIGHT(cluster) *cluster = LIGHT(touch)(ht, slot);
	if (LIGHT_UNLIKELY(!cluster))
		return -1; /* memory failure */
	uint32_t nice_slot = LIGHT(nice_cluster)(ht);
	if (nice_slot == LIGHT(end)) {
		if (LIGHT(grow)(ht))
			return -1; /* memory failure */
		return LIGHT(insert_tr)(ht, hash, data);
	}
	struct LIGHT(cluster) *nice_cluster = LIGHT(touch)(ht, nice_slot);
	if (LIGHT_UNLIKELY(!nice_cluster))
		return -1; /* memory failure */
	uint64_t chain_mask = LIGHT(cluster_chain_mask)(cluster);
	uint64_t free_mask = LIGHT(cluster_free_mask)(nice_cluster);
	if (!(chain_mask & (chain_mask - 1))) {
		int offset = __builtin_ctzll(chain_mask);
		uint32_t origin_hash = LIGHT(cluster_get_hash)(cluster, offset);
		uint32_t origin_slot = LIGHT(slot)(ht, origin_hash);
		struct LIGHT(cluster) *origin_cluster;
		origin_cluster = LIGHT(cluster)(ht, origin_slot);
		uint32_t origin_next = LIGHT(cluster_get_next)(origin_cluster);
		while (origin_next != slot) {
			origin_slot = origin_next;
			origin_cluster = LIGHT(cluster)(ht, origin_slot);
			origin_next = LIGHT(cluster_get_next)(origin_cluster);
		}
		origin_cluster = LIGHT(touch)(ht, origin_slot);
		if (LIGHT_UNLIKELY(!origin_cluster))
			return -1; /* memory failure */
		LIGHT(cluster_set_next)(origin_cluster, nice_slot);
	} else {
		LIGHT(cluster_set_next)(cluster, nice_slot);
	}
	LIGHT(cluster_move_hash_and_val_one)(nice_cluster, cluster,
					     &free_mask, &chain_mask);
	return LIGHT(cluster_insert)(ht, cluster, hash, data, 0);
}

/**
 * light_insert helper
 */
inline int
LIGHT(insert_this_chain)(struct LIGHT(core) *ht,
			 uint32_t slot, struct LIGHT(cluster) *cluster,
			 uint32_t hash, LIGHT_DATA_TYPE data)
{
	while (LIGHT(cluster_has_next)(cluster)) {
		slot = LIGHT(cluster_get_next)(cluster);
		cluster = LIGHT(cluster)(ht, slot);
#ifdef LIGHT_DEBUG
		ht->debug_counters.ins_lookups++;
#endif
		if (LIGHT(cluster_free_mask)(cluster))
			return LIGHT(cluster_touch_insert)(ht, slot, hash,
							   data, 1);
	}

	cluster = LIGHT(touch)(ht, slot);
	if (LIGHT_UNLIKELY(!cluster))
		return -1;
	uint32_t nice_slot = LIGHT(nice_cluster)(ht);
	if (nice_slot == LIGHT(end)) {
		if (LIGHT(grow)(ht))
			return -1;
		return LIGHT(insert_tr)(ht, hash, data);
	}
	struct LIGHT(cluster) *nice_cluster = LIGHT(touch)(ht, nice_slot);
	if (LIGHT_UNLIKELY(!nice_cluster))
		return -1;
	LIGHT(cluster_set_next)(cluster, nice_slot);
	return LIGHT(cluster_insert)(ht, nice_cluster, hash, data, 1);
}

inline int
LIGHT(insert_tr)(struct LIGHT(core) *ht, uint32_t hash, LIGHT_DATA_TYPE data)
{
#ifdef LIGHT_DEBUG
	ht->debug_counters.ins_calls++;
#endif
	if (LIGHT_UNLIKELY(LIGHT(test_grow)(ht)))
		return -1;

	uint32_t slot = LIGHT(slot)(ht, hash);
	struct LIGHT(cluster) *cluster = LIGHT(touch)(ht, slot);
#ifdef LIGHT_DEBUG
	ht->debug_counters.ins_lookups++;
#endif
	if (LIGHT_LIKELY(!LIGHT(cluster_is_full)(cluster)))
		return LIGHT(cluster_insert)(ht, cluster, hash, data, 0);

	if (!LIGHT(cluster_full_has_chain)(cluster)) {
		/* this chain */
		return LIGHT(insert_this_chain)(ht, slot, cluster, hash, data);
	}
	/* other chain */
	if (LIGHT(cluster_has_next)(cluster))
		return LIGHT(insert_other_chain_mid)(ht, slot, hash, data);
	else
		return LIGHT(insert_other_chain_end)(ht, slot, hash, data);
}

inline int
LIGHT(insert)(struct LIGHT(core) *ht, uint32_t hash, LIGHT_DATA_TYPE data)
{
	hash = LIGHT_HASH_TRANSFORM(hash);
	return LIGHT(insert_tr)(ht, hash, data);
}

/**
 * delete_* helper
 */

inline int
LIGHT(delete_by_coords)(struct LIGHT(core) *ht,
			uint32_t prev_slot, uint32_t slot, uint32_t pos)
{
	struct LIGHT(cluster) *cluster = LIGHT(touch)(ht, slot);
	if (LIGHT_UNLIKELY(!cluster))
		return -1;
	assert((cluster->bits[2] & (1ull << (11 * pos + 8))));

	if (LIGHT(cluster_has_next)(cluster)) {
		uint32_t pre_last_slot = slot;
		uint32_t last_slot = LIGHT(cluster_get_next)(cluster);
		struct LIGHT(cluster) *last_cluster =
			LIGHT(cluster)(ht, last_slot);
#ifdef LIGHT_DEBUG
		ht->debug_counters.del_lookups++;
#endif
		while (LIGHT(cluster_has_next)(last_cluster)) {
			pre_last_slot = last_slot;
			last_slot = LIGHT(cluster_get_next)(last_cluster);
			last_cluster = LIGHT(cluster)(ht, last_slot);
#ifdef LIGHT_DEBUG
			ht->debug_counters.del_lookups++;
#endif
		}
		last_cluster = LIGHT(touch)(ht, last_slot);
		if (LIGHT_UNLIKELY(!last_cluster))
			return -1;
		uint64_t last_mask = LIGHT(cluster_chain_mask)(last_cluster);
		if ((last_mask & (last_mask - 1)) == 0) {
			struct LIGHT(cluster) *prev_cluster =
				LIGHT(touch)(ht, pre_last_slot);
			if (LIGHT_UNLIKELY(!prev_cluster))
				return -1;
			LIGHT(cluster_clear_next)(prev_cluster);
		}
		uint64_t was_chain_mask = LIGHT(cluster_chain_mask)(cluster);
		uint64_t mask = 1ull << (11 * pos);
		cluster->bits[2] ^= (mask << 8);
		uint64_t clr_mask = (mask << 9);
		LIGHT(cluster_move_hash_and_val_one)(cluster, last_cluster,
						     &mask, &last_mask);
		if (was_chain_mask == 0)
			cluster->bits[2] ^= clr_mask;
		assert(mask == 0);
	} else {
		uint64_t chain_mask = LIGHT(cluster_chain_mask)(cluster);
		chain_mask = chain_mask & (chain_mask - 1);
		if (prev_slot != slot && chain_mask == 0) {
			struct LIGHT(cluster) *prev_cluster =
				LIGHT(touch)(ht, prev_slot);
			if (LIGHT_UNLIKELY(!prev_cluster))
				return -1;
			LIGHT(cluster_clear_next)(prev_cluster);
		}
		cluster->bits[2] ^= 1ull << (11 * pos + 8);
	}
	ht->count--;
	return 0;
}

inline int
LIGHT(delete)(struct LIGHT(core) *ht, struct LIGHT(iterator) *itr)
{
#ifdef LIGHT_DEBUG
	ht->debug_counters.del_calls++;
#endif
	assert(itr->slot < ht->table_size);
	uint32_t slot = itr->slot;
	uint32_t pos = itr->pos;

	struct LIGHT(cluster) *cluster = LIGHT(cluster)(ht, slot);
#ifdef LIGHT_DEBUG
	ht->debug_counters.del_lookups++;
#endif
	int offset = 11 * pos;
	uint64_t bit = 1ull << offset;
	assert(cluster->bits[2] & (bit << 8));
	uint32_t prev_slot = slot;
	if (cluster->bits[2] & (bit << 9)) {
		uint32_t hash = LIGHT(cluster_get_hash)(cluster, offset);
		prev_slot = LIGHT(slot)(ht, hash);
		struct LIGHT(cluster) *prev_cluster = LIGHT(cluster)(ht,
								     prev_slot);
#ifdef LIGHT_DEBUG
		ht->debug_counters.del_lookups++;
#endif
		uint32_t prevs_next = LIGHT(cluster_get_next)(prev_cluster);
		while (prevs_next != slot) {
			prev_slot = prevs_next;
			prev_cluster = LIGHT(cluster)(ht, prev_slot);
#ifdef LIGHT_DEBUG
			ht->debug_counters.del_lookups++;
#endif
			prevs_next = LIGHT(cluster_get_next)(prev_cluster);
		}
	}
	return LIGHT(delete_by_coords)(ht, prev_slot, slot, pos);

}

inline uint32_t
LIGHT(selfcheck)(const struct LIGHT(core) *ht)
{
	/* test that 'next' slots are in 'table_size' range */
	uint32_t res = 0;
	uint32_t total_count = 0;
	for (uint32_t slot = 0; slot < ht->table_size; slot++) {
		struct LIGHT(cluster) *cluster = LIGHT(cluster)(ht, slot);
		total_count += LIGHT(cluster_val_count)(cluster);
		uint64_t present = LIGHT(cluster_present_mask)(cluster);
		uint64_t chain = LIGHT(cluster_chain_mask)(cluster);
		uint64_t true_mask = present ^chain;
		while (true_mask) {
			int pos = LIGHT(grab_bit_exclude)(&true_mask);
			int offs = pos * 11;
			uint32_t hash = LIGHT(cluster_get_hash)(cluster, offs);
			uint32_t true_slot = LIGHT(slot)(ht, hash);
			if (slot != true_slot)
				res |= 1 << 0;
		}
		true_mask = present ^ chain;
		if (LIGHT(cluster_has_next)(cluster) &&
		    LIGHT(cluster_val_count)(cluster) != 5)
			res |= 1 << 1;

		uint32_t any_true_slot = LIGHT(end);
		while (chain) {
			int pos = LIGHT(grab_bit_exclude)(&chain);
			int offs = pos * 11;
			uint32_t hash = LIGHT(cluster_get_hash)(cluster, offs);
			uint32_t true_slot = LIGHT(slot)(ht, hash);
			if (true_slot == slot)
				res |= 1 << 2;
			if (any_true_slot == LIGHT(end)) {
				any_true_slot = true_slot;
			} else if (any_true_slot != true_slot) {
				res |= 1 << 3;
			}
			struct LIGHT(cluster) *origin;
			origin = LIGHT(cluster)(ht, true_slot);
			if (LIGHT(cluster_chain_mask)(origin) ||
			    LIGHT(cluster_val_count)(origin) != 5)
				res |= 1 << 4;
			bool origin_lead_to = false;
			uint32_t rounds = 0;
			while (LIGHT(cluster_has_next)(origin)) {
				uint32_t next = LIGHT(cluster_get_next)(origin);
				if (next == slot) {
					origin_lead_to = true;
					break;
				}
				origin = LIGHT(cluster)(ht, next);
				if (rounds++ > ht->table_size) {
					res |= 1 << 5;
					break;
				}
			}
			if (!origin_lead_to)
				res |= 1 << 6;
		}
		chain = LIGHT(cluster_chain_mask)(cluster);

		if (true_mask == LIGHT(bit_mask)(0)) {
			struct LIGHT(cluster) *test = cluster;
			uint32_t rounds = 0;
			while (LIGHT(cluster_has_next)(test)) {
				uint32_t next_slot;
				next_slot = LIGHT(cluster_get_next)(test);
				if (next_slot >= ht->table_size)
					res |= 1 << 8;
				struct LIGHT(cluster) *next;
				next = LIGHT(cluster)(ht, next_slot);
				if (!LIGHT(cluster_chain_mask)(next))
					res |= 1 << 9;
				uint64_t chain_mask;
				chain_mask = LIGHT(cluster_chain_mask)(next);
				bool found = false;
				while (chain_mask) {
					int pos =
						LIGHT(grab_bit_exclude)(
							&chain_mask);
					int offs = pos * 11;
					uint32_t hash;
					hash = LIGHT(cluster_get_hash)(next,
								       offs);
					uint32_t true_slot = LIGHT(slot)(ht,
									 hash);
					if (true_slot == slot)
						found = true;
					else
						res |= 1 << 10;
				}
				if (!found)
					res |= 1 << 11;
				test = next;
				if (rounds++ > ht->table_size) {
					res |= 1 << 12;
					break;
				}
			}
		}
	}


	if (ht->table_size != ht->mtable.head.block_count)
		res |= 1 << 14;

	if (ht->count != total_count)
		res |= 1 << 15;

	uint32_t cover = ht->cover_mask + 1;
	if (ht->cover_mask & cover)
		res |= 1 << 16;

	if (ht->table_size && cover < ht->table_size)
		res |= 1 << 17;
	if (ht->table_size && cover / 2 >= ht->table_size)
		res |= 1 << 18;
	return res;
}

#undef LIGHT
#undef LIGHT_LIKELY
#undef LIGHT_UNLIKELY
#undef LIGHT_GROW_FILLNESS
#undef LIGHT_CLEAN_ATTEMPTS
#undef LIGHT_NICE_FILLNESS
#undef LIGHT_HASH_TRANSFORM
