#include <iostream>
#include "small/salloc.h"

#include <string.h>
#include "small/salloc.h"

#include "unit.h"

using namespace std;

enum {
	UI32S = sizeof(uint32_t)
};


struct test_run_allocer {
	uintptr_t size;
	uintptr_t mmap_size;
	char *mmap_cur;
	char *mmap_end;
	void *freelist;
	void *mmap_list;
	size_t alloc_count;
};

inline void *
test_run_alloc(struct test_run_allocer *tra)
{
	tra->alloc_count++;
	if (tra->freelist) {
		void *res = tra->freelist;
		tra->freelist = *(void **)tra->freelist;
		return res;
	}
	if (!tra->mmap_cur || tra->mmap_cur + tra->size > tra->mmap_end) {
		char *m = (char *)malloc(tra->mmap_size);
		tra->mmap_end = m + tra->mmap_size;
		*(void **)m = tra->mmap_list;
		tra->mmap_list = m;
		m += sizeof(void *);
		uintptr_t aligner = (uintptr_t)m;
		uintptr_t align_mask = tra->size - 1;
		aligner = (aligner + align_mask) & ~align_mask;
		tra->mmap_cur = (char *)aligner;
		assert(tra->mmap_cur + tra->size <= tra->mmap_end);
	}
	void *res = tra->mmap_cur;
	tra->mmap_cur += tra->size;
	return res;
}

inline void
test_run_free(struct test_run_allocer *tra, void *data)
{
	tra->alloc_count--;
	*(void **)data = tra->freelist;
	tra->freelist = data;
}

inline void
test_run_alloc_die(struct test_run_allocer *tra)
{
	void *ptr = tra->mmap_list;
	while (ptr) {
		void *tmp = ptr;
		ptr = *(void **)ptr;
		free(tmp);
	}
}

static const size_t test_slab_size = 16 * 1024;
static const size_t test_slab_mmap_size = 16 * 1024 * 1024;
static const size_t test_matras_ext_size = 16 * 1024;
static const size_t test_matras_ext_mmap_size = 16 * 1024 * 1024;

static const uint32_t min_alloc_size = 16;
static const uint32_t max_alloc_size = 256;
static const uint32_t test_alloc_count = 32 * 1024;
static const uint32_t test_alloc_test_rounds = 8 * test_alloc_count;


static struct test_run_allocer slab_allocer = {test_slab_size,
					       test_slab_mmap_size,
					       0, 0, 0, 0, 0};
static struct test_run_allocer mext_allocer = {test_matras_ext_size,
					       test_matras_ext_mmap_size,
					       0, 0, 0, 0, 0};

void *slab_alloc_ctx = (void *)&slab_allocer;

struct slab *slab_alloc(void *slab_alloc_ctx)
{
	struct test_run_allocer *allocer =
		(struct test_run_allocer *) slab_alloc_ctx;
	return (struct slab *)test_run_alloc(allocer);
}

void slab_free(struct slab *slab, void *slab_alloc_ctx)
{
	struct test_run_allocer *allocer =
		(struct test_run_allocer *) slab_alloc_ctx;
	test_run_free(allocer, slab);
}

void *
mextent_alloc()
{
	return test_run_alloc(&mext_allocer);
}

void
mextent_free(void *ext)
{
	test_run_free(&mext_allocer, ext);
}

size_t
get_allocation_size(void *data)
{
	return (size_t)*(uint32_t *)data;
}


void
set_allocation_size(void *data, size_t size)
{
	*(uint32_t *)data = (uint32_t)size;
	assert((size_t)*(uint32_t *)data == size);
}


struct testalloc_data {
	uint32_t id;
	uint32_t size;
	void *alr_ptr;
};

struct testalloc_data testalloc_data_arr[test_alloc_count];

#if 0
struct debug_entry {
	char *ptr;
	size_t size;
	bool operator <(const struct debug_entry &other) const {
		return ptr < other.ptr;
	}
};

typedef set<struct debug_entry> debug_set_t;
debug_set_t debug_set;

template <class T>
bool
debug_set_check(T *tptr, size_t size)
{
	if (debug_set.empty())
		return true;
	char *ptr = (char *)tptr;
	struct debug_entry entry = {ptr, size};
	debug_set_t::iterator itr = debug_set.lower_bound(entry);
	if (itr != debug_set.end()) {
		char *aptr = itr->ptr;
		assert(aptr >= ptr);
		if (aptr < ptr + size) {
			assert(false);
			return false;
		}
		if (itr == debug_set.begin())
			return true;
		--itr;
		if (itr == debug_set.end())
			return true;
		aptr = itr->ptr;
		size_t asize = itr->size;
		assert(aptr < ptr);
		if (aptr + asize >= ptr) {
			assert(false);
			return false;
		}
	} else {
		debug_set_t::reverse_iterator itr
			= debug_set.rbegin();
		char *aptr = itr->ptr;
		size_t asize = itr->size;
		assert(aptr < ptr);
		if (aptr + asize >= ptr) {
			assert(false);
			return false;
		}
	}
	return true;
}

template <class T>
bool
debug_set_add(T *tptr, size_t size)
{
	if (!debug_set_check(tptr, size))
		return false;
	char *ptr = (char *)tptr;
	struct debug_entry entry = {ptr, size};
	debug_set.insert(entry);
	return true;
};

template <class T>
bool
debug_set_remove(T *tptr, size_t size)
{
	char *ptr = (char *)tptr;
	struct debug_entry entry = {ptr, size};
	debug_set_t::iterator itr = debug_set.find(entry);
	if (itr == debug_set.end()) {
		assert(false);
		return false;
	}
	assert(itr->size == size);
	debug_set.erase(itr);
	return true;
};
#endif

size_t check_used;

size_t
get_total(struct salloc *f)
{
	size_t used = f->used_size;
	size_t frag = f->fragmented_size;
	size_t waste = f->wasted_size;
	size_t sys = f->system_size;
	size_t res = f->reserved_size;
	size_t total = used + frag + waste + sys + res;
	return total;
}

bool outed = false;
#define OUT(thing) do { \
		if (outed) \
			cout << ", "; \
		outed = true; \
		cout << #thing << " = " << thing; \
	} while(0)
#define END do { outed = false; cout << endl; } while (0)

#

void
print_stats(struct salloc *f)
{
	size_t used = f->used_size;
	size_t frag = f->fragmented_size;
	size_t waste = f->wasted_size;
	size_t sys = f->system_size;
	size_t res = f->reserved_size;
	size_t total = used + frag + waste + sys + res;
	size_t check_total = slab_allocer.alloc_count * test_slab_size;

	OUT(used); OUT(frag); OUT(waste);
	OUT(sys); OUT(res); OUT(total);
	END;

	OUT(check_used); OUT(check_total); END;
	double pused = used * 100 / total;
	double pfrag = frag * 100 / total;
	double pwaste = waste * 100 / total;
	double psys = sys * 100 / total;
	double pres = res * 100 / total;

	OUT(pused); OUT(pfrag); OUT(pwaste);
	OUT(psys); OUT(pres);
	END;
}

void *
set_data_arr(struct salloc *f, uint32_t i)
{
	uint32_t size = min_alloc_size + rand() % (max_alloc_size -
						   min_alloc_size);
	check_used += size;
	testalloc_data_arr[i].size = size;
	void *ptr = salloc(f, size, &testalloc_data_arr[i].id);
	set_allocation_size(ptr, size);
	for (uint32_t j = UI32S; j < size; j++)
		((char *)ptr)[j] = (char)rand();
	assert(get_allocation_size(ptr) == size);
	testalloc_data_arr[i].alr_ptr = malloc(size);
	memcpy(testalloc_data_arr[i].alr_ptr, ptr, size);
	return ptr;
}

void
del_data_arr(struct salloc *f, uint32_t i, void *ptr)
{
	sfree(f, ptr, testalloc_data_arr[i].id, testalloc_data_arr[i].size);
	free(testalloc_data_arr[i].alr_ptr);
	check_used -= testalloc_data_arr[i].size;
}

void
check_data_arr(struct salloc *f, uint32_t i, void *ptr)
{
	if (salloc_data_by_id(f, testalloc_data_arr[i].id) != ptr) {
		cout << "testalloc_data_arr failed " << i << endl;
	}
	if (salloc_id_by_data(f, ptr) != testalloc_data_arr[i].id) {
		cout << "salloc_id_by_data failed " << i << endl;
	}
	if (memcmp(testalloc_data_arr[i].alr_ptr, ptr,
		   testalloc_data_arr[i].size)) {
		cout << "cmp failed " << i << endl;
	}
}

void
test_salloc()
{
	header();

	struct salloc f;
	salloc_create(&f, test_slab_size, slab_alloc_ctx, slab_alloc,
		      slab_free, get_allocation_size, test_matras_ext_size,
		      mextent_alloc, mextent_free);
	for (uint32_t i = 0; i < test_alloc_count; i++) {
		void *ptr = set_data_arr(&f, i);
		check_data_arr(&f, i, ptr);
	}
	for (uint32_t k = 0; k < test_alloc_test_rounds; k++) {
#if 0
		if (k % test_alloc_count == 0)
			print_stats(&f);
#endif
		uint32_t i = rand() % test_alloc_count;
		void *ptr = salloc_data_by_id(&f, testalloc_data_arr[i].id);
		check_data_arr(&f, i, ptr);
		del_data_arr(&f, i, ptr);
		ptr = set_data_arr(&f, i);
		check_data_arr(&f, i, ptr);
	}
#if 0
	print_stats(&f);
#endif
	for (uint32_t i = 0; i < test_alloc_count; i++) {
		void *ptr = salloc_data_by_id(&f, testalloc_data_arr[i].id);
		check_data_arr(&f, i, ptr);
		del_data_arr(&f, i, ptr);
	}
#if 0
	print_stats(&f);
#endif
	salloc_destroy(&f);
	OUT(slab_allocer.alloc_count); OUT(mext_allocer.alloc_count); END;

	footer();
}

#if 0
void
test_perf_salloc()
{
	CTimer t(true);
	struct salloc f;
	salloc_create(&f, test_slab_size, slab_alloc_ctx, slab_alloc,
		      slab_free, get_allocation_size, test_matras_ext_size,
		      mextent_alloc, mextent_free);
	for (uint32_t i = 0; i < test_alloc_count; i++) {
		uint32_t size = min_alloc_size + rand() % (max_alloc_size -
							   min_alloc_size);
		testalloc_data_arr[i].size = size;
		void *ptr = salloc(&f, size, &testalloc_data_arr[i].id);
		set_allocation_size(ptr, size);
		memset(((char *)ptr) + UI32S, 0, size - UI32S);
	}
	for (uint32_t k = 0; k < test_alloc_test_rounds; k++) {
		uint32_t i = rand() % test_alloc_count;
		uint32_t size = min_alloc_size + rand() % (max_alloc_size -
							   min_alloc_size);
		void *ptr = salloc_data_by_id(&f, testalloc_data_arr[i].id);
		sfree(&f, ptr, testalloc_data_arr[i].id,
		      testalloc_data_arr[i].size);
		testalloc_data_arr[i].size = size;
		ptr = salloc(&f, size, &testalloc_data_arr[i].id);
		set_allocation_size(ptr, size);
		memset(((char *)ptr) + UI32S, 0, size - UI32S);
	}
	for (uint32_t i = 0; i < test_alloc_count; i++) {
		void *ptr = salloc_data_by_id(&f, testalloc_data_arr[i].id);
		sfree(&f, ptr, testalloc_data_arr[i].id,
		      testalloc_data_arr[i].size);
	}
	t.Stop();
	size_t total_op = test_alloc_test_rounds + test_alloc_count;
	COUTF(t.Mrps(total_op));
	salloc_destroy(&f);
	COUTF(slab_allocer.alloc_count, mext_allocer.alloc_count);

	COUTF(f.allow_memmove_factor, min_alloc_size, max_alloc_size);
}

void
test_perf_malloc()
{
	CTimer t(true);
	for (uint32_t i = 0; i < test_alloc_count; i++) {
		uint32_t size = min_alloc_size + rand() % (max_alloc_size -
							   min_alloc_size);
		testalloc_data_arr[i].size = size;
		testalloc_data_arr[i].alr_ptr = malloc(size);
		memset(testalloc_data_arr[i].alr_ptr, 0, size);
	}
	for (uint32_t k = 0; k < test_alloc_test_rounds; k++) {
		uint32_t i = rand() % test_alloc_count;
		uint32_t size = min_alloc_size + rand() % (max_alloc_size -
							   min_alloc_size);
		testalloc_data_arr[i].size = size;
		free(testalloc_data_arr[i].alr_ptr);
		testalloc_data_arr[i].alr_ptr = malloc(size);
		memset(testalloc_data_arr[i].alr_ptr, 0, size);
	}
	for (uint32_t i = 0; i < test_alloc_count; i++) {
		free(testalloc_data_arr[i].alr_ptr);
	}
	t.Stop();
	size_t total_op = test_alloc_test_rounds + test_alloc_count;
	COUTF("MALLOC", t.Mrps(total_op));
}
#endif

int main()
{
	test_salloc();
	#if 0
	test_perf_salloc();
	test_perf_malloc();
	#endif

	test_run_alloc_die(&slab_allocer);
	test_run_alloc_die(&mext_allocer);
	return 0;
}