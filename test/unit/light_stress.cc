#include <stdint.h>
#include <time.h>

#include "unit.h"

/* Settings: */
/* average number of records in hash table */
const size_t record_count = 128 * 1024;
/* number of iteraions in test, should be greater than record_count */
const size_t round_count = record_count * 10;

typedef uint64_t hash_value_t;
typedef uint32_t hash_t;

static const size_t light_extent_size = 16 * 1024;
static size_t extents_count = 0;

hash_t
hash(hash_value_t value)
{
	return (hash_t) value;
}

bool
equal(hash_value_t v1, hash_value_t v2)
{
	return v1 == v2;
}

bool
equal_key(hash_value_t v1, hash_value_t v2)
{
	return v1 == v2;
}

#define LIGHT_NAME
#define LIGHT_DATA_TYPE uint64_t
#define LIGHT_KEY_TYPE uint64_t
#define LIGHT_CMP_ARG_TYPE int
#define LIGHT_EQUAL(a, b, arg) equal(a, b)
#define LIGHT_EQUAL_KEY(a, b, arg) equal_key(a, b)
#include "salad/light.h"

inline void *
my_light_alloc()
{
	extents_count++;
	return malloc(light_extent_size);
}

inline void
my_light_free(void *p)
{
	extents_count--;
	free(p);
}

static double test_time()
{
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return ts.tv_sec + ts.tv_nsec * 1e-9;
}

static void
stress_test()
{
	header();

	struct light_core ht;
	light_create(&ht, light_extent_size, my_light_alloc, my_light_free, 0);
	uint64_t value_limit = record_count * 2;

	double start = test_time();

	for(size_t r = 0; r < round_count; r++) {
		hash_value_t val = rand() % value_limit;
		hash_t h = hash(val);
		hash_t fnd = light_find(&ht, h, val);
		bool has = fnd != light_end;
		if (has) {
			light_delete(&ht, fnd);
		} else {
			light_insert(&ht, h, val);
		}
	}

	double end = test_time();
	double diff = end - start;
	double Mrps = round_count / diff / 1000000;
	const char *log_name = "light_stress.log";
	double MB = light_extent_size * extents_count * 1e-6;

	FILE *log = fopen(log_name, "w+");
#ifndef NDEBUG
	fprintf(log, "debug build\n");
#endif
	fprintf(log, "record_count: %ld\n", (long)record_count);
	fprintf(log, "round_count: %ld\n", (long)round_count);
	fprintf(log, "time spent: %lfs\n", diff);
	fprintf(log, "Mrps: %lf\n", Mrps);
	fprintf(log, "Memory spent %lf MB\n", MB);
	fclose(log);
	printf("test result are saved in %s\n", log_name);

	light_destroy(&ht);

	footer();
}

int
main(int, const char**)
{
	srand(0);
	stress_test();
	if (extents_count != 0)
		fail("memory leak!", "true");
}
