#include <algorithm>
#include <set>
#include <vector>
#include <iostream>

#include "unit.h"

#include "salad/heap.h"

using namespace std;

const uint32_t max_size = 100;
const uint32_t rounds = 100;
uint32_t size;
const uint32_t stress_size = 1 * 1024 * 1024;
uint32_t vals[max_size];
uint32_t idxs[stress_size]; /* stress_size > max_size */
uint32_t sorted[max_size];

static void
prn_vals()
{
	if (!size)
		printf("<no data>\n");
	printf("[%u", vals[0]);
	for (uint32_t i = 1; i < size; i++)
		printf(", %u", vals[i]);
	printf("]\n");
}

#define die(message) do { \
 	prn_vals(); \
	printf("fail: %s on %s:%d\n", (message), __FILE__, __LINE__); \
	exit(0); \
	} while (0)
#define check(expr, message) if (!(expr)) die(message)

static void
test_heap_push_pop_visual()
{
	header();

	struct heap h;
	heap_create(&h);
	std::cout << "rand   ";
	uint32_t testvals[10] = {12, 50, 11, 16, 0, 1, 2, 42, 41, 40};
	for (uint32_t i = 0; i < 10; i++) {
		vals[i] = testvals[i];
		std::cout << (i ? ", " : "") << vals[i];
		heap_push(&h, vals + i, vals[i], idxs + i);
	}
	std::cout << std::endl;
	std::cout << "sorted ";
	for (uint32_t i = 0; i < 10; i++) {
		uint32_t *v = (uint32_t *)heap_max(&h);
		heap_pop(&h);
		std::cout << (i ? ", " : "") << *v;
	}
	std::cout << std::endl;
	heap_destroy(&h);

	footer();
}

static void
test_heap_build_visual()
{
	header();

	struct heap h;
	heap_create(&h);
	std::cout << "rand   ";
	uint32_t testvals[10] = {12, 50, 11, 16, 0, 1, 2, 42, 41, 40};
	for (uint32_t i = 0; i < 10; i++) {
		vals[i] = testvals[i];
		std::cout << (i ? ", " : "") << vals[i];
		heap_bulk_add(&h, vals + i, vals[i], idxs + i);
	}
	std::cout << std::endl;
	heap_build(&h);
	std::cout << "sorted ";
	for (uint32_t i = 0; i < 10; i++) {
		uint32_t *v = (uint32_t *)heap_max(&h);
		heap_pop(&h);
		std::cout << (i ? ", " : "") << *v;
	}
	std::cout << std::endl;
	heap_destroy(&h);

	footer();
}

static void
test_heap_stupid()
{
	header();

	struct heap h;
	heap_create(&h);
	check(heap_self_check(&h) == 0, "self check");
	heap_build(&h);
	check(heap_self_check(&h) == 0, "self check");
	heap_destroy(&h);

	footer();
}

static void
test_heap_clear()
{
	header();

	struct heap h;
	heap_create(&h);
	for (uint32_t i = 0; i < size; i++) {
		vals[i] = (uint32_t)rand();
		heap_push(&h, vals + i, vals[i], idxs + i);
	}
	heap_clear(&h);
	check(heap_self_check(&h) == 0, "self check");
	check(h.count == 0, "wrong size");
	for (uint32_t i = 0; i < size; i++) {
		vals[i] = (uint32_t)rand();
		heap_bulk_add(&h, vals + i, vals[i], idxs + i);
	}
	heap_clear(&h);
	check(heap_self_check(&h) == 0, "self check");
	check(h.count == 0, "wrong size");
	heap_destroy(&h);

	footer();
}

static void
test_heap_push_pop()
{
	header();

	for (size = 1; size < max_size; size++) {
		struct heap h;
		heap_create(&h);
		for (uint32_t r = 0; r < rounds; r++) {
			for(uint32_t i = 0; i < size; i++) {
				vals[i] = (uint32_t)rand();
				sorted[i] = vals[i];
				heap_push(&h, vals + i, vals[i], idxs + i);
				check(heap_self_check(&h) == 0, "self check");
			}
			std::sort(sorted, sorted + size);
			std::reverse(sorted, sorted + size);
			for(uint32_t i = 0; i < size; i++) {
				check(h.count == size - i, "wrong size");
				uint32_t p;
				uint32_t *v = (uint32_t *)heap_maxp(&h, &p);
				check(v == (uint32_t *)heap_max(&h), "wtf");
				check(*v == p, "broken data");
				check(p == sorted[i], "wrong order");
				heap_pop(&h);
				check(heap_self_check(&h) == 0, "self check");
			}
			check(h.count == 0, "wrong size");
		}
		heap_destroy(&h);
	}

	footer();
}

static void
test_heap_build()
{
	header();

	for (size = 1; size < max_size; size++) {
		struct heap h;
		heap_create(&h);
		for (uint32_t r = 0; r < rounds; r++) {
			for(uint32_t i = 0; i < size; i++) {
				vals[i] = (uint32_t)rand();
				sorted[i] = vals[i];
				heap_bulk_add(&h, vals + i, vals[i], idxs + i);
			}
			heap_build(&h);
			check(heap_self_check(&h) == 0, "self check");
			std::sort(sorted, sorted + size);
			std::reverse(sorted, sorted + size);
			for(uint32_t i = 0; i < size; i++) {
				check(h.count == size - i, "wrong size");
				uint32_t p;
				uint32_t *v = (uint32_t *)heap_maxp(&h, &p);
				check(v == (uint32_t *)heap_max(&h), "wtf");
				check(*v == p, "broken data");
				check(p == sorted[i], "wrong order");
				heap_pop(&h);
				check(heap_self_check(&h) == 0, "self check");
			}
			check(h.count == 0, "wrong size");
		}
		heap_destroy(&h);
	}

	footer();
}

static void
test_heap_delete()
{
	header();

	for (size = 1; size < max_size; size++) {
		struct heap h;
		heap_create(&h);
		for (uint32_t r = 0; r < rounds; r++) {
			std::set<uint32_t> s;
			for (uint32_t i = 0; i < size; i++) {
				vals[i] = (uint32_t) rand();
				heap_push(&h, vals + i, vals[i], idxs + i);
				s.insert(vals[i]);
				check(heap_self_check(&h) == 0, "self check");
			}
			uint32_t to_delete = (uint32_t) rand() % size + 1;
			uint32_t size_left = size;
			for (uint32_t i = 0; i < to_delete; i++) {
				uint32_t del_pos =
					(uint32_t) rand() % size_left;
				s.erase(h.records[del_pos].prior);
				heap_remove(&h, del_pos);
				size_left--;
				check(size_left == h.count, "wrong size");
				check(size_left == (uint32_t) s.size(),
				      "wrong set size");
				check(heap_self_check(&h) == 0, "self check");
			}
			for (uint32_t i = 0; i < size_left; i++) {
				check(h.count == size_left - i, "wrong size");
				uint32_t p;
				uint32_t *v = (uint32_t *) heap_maxp(&h, &p);
				check(v == (uint32_t *) heap_max(&h), "wtf");
				check(*v == p, "broken data");
				check(p == *s.rbegin(), "wrong order");
				s.erase(p);
				heap_pop(&h);
				check(heap_self_check(&h) == 0, "self check");
			}
			check(h.count == 0, "wrong size");
		}
		heap_destroy(&h);
	}

	footer();
}

#if 0
void
test_heap_stress()
{
	{
		struct heap h;
		heap_create(&h);
		CTimer t;
		double Mrps;

		t.Start();
		for (uint32_t i = 0; i < stress_size; i++) {
			uint32_t val = (uint32_t)rand();
			heap_push(&h, 0, val, idxs + i);
		}
		t.Stop();
		Mrps = (double)stress_size / t.ElapsedMicrosec();
		COUT("Pushing ", stress_size, ": ", t.ElapsedMilliSec(), " "
			"ms, ", Mrps, " Mrps");

		t.Start();
		for (uint32_t i = 0; i < stress_size; i++) {
			heap_pop(&h);
		}
		t.Stop();
		Mrps = (double)stress_size / t.ElapsedMicrosec();
		COUT("Poping ", stress_size, ": ", t.ElapsedMilliSec(), " "
			"ms, ", Mrps, " Mrps");


		heap_destroy(&h);
	}
	{
		struct heap h;
		heap_create(&h);
		CTimer t;
		double Mrps;

		t.Start();
		for (uint32_t i = 0; i < stress_size; i++) {
			uint32_t val = (uint32_t)rand();
			heap_bulk_add(&h, 0, val, idxs + i);
		}
		t.Stop();
		Mrps = (double)stress_size / t.ElapsedMicrosec();
		COUT("Adding ", stress_size, ": ", t.ElapsedMilliSec(), " "
			"ms, ", Mrps, " Mrps");

		t.Start();
		heap_build(&h);
		t.Stop();
		Mrps = (double)stress_size / t.ElapsedMicrosec();
		COUT("Buildin ", stress_size, ": ", t.ElapsedMilliSec(), " "
			"ms, ", Mrps, " Mrps");

		t.Start();
		for (uint32_t i = 0; i < stress_size; i++) {
			heap_pop(&h);
		}
		t.Stop();
		Mrps = (double)stress_size / t.ElapsedMicrosec();
		COUT("Poping ", stress_size, ": ", t.ElapsedMilliSec(), " "
			"ms, ", Mrps, " Mrps");


		heap_destroy(&h);
	}
	{
		vector<uint32_t> v;
		CTimer t;
		double Mrps;

		t.Start();
		for (uint32_t i = 0; i < stress_size; i++) {
			uint32_t val = (uint32_t)rand();
			v.push_back(val);
		}
		t.Stop();
		Mrps = (double)stress_size / t.ElapsedMicrosec();
		COUT("std Adding ", stress_size, ": ", t.ElapsedMilliSec(),
		     " ms, ", Mrps, " Mrps");

		t.Start();
		make_heap(v.begin(), v.end());
		t.Stop();
		Mrps = (double)stress_size / t.ElapsedMicrosec();
		COUT("std Buildin ", stress_size, ": ", t.ElapsedMilliSec(), " "
			" ms, ", Mrps, " Mrps");

		t.Start();
		for (uint32_t i = 0; i < stress_size; i++) {
			pop_heap(v.begin(), v.end() - i);
		}
		t.Stop();
		Mrps = (double)stress_size / t.ElapsedMicrosec();
		COUT("std Poping ", stress_size, ": ", t.ElapsedMilliSec(), " "
			" ms, ", Mrps, " Mrps");
	}
	footer();
}
#endif

int
main(int, const char **)
{
	test_heap_push_pop_visual();
	test_heap_build_visual();
	test_heap_stupid();
	test_heap_clear();
	test_heap_push_pop();
	test_heap_build();
	test_heap_delete();
}

