#include <stdio.h>

#include "fiber.h"

namespace v8 {
namespace internal {

const int kMaxInt = 0x7FFFFFFF;
const int kMinInt = -kMaxInt - 1;

namespace Thread { /* same as class Thread with static methods */

enum LocalStorageKey {
	LOCAL_STORAGE_KEY_MIN_VALUE = kMinInt,
	LOCAL_STORAGE_KEY_MAX_VALUE = kMaxInt
};

LocalStorageKey
CreateThreadLocalKey()
{
	size_t fiber_key;

	int result = fiber_create_local(&fiber_key, NULL);
	assert(result == 0);

	fprintf(stderr, "overloaded CreateThreadLocalKey: %zu\n", fiber_key);

	return static_cast<LocalStorageKey>(fiber_key);
}

void
DeleteThreadLocalKey(LocalStorageKey key)
{
	size_t fiber_key = static_cast<size_t>(key);

	fprintf(stderr, "overloaded DeleteThreadLocalKey: %zu\n", fiber_key);

	int result = fiber_delete_local(fiber_key);
	assert(result == 0);
}

#include <stdio.h>

void *
GetThreadLocal(LocalStorageKey key)
{
	if (fiber == NULL || fiber->fid < 1)
		return NULL;

	V8_THREAD_LOCAL_HACK_WORKS = true;

	size_t fiber_key = static_cast<size_t>(key);
	return fiber_get_local(fiber, fiber_key);
}

void
SetThreadLocal(LocalStorageKey key, void *value)
{
	if (fiber == NULL || fiber->fid < 1)
		return;

	size_t fiber_key = static_cast<size_t>(key);
	fprintf(stderr, "SetThreadLocal: %zu %p\n", fiber_key, value);
	fiber_set_local(fiber, fiber_key, value);
}

int
GetThreadLocalInt(LocalStorageKey key) {
	return static_cast<int>(reinterpret_cast<intptr_t>(GetThreadLocal(key)));
}

void
SetThreadLocalInt(LocalStorageKey key, int value) {
	SetThreadLocal(key, reinterpret_cast<void*>(static_cast<intptr_t>(value)));
}

bool
HasThreadLocal(LocalStorageKey key) {
	return GetThreadLocal(key) != NULL;
}

} /* namespace Thread */
} /* namespace internal */
} /* namespace v8 */
