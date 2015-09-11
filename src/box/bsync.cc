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
#define MH_SOURCE 1
#include "box/bsync.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#include "cfg.h"
#include "fio.h"
#include "coio.h"
#include "coio_buf.h"
#include "memory.h"
#include "scoped_guard.h"
#include "box/box.h"
#include "box/txn.h"
#include "box/port.h"
#include "box/schema.h"
#include "box/space.h"
#include "box/tuple.h"
#include "box/request.h"
#include "box/replica.h"
#include "msgpuck/msgpuck.h"

#include "box/bsync_hash.h"
#include "box/iproto_constants.h"

#define BSYNC_MAX_HOSTS VCLOCK_MAX
#define BSYNC_PAIR_TIMEOUT 30

static void* bsync_thread(void*);
static void bsync_process_fiber(va_list ap);
static void bsync_start_election();
static void bsync_disconnected(uint8_t host_id);
static void bsync_check_consensus(uint8_t host_id);
static void bsync_process(struct bsync_txn_info *info);
static void bsync_process_wal(struct bsync_txn_info *info);
static void txn_process(struct bsync_txn_info *info);
static void bsync_update_local(struct bsync_txn_info *info);
static void bsync_start_connect(struct bsync_txn_info *info);
static void bsync_process_connect(struct bsync_txn_info *info);
static void bsync_process_disconnect(struct bsync_txn_info *info);
static void bsync_start_event(struct bsync_txn_info *info);
static void bsync_process_follow(struct bsync_txn_info *info);
static void bsync_set_follow(struct bsync_txn_info *info);
static void bsync_do_reject(uint8_t host_id, struct bsync_send_elem *info);

static struct recovery_state *local_state;

static struct ev_loop* txn_loop;
static struct ev_loop* bsync_loop;
static struct ev_async txn_process_event;
static struct ev_async bsync_process_event;

struct bsync_send_elem {/* for save in host queue */
	bool system;
	uint8_t code;
	void *arg;

	struct rlist list;
};

struct bsync_region {
	struct region pool;
	struct rlist list;
};

struct bsync_incoming {
	struct fiber *f;
	const struct tt_uuid *uuid;
	int remote_id;
	struct rlist list;
};

struct bsync_common {
	struct bsync_region *region;
	struct bsync_key **dup_key;
};

static struct bsync_txn_state_ {
	uint8_t leader_id;
	uint8_t local_id;

	uint8_t state;
	bool join[BSYNC_MAX_HOSTS];
	bool iproto[BSYNC_MAX_HOSTS];
	bool wait_local[BSYNC_MAX_HOSTS];
	bool recovery;
	uint8_t id2index[BSYNC_MAX_HOSTS];
	pthread_mutex_t mutex[BSYNC_MAX_HOSTS];
	pthread_cond_t cond[BSYNC_MAX_HOSTS];
	struct fiber *snapshot_fiber;

	struct rlist incoming_connections;
	struct rlist wait_start;

	struct rlist txn_queue;
	struct rlist execute_queue; // executing operations

	struct vclock vclock;
} txn_state;

struct bsync_txn_info;
typedef void (*bsync_request_f)(struct bsync_txn_info *);

struct bsync_txn_info { /* txn information about operation */
	struct wal_request *req;
	struct fiber *owner;
	struct bsync_operation *oper;
	struct bsync_common *common;
	uint8_t connection;
	int64_t sign;
	int result;
	bool repeat;
	bool proxy;
	bsync_request_f process;

	struct rlist list;
	STAILQ_ENTRY(bsync_txn_info) fifo;
};
STAILQ_HEAD(bsync_fifo, bsync_txn_info);

enum bsync_operation_status {
	bsync_op_status_proxy = 0,
	bsync_op_status_init = 1,
	bsync_op_status_wal = 2,
	bsync_op_status_txn = 3,
	bsync_op_status_submit = 4,
	bsync_op_status_yield = 5,
	bsync_op_status_fail = 6,
	bsync_op_status_submit_yield = 7,
	bsync_op_status_finish_yield = 8,
	bsync_op_status_accept_yield = 9,
};

static const char* bsync_op_status_name[] = {
	"proxy",
	"init",
	"wal",
	"txn",
	"submit",
	"yield",
	"fail",
	"submit_yield",
	"finish_yield",
	"accept_yield"
};

#define bsync_status(oper, s) do {\
	if ((oper)->req) { \
		say_debug("change status of %d:%ld (%ld) from %s to %s", \
			  LAST_ROW((oper)->req)->server_id, \
			  LAST_ROW((oper)->req)->lsn, (oper)->sign, \
			  bsync_op_status_name[(oper)->status], \
			  bsync_op_status_name[(s)]); \
	} else { \
		say_debug("change status of %d:%ld (%ld) from %s to %s", \
			  LAST_ROW((oper)->txn_data->req)->server_id, \
			  LAST_ROW((oper)->txn_data->req)->lsn, (oper)->sign, \
			  bsync_op_status_name[(oper)->status], \
			  bsync_op_status_name[(s)]); \
	} \
	assert((oper)->status != bsync_op_status_accept_yield); \
	(oper)->status = (s); \
} while(0)

struct bsync_operation {
	uint64_t sign;
	uint8_t host_id;
	uint8_t status;
	uint8_t accepted;
	uint8_t rejected;
	bool submit;

	struct fiber *owner;
	struct bsync_common *common;
	struct bsync_txn_info *txn_data;
	struct wal_request *req;

	struct rlist list;
	struct rlist txn;
	struct rlist accept;
};

enum bsync_host_flags {
	bsync_host_active_write = 0x01,
	bsync_host_rollback = 0x02,
	bsync_host_reconnect_sleep = 0x04,
	bsync_host_ping_sleep = 0x08
};

enum bsync_host_state {
	bsync_host_disconnected = 0,
	bsync_host_recovery = 1,
	bsync_host_follow = 2,
	bsync_host_connected = 3
};

struct bsync_host_data {
	char name[1024];
	int remote_id;
	uint8_t state;
	uint8_t flags;
	bool fiber_out_fail;
	struct fiber *fiber_out;
	struct fiber *fiber_in;
	int64_t sign;
	int64_t commit_sign;

	ssize_t send_queue_size;
	struct rlist send_queue;
	ssize_t follow_queue_size;
	struct rlist follow_queue;
	ssize_t op_queue_size;
	struct rlist op_queue;
	struct mh_bsync_t *active_ops;

	struct bsync_txn_info sysmsg;
	struct bsync_send_elem ping_msg;

	struct mempool proxy_pool;
	struct xrow_header **proxy_rows;
	size_t proxy_size;
	size_t proxy_cur;
	uint64_t proxy_sign;
	size_t body_cur;
};
static struct bsync_host_data bsync_index[BSYNC_MAX_HOSTS];

struct bsync_fiber_cache {
	size_t size;
	size_t active;
	size_t active_proxy;
	size_t active_local;
	size_t proxy_yield;
	struct rlist data;
};

static struct bsync_state_ {
	uint8_t local_id;
	uint8_t leader_id;
	uint8_t accept_id;
	uint8_t num_hosts;
	uint8_t num_connected;
	uint8_t state;
	uint8_t num_accepted;
	uint64_t wal_commit_sign;
	uint64_t wal_rollback_sign;
	struct fiber *wal_push_fiber;

	ev_tstamp read_timeout;
	ev_tstamp write_timeout;
	ev_tstamp operation_timeout;
	ev_tstamp ping_timeout;
	ev_tstamp submit_timeout;
	ev_tstamp election_timeout;
	ssize_t max_host_queue;

	uint8_t proxy_host;

	struct rlist proxy_queue;
	struct rlist wal_queue;
	struct rlist submit_queue; // submitted operations
	struct rlist txn_queue;
	struct rlist commit_queue; // commited operations
	struct rlist accept_queue; // accept queue

	size_t active_ops;
	struct rlist wait_queue;

	struct bsync_fiber_cache txn_fibers;
	struct bsync_fiber_cache bsync_fibers;

	struct rlist election_ops;

	struct bsync_fifo txn_proxy_queue;
	struct bsync_fifo txn_proxy_input;

	struct bsync_fifo bsync_proxy_queue;
	struct bsync_fifo bsync_proxy_input;

	struct mempool region_pool;
	struct mempool system_send_pool;
	struct rlist region_free;
	size_t region_free_size;
	struct rlist region_gc;

	/* TODO : temporary hack for support system operations on proxy side */
	struct cord cord;
	pthread_mutex_t mutex;
	pthread_mutex_t active_ops_mutex;
	pthread_cond_t cond;
	struct bsync_txn_info sysmsg;

	struct vclock vclock;
	bool bsync_rollback;
} bsync_state;

#define bsync_commit_foreach(f) { \
	struct bsync_operation *oper; \
	rlist_foreach_entry(oper, &bsync_state.commit_queue, list) { \
		if (f(oper)) \
			break; \
	} \
	rlist_foreach_entry(oper, &bsync_state.submit_queue, list) { \
		if (f(oper)) \
			break; \
	} \
}

#define BSYNC_LOCAL bsync_index[bsync_state.local_id]
#define BSYNC_LEADER bsync_index[bsync_state.leader_id]
#define BSYNC_REMOTE bsync_index[host_id]
#define BSYNC_LOCK(M) \
	tt_pthread_mutex_lock(&M); \
	auto guard = make_scoped_guard([&]() { \
		tt_pthread_mutex_unlock(&M); \
	})
#define LAST_ROW(REQ) (REQ)->rows[(REQ)->n_rows - 1]

static struct fiber*
bsync_fiber(struct bsync_fiber_cache *lst, void (*f)(va_list), ...)
{
	struct fiber *result = NULL;
	if (! rlist_empty(&lst->data)) {
		result = rlist_shift_entry(&lst->data, struct fiber, state);
	} else {
		result = fiber_new("bsync_proc", f);
		++lst->size;
	}
	++lst->active;
	return result;
}

static struct bsync_region *
bsync_new_region()
{
	assert(cord() != &bsync_state.cord);
	if (!rlist_empty(&bsync_state.region_free)) {
		return rlist_shift_entry(&bsync_state.region_free,
			struct bsync_region, list);
	} else {
		++bsync_state.region_free_size;
		struct bsync_region* region = (struct bsync_region *)
			mempool_alloc0(&bsync_state.region_pool);
		region_create(&region->pool, &cord()->slabc);
		return region;
	}
}

static void
bsync_free_region(struct bsync_common *data)
{
	if (!data->region)
		return;
	tt_pthread_mutex_lock(&bsync_state.mutex);
	rlist_add_tail_entry(&bsync_state.region_gc, data->region, list);
	tt_pthread_mutex_unlock(&bsync_state.mutex);
	data->region = NULL;
}

static void
bsync_dump_region()
{
	struct rlist region_gc;
	rlist_create(&region_gc);
	tt_pthread_mutex_lock(&bsync_state.mutex);
	rlist_swap(&region_gc, &bsync_state.region_gc);
	tt_pthread_mutex_unlock(&bsync_state.mutex);
	if (rlist_empty(&region_gc))
		return;
	struct bsync_region *cur = NULL;
	while (!rlist_empty(&region_gc)) {
		cur = rlist_shift_entry(&region_gc, struct bsync_region, list);
		region_free(&cur->pool);
		rlist_add_entry(&bsync_state.region_free, cur, list);
	}
}

static uint8_t
bsync_max_host()
{
	uint8_t max_host_id = 0;
	for (uint8_t i = 1; i < bsync_state.num_hosts; ++i) {
		if (bsync_index[i].commit_sign >= bsync_index[max_host_id].commit_sign &&
			bsync_index[i].state > bsync_host_disconnected)
		{
			max_host_id = i;
		}
	}
	return max_host_id;
}

enum bsync_message_type {
	bsync_mtype_leader_proposal = 0,
	bsync_mtype_leader_promise = 1,
	bsync_mtype_leader_accept = 2,
	bsync_mtype_leader_submit = 3,
	bsync_mtype_leader_reject = 4,
	bsync_mtype_ping = 5,
	bsync_mtype_iproto_switch = 6,
	bsync_mtype_bsync_switch = 7,
	bsync_mtype_ready_switch = 8,
	bsync_mtype_close = 9,
	bsync_mtype_rollback = 10,
	bsync_mtype_proxy_reject = 11,
	bsync_mtype_sysend = 12,
	bsync_mtype_body = 13,
	bsync_mtype_submit = 14,
	bsync_mtype_reject = 15,
	bsync_mtype_proxy_request = 16,
	bsync_mtype_proxy_accept = 17,
	bsync_mtype_proxy_join = 18,
	bsync_mtype_count = 19,
	bsync_mtype_none = 20
};

enum bsync_machine_state {
	bsync_state_election = 0,
	bsync_state_initial = 1,
	bsync_state_promise = 2,
	bsync_state_accept = 3,
	bsync_state_recovery = 4,
	bsync_state_ready = 5,
	bsync_state_shutdown = 6
};

enum txn_machine_state {
	txn_state_join = 0,
	txn_state_snapshot = 1,
	txn_state_subscribe = 2,
	txn_state_recovery = 3,
	txn_state_ready = 4,
	txn_state_rollback = 5
};

enum bsync_iproto_flags {
	bsync_iproto_commit_sign = 0x01,
	bsync_iproto_txn_size = 0x02
};

static const char* bsync_mtype_name[] = {
	"leader_proposal",
	"leader_promise",
	"leader_accept",
	"leader_submit",
	"leader_reject",
	"ping",
	"iproto_switch",
	"bsync_switch",
	"ready_switch",
	"close",
	"rollback",
	"proxy_reject",
	"INVALID",
	"body",
	"submit",
	"reject",
	"proxy_request",
	"proxy_accept",
	"proxy_join",
	"INVALID",
	"INVALID"
};

#define SWITCH_TO_BSYNC_UNSAFE(cb) do {\
	{ \
		struct bsync_txn_info *tmp; \
		STAILQ_FOREACH(tmp, &bsync_state.bsync_proxy_queue, fifo) { \
			assert (tmp != info); \
		} \
	} \
	info->process = cb; \
	bool was_empty = STAILQ_EMPTY(&bsync_state.bsync_proxy_queue); \
	STAILQ_INSERT_TAIL(&bsync_state.bsync_proxy_queue, info, fifo); \
	if (was_empty) \
		ev_async_send(bsync_loop, &bsync_process_event); \
} while (0)

#define SWITCH_TO_BSYNC(cb) \
	tt_pthread_mutex_lock(&bsync_state.mutex); \
	SWITCH_TO_BSYNC_UNSAFE(cb); \
	tt_pthread_mutex_unlock(&bsync_state.mutex);

#define SWITCH_TO_TXN(info, cb) do {\
	tt_pthread_mutex_lock(&bsync_state.mutex); \
	{ \
		struct bsync_txn_info *tmp; \
		STAILQ_FOREACH(tmp, &bsync_state.txn_proxy_queue, fifo) { \
			assert (tmp != info); \
		} \
	} \
	(info)->process = cb; \
	bool was_empty = STAILQ_EMPTY(&bsync_state.txn_proxy_queue); \
	STAILQ_INSERT_TAIL(&bsync_state.txn_proxy_queue, info, fifo); \
	tt_pthread_mutex_unlock(&bsync_state.mutex); \
	if (was_empty) \
		ev_async_send(txn_loop, &txn_process_event); \
} while(0)

static bool
bsync_op_begin(struct bsync_key *key, uint32_t server_id)
{
	if (!local_state->remote[txn_state.leader_id].localhost)
		return true;
	/*
	 * TODO : special analyze for operations with spaces (remove/create)
	 */
	BSYNC_LOCK(bsync_state.active_ops_mutex);
	mh_int_t keys[BSYNC_MAX_HOSTS];
	for (uint8_t host_id = 0; host_id < bsync_state.num_hosts; ++host_id) {
		if (host_id == txn_state.local_id)
			continue;
		keys[host_id] = mh_bsync_find(BSYNC_REMOTE.active_ops, *key, NULL);
		if (keys[host_id] == mh_end(BSYNC_REMOTE.active_ops))
			continue;
		struct mh_bsync_node_t *node =
			mh_bsync_node(BSYNC_REMOTE.active_ops, keys[host_id]);
		if (server_id != local_state->server_id && node->val.remote_id != server_id)
			return false;
	}
	for (uint8_t host_id = 0; host_id < bsync_state.num_hosts; ++host_id) {
		if (host_id == txn_state.local_id)
			continue;
		if (keys[host_id] != mh_end(BSYNC_REMOTE.active_ops)) {
			struct mh_bsync_node_t *node =
				mh_bsync_node(BSYNC_REMOTE.active_ops, keys[host_id]);
			if (server_id == 0) {
				++node->val.local_ops;
			} else {
				node->val.remote_id = server_id;
				++node->val.remote_ops;
			}
			node->key.data = key->data;
		} else {
			struct mh_bsync_node_t node;
			node.key = *key;
			if (server_id == 0) {
				node.val.local_ops = 1;
				node.val.remote_ops = 0;
			} else {
				node.val.local_ops = 0;
				node.val.remote_ops = 1;
			}
			node.val.remote_id = server_id;
			mh_bsync_put(BSYNC_REMOTE.active_ops, &node, NULL, NULL);
		}
	}
	return true;
}

static void
bsync_op_end(uint8_t host_id, struct bsync_key *key, uint32_t server_id)
{
	if (!local_state->remote[txn_state.leader_id].localhost)
		return;
	BSYNC_LOCK(bsync_state.active_ops_mutex);
	mh_int_t k = mh_bsync_find(BSYNC_REMOTE.active_ops, *key, NULL);
	if (k == mh_end(BSYNC_REMOTE.active_ops))
		return;

	struct mh_bsync_node_t *node = mh_bsync_node(BSYNC_REMOTE.active_ops, k);
	if (server_id != 0)
		--node->val.remote_ops;
	else
		--node->val.local_ops;
	if ((node->val.local_ops + node->val.remote_ops) == 0)
		mh_bsync_del(BSYNC_REMOTE.active_ops, k, NULL);
}

struct bsync_parse_data {
	uint32_t space_id;
	bool is_tuple;
	const char *data;
	const char *end;
	const char *key;
	const char *key_end;
};

static bool
txn_in_remote_recovery()
{
	return txn_state.leader_id != BSYNC_MAX_HOSTS &&
		((txn_state.state == txn_state_recovery && txn_state.leader_id != txn_state.local_id) ||
		(txn_state.state == txn_state_subscribe && txn_state.leader_id == txn_state.local_id));
}

static bool
txn_in_local_recovery()
{
	return txn_state.local_id == BSYNC_MAX_HOSTS ||
		txn_state.state == txn_state_snapshot;

}

#define BSYNC_MAX_KEY_PART_LEN 256
static void
bsync_parse_dup_key(struct bsync_common *data, struct key_def *key,
		    struct tuple *tuple, int i)
{
	if (!local_state->remote[txn_state.leader_id].localhost) {
		data->dup_key[i] = NULL;
		return;
	}
	data->dup_key[i] = (struct bsync_key *)
		region_alloc(&data->region->pool, sizeof(struct bsync_key));
	data->dup_key[i]->data = (char *)region_alloc(&data->region->pool,
					BSYNC_MAX_KEY_PART_LEN * key->part_count);
	data->dup_key[i]->size = 0;
	char *i_data = data->dup_key[i]->data;
	for (uint32_t p = 0; p < key->part_count; ++p) {
		if (key->parts[p].type == NUM) {
			uint32_t v = tuple_field_u32(tuple, key->parts[p].fieldno);
			memcpy(i_data, &v, sizeof(uint32_t));
			data->dup_key[i]->size += sizeof(uint32_t);
			i_data += sizeof(uint32_t);
			continue;
		}
		const char *key_part =
			tuple_field_cstr(tuple, key->parts[p].fieldno);
		size_t key_part_len = strlen(key_part);
		data->dup_key[i]->size += key_part_len;
		memcpy(i_data, key_part, key_part_len + 1);
		i_data += data->dup_key[i]->size;
	}
	data->dup_key[i]->space_id = key->space_id;
}

static int
bsync_find_incoming(int id, struct tt_uuid *uuid)
{
	say_debug("try to find incoming with id=%d and uuid=%s", id, tt_uuid_str(uuid));
	int result = -1;
	struct bsync_incoming *inc;
	rlist_foreach_entry(inc, &txn_state.incoming_connections, list) {
		if (!tt_uuid_is_equal(uuid, inc->uuid))
			continue;
		if (id >= 0)
			inc->remote_id = id;
		say_debug("found pair in incoming connections");
		result = inc->remote_id;
		fiber_call(inc->f);
		goto finish;
	}
	if (id < 0) {
		for (int i = 0; i < local_state->remote_size; ++i) {
			if (tt_uuid_is_equal(
				&local_state->remote[i].server_uuid, uuid)
				&& local_state->remote[i].switched)
			{
				say_debug("found pair in switched connections");
				result = i;
				goto finish;
			}
		}
	}
	inc = (struct bsync_incoming *)
		region_alloc(&fiber()->gc, sizeof(struct bsync_incoming));
	inc->f = fiber();
	inc->uuid = uuid;
	inc->remote_id = id;
	rlist_add_entry(&txn_state.incoming_connections, inc, list);
	if (fiber_yield_timeout(BSYNC_PAIR_TIMEOUT)) {
		say_debug("pair wasnt found");
		rlist_del_entry(inc, list);
		return -1;
	}
	rlist_del_entry(inc, list);
	if (inc->remote_id >= 0) {
		assert(!ev_is_active(&local_state->remote[inc->remote_id].out));
		say_info("connected with %s",
			local_state->remote[inc->remote_id].source);
	}
	result = inc->remote_id;
finish:
	if (txn_state.leader_id < BSYNC_MAX_HOSTS &&
		txn_state.local_id != txn_state.leader_id)
		result = id;
	return result;
}

void
bsync_push_connection(int id)
{
	if (!local_state->bsync_remote)
		return;
	assert(!ev_is_active(&local_state->remote[id].out));
	if (txn_state.local_id == BSYNC_MAX_HOSTS) {
		txn_state.wait_local[id] = true;
		fiber_yield();
	}
	if (bsync_find_incoming(id, &local_state->remote[id].server_uuid) < 0)
		tnt_raise(ClientError, ER_NO_CONNECTION);
}

void
bsync_push_localhost(int id)
{
	if (!local_state->bsync_remote)
		return;
	txn_state.local_id = id;
	struct bsync_txn_info *info = &bsync_index[id].sysmsg;
	if (txn_state.state < txn_state_subscribe)
		info->sign = -1;
	else
		info->sign = vclock_sum(&local_state->vclock);
	SWITCH_TO_BSYNC(bsync_update_local);
	for (int i = 0; i < local_state->remote_size; ++i) {
		if (!txn_state.wait_local[i])
			continue;
		fiber_call(local_state->remote[i].connecter);
		txn_state.wait_local[i] = false;
	}
}

void
bsync_init_in(uint8_t host_id, int fd)
{
	coio_init(&local_state->remote[host_id].in);
	local_state->remote[host_id].in.fd = fd;
	local_state->remote[host_id].writer = fiber();
}

void
bsync_switch_2_election(uint8_t host_id, struct recovery_state *state)
{
	struct bsync_txn_info *info = &BSYNC_REMOTE.sysmsg;
	if (state)
		info->sign = vclock_sum(&state->vclock);
	else
		info->sign = -1;
	txn_state.iproto[host_id] = true;
	assert(local_state->remote[host_id].in.fd >= 0);
	assert(local_state->remote[host_id].out.fd >= 0);
	assert(!ev_is_active(&local_state->remote[host_id].in));
	assert(!ev_is_active(&local_state->remote[host_id].out));
	SWITCH_TO_BSYNC(bsync_process_connect);
	fiber_yield();
	txn_state.iproto[host_id] = false;
}

bool
bsync_process_join(int fd, struct tt_uuid *uuid,
		   void (*on_join)(const struct tt_uuid *))
{
	if (!local_state->bsync_remote)
		return true;
	int host_id = 0;
	if ((host_id = bsync_find_incoming(-1, uuid)) == -1)
		return false;
	local_state->remote[host_id].switched = false;
	assert(BSYNC_REMOTE.state == bsync_host_disconnected ||
		BSYNC_REMOTE.state == bsync_host_follow);
	bsync_init_in(host_id, fd);
	txn_state.join[host_id] = true;
	bsync_switch_2_election(host_id, NULL);
	if (local_state->remote[txn_state.leader_id].localhost) {
		if (txn_state.state < txn_state_subscribe) {
			on_join(uuid);
			fiber_yield();
			txn_state.join[host_id] = false;
			bsync_txn_info *info = &BSYNC_REMOTE.sysmsg;
			info->sign = vclock_sum(&local_state->vclock);
			SWITCH_TO_BSYNC(bsync_set_follow);
		} else {
			txn_state.join[host_id] = false;
			box_on_cluster_join(uuid);
		}
		say_info("start sending snapshot to %s", tt_uuid_str(uuid));
		return true;
	} else
		txn_state.join[host_id] = false;
	return false;
}

bool
bsync_process_subscribe(int fd, struct tt_uuid *uuid,
			struct recovery_state *state)
{
	if (!local_state->bsync_remote)
		return true;
	int host_id = 0;
	if ((host_id = bsync_find_incoming(-1, uuid)) == -1)
		return false;
	bsync_init_in(host_id, fd);
	txn_state.id2index[state->server_id] = host_id;
	char *vclock = vclock_to_string(&state->vclock);
	say_info("set host_id %d to server_id %d. remote vclock is %s",
		 host_id, state->server_id, vclock);
	free(vclock);
	bsync_switch_2_election(host_id, state);
	return local_state->remote[txn_state.leader_id].localhost;
}

int
bsync_join()
{
	if (!local_state->bsync_remote)
		return 0;
	if (txn_state.leader_id < BSYNC_MAX_HOSTS) {
		if (txn_state.leader_id == txn_state.local_id)
		return txn_state.leader_id;
	}
	txn_state.recovery = true;
	fiber_yield();
	txn_state.recovery = false;
	if (txn_state.snapshot_fiber) {
		say_info("wait for finish snapshot generating");
		fiber_join(txn_state.snapshot_fiber);
		txn_state.snapshot_fiber = NULL;
		vclock_copy(&txn_state.vclock, &local_state->vclock);
	} else
		txn_state.state = txn_state_snapshot;
	return txn_state.leader_id;
}

int
bsync_subscribe()
{
	if (!local_state->bsync_remote)
		return 0;
	auto state_guard = make_scoped_guard([]() {
		txn_state.state = txn_state_recovery;
	});
	if (txn_state.leader_id < BSYNC_MAX_HOSTS)
		return txn_state.leader_id;
	txn_state.recovery = true;
	fiber_yield();
	txn_state.recovery = false;
	return txn_state.leader_id;
}

int
bsync_replica_stop()
{
	uint8_t host_id = txn_state.local_id;
	struct bsync_txn_info *info = &BSYNC_REMOTE.sysmsg;
	info->sign = vclock_sum(&local_state->vclock);
	SWITCH_TO_BSYNC(bsync_start_connect);
	fiber_yield();
	if (txn_state.state == txn_state_ready)
		return txn_state.local_id;
	else
		return txn_state.leader_id;
}

static void
txn_leader(struct bsync_txn_info *info)
{
	STAILQ_REMOVE_HEAD(&bsync_state.txn_proxy_input, fifo);
	txn_state.leader_id = info->connection;
	say_info("set leader in TXN to %s, state is %d",
		 local_state->remote[txn_state.leader_id].source,
		 txn_state.state);
	if (txn_state.state > txn_state_snapshot &&
		local_state->remote[info->connection].localhost &&
		local_state->remote[txn_state.leader_id].reader != NULL)
	{
		fiber_call(local_state->remote[txn_state.leader_id].reader);
	}
}

static void
txn_join(struct bsync_txn_info * /*info*/)
{
	STAILQ_REMOVE_HEAD(&bsync_state.txn_proxy_input, fifo);
	txn_state.state = txn_state_snapshot;
	txn_state.snapshot_fiber = fiber_new("generate_initial_snapshot",
		(fiber_func) box_generate_initial_snapshot);
	fiber_set_joinable(txn_state.snapshot_fiber, true);
	fiber_call(txn_state.snapshot_fiber);
	for (int i = 0; i < local_state->remote_size; ++i) {
		if (txn_state.join[i])
			fiber_call(local_state->remote[i].writer);
	}
	fiber_call(txn_state.snapshot_fiber);
	fiber_call(local_state->remote[txn_state.leader_id].reader);
}

static void
txn_process_recovery(struct bsync_txn_info *info)
{
	STAILQ_REMOVE_HEAD(&bsync_state.txn_proxy_input, fifo);
	if (local_state->remote[txn_state.leader_id].localhost) {
		uint8_t hi = info->connection;
		local_state->remote[hi].switched = true;
		say_debug("bsync_process_recovery: host_id=%d, join=%d, iproto=%d, snapshot=%p",
			hi, txn_state.join[hi] ? 1 : 0, txn_state.iproto[hi] ? 1 : 0,
			txn_state.snapshot_fiber);
		if (txn_state.join[hi] && txn_state.snapshot_fiber != NULL)
			return;
		if (txn_state.iproto[hi])
			fiber_call(local_state->remote[hi].writer);
		if (txn_state.join[hi])
			fiber_call(local_state->remote[hi].writer);
	} else {
		char *vclock = vclock_to_string(&local_state->vclock);
		say_info("start recovery from %s, vclock is %s",
			local_state->remote[txn_state.leader_id].source, vclock);
		free(vclock);
		if (txn_state.recovery &&
		    local_state->remote[txn_state.leader_id].reader)
		{
			fiber_call(local_state->remote[txn_state.leader_id].reader);
		} else
			recovery_follow_remote(local_state);
	}
}

static void
txn_process_close(struct bsync_txn_info *info)
{
	STAILQ_REMOVE_HEAD(&bsync_state.txn_proxy_input, fifo);
	evio_close(loop(), &local_state->remote[info->connection].in);
	evio_close(loop(), &local_state->remote[info->connection].out);
	local_state->remote[info->connection].switched = false;
	local_state->remote[info->connection].connected = false;
	if (local_state->remote[txn_state.leader_id].localhost)
		return;
	if (txn_state.iproto[info->connection])
		fiber_call(local_state->remote[info->connection].writer);
	if (txn_state.join[info->connection])
		fiber_call(local_state->remote[info->connection].writer);
}

static void
txn_reconnect_all()
{
	if (txn_state.leader_id != BSYNC_MAX_HOSTS) {
		struct bsync_txn_info *info = &bsync_index[txn_state.local_id].sysmsg;
		info->sign = vclock_sum(&local_state->vclock);
		info->repeat = false;
		SWITCH_TO_BSYNC(bsync_update_local);
	}
	txn_state.leader_id = BSYNC_MAX_HOSTS;
	txn_state.state = txn_state_subscribe;
	for (int i = 0; i < local_state->remote_size; ++i) {
		if (local_state->remote[i].localhost)
			continue;
		if (evio_has_fd(&local_state->remote[i].in))
			evio_close(loop(), &local_state->remote[i].in);
		if (evio_has_fd(&local_state->remote[i].out))
			evio_close(loop(), &local_state->remote[i].out);
		local_state->remote[i].switched = false;
		fiber_call(local_state->remote[i].connecter);
	}
}

static void
txn_process_reconnnect(struct bsync_txn_info *info)
{
	if (info->proxy) {
		STAILQ_REMOVE_HEAD(&bsync_state.txn_proxy_input, fifo);
		say_info("start to reconnect all");
		vclock_copy(&bsync_state.vclock, &local_state->vclock);
		txn_reconnect_all();
	} else {
		say_info("start to reconnect to %s",
			local_state->remote[info->connection].source);
		txn_process_close(info);
		fiber_call(local_state->remote[info->connection].connecter);
	}
}

bool
bsync_follow(struct recovery_state * r)
{
	/* try to switch to normal mode */
	if (!local_state->bsync_remote || !local_state->finalize)
		return false;
	int host_id = txn_state.id2index[r->server_id];
	assert(host_id < BSYNC_MAX_HOSTS);
	BSYNC_LOCK(txn_state.mutex[host_id]);
	bsync_txn_info *info = &BSYNC_REMOTE.sysmsg;
	info->sign = vclock_sum(&r->vclock);
	say_debug("try to follow for %s", local_state->remote[host_id].source);
	SWITCH_TO_BSYNC(bsync_process_follow);
	tt_pthread_cond_wait(&txn_state.cond[host_id],
		&txn_state.mutex[host_id]);
	return info->proxy;
}

static void
txn_set_ready(struct bsync_txn_info *info)
{
	(void)info;
	STAILQ_REMOVE_HEAD(&bsync_state.txn_proxy_input, fifo);
	char *vclock = vclock_to_string(&local_state->vclock);
	say_info("change TXN state to ready, vclock is %s", vclock);
	free(vclock);
	txn_state.state = txn_state_ready;
	vclock_copy(&txn_state.vclock, &local_state->vclock);
	struct fiber *f = NULL;
	while (!rlist_empty(&txn_state.wait_start)) {
		f = rlist_shift_entry(&txn_state.wait_start,
					struct fiber, state);
		fiber_call(f);
	}
	say_info("finish cleanup wait queue");
	if (!local_state->remote[info->connection].localhost &&
		local_state->remote[txn_state.leader_id].reader != NULL)
	{
		fiber_call(local_state->remote[txn_state.leader_id].reader);
	}
}

void
bsync_replication_fail(struct recovery_state *r)
{
	if (!local_state->bsync_remote)
		return;
	uint8_t host_id = txn_state.id2index[r->server_id];
	txn_state.id2index[r->server_id] = BSYNC_MAX_HOSTS;
	local_state->remote[host_id].switched = false;
	if (evio_has_fd(&local_state->remote[host_id].in))
		evio_close(loop(), &local_state->remote[host_id].in);
	if (evio_has_fd(&local_state->remote[host_id].out))
		evio_close(loop(), &local_state->remote[host_id].out);
	struct bsync_txn_info *info = &bsync_index[host_id].sysmsg;
	info->proxy = false;
	SWITCH_TO_BSYNC(bsync_process_disconnect);
	fiber_call(local_state->remote[host_id].connecter);
}

void
bsync_replica_fail()
{
	if (!local_state->bsync_remote)
		return;
	say_info("offline replication from %s didnt finished",
		 bsync_index[txn_state.leader_id].name);
	struct bsync_txn_info *info = &bsync_index[txn_state.leader_id].sysmsg;
	SWITCH_TO_BSYNC(bsync_process_disconnect);
	txn_reconnect_all();
	txn_state.leader_id = BSYNC_MAX_HOSTS;
}

static void bsync_outgoing(va_list ap);
static void bsync_accept_handler(va_list ap);

static void
bsync_fill_lsn(struct xrow_header *row)
{
	assert (row->server_id == 0);
	row->server_id = local_state->server_id;
	row->lsn = vclock_inc(&txn_state.vclock, row->server_id);
}

static struct bsync_txn_info *
bsync_alloc_txn_info(struct wal_request *req, bool proxy)
{
	struct bsync_txn_info *info = (struct bsync_txn_info *)
		region_alloc(&fiber()->gc, sizeof(struct bsync_txn_info));
	info->common = (struct bsync_common *) region_alloc0(
		&fiber()->gc, sizeof(struct bsync_common));
	if (!proxy) {
		info->common->region = bsync_new_region();
		info->common->dup_key = (struct bsync_key **)
			region_alloc(&info->common->region->pool,
				req ? sizeof(struct bsync_key *) * req->n_rows : 0);
	}
	info->req = req;
	info->oper = NULL;
	info->repeat = false;
	info->result = 0;
	info->proxy = proxy;
	info->process = bsync_process;
	return info;
}

static int
bsync_write_local(struct recovery_state *r, struct wal_request *req)
{
	for (int i = 0; i < req->n_rows; ++i) {
		recovery_fill_lsn(local_state, req->rows[i]);
		vclock_follow(&txn_state.vclock, req->rows[i]->server_id,
			      req->rows[i]->lsn);
		req->rows[i]->commit_sn = vclock_sum(&txn_state.vclock);
		say_debug("bsync_write_local receive %d:%ld",
			  req->rows[i]->server_id,
			  req->rows[i]->lsn);
	}
	return wal_write(r, req);
}

static int
bsync_write_remote(struct wal_request *req)
{
	struct bsync_txn_info *info = bsync_alloc_txn_info(req, true);
	if (req->rows[0]->server_id == 0) {
		assert(local_state->server_id > 0);
		if (txn_state.state == txn_state_recovery)
			return -1;
		for (int i = 0; i < req->n_rows; ++i)
			recovery_fill_lsn(local_state, req->rows[i]);
	} else {
		assert(local_state->server_id != 0 ||
			txn_state.leader_id != txn_state.local_id);
		vclock_follow(&local_state->vclock, LAST_ROW(req)->server_id,
			LAST_ROW(req)->lsn);
	}
	say_debug("bsync_write_remote receive %d:%ld",
		  LAST_ROW(req)->server_id, LAST_ROW(req)->lsn);
	if (vclock_get(&txn_state.vclock, LAST_ROW(req)->server_id) < LAST_ROW(req)->lsn)
		vclock_follow(&txn_state.vclock, LAST_ROW(req)->server_id, LAST_ROW(req)->lsn);
	info->sign = vclock_sum(&local_state->vclock);
	info->owner = fiber();
	SWITCH_TO_BSYNC(bsync_process_wal);
	fiber_yield();
	info->owner = NULL;
	assert(info->result >= 0);
	say_debug("result of recovery operation is %d", info->result);
	return info->result;
}

void
bsync_commit_local(uint32_t server_id, uint64_t lsn)
{
	if (!local_state->bsync_remote)
		return;
	if (vclock_get(&local_state->vclock, server_id) == lsn)
		return;
	vclock_follow(&local_state->vclock, server_id, lsn);
	if (vclock_get(&txn_state.vclock, server_id) < lsn)
		vclock_follow(&txn_state.vclock, server_id, lsn);
	struct bsync_txn_info *info = bsync_alloc_txn_info(NULL, false);
	info->sign = vclock_sum(&local_state->vclock);
	info->owner = fiber();
	SWITCH_TO_BSYNC(bsync_process_wal);
	fiber_yield();
	assert(info->result >= 0);
}

static void
txn_cleanup_bsync_queue(struct bsync_fifo *queue)
{
	struct bsync_txn_info *info;
	struct bsync_fifo buffer;
	STAILQ_INIT(&buffer);
	while (!STAILQ_EMPTY(queue)) {
		info = STAILQ_FIRST(queue);
		STAILQ_REMOVE_HEAD(queue, fifo);
		if (info->oper && (info->oper->status == bsync_op_status_proxy ||
			info->oper->status == bsync_op_status_finish_yield))
		{
			STAILQ_INSERT_TAIL(&buffer, info, fifo);
			say_info("save in proxy_queue (%d:%ld), status=%d",
				 LAST_ROW(info->req)->server_id,
				 LAST_ROW(info->req)->lsn,
				 info->oper ? info->oper->status : -1);
		} else {
			say_info("drop from proxy_queue (%d:%ld), status=%d",
				 LAST_ROW(info->req)->server_id,
				 LAST_ROW(info->req)->lsn,
				 info->oper ? info->oper->status : -1);
		}
	}
	STAILQ_INIT(queue);
	STAILQ_CONCAT(queue, &buffer);
}

static void
txn_reapply_ops(struct trigger * /* t */, void * /* data */)
{
	say_info("start to reapply commited ops");
	bsync_commit_foreach([](struct bsync_operation *oper) {
		oper->txn_data->result = 1;
		if (!rlist_empty(&oper->txn_data->list))
			say_info("ignore commited row %d:%ld",
				 LAST_ROW(oper->txn_data->req)->server_id,
				 LAST_ROW(oper->txn_data->req)->lsn);
		else if (oper->txn_data->owner) {
			say_info("reapply commited row %d:%ld",
				 LAST_ROW(oper->txn_data->req)->server_id,
				 LAST_ROW(oper->txn_data->req)->lsn);
			fiber_call(oper->txn_data->owner);
		}
		return false;
	});
	say_info("finish rollback execution");
	txn_state.state = txn_state_ready;
	tt_pthread_cond_signal(&bsync_state.cond);
	tt_pthread_mutex_unlock(&bsync_state.mutex);
}

int
bsync_write(struct recovery_state *r, struct txn *txn, struct wal_request *req)
try {
	if (!local_state->bsync_remote)
		return wal_write(r, req);
	assert(local_state == NULL || local_state == r);
	if (txn_in_local_recovery())
		return bsync_write_local(r, req);
	if (txn_in_remote_recovery())
		return bsync_write_remote(req);
	if (txn_state.state < txn_state_ready) {
		say_info("add operation to wait queue, state: %d", txn_state.state);
		rlist_add_tail_entry(&txn_state.wait_start, fiber(), state);
		fiber_yield();
		rlist_del_entry(fiber(), state);
	}
	struct bsync_txn_info *info = NULL;
	auto vclock_guard = make_scoped_guard([&]() {
		if (!info)
			return;
		info->owner = NULL;
		if (info->result < 0) {
			say_debug("row %d:%ld rejected from %d, state is %d",
				  LAST_ROW(req)->server_id, LAST_ROW(req)->lsn,
				  fiber()->caller->fid, txn_state.state);
			return;
		}
		say_debug("commit request %d:%ld sign=%ld",
			  LAST_ROW(req)->server_id,
			  LAST_ROW(req)->lsn, info->sign);
		if (txn_state.state != txn_state_recovery)
			vclock_follow(&local_state->vclock,
					LAST_ROW(req)->server_id,
					LAST_ROW(req)->lsn);
	});
	if (txn_state.state == txn_state_rollback) {
		if (LAST_ROW(req)->server_id == 0)
			return -1;
		info = rlist_shift_entry(&txn_state.txn_queue,
					 struct bsync_txn_info, list);
		if (info->oper->status == bsync_op_status_yield ||
			info->oper->status == bsync_op_status_submit_yield ||
			info->oper->status == bsync_op_status_finish_yield)
		{
			say_debug("yield row %d:%ld (%ld) in rollback, status %d",
				  LAST_ROW(info->req)->server_id,
				  LAST_ROW(info->req)->lsn,
				  info->oper->sign, info->oper->status);
			goto repair_yield;
		}
		if (info->oper->status != bsync_op_status_txn) {
			say_debug("ignore row %d:%ld (%ld) in rollback, status %d",
				  LAST_ROW(info->req)->server_id,
				  LAST_ROW(info->req)->lsn,
				  info->oper->sign, info->oper->status);
			info = NULL;
			return 0;
		}
		goto repair_switch;
	}
	bsync_dump_region();
	if (LAST_ROW(req)->server_id == 0) {
		info = bsync_alloc_txn_info(req, txn_state.local_id != txn_state.leader_id);
		struct txn_stmt *stmt;
		int i = 0;
		rlist_foreach_entry(stmt, &txn->stmts, next) {
			if (stmt->row == NULL || (stmt->new_tuple == NULL &&
						  stmt->old_tuple == NULL))
				continue;
			bsync_fill_lsn(req->rows[i]);
			if (txn_state.local_id != txn_state.leader_id) {
				++i;
				continue;
			}
			if (stmt->new_tuple) {
				bsync_parse_dup_key(info->common,
					stmt->space->index[0]->key_def,
					stmt->new_tuple, i);
			} else {
				bsync_parse_dup_key(info->common,
					stmt->space->index[0]->key_def,
					stmt->old_tuple, i);
			}
			bool begin_result = bsync_op_begin(info->common->dup_key[i],
							   stmt->row->server_id);
			(void) begin_result;
			assert(begin_result);
			++i;
		}
		assert(i == req->n_rows);
	} else { /* proxy request */
		info = rlist_shift_entry(&txn_state.txn_queue,
					 struct bsync_txn_info, list);
		vclock_follow(&txn_state.vclock, LAST_ROW(req)->server_id,
			      LAST_ROW(req)->lsn);
		assert(LAST_ROW(info->req)->server_id == LAST_ROW(req)->server_id &&
			LAST_ROW(info->req)->lsn == LAST_ROW(req)->lsn);
	}
	info->owner = fiber();
	info->sign = vclock_sum(&txn_state.vclock);
	assert(info->oper == NULL || info->oper->status == bsync_op_status_txn);
repair_switch:
	SWITCH_TO_BSYNC(bsync_process);

	say_debug("send request %d:%ld (%ld) to bsync",
		  LAST_ROW(req)->server_id, LAST_ROW(req)->lsn,
		  info->oper ? info->oper->sign : vclock_sum(&txn_state.vclock));
repair_yield:
	say_debug("add request %d:%ld (%ld) to execute queue",
		  LAST_ROW(req)->server_id, LAST_ROW(req)->lsn,
		  info->oper ? info->oper->sign : vclock_sum(&txn_state.vclock));
	rlist_add_tail_entry(&txn_state.execute_queue, info, list);
	fiber_yield();
	if (txn_state.state == txn_state_rollback) {
		return info->result;
	}
	if (info->result >= 0 || txn_state.state == txn_state_subscribe) {
		rlist_del_entry(info, list);
		info->repeat = false;
		return info->result;
	}
	assert(txn_state.state == txn_state_ready);
	tt_pthread_mutex_lock(&bsync_state.mutex);
	txn_state.state = txn_state_rollback;
	/* rollback in reverse order local operations */
	say_info("conflict detected, start to rollback (%d:%ld)",
		 LAST_ROW(info->req)->server_id, LAST_ROW(info->req)->lsn);
	txn_cleanup_bsync_queue(&bsync_state.bsync_proxy_input);
	txn_cleanup_bsync_queue(&bsync_state.bsync_proxy_queue);
	struct bsync_txn_info *s;
	while (!rlist_empty(&txn_state.execute_queue)) {
		s = rlist_shift_tail_entry(&txn_state.execute_queue,
					   struct bsync_txn_info, list);
		if (s == info)
			break;
		s->result = -1;
		say_info("reject row %d:%ld", LAST_ROW(s->req)->server_id,
			LAST_ROW(s->req)->lsn);
		assert(s->oper == NULL || s->oper->status != bsync_op_status_submit);
		if (s->oper && s->oper->submit && s->oper->status == bsync_op_status_txn) {
			s->repeat = true;
			rlist_add_entry(&bsync_state.submit_queue, s->oper, list);
		}
		fiber_call(s->owner);
	}
	struct trigger *after_rollback = (struct trigger *)
		region_alloc0(&fiber()->gc, sizeof(struct trigger));
	after_rollback->run = txn_reapply_ops;
	after_rollback->data = NULL;
	after_rollback->destroy = NULL;
	after_rollback->link = RLIST_LINK_INITIALIZER;
	trigger_add(&txn->after_rollback, after_rollback);
	return info->result;
}
catch (...) {
	say_crit("bsync_write found unhandled exception");
	throw;
}

static void
bsync_send_data(struct bsync_host_data *host, struct bsync_send_elem *elem)
{
	/* TODO : check all send ops for memory leak case */
	if (host->state == bsync_host_disconnected || !host->fiber_out)
		return;
	assert(elem->code < bsync_mtype_count);
	rlist_add_tail_entry(&host->send_queue, elem, list);
	++host->send_queue_size;
	if ((host->flags & bsync_host_active_write) == 0 &&
		host->fiber_out != fiber())
	{
		fiber_call(host->fiber_out);
		return;
	}
	if (elem->code == bsync_mtype_body ||
		elem->code == bsync_mtype_proxy_accept)
	{
		struct wal_request *req = ((bsync_operation *)elem->arg)->req;
		say_debug("add message %s to send queue %s, lsn=%d:%ld(%ld)",
			bsync_mtype_name[elem->code],
			host->name,
			LAST_ROW(req)->server_id,
			LAST_ROW(req)->lsn,
			((bsync_operation *)elem->arg)->sign);
	} else if (elem->code == bsync_mtype_proxy_request) {
		struct wal_request *req = ((bsync_operation *)elem->arg)->req;
		say_debug("add message %s to send queue %s, lsn=%d:%ld",
			bsync_mtype_name[elem->code],
			host->name,
			LAST_ROW(req)->server_id,
			LAST_ROW(req)->lsn);
	} else {
		say_debug("add message %s to send queue %s",
			bsync_mtype_name[elem->code], host->name);
	}
}

static struct bsync_send_elem *
bsync_alloc_send(uint8_t type)
{
	struct bsync_send_elem *elem = (struct bsync_send_elem *)
		mempool_alloc0(&bsync_state.system_send_pool);
	elem->code = type;
	elem->system = true;
	return elem;
}

static int
bsync_wal_write(struct wal_request *req, uint64_t sign)
{
	if (bsync_state.state == bsync_state_shutdown)
		return -1;
	if (bsync_state.state < bsync_state_recovery)
		say_info("send to WAL %d:%ld (%ld) in state %d",
			 LAST_ROW(req)->server_id, LAST_ROW(req)->lsn,
			 sign, bsync_state.state);
	else if (bsync_state.state == bsync_state_recovery)
		say_debug("send to WAL %d:%ld (%ld) in state %d, row commit is %ld",
			  LAST_ROW(req)->server_id, LAST_ROW(req)->lsn,
			  sign, bsync_state.state, LAST_ROW(req)->commit_sn);
	else
		say_debug("send to WAL %d:%ld (%ld) in state %d, commit is %ld",
			  LAST_ROW(req)->server_id, LAST_ROW(req)->lsn,
			  sign, bsync_state.state, bsync_state.wal_commit_sign);

	if (bsync_state.state >= bsync_state_ready) {
		for (int i = 0; i < req->n_rows; ++i) {
			req->rows[i]->commit_sn = req->rows[i]->rollback_sn = 0;
		}
		if (bsync_state.wal_commit_sign) {
			BSYNC_LOCAL.commit_sign = bsync_state.wal_commit_sign;
			if (sign < bsync_state.wal_commit_sign) {
				LAST_ROW(req)->commit_sn = sign;
			} else {
				LAST_ROW(req)->commit_sn = bsync_state.wal_commit_sign;
				bsync_state.wal_commit_sign = 0;
			}
			say_info("commit to WAL sign %ld", LAST_ROW(req)->commit_sn);
		}
		if (bsync_state.wal_rollback_sign) {
			LAST_ROW(req)->rollback_sn = bsync_state.wal_rollback_sign;
			bsync_state.wal_rollback_sign = 0;
		}
	}
	for (int i = req->n_rows - 1; i >= 0; --i) {
		if (req->rows[i]->type == IPROTO_WAL_FLAG)
			continue;
		vclock_follow(&bsync_state.vclock,
			      req->rows[i]->server_id, req->rows[i]->lsn);
		break;
	}
	int64_t res = wal_write(local_state, req);
	if (bsync_state.wal_push_fiber) {
		fiber_call(bsync_state.wal_push_fiber);
	}
	return res;
}

static void
bsync_wal_system(bool immediately)
{
	if (!immediately) {
		if (bsync_state.wal_push_fiber) {
			fiber_call(bsync_state.wal_push_fiber);
		}
		bsync_state.wal_push_fiber = fiber();
		uint64_t commit_sign = bsync_state.wal_commit_sign;
		fiber_yield_timeout(bsync_state.submit_timeout);
		if (bsync_state.wal_commit_sign != commit_sign) {
			bsync_state.wal_push_fiber = NULL;
			return;
		}
	}
	bsync_state.wal_push_fiber = NULL;
	struct wal_request *req = (struct wal_request *)
		region_alloc0(&fiber()->gc, sizeof(struct wal_request) +
				sizeof(struct xrow_header));
	req->n_rows = 1;
	req->rows[0] = (struct xrow_header *)
		region_alloc0(&fiber()->gc, sizeof(struct xrow_header));
	req->rows[0]->type = IPROTO_WAL_FLAG;
	BSYNC_LOCK(bsync_state.mutex);
	bsync_wal_write(req, bsync_state.wal_commit_sign);
	if (bsync_state.leader_id != bsync_state.local_id)
		return;
	for (int host_id = 0; host_id < bsync_state.num_hosts; ++host_id) {
		if (BSYNC_REMOTE.flags & bsync_host_ping_sleep)
			fiber_call(BSYNC_REMOTE.fiber_out);
	}
}

static void
bsync_rollback_slave(struct bsync_operation *oper, bool retry_commit)
{
	struct bsync_operation *op;
	oper->txn_data->process = txn_process;
	tt_pthread_mutex_lock(&bsync_state.mutex);
	bsync_commit_foreach([retry_commit](struct bsync_operation *oper) {
		if (retry_commit) {
			say_debug("%d:%ld (%ld) saved for repeat",
				  LAST_ROW(oper->req)->server_id,
				  LAST_ROW(oper->req)->lsn, oper->sign);
			oper->txn_data->repeat = true;
		} else
			bsync_status(oper, bsync_op_status_fail);
		return false;
	});
	if (!retry_commit)
		rlist_create(&bsync_state.submit_queue);
	while (!rlist_empty(&bsync_state.proxy_queue)) {
		op = rlist_shift_entry(&bsync_state.proxy_queue,
					struct bsync_operation, list);
		say_debug("failed request %d:%ld(%ld), status is %d",
			  LAST_ROW(op->req)->server_id,
			  LAST_ROW(op->req)->lsn, op->sign, op->status);
		op->txn_data->result = -1;
		bsync_status(op, bsync_op_status_fail);
		if (op->owner != fiber())
			fiber_call(op->owner);
	}
	rlist_create(&bsync_state.proxy_queue);
	if (!retry_commit) {
		while (!rlist_empty(&bsync_state.wal_queue)) {
			op = rlist_shift_entry(&bsync_state.wal_queue,
						struct bsync_operation, list);
			say_debug("failed request %d:%ld(%ld), status is %d",
				  LAST_ROW(op->req)->server_id,
				  LAST_ROW(op->req)->lsn, op->sign, op->status);
			op->txn_data->result = -1;
			bsync_status(op, bsync_op_status_fail);
		}
	}
	bool was_empty = STAILQ_EMPTY(&bsync_state.txn_proxy_queue);
	assert(oper->txn_data->owner);
	STAILQ_INSERT_TAIL(&bsync_state.txn_proxy_queue, oper->txn_data, fifo);
	if (was_empty)
		ev_async_send(txn_loop, &txn_process_event);
	bsync_state.bsync_rollback = true;
	tt_pthread_cond_wait(&bsync_state.cond, &bsync_state.mutex);
	bsync_state.bsync_rollback = false;
	tt_pthread_mutex_unlock(&bsync_state.mutex);
	bsync_send_data(&BSYNC_LEADER, bsync_alloc_send(bsync_mtype_proxy_join));
}

static struct bsync_send_elem *
bsync_slave_wal(struct bsync_operation *oper)
{
	say_debug("[%p] start to apply request  %d:%ld(%ld) to WAL", fiber(),
		LAST_ROW(oper->req)->server_id, LAST_ROW(oper->req)->lsn, oper->sign);
	bsync_status(oper, bsync_op_status_wal);
	rlist_add_tail_entry(&bsync_state.wal_queue, oper, list);
	BSYNC_LOCAL.sign = oper->sign;
	assert(bsync_state.leader_id != BSYNC_MAX_HOSTS);
	int wal_res = bsync_wal_write(oper->txn_data->req, oper->sign);
	assert(oper->status == bsync_op_status_wal ||
		oper->status == bsync_op_status_submit ||
		oper->status == bsync_op_status_fail);
	if (oper->status == bsync_op_status_wal) {
		rlist_shift_entry(&bsync_state.wal_queue,
				struct bsync_operation, list);
	}
	if (oper->status == bsync_op_status_fail)
		return NULL;
	if (oper->sign <= BSYNC_LOCAL.commit_sign)
		bsync_status(oper, bsync_op_status_submit);
	if (wal_res >= 0) {
		if (oper->status != bsync_op_status_submit) {
			if (!rlist_empty(&bsync_state.submit_queue)) {
				assert(oper->sign > rlist_last_entry(
					&bsync_state.submit_queue,
					struct bsync_operation, list)->sign);
			}
			say_debug("submit request %d:%ld (%ld)",
				LAST_ROW(oper->req)->server_id,
				LAST_ROW(oper->req)->lsn, oper->sign);
			assert(rlist_empty(&oper->list));
			rlist_add_tail_entry(&bsync_state.submit_queue, oper, list);
		} else {
			say_debug("commit request %d:%ld(%ld)",
				LAST_ROW(oper->req)->server_id,
				LAST_ROW(oper->req)->lsn, oper->sign);
		}
	} else {
		if (oper->status == bsync_op_status_submit) {
			panic("cant push to WAL submitted operation");
		}
		say_debug("reject request %d:%ld(%ld)",
			LAST_ROW(oper->req)->server_id,
			LAST_ROW(oper->req)->lsn, oper->sign);
	}
	oper->txn_data->result = wal_res;
	struct bsync_send_elem *elem =
		bsync_alloc_send(oper->txn_data->result < 0 ?
				 bsync_mtype_reject : bsync_mtype_submit);
	elem->arg = oper;
	assert(oper->txn_data->result >= 0);
	if (oper->txn_data->result >= 0)
		return elem;
	say_debug("rollback request %d:%ld(%ld)", LAST_ROW(oper->req)->server_id,
		LAST_ROW(oper->req)->lsn, oper->sign);
	bsync_rollback_slave(oper, false);
	if (bsync_state.leader_id == BSYNC_MAX_HOSTS)
		bsync_disconnected(bsync_state.leader_id);
	return elem;
}

static void
bsync_slave_accept(struct bsync_operation *oper, struct bsync_send_elem *elem)
{
	struct bsync_operation *next =
		rlist_first_entry(&bsync_state.accept_queue,
				  struct bsync_operation, accept);
	if (oper != next) {
		uint8_t status = oper->status;
		bsync_status(oper, bsync_op_status_accept_yield);
		fiber_yield();
		if (oper->status == bsync_op_status_fail)
			return;
		if (oper->submit)
			oper->status = bsync_op_status_submit;
		else
			oper->status = status;
	}
	next = rlist_shift_entry(&bsync_state.accept_queue,
				  struct bsync_operation, accept);
	assert(next == oper);
	bsync_send_data(&BSYNC_LEADER, elem);
	if (rlist_empty(&bsync_state.accept_queue))
		return;
	next = rlist_first_entry(&bsync_state.accept_queue,
				  struct bsync_operation, accept);
	if (next->status != bsync_op_status_accept_yield)
		return;
	say_debug("call operation %d:%ld (%ld) from %d:%ld (%ld)",
		  LAST_ROW(next->req)->server_id, LAST_ROW(next->req)->lsn,
		  next->sign, LAST_ROW(oper->req)->server_id,
		  LAST_ROW(oper->req)->lsn, oper->sign);
	fiber_wakeup(next->owner);
}

static void
bsync_slave_finish(struct bsync_operation *oper, bool switch_2_txn)
{
	struct bsync_operation *next = NULL;
	say_debug("finish operation, status=%d", oper->status);
	if (oper->status == bsync_op_status_fail) {
		oper->txn_data->result = -1;
		if (!rlist_empty(&oper->txn))
			rlist_del(&oper->txn);
		if (!rlist_empty(&bsync_state.txn_queue)) {
			next = rlist_first_entry(&bsync_state.txn_queue,
						 struct bsync_operation, txn);
		}
	} else {
		assert (oper->status == bsync_op_status_submit);
		oper->txn_data->result = 1;
		assert(!rlist_empty(&bsync_state.txn_queue));
		next = rlist_first_entry(&bsync_state.txn_queue,
					 struct bsync_operation, txn);
		if (next != oper) {
			say_debug("operation %d:%ld (%ld) wait for %d:%ld (%ld), status is %d",
				  LAST_ROW(oper->req)->server_id, LAST_ROW(oper->req)->lsn,
				  oper->sign, LAST_ROW(next->req)->server_id, LAST_ROW(next->req)->lsn,
				  next->sign, next->status);
			bsync_status(oper, bsync_op_status_submit_yield);
			if (!rlist_empty(&oper->list)) {
				// operation was already added to submit queue
				assert(!rlist_empty(&bsync_state.submit_queue));
				rlist_del_entry(oper, list);
			}
			rlist_add_tail_entry(&bsync_state.commit_queue, oper, list);
			fiber_yield();
			assert(!rlist_empty(&bsync_state.commit_queue));
			rlist_del_entry(oper, list);
		}
		assert(!rlist_empty(&bsync_state.txn_queue));
		next = rlist_shift_entry(&bsync_state.txn_queue,
					  struct bsync_operation, txn);
		assert(next == oper);
		if (rlist_empty(&bsync_state.txn_queue))
			next = NULL;
		else
			next = rlist_first_entry(&bsync_state.txn_queue,
						 struct bsync_operation, txn);
	}
	if (switch_2_txn) {
		SWITCH_TO_TXN(oper->txn_data, txn_process);
	}
	if (next && next->status == bsync_op_status_submit_yield)
		fiber_wakeup(next->owner);
}

static void
bsync_queue_slave(struct bsync_operation *oper)
{
	auto guard = make_scoped_guard([oper]() {
		bsync_free_region(oper->common);
	});
	struct bsync_send_elem *elem = (struct bsync_send_elem *)
		region_alloc0(&fiber()->gc, sizeof(struct bsync_send_elem));
	elem->code = bsync_mtype_proxy_request;
	elem->arg = oper;
	oper->txn_data->result = 0;
	say_debug("start to proceed request %d:%ld(%ld)",
		LAST_ROW(oper->req)->server_id,
		LAST_ROW(oper->req)->lsn, oper->sign);
	rlist_add_tail_entry(&bsync_state.proxy_queue, oper, list);
	bsync_send_data(&BSYNC_LEADER, elem);
	/* wait accept or reject */
	fiber_yield();
	if (oper->status == bsync_op_status_fail) {
		bsync_slave_finish(oper, false);
		return;
	}
	if (oper->txn_data->result < 0) {
		say_warn("request %d:%ld rejected",
			LAST_ROW(oper->req)->server_id,
			LAST_ROW(oper->req)->lsn);
		bsync_rollback_slave(oper, true);
		oper->status = bsync_op_status_fail;
		oper->req = NULL;
		bsync_slave_finish(oper, false);
		return;
	}
	say_debug("request %d:%ld accepted to %ld",
		LAST_ROW(oper->req)->server_id,
		LAST_ROW(oper->req)->lsn, oper->sign);
	oper->txn_data->sign = oper->sign;
	if ((elem = bsync_slave_wal(oper)) != NULL)
		bsync_slave_accept(oper, elem);
	say_debug("request %d:%ld pushed to WAL, status is %d",
		LAST_ROW(oper->req)->server_id,
		LAST_ROW(oper->req)->lsn, oper->status);
	if (oper->status == bsync_op_status_wal) {
		bsync_status(oper, bsync_op_status_yield);
		fiber_yield();
	}
	bsync_slave_finish(oper, true);
	assert(rlist_empty(&oper->list));
}

static void
bsync_proxy_slave(struct bsync_operation *oper)
{
	auto guard = make_scoped_guard([oper]() { bsync_free_region(oper->common); });
	struct bsync_send_elem *elem = bsync_slave_wal(oper);
	if (oper->status == bsync_op_status_fail || oper->txn_data->result < 0)
		return;
	bsync_status(oper, bsync_op_status_txn);
	say_debug("start to apply request %d:%ld (%ld) to TXN",
		LAST_ROW(oper->req)->server_id,
		LAST_ROW(oper->req)->lsn, oper->sign);
	SWITCH_TO_TXN(oper->txn_data, txn_process);
	fiber_yield();
	if (elem)
		bsync_slave_accept(oper, elem);
	say_debug("finish apply request %d:%ld(%ld) to TXN, status is %d",
		LAST_ROW(oper->req)->server_id,
		LAST_ROW(oper->req)->lsn, oper->sign, oper->status);
	if (oper->status == bsync_op_status_fail)
		return;
	rlist_add_tail_entry(&bsync_state.txn_queue, oper, txn);
	if (oper->submit)
		bsync_status(oper, bsync_op_status_submit);
	else {
		assert (oper->status == bsync_op_status_txn);
		bsync_status(oper, bsync_op_status_yield);
		fiber_yield();
	}
	say_info("%d:%ld(%ld), status=%d", LAST_ROW(oper->req)->server_id,
		LAST_ROW(oper->req)->lsn, oper->sign, oper->status);
	bsync_slave_finish(oper, true);
	++bsync_state.bsync_fibers.proxy_yield;
	bsync_status(oper, bsync_op_status_finish_yield);
	bool timeout = fiber_yield_timeout(bsync_state.operation_timeout);
	assert(!timeout);
	--bsync_state.bsync_fibers.proxy_yield;
	if (!rlist_empty(&oper->list)) {
		// operation was already added to submit queue
		assert(!rlist_empty(&bsync_state.submit_queue));
		struct bsync_operation *op =
			rlist_shift_entry(&bsync_state.submit_queue,
					  struct bsync_operation, list);
		assert(op == oper);
	}
}

static void
bsync_wait_slow(struct bsync_operation *oper)
{
	if (2 * oper->accepted > bsync_state.num_hosts) {
		bsync_state.wal_commit_sign = oper->sign;
	}
	while ((oper->accepted + oper->rejected) < bsync_state.num_hosts) {
		bsync_status(oper, bsync_op_status_yield);
		fiber_yield();
		assert(oper->status != bsync_op_status_fail);
	}
	for (uint8_t host_id = 0; host_id < bsync_state.num_hosts; ++host_id) {
		if (BSYNC_REMOTE.commit_sign == BSYNC_LOCAL.commit_sign ||
			bsync_state.local_id == host_id ||
			(BSYNC_REMOTE.flags & bsync_host_active_write) != 0)
		{
			continue;
		}
		if (BSYNC_REMOTE.fiber_out && !fiber_checkstack() &&
		    BSYNC_REMOTE.state > bsync_host_disconnected)
			fiber_call(BSYNC_REMOTE.fiber_out);
	}
	if (bsync_state.wal_commit_sign == oper->sign)
		bsync_wal_system(false);
	say_debug("finish request %d:%ld, acc/rej: %d/%d",
		LAST_ROW(oper->req)->server_id,
		LAST_ROW(oper->req)->lsn, oper->accepted, oper->rejected);
}

static void
bsync_leader_cleanup_followed(uint8_t host_id, struct bsync_operation *oper)
{
	struct rlist queue;
	size_t queue_size = 0;
	rlist_create(&queue);
	struct bsync_send_elem *elem = NULL;
	if (oper) {
		while(!rlist_empty(&BSYNC_REMOTE.follow_queue)) {
			elem = rlist_shift_entry(&BSYNC_REMOTE.follow_queue,
						 struct bsync_send_elem, list);
			if (oper->sign >= ((struct bsync_operation *)elem->arg)->sign)
				break;
			rlist_add_tail_entry(&queue, elem, list);
			++queue_size;
		}
	}
	while(!rlist_empty(&BSYNC_REMOTE.follow_queue)) {
		elem = rlist_shift_entry(&BSYNC_REMOTE.follow_queue,
					 struct bsync_send_elem, list);
		bsync_do_reject(host_id, elem);
	}
	rlist_swap(&BSYNC_REMOTE.follow_queue, &queue);
	BSYNC_REMOTE.follow_queue_size = queue_size;
}

static void
bsync_check_follow(uint8_t host_id)
{
	struct bsync_send_elem *elem = NULL;
	if (BSYNC_REMOTE.follow_queue_size < bsync_state.max_host_queue)
		return;
	ssize_t remove_count = BSYNC_REMOTE.follow_queue_size -
					bsync_state.max_host_queue;
	for (ssize_t i = 0; i < remove_count; ++i) {
		elem = rlist_shift_entry(&BSYNC_REMOTE.follow_queue,
					struct bsync_send_elem, list);
		--BSYNC_REMOTE.follow_queue_size;
		bsync_do_reject(host_id, elem);
	}
}

static void
bsync_check_slow(uint8_t host_id)
{
	if (BSYNC_REMOTE.state == bsync_host_follow) {
		bsync_check_follow(host_id);
		return;
	}
	struct bsync_send_elem *elem = NULL;
	ssize_t queue_size =
		BSYNC_REMOTE.send_queue_size + BSYNC_REMOTE.op_queue_size;
	if (queue_size <= bsync_state.max_host_queue ||
		bsync_state.max_host_queue <= 0)
	{
		return;
	}
	while (!rlist_empty(&BSYNC_REMOTE.op_queue)) {
		elem = rlist_shift_entry(&BSYNC_REMOTE.op_queue,
					 bsync_send_elem, list);
		bsync_do_reject(host_id, elem);
	}
	while (!rlist_empty(&BSYNC_REMOTE.send_queue)) {
		elem = rlist_shift_entry(&BSYNC_REMOTE.send_queue,
					 bsync_send_elem, list);
		if (elem->code == bsync_mtype_body ||
			elem->code == bsync_mtype_proxy_accept)
		{
			bsync_do_reject(host_id, elem);
		}
		if (elem->system)
			mempool_free(&bsync_state.system_send_pool, elem);
	}
	BSYNC_REMOTE.op_queue_size = 0;
	BSYNC_REMOTE.send_queue_size = 0;
	if (BSYNC_REMOTE.state == bsync_host_connected) {
		say_info("host %s detected as slow, disconnecting", BSYNC_REMOTE.name);
		bsync_disconnected(host_id);
	}
}

static void
bsync_leader_cleanup_connected(uint8_t host_id, struct bsync_operation *oper)
{
	struct rlist queue;
	size_t queue_size = 0;
	rlist_create(&queue);
	struct bsync_send_elem *elem = NULL;
	uint64_t sign = 0;
	while(!rlist_empty(&BSYNC_REMOTE.op_queue)) {
		elem = rlist_shift_entry(&BSYNC_REMOTE.op_queue,
					 struct bsync_send_elem, list);
		if (oper->sign <= ((struct bsync_operation *)elem->arg)->sign)
			break;
		sign = ((struct bsync_operation *)elem->arg)->sign;
		rlist_add_tail_entry(&queue, elem, list);
		++queue_size;
	}
	say_info("op queue shifted");
	while(!rlist_empty(&BSYNC_REMOTE.op_queue)) {
		elem = rlist_shift_entry(&BSYNC_REMOTE.op_queue,
					 struct bsync_send_elem, list);
		assert(sign < ((struct bsync_operation *)elem->arg)->sign);
		sign = ((struct bsync_operation *)elem->arg)->sign;
		bsync_do_reject(host_id, elem);
	}
	say_info("op queue cleanuped");
	rlist_swap(&BSYNC_REMOTE.op_queue, &queue);
	BSYNC_REMOTE.op_queue_size = queue_size;
	while(!rlist_empty(&BSYNC_REMOTE.send_queue)) {
		elem = rlist_shift_entry(&BSYNC_REMOTE.send_queue,
					 struct bsync_send_elem, list);
		if (elem->code == bsync_mtype_body ||
			elem->code == bsync_mtype_proxy_accept)
		{
			assert(oper->sign < ((struct bsync_operation *)elem->arg)->sign);
			bsync_do_reject(host_id, elem);
		}
		if (elem->system)
			mempool_free(&bsync_state.system_send_pool, elem);
	}
	say_info("send queue cleanuped");
	BSYNC_REMOTE.send_queue_size = 0;
	elem = bsync_alloc_send(bsync_mtype_rollback);
	elem->arg = (void*)oper->sign;
	bsync_send_data(&BSYNC_REMOTE, elem);
}

static void
bsync_leader_rollback(struct bsync_operation *oper)
{
	say_info("start leader rollback from %d:%ld (%ld), num_connected is %d",
		LAST_ROW(oper->req)->server_id,
		LAST_ROW(oper->req)->lsn, oper->sign, bsync_state.num_connected);
	tt_pthread_mutex_lock(&bsync_state.mutex);
	bsync_commit_foreach([oper](struct bsync_operation *op) {
		if (op->sign >= oper->sign) {
			say_debug("failed request %d:%ld(%ld), status is %d",
				  LAST_ROW(op->req)->server_id,
				  LAST_ROW(op->req)->lsn, op->sign, op->status);
			uint8_t status = op->status;
			bsync_status(op, bsync_op_status_fail);
			op->txn_data->result = -1;
			if (status == bsync_op_status_yield)
				fiber_call(op->owner);
		}
		return false;
	});
	say_info("commit queue cleanuped");
	rlist_create(&bsync_state.submit_queue);
	struct bsync_operation *op;
	while (!rlist_empty(&bsync_state.election_ops)) {
		op = rlist_shift_entry(&bsync_state.election_ops,
					struct bsync_operation, list);
		say_debug("failed request %d:%ld, status is %d",
			  LAST_ROW(op->txn_data->req)->server_id,
			  LAST_ROW(op->txn_data->req)->lsn, op->status);
		op->txn_data->result = -1;
		bsync_status(op, bsync_op_status_fail);
		fiber_call(op->owner);
	}
	say_info("election queue cleanuped");
	while (!rlist_empty(&bsync_state.wal_queue)) {
		op = rlist_shift_entry(&bsync_state.wal_queue,
					struct bsync_operation, list);
		if (op->sign < oper->sign)
			continue;
		say_debug("failed request %d:%ld(%ld), status is %d",
			  LAST_ROW(op->req)->server_id,
			  LAST_ROW(op->req)->lsn, op->sign, op->status);
		op->txn_data->result = -1;
		bsync_status(op, bsync_op_status_fail);
	}
	say_info("wal queue cleanuped");
	bsync_state.wal_rollback_sign = oper->sign;
	for (uint8_t host_id = 0; host_id < bsync_state.num_hosts; ++host_id) {
		if (host_id == bsync_state.local_id ||
			BSYNC_REMOTE.state < bsync_host_follow)
			continue;
		if (BSYNC_REMOTE.state == bsync_host_connected)
			bsync_leader_cleanup_connected(host_id, oper);
		else
			bsync_leader_cleanup_followed(host_id, oper);
	}
	oper->txn_data->process = txn_process;
	uint8_t state = bsync_state.state;
	bool was_empty = STAILQ_EMPTY(&bsync_state.txn_proxy_queue);
	assert(oper->txn_data->owner);
	STAILQ_INSERT_TAIL(&bsync_state.txn_proxy_queue, oper->txn_data, fifo);
	if (was_empty)
		ev_async_send(txn_loop, &txn_process_event);
	tt_pthread_cond_wait(&bsync_state.cond, &bsync_state.mutex);
	bsync_state.state = state;
	tt_pthread_mutex_unlock(&bsync_state.mutex);
}

#define BSYNC_SEND_2_BSYNC {\
	bool was_empty = STAILQ_EMPTY(&bsync_state.bsync_proxy_input); \
	STAILQ_INSERT_TAIL(&bsync_state.bsync_proxy_input, oper->txn_data, fifo); \
	if (was_empty) \
		ev_async_send(loop(), &bsync_process_event); \
}

static void
bsync_inc_rejected(uint8_t host_id, struct bsync_operation *oper, const char *reason)
{
	assert (host_id != bsync_state.local_id || bsync_state.local_id != bsync_state.leader_id);
	++oper->rejected;
	say_info("reject operation %d:%ld (%ld) for host %s by reason '%s', status is %d, state: %d/%d",
		 LAST_ROW(oper->req)->server_id, LAST_ROW(oper->req)->lsn,
		 oper->sign, BSYNC_REMOTE.name, reason, oper->status,
		 oper->accepted, oper->rejected);
	for (int i = 0; i < oper->req->n_rows; ++i)
		bsync_op_end(host_id, oper->common->dup_key[i], oper->host_id);
}

static void
bsync_queue_leader(struct bsync_operation *oper, bool proxy)
{
	if (!proxy) {
		oper->req = (struct wal_request *)
			region_alloc0(&fiber()->gc, sizeof(struct wal_request) +
				sizeof(xrow_header) * oper->txn_data->req->n_rows);
		memcpy(oper->req, oper->txn_data->req, sizeof(struct wal_request));
		for (int i = 0; i < oper->req->n_rows; ++i) {
			oper->req->rows[i] = (struct xrow_header *)
				region_alloc0(&fiber()->gc, sizeof(struct xrow_header));
			xrow_copy(oper->txn_data->req->rows[i], oper->req->rows[i]);
		}
	}
	BSYNC_LOCAL.sign = oper->sign;
	say_debug("start to proceed request %d:%ld, sign is %ld",
		LAST_ROW(oper->req)->server_id, LAST_ROW(oper->req)->lsn, oper->sign);
	oper->rejected = oper->accepted = 0;
	if (bsync_state.active_ops > (MAX_UNCOMMITED_REQ - 1)) {
		rlist_add_tail_entry(&bsync_state.wait_queue, oper, list);
		fiber_yield();
		if (oper->status == bsync_op_status_fail) {
			oper->txn_data->result = -1;
			bsync_free_region(oper->common);
			SWITCH_TO_TXN(oper->txn_data, txn_process);
			return;
		}
	}
	++bsync_state.active_ops;
	for (uint8_t host_id = 0; host_id < bsync_state.num_hosts; ++host_id) {
		if (host_id == bsync_state.local_id ||
			BSYNC_REMOTE.state < bsync_host_follow)
		{
			if (host_id != bsync_state.local_id)
				bsync_inc_rejected(host_id, oper, "disconnected");
			continue;
		}
		struct bsync_send_elem *elem = (struct bsync_send_elem *)
			region_alloc0(&fiber()->gc, sizeof(struct bsync_send_elem));
		elem->arg = oper;
		if (oper->host_id == host_id)
			elem->code = bsync_mtype_proxy_accept;
		else
			elem->code = bsync_mtype_body;
		if (BSYNC_REMOTE.state == bsync_host_connected) {
			bsync_send_data(&BSYNC_REMOTE, elem);
		} else {
			rlist_add_tail_entry(&BSYNC_REMOTE.follow_queue,
					     elem, list);
			++BSYNC_REMOTE.follow_queue_size;
		}
		bsync_check_slow(host_id);
	}
	bsync_status(oper, bsync_op_status_wal);
	assert(bsync_state.local_id == bsync_state.leader_id);
	assert(rlist_empty(&oper->list));
	rlist_add_tail_entry(&bsync_state.submit_queue, oper, list);
	oper->txn_data->result = bsync_wal_write(oper->req, oper->sign);
	if (oper->txn_data->result < 0)
		bsync_inc_rejected(bsync_state.local_id, oper, "WAL");
	else {
		++oper->accepted;
		say_debug("submit operation %d:%ld for host %s",
			  LAST_ROW(oper->req)->server_id, LAST_ROW(oper->req)->lsn,
			  BSYNC_LOCAL.name);
	}
	assert(oper->status == bsync_op_status_wal);
	bsync_status(oper, bsync_op_status_yield);
	while (2 * oper->accepted <= bsync_state.num_hosts &&
		oper->status == bsync_op_status_yield &&
		2 * oper->rejected < bsync_state.num_hosts)
	{
		if (fiber_yield_timeout(bsync_state.operation_timeout))
			break;
	}
	--bsync_state.active_ops;
	if (!rlist_empty(&bsync_state.wait_queue)) {
		fiber_wakeup(
			rlist_shift_entry(&bsync_state.wait_queue,
				bsync_operation, list)->owner
		);
	}
	if (oper->status != bsync_op_status_fail)
		oper->txn_data->result =
			(2 * oper->accepted > bsync_state.num_hosts ? 0 : -1);
	else
		assert(oper->txn_data->result < 0);
	say_info("%d:%ld(%ld) result: num_accepted=%d, num_rejected=%d",
		 LAST_ROW(oper->req)->server_id, LAST_ROW(oper->req)->lsn,
		 oper->sign, oper->accepted, oper->rejected);
	if (oper->txn_data->result < 0) {
		if (oper->status != bsync_op_status_fail) {
			rlist_del_entry(oper, list);
			bsync_leader_rollback(oper);
		}
	} else {
		rlist_del_entry(oper, list);
		oper->submit = true;
		bsync_status(oper, bsync_op_status_txn);
		if (proxy) {
			SWITCH_TO_TXN(oper->txn_data, txn_process);
			fiber_yield();
		} else {
			oper->txn_data->oper = NULL;
			SWITCH_TO_TXN(oper->txn_data, txn_process);
		}
	}
	bsync_wait_slow(oper);
	bsync_free_region(oper->common);
	say_debug("finish request %d:%ld", LAST_ROW(oper->req)->server_id,
		  LAST_ROW(oper->req)->lsn);
	assert((oper->accepted + oper->rejected) == bsync_state.num_hosts ||
		oper->status == bsync_op_status_fail);
}

static void
bsync_cleanup_queue(struct bsync_fifo *queue, uint8_t host_id)
{
	struct bsync_txn_info *info;
	struct bsync_fifo tmp_queue;
	STAILQ_INIT(&tmp_queue);
	while (!STAILQ_EMPTY(queue)) {
		info = STAILQ_FIRST(queue);
		STAILQ_REMOVE_HEAD(queue, fifo);
		if (info->oper && info->oper->host_id == host_id) {
			say_info("drop %d:%ld from %s from txn queue, status is %d %p",
				 LAST_ROW(info->oper->req)->server_id,
				 LAST_ROW(info->oper->req)->lsn,
				 BSYNC_REMOTE.name, info->oper->status,
				 info->oper->req);
			assert(info->oper->txn_data == info);
			info->result = -1;
			bsync_status(info->oper, bsync_op_status_fail);
			assert(info->oper->txn_data->result < 0);
			fiber_call(info->oper->owner);
		} else {
			STAILQ_INSERT_TAIL(&tmp_queue, info, fifo);
		}
	}
	STAILQ_CONCAT(queue, &tmp_queue);
}

static void
bsync_proxy_leader(struct bsync_operation *oper)
{
	uint8_t host_id = oper->host_id;
	say_debug("start to apply request %d:%ld to TXN",
		  LAST_ROW(oper->req)->server_id, LAST_ROW(oper->req)->lsn);
	bsync_status(oper, bsync_op_status_txn);
	SWITCH_TO_TXN(oper->txn_data, txn_process);
	fiber_yield();
	if (oper->txn_data->result >= 0) {
		oper->sign = oper->txn_data->sign;
		assert(oper->status != bsync_op_status_fail);
		oper->common->dup_key = oper->txn_data->common->dup_key;
		oper->common->region = oper->txn_data->common->region;
		bsync_queue_leader(oper, true);
		return;
	}
	if (BSYNC_REMOTE.flags & bsync_host_rollback) {
		say_debug("%d:%ld ignored for rolled back host",
			  LAST_ROW(oper->req)->server_id,
			  LAST_ROW(oper->req)->lsn);
		return;
	}
	say_debug("%d:%ld rolled back, host %s marked as bad",
		  LAST_ROW(oper->req)->server_id,
		  LAST_ROW(oper->req)->lsn, BSYNC_REMOTE.name);
	BSYNC_REMOTE.flags |= bsync_host_rollback;
	/* drop all active operations from host */
	tt_pthread_mutex_lock(&bsync_state.mutex);

	bsync_cleanup_queue(&bsync_state.txn_proxy_input, oper->host_id);
	bsync_cleanup_queue(&bsync_state.txn_proxy_queue, oper->host_id);

	tt_pthread_cond_signal(&bsync_state.cond);
	tt_pthread_mutex_unlock(&bsync_state.mutex);
	bsync_send_data(&BSYNC_REMOTE, bsync_alloc_send(bsync_mtype_proxy_reject));
}

static void
bsync_proxy_processor()
{
	uint8_t host_id = bsync_state.proxy_host;
	bsync_state.proxy_host = BSYNC_MAX_HOSTS;
	struct bsync_operation *oper = (struct bsync_operation *)
		region_alloc0(&fiber()->gc, sizeof(struct bsync_operation));
	struct wal_request *req = (struct wal_request *)
		region_alloc(&fiber()->gc, sizeof(struct wal_request)+
			sizeof(struct xrow_header) * BSYNC_REMOTE.proxy_size);
	req->n_rows = BSYNC_REMOTE.proxy_size;
	for (int i = 0; i < req->n_rows; ++i)
		req->rows[i] = BSYNC_REMOTE.proxy_rows[i];
	auto guard = make_scoped_guard([&]() {
		for (size_t i = 0; i < req->n_rows; ++i) {
			if (req->rows[i]->bodycnt > 0) {
				assert(req->rows[i]->bodycnt == 1);
				void *data = req->rows[i]->body[0].iov_base;
				slab_put(&cord()->slabc, slab_from_data(data));
			}
			mempool_free(&BSYNC_REMOTE.proxy_pool, req->rows[i]);
		}
		oper->owner = NULL;
		assert(rlist_empty(&oper->accept));
		assert(rlist_empty(&oper->txn));
		assert(rlist_empty(&oper->list));
	});
	oper->txn_data = bsync_alloc_txn_info(req, true);
	oper->common = oper->txn_data->common;
	oper->txn_data->oper = oper;
	oper->txn_data->owner = NULL;
	oper->req = oper->txn_data->req;
	rlist_create(&oper->txn);
	rlist_create(&oper->list);
	rlist_create(&oper->accept);
	say_debug("try to proxy %d:%ld", LAST_ROW(oper->req)->server_id,
		  LAST_ROW(oper->req)->lsn);
	oper->host_id = host_id;
	if (BSYNC_REMOTE.proxy_sign)
		oper->sign = BSYNC_REMOTE.proxy_sign;
	oper->owner = fiber();
	bsync_status(oper, bsync_op_status_proxy);
	if (bsync_state.leader_id != bsync_state.local_id) {
		rlist_add_tail_entry(&bsync_state.accept_queue, oper, accept);
	}
	BSYNC_SEND_2_BSYNC
	fiber_yield();
	say_debug("start to proxy %d:%ld", LAST_ROW(oper->req)->server_id,
		  LAST_ROW(oper->req)->lsn);
	if (oper->status == bsync_op_status_fail) {
		say_debug("failed %d:%ld from %d", LAST_ROW(oper->req)->server_id,
			  LAST_ROW(oper->req)->lsn, fiber()->caller->fid);
		return;
	}
	bsync_status(oper, bsync_op_status_init);
	if (bsync_state.leader_id != bsync_state.local_id)
		bsync_proxy_slave(oper);
	else {
		if (oper->txn_data->result < 0) {
			assert(BSYNC_REMOTE.flags & bsync_host_rollback);
			return;
		}
		bsync_proxy_leader(oper);
	}
	say_debug("finish proxy %d:%ld", LAST_ROW(oper->req)->server_id,
		  LAST_ROW(oper->req)->lsn);
}

static void
bsync_cleanup_proxy(uint8_t host_id)
{
	BSYNC_REMOTE.proxy_cur = BSYNC_REMOTE.proxy_size =
		BSYNC_REMOTE.proxy_sign = 0;
	slab_put(&cord()->slabc, slab_from_data(BSYNC_REMOTE.proxy_rows));
	BSYNC_REMOTE.proxy_rows = NULL;
}

static void
bsync_decode_proxy(uint8_t host_id, const char **ipos, const char *iend)
{
	struct xrow_header *xrow = BSYNC_REMOTE.proxy_rows[BSYNC_REMOTE.proxy_cur];
	xrow_header_decode(xrow, ipos, iend);
	say_debug("decode proxy row %d:%ld", xrow->server_id, xrow->lsn);
	if (xrow->bodycnt) {
		assert(xrow->bodycnt == 1);
		struct slab *slab = slab_get(&cord()->slabc, xrow->body[0].iov_len);
		if (slab == NULL)
			panic("cant alloc slab for proxy request");
		void *data = slab_data(slab);
		memcpy(data, xrow->body[0].iov_base, xrow->body[0].iov_len);
		xrow->body[0].iov_base = data;
	}
	assert(*ipos == iend);
	if (++BSYNC_REMOTE.proxy_cur < BSYNC_REMOTE.proxy_size)
		return;
	bsync_state.proxy_host = host_id;
	++bsync_state.bsync_fibers.active_proxy;
	fiber_call(bsync_fiber(&bsync_state.bsync_fibers, bsync_process_fiber));
	bsync_cleanup_proxy(host_id);
	say_debug("cleanup proxy row %d:%ld", xrow->server_id, xrow->lsn);
}

/*
 * Command handlers block
 */

static void
bsync_body(uint8_t host_id, const char **ipos, const char *iend)
{
	if (bsync_state.state == bsync_state_shutdown) {
		*ipos = iend;
		return;
	}
	assert(host_id == bsync_state.leader_id);
	if (BSYNC_REMOTE.proxy_cur == (BSYNC_REMOTE.proxy_size - 1))
		BSYNC_REMOTE.proxy_sign = mp_decode_uint(ipos);

	bsync_decode_proxy(host_id, ipos, iend);
}

static void
bsync_do_submit(uint8_t host_id, struct bsync_send_elem *info)
{
	struct bsync_operation *oper = (struct bsync_operation *)info->arg;
	++oper->accepted;
	say_debug("submit operation %d:%ld for host %s",
		  LAST_ROW(oper->req)->server_id, LAST_ROW(oper->req)->lsn,
		  BSYNC_REMOTE.name);
	for (int i = 0; i < oper->req->n_rows; ++i)
		bsync_op_end(host_id, oper->common->dup_key[i], oper->host_id);
	if (oper->status == bsync_op_status_yield)
		fiber_call(oper->owner);
}

static void
bsync_submit(uint8_t host_id, const char **ipos, const char *iend)
{
	uint64_t sign = mp_decode_uint(ipos);
	assert(*ipos == iend);
	assert(!rlist_empty(&BSYNC_REMOTE.op_queue));
	--BSYNC_REMOTE.op_queue_size;
	struct bsync_send_elem *info =
		rlist_shift_entry(&BSYNC_REMOTE.op_queue,
				  struct bsync_send_elem, list);
	assert(((struct bsync_operation*)info->arg)->sign == sign);
	bsync_do_submit(host_id, info);
}

static void
bsync_do_reject(uint8_t host_id, struct bsync_send_elem *info)
{
	struct bsync_operation *oper = (struct bsync_operation *)info->arg;
	bsync_inc_rejected(host_id, oper, "rejected");
	if (oper->status == bsync_op_status_yield && oper->owner != fiber())
		fiber_call(oper->owner);
	else
		say_debug("owner call for reject delayed, owner is %d, status %d",
			  oper->owner->fid, oper->status);
}

static void
bsync_reject(uint8_t host_id, const char **ipos, const char *iend)
{
	uint64_t sign = mp_decode_uint(ipos);
	assert(*ipos == iend);
	assert(!rlist_empty(&BSYNC_REMOTE.op_queue));
	--BSYNC_REMOTE.op_queue_size;
	struct bsync_send_elem *info =
		rlist_shift_entry(&BSYNC_REMOTE.op_queue,
				  struct bsync_send_elem, list);
	assert(((struct bsync_operation*)info->arg)->sign == sign);
	bsync_do_reject(host_id, info);
}

static void
bsync_proxy_request(uint8_t host_id, const char **ipos, const char *iend)
{
	if ((BSYNC_REMOTE.flags & bsync_host_rollback) != 0 ||
		bsync_state.state != bsync_state_ready)
	{
		/*
		 * skip all proxy requests from node in conflict state or
		 * if we lost consensus
		 */
		if (BSYNC_REMOTE.proxy_rows)
			bsync_cleanup_proxy(host_id);
		*ipos = iend;
	} else
		bsync_decode_proxy(host_id, ipos, iend);
}

static void
bsync_proxy_accept(uint8_t host_id, const char **ipos, const char *iend)
{
	assert(!rlist_empty(&bsync_state.proxy_queue));
	assert(host_id == bsync_state.leader_id);
	struct bsync_operation *oper = rlist_shift_entry(&bsync_state.proxy_queue,
							 bsync_operation, list);
	oper->sign = mp_decode_uint(ipos);
	say_debug("accept proxy operation %d:%ld to %ld",
		  LAST_ROW(oper->req)->server_id,
		  LAST_ROW(oper->req)->lsn, oper->sign);
	bsync_status(oper, bsync_op_status_proxy);
	assert(*ipos == iend);
	rlist_add_tail_entry(&bsync_state.accept_queue, oper, accept);
	BSYNC_SEND_2_BSYNC
}

static void
bsync_do_proxy_reject(struct bsync_operation *op)
{
	op->txn_data->result = -1;
	fiber_call(op->owner);
}

static void
bsync_proxy_reject(uint8_t host_id, const char **ipos, const char *iend)
{
	(void) host_id; (void) ipos; (void) iend;
	assert(!rlist_empty(&bsync_state.proxy_queue));
	struct bsync_operation *op =
		rlist_shift_entry(&bsync_state.proxy_queue,
				  bsync_operation, list);
	assert(*ipos == iend);
	bsync_do_proxy_reject(op);
}

static void
bsync_proxy_join(uint8_t host_id, const char **ipos, const char *iend)
{
	*ipos = iend;
	BSYNC_REMOTE.flags &= ~bsync_host_rollback;
}

static void
bsync_commit(uint64_t commit_sign)
{
	struct bsync_operation *oper = NULL;
	uint64_t op_sign = 0;
	bsync_state.wal_commit_sign = commit_sign;
	do {
		if (rlist_empty(&bsync_state.submit_queue)) {
			if (rlist_empty(&bsync_state.wal_queue))
				break;
			oper = rlist_shift_entry(&bsync_state.wal_queue,
						bsync_operation, list);
		} else
			oper = rlist_shift_entry(&bsync_state.submit_queue,
						bsync_operation, list);
		op_sign = oper->sign;
		say_debug("commit operation %d:%ld, %ld < %ld, status is %d",
			LAST_ROW(oper->req)->server_id,
			LAST_ROW(oper->req)->lsn, op_sign, commit_sign,
			oper->status);
		bool yield = (oper->status == bsync_op_status_yield);
		assert(oper->status != bsync_op_status_fail);
		if (oper->status != bsync_op_status_fail) {
			oper->submit = true;
			switch(oper->status) {
			case bsync_op_status_txn:
			case bsync_op_status_accept_yield:
				break;
			default:
				bsync_status(oper, bsync_op_status_submit);
				break;
			}
		}
		if (yield)
			fiber_call(oper->owner);
	} while (op_sign < commit_sign);
}

static void
bsync_rollback(uint8_t host_id, const char **ipos, const char *iend)
{
	(void) host_id; (void) iend;
	assert(bsync_state.local_id != bsync_state.leader_id);
	uint64_t sign = mp_decode_uint(ipos);
	struct bsync_operation *oper = NULL;
	rlist_foreach_entry(oper, &bsync_state.submit_queue, list) {
		if (oper->sign != sign)
			continue;
		break;
	}
	assert(oper != NULL);
	bsync_state.wal_rollback_sign = sign;
	bsync_rollback_slave(oper, false);
}

/*
 * Fibers block
 */

static void
bsync_election_ops();

static void
txn_parse_request(struct bsync_txn_info *info, int i, struct request *req)
{
	request_create(req, info->req->rows[i]->type);
	request_decode(req, (const char*) info->req->rows[i]->body[0].iov_base,
		info->req->rows[i]->body[0].iov_len);
	req->header = info->req->rows[i];
}

static void
txn_space_cb(void *d, uint8_t key, uint32_t v)
{
	if (key == IPROTO_SPACE_ID)
		((struct bsync_parse_data *) d)->space_id = v;
}

static void
txn_tuple_cb(void *d, uint8_t key, const char *v, const char *v_end)
{
	if (key == IPROTO_KEY || key == IPROTO_FUNCTION_NAME ||
		key == IPROTO_USER_NAME || key == IPROTO_EXPR)
	{
		((struct bsync_parse_data *) d)->key = v;
		((struct bsync_parse_data *) d)->key_end = v_end;
	}
	if (key == IPROTO_TUPLE)
		((struct bsync_parse_data *) d)->is_tuple = true;
	((struct bsync_parse_data *) d)->data = v;
	((struct bsync_parse_data *) d)->end = v_end;
}

static bool
txn_verify_leader(struct bsync_txn_info *info)
{
	struct bsync_parse_data data;
	int i = 0;
	info->common->region = bsync_new_region();
	info->common->dup_key = (struct bsync_key **)
		region_alloc(&info->common->region->pool,
			sizeof(struct bsync_key *) * info->req->n_rows);
	for (; i < info->req->n_rows; ++i) {
		memset(&data, 0, sizeof(data));
		request_header_decode(info->req->rows[i], txn_space_cb,
					txn_tuple_cb, &data);
		struct tuple *tuple = NULL;
		struct space *space = space_cache_find(data.space_id);
		assert(space && space->index_count > 0 &&
			space->index[0]->key_def->iid == 0);
		if (data.is_tuple) {
			tuple = tuple_new(space->format, data.data, data.end);
			space_validate_tuple(space, tuple);
		} else {
			const char *key = data.key;
			uint32_t part_count = mp_decode_array(&key);
			tuple = space->index[0]->findByKey(key, part_count);
		}
		if (!tuple)
			goto error;

		bsync_parse_dup_key(info->common, space->index[0]->key_def, tuple, i);
		TupleGuard guard(tuple);
		if (!bsync_op_begin(info->common->dup_key[i], info->oper->host_id))
			goto error;
	}
	return true;
error:
	say_error("conflict in operation from %s, last row is %d:%ld",
		  bsync_index[info->oper->host_id].name,
		  LAST_ROW(info->req)->server_id, LAST_ROW(info->req)->lsn);
	for (; i >= 0; --i)
		for (int h = 0; h < bsync_state.num_hosts; ++h)
			bsync_op_end(h, info->common->dup_key[i], info->oper->host_id);
	info->result = -1;
	region_free(&info->common->region->pool);
	rlist_add_entry(&bsync_state.region_free, info->common->region, list);
	tt_pthread_mutex_lock(&bsync_state.mutex);
	SWITCH_TO_BSYNC_UNSAFE(bsync_process);
	tt_pthread_cond_wait(&bsync_state.cond, &bsync_state.mutex);
	tt_pthread_mutex_unlock(&bsync_state.mutex);
	return false;
}

static bool
txn_queue_leader(struct bsync_txn_info *info)
{
	if (!txn_verify_leader(info))
		return false;
	txn_begin(info->req->n_rows == 1);
	struct request req;
	rlist_add_tail_entry(&txn_state.txn_queue, info, list);
	for (int i = 0; i < info->req->n_rows; ++i) {
		txn_parse_request(info, i, &req);
		assert(!box_is_ro());
		try {
			box_process(&req, &null_port);
		} catch (LoggedError *e) {
			if (e->errcode() != ER_WAL_IO) {
				panic("Leader cant proceed verified operation");
			}
		}
	}
	if (info->req->n_rows > 1)
		txn_commit(in_txn());
	return true;
}

static void
txn_process_slave(struct bsync_txn_info *info)
{
	say_debug("send %d:%ld (%ld) to TXN, status is %d", LAST_ROW(info->req)->server_id,
		  LAST_ROW(info->req)->lsn, info->oper->sign, info->oper->status);
	txn_begin(info->req->n_rows == 1);
	struct request req;
	rlist_add_tail_entry(&txn_state.txn_queue, info, list);
	for(int i = 0; i < info->req->n_rows; ++i) {
		txn_parse_request(info, i, &req);
		assert(!box_is_ro());
		box_process(&req, &null_port);
	}
	if (info->req->n_rows > 1)
		txn_commit(in_txn());
	say_debug("done %d:%ld (%ld) in TXN", LAST_ROW(info->req)->server_id,
		  LAST_ROW(info->req)->lsn, info->oper->sign);
	assert (rlist_empty(&info->list) || info->repeat);
}

static bool
txn_queue_slave(struct bsync_txn_info *info)
{
	if (info->result < 0) {
		say_debug("send %d:%ld (%ld) back to BSYNC",
			  LAST_ROW(info->req)->server_id,
			  LAST_ROW(info->req)->lsn, info->oper->sign);
		SWITCH_TO_BSYNC(bsync_process);
		return false;
	}
	do {
		try {
			txn_process_slave(info);
			return true;
		} catch (Exception *e) {
			if (txn_state.state == txn_state_subscribe)
				return true;
			txn_rollback();
			assert(info->repeat);
			if (!info->repeat) {
				panic("Slave cant proceed submited operation, reason: %s",
					e->errmsg());
			}
			say_debug("cant proceed operation %d:%ld, reason: %s",
				LAST_ROW(info->req)->server_id,
				LAST_ROW(info->req)->lsn, e->errmsg());
		} catch (...) {
			panic("Receive unknown exception");
		}
		say_debug("restart %d:%ld (%ld) in TXN",
			  LAST_ROW(info->req)->server_id,
			  LAST_ROW(info->req)->lsn, info->oper->sign);
		if (txn_state.state != txn_state_rollback)
			rlist_del_entry(info, list);
		info->owner = fiber();
		fiber_yield();
	} while (true);
	return true;
}

static void
txn_process_fiber(va_list /* ap */)
{
restart:
	struct bsync_txn_info *info = STAILQ_FIRST(
		&bsync_state.txn_proxy_input);
	STAILQ_REMOVE_HEAD(&bsync_state.txn_proxy_input, fifo);
	say_debug("receive %d:%ld (%ld) in TXN",
		  LAST_ROW(info->req)->server_id,
		  LAST_ROW(info->req)->lsn, info->oper->sign);
	bool do_switch = true;
	if (txn_state.leader_id == txn_state.local_id)
		do_switch = txn_queue_leader(info);
	else
		do_switch = txn_queue_slave(info);
	if (do_switch && txn_state.state != txn_state_subscribe) {
		SWITCH_TO_BSYNC(bsync_process);
	}

	assert(bsync_state.txn_fibers.active > 0);
	--bsync_state.txn_fibers.active;
	rlist_add_tail_entry(&bsync_state.txn_fibers.data, fiber(), state);
	fiber_yield();
	goto restart;
}

static void
bsync_txn_process(ev_loop * /*loop*/, ev_async */*watcher*/, int /*event*/)
{
	tt_pthread_mutex_lock(&bsync_state.mutex);
	STAILQ_CONCAT(&bsync_state.txn_proxy_input,
		&bsync_state.txn_proxy_queue);
	tt_pthread_mutex_unlock(&bsync_state.mutex);
	while (!STAILQ_EMPTY(&bsync_state.txn_proxy_input)) {
		struct bsync_txn_info *info = STAILQ_FIRST(
			&bsync_state.txn_proxy_input);
		if (info->owner) {
			STAILQ_REMOVE_HEAD(&bsync_state.txn_proxy_input, fifo);
			fiber_call(info->owner);
		} else {
			info->process(info);
		}
	}
}

static void
txn_process(struct bsync_txn_info */*info*/)
{
	fiber_call(bsync_fiber(&bsync_state.txn_fibers, txn_process_fiber));
}

static void
bsync_process_fiber(va_list /* ap */)
{
restart:
	struct bsync_txn_info *info = NULL;
	struct bsync_operation *oper = NULL;
	if (bsync_state.state == bsync_state_shutdown)
		goto exit;
	if (bsync_state.proxy_host < BSYNC_MAX_HOSTS) {
		assert(bsync_state.state == bsync_state_ready ||
			bsync_state.state == bsync_state_recovery);
		bsync_proxy_processor();
		--bsync_state.bsync_fibers.active_proxy;
		goto exit;
	}
	++bsync_state.bsync_fibers.active_local;
	// TODO : try to fix
	info = STAILQ_FIRST(&bsync_state.bsync_proxy_input);
	STAILQ_REMOVE_HEAD(&bsync_state.bsync_proxy_input, fifo);
	oper = (struct bsync_operation *)
		region_alloc0(&fiber()->gc, sizeof(struct bsync_operation));
	oper->common = (struct bsync_common *)
		region_alloc(&fiber()->gc, sizeof(struct bsync_common));
	oper->req = NULL;
	assert(info->oper == NULL);
	oper->txn_data = info;
	oper->txn_data->oper = oper;
	oper->sign = oper->txn_data->sign;
	oper->owner = fiber();
	oper->host_id = bsync_state.local_id;
	oper->common->dup_key = oper->txn_data->common->dup_key;
	oper->common->region = oper->txn_data->common->region;
	rlist_create(&oper->txn);
	rlist_create(&oper->list);
	bsync_status(oper, bsync_op_status_init);
	if (bsync_state.state != bsync_state_ready) {
		say_debug("push request %d:%ld to election ops",
			  LAST_ROW(info->req)->server_id,
			  LAST_ROW(info->req)->lsn);
		rlist_add_tail_entry(&bsync_state.election_ops, oper, list);
		fiber_yield_timeout(bsync_state.operation_timeout);
		if (oper->status == bsync_op_status_fail) {
			bsync_free_region(oper->common);
			--bsync_state.bsync_fibers.active_local;
			goto exit;
		}
		if (bsync_state.state != bsync_state_ready) {
			rlist_del_entry(oper, list);
			oper->req = oper->txn_data->req;
			bsync_leader_rollback(oper);
			bsync_free_region(oper->common);
			--bsync_state.bsync_fibers.active_local;
			goto exit;
		}
	}
	if (bsync_state.leader_id == bsync_state.local_id) {
		bsync_queue_leader(oper, false);
	} else {
		oper->req = oper->txn_data->req;
		rlist_add_tail_entry(&bsync_state.txn_queue, oper, txn);
		bsync_queue_slave(oper);
	}
	oper->owner = NULL;
	--bsync_state.bsync_fibers.active_local;
exit:
	--bsync_state.bsync_fibers.active;
	fiber_gc();
	rlist_add_tail_entry(&bsync_state.bsync_fibers.data, fiber(), state);
	fiber_yield();
	goto restart;
}

static void
bsync_shutdown_fiber(va_list /* ap */)
{
	if (bsync_state.wal_commit_sign > 0 ||
		bsync_state.wal_rollback_sign > 0)
	{
		bsync_wal_system(true);
		fiber_gc();
	}
	for (int i = 0; i < bsync_state.num_hosts; ++i) {
		if (!bsync_index[i].fiber_out)
			continue;
		bsync_send_data(&bsync_index[i],
			bsync_alloc_send(bsync_mtype_close));
	}
	bsync_state.state = bsync_state_shutdown;
	for (int i = 0; i < bsync_state.num_hosts; ++i) {
		if (!bsync_index[i].fiber_out)
			continue;
		if ((bsync_index[i].flags & bsync_host_ping_sleep) != 0)
			fiber_call(bsync_index[i].fiber_out);
		bsync_index[i].state = bsync_host_disconnected;
	}
	recovery_exit(local_state);
	ev_break(bsync_loop, 1);
}

static void
bsync_shutdown(struct bsync_txn_info * info)
{
	assert(info == STAILQ_FIRST(&bsync_state.bsync_proxy_input));
	STAILQ_REMOVE_HEAD(&bsync_state.bsync_proxy_input, fifo);
	fiber_call(fiber_new("bsync_shutdown", bsync_shutdown_fiber));
}

static void
bsync_process_wal_fiber(va_list ap)
{
	struct bsync_txn_info * info = va_arg(ap, struct bsync_txn_info *);
	info->result = bsync_wal_write(info->req, info->sign);
	SWITCH_TO_TXN(info, txn_process);
	fiber_gc();
	return;
}

static void
bsync_process_wal(struct bsync_txn_info * info)
{
	STAILQ_REMOVE_HEAD(&bsync_state.bsync_proxy_input, fifo);
	fiber_start(fiber_new("bsync_wal", bsync_process_wal_fiber), info);
}

static void
bsync_process(struct bsync_txn_info *info)
{
	struct bsync_txn_info *tmp = STAILQ_FIRST(&bsync_state.bsync_proxy_input);
	assert(tmp == info && info);
	struct fiber *f = bsync_fiber(&bsync_state.bsync_fibers, bsync_process_fiber);
	assert(rlist_empty(&bsync_state.bsync_fibers.data) ||
		f != rlist_first_entry(&bsync_state.bsync_fibers.data, struct fiber, state));
	fiber_call(f);
}

static bool
bsync_has_consensus()
{
	return 2 * bsync_state.num_connected > bsync_state.num_hosts
		&& bsync_state.local_id < BSYNC_MAX_HOSTS;
}

static void
bsync_stop_io(uint8_t host_id, bool close, bool cleanup)
{
	if (BSYNC_REMOTE.state == bsync_host_disconnected)
		return;
	assert(bsync_state.num_connected > 0);
	assert(bsync_state.local_id == BSYNC_MAX_HOSTS ||
		bsync_state.num_connected > 1 ||
		bsync_state.state == bsync_state_shutdown ||
		BSYNC_REMOTE.state == bsync_host_follow);
	if (BSYNC_REMOTE.state != bsync_host_follow) {
		--bsync_state.num_connected;
		BSYNC_REMOTE.state = bsync_host_disconnected;
	}
	if (BSYNC_REMOTE.fiber_out && BSYNC_REMOTE.fiber_out != fiber() &&
		(BSYNC_REMOTE.flags & bsync_host_ping_sleep) != 0)
	{
		fiber_call(BSYNC_REMOTE.fiber_out);
	}
	if (BSYNC_REMOTE.fiber_in && BSYNC_REMOTE.fiber_in != fiber()) {
		BSYNC_REMOTE.fiber_in = NULL;
	}
	while (!rlist_empty(&BSYNC_REMOTE.send_queue) && cleanup) {
		struct bsync_send_elem *elem =
			rlist_shift_entry(&BSYNC_REMOTE.send_queue,
					  struct bsync_send_elem, list);
		if (elem->code == bsync_mtype_body ||
			elem->code == bsync_mtype_proxy_accept)
		{
			bsync_do_reject(host_id, elem);
		}
		else if (elem->code == bsync_mtype_proxy_request) {
			bsync_do_proxy_reject((struct bsync_operation*)elem->arg);
		}
		if (elem->system)
			mempool_free(&bsync_state.system_send_pool, elem);
	}
	BSYNC_REMOTE.send_queue_size = 0;
	if (close) {
		SWITCH_TO_TXN(&BSYNC_REMOTE.sysmsg, txn_process_close);
	}
}

static void
bsync_stop_io_fiber(va_list ap)
{
	uint8_t host_id = va_arg(ap, int);
	bsync_send_data(&BSYNC_REMOTE, bsync_alloc_send(bsync_mtype_close));
	bsync_stop_io(host_id, true, true);
}

static void
bsync_start_event(struct bsync_txn_info * /* info */)
{
	STAILQ_REMOVE_HEAD(&bsync_state.bsync_proxy_input, fifo);
	wal_writer_start(local_state);
}

static void
bsync_cleanup_follow(uint8_t host_id, int64_t sign)
{
	say_debug("bsync_cleanup_follow for %s (%ld)", BSYNC_REMOTE.name, sign);
	while (BSYNC_REMOTE.follow_queue_size > 0) {
		struct bsync_send_elem *elem =
			rlist_shift_entry(&BSYNC_REMOTE.follow_queue,
				struct bsync_send_elem, list);
		if ((int64_t)((struct bsync_operation *)elem->arg)->sign <= sign) {
			--BSYNC_REMOTE.follow_queue_size;
			bsync_do_submit(host_id, elem);
		} else {
			rlist_add_entry(&BSYNC_REMOTE.follow_queue, elem, list);
			break;
		}
	}
}

static void
bsync_update_local(struct bsync_txn_info *info)
{
	assert(loop() == bsync_loop);
	STAILQ_REMOVE_HEAD(&bsync_state.bsync_proxy_input, fifo);
	bsync_state.local_id = info->connection;
	say_info("bsync_update_local %d to signature %ld",
		 bsync_state.local_id, info->sign);
	if (BSYNC_LOCAL.sign >= 0)
		BSYNC_LOCAL.commit_sign = info->sign;
	else
		BSYNC_LOCAL.sign = BSYNC_LOCAL.commit_sign = info->sign;
	if (BSYNC_LOCAL.state == bsync_host_connected)
		return;
	++bsync_state.num_connected;
	BSYNC_LOCAL.state = bsync_host_connected;
	bsync_start_election();
}

static bool
bsync_check_follow(uint8_t host_id, int64_t sign)
{
	bsync_cleanup_follow(host_id, sign);
	uint64_t follow_sign = BSYNC_LOCAL.sign;
	if (BSYNC_REMOTE.follow_queue_size > 0) {
		void *arg = rlist_first_entry(&BSYNC_REMOTE.follow_queue,
			struct bsync_send_elem, list)->arg;
		follow_sign = ((struct bsync_operation *)arg)->sign;
	}
	say_debug("follow_sign=%ld, remote_sign=%ld", follow_sign, sign);
	return sign < (int64_t)(follow_sign - 1);
}

static void
bsync_start_connect(struct bsync_txn_info *info)
{
	STAILQ_REMOVE_HEAD(&bsync_state.bsync_proxy_input, fifo);
	uint8_t host_id = bsync_state.leader_id;
	if (BSYNC_LOCAL.sign >= 0)
		BSYNC_LOCAL.commit_sign = info->sign;
	else
		BSYNC_LOCAL.sign = BSYNC_LOCAL.commit_sign = info->sign;
	say_debug("start connection to %s", BSYNC_REMOTE.name);
	fiber_start(fiber_new(BSYNC_REMOTE.name, bsync_outgoing),
			local_state->remote[host_id].out.fd, host_id);
	fiber_start(fiber_new(BSYNC_REMOTE.name, bsync_accept_handler),
			local_state->remote[host_id].in.fd, host_id);
	if (BSYNC_REMOTE.state == bsync_host_disconnected)
		++bsync_state.num_connected;
	if (BSYNC_REMOTE.state != bsync_host_follow)
		BSYNC_REMOTE.state = bsync_host_recovery;
}

static void
bsync_process_connect(struct bsync_txn_info *info)
{
	assert(loop() == bsync_loop);
	STAILQ_REMOVE_HEAD(&bsync_state.bsync_proxy_input, fifo);
	uint8_t host_id = info->connection;
	say_debug("bsync_process_connect with %s, signature is %ld, local is %ld",
		  BSYNC_REMOTE.name, info->sign, BSYNC_LOCAL.commit_sign);
	BSYNC_REMOTE.sign = BSYNC_REMOTE.commit_sign = info->sign;
	bool do_switch = true;
	if (bsync_state.leader_id != bsync_state.local_id &&
		bsync_state.leader_id == host_id &&
		bsync_state.state == bsync_state_recovery &&
		bsync_state.leader_id < BSYNC_MAX_HOSTS)
	{
		do_switch = false;
	}
	else if (bsync_state.leader_id == bsync_state.local_id &&
		BSYNC_REMOTE.state == bsync_host_follow)
	{
		do_switch = bsync_check_follow(host_id, info->sign);
	}
	if (!do_switch) {
		uint8_t num_connected = bsync_state.num_connected;
		if (2 * num_connected > bsync_state.num_hosts &&
			bsync_state.state < bsync_state_ready)
		{
			say_info("set state to ready in connect, lsign=%ld, rsign=%ld",
				 BSYNC_LOCAL.commit_sign, BSYNC_REMOTE.commit_sign);
			bsync_state.state = bsync_state_ready;
			SWITCH_TO_TXN(info, txn_set_ready);
			if (!rlist_empty(&bsync_state.submit_queue)) {
				uint64_t commit_sign =
					rlist_last_entry(&bsync_state.submit_queue,
						struct bsync_operation, list)->sign;
				bsync_commit(commit_sign);
			}
		}
	}
	auto guard = make_scoped_guard([&]() {
		bsync_start_election();
		bsync_election_ops();
	});
	if (BSYNC_REMOTE.state == bsync_host_disconnected)
		++bsync_state.num_connected;
	if (do_switch) {
		if (BSYNC_REMOTE.state != bsync_host_follow)
			BSYNC_REMOTE.state = bsync_host_recovery;
	}
	assert(host_id != bsync_state.local_id && host_id < BSYNC_MAX_HOSTS);
	bool has_leader = bsync_state.leader_id < BSYNC_MAX_HOSTS;
	bool local_leader = has_leader &&
		bsync_state.leader_id == bsync_state.local_id;
	assert(rlist_empty(&BSYNC_REMOTE.send_queue));
	say_debug("push to bsync loop out_fd=%d, in_fd=%d, switch=%d",
		local_state->remote[host_id].out.fd,
		local_state->remote[host_id].in.fd,
		do_switch ? 1 : 0);
	fiber_start(fiber_new(BSYNC_REMOTE.name, bsync_outgoing),
			local_state->remote[host_id].out.fd, host_id);
	fiber_start(fiber_new(BSYNC_REMOTE.name, bsync_accept_handler),
			local_state->remote[host_id].in.fd, host_id);
	if (has_leader && !local_leader && host_id != bsync_state.leader_id) {
		fiber_start(fiber_new("bsync_stop_io", bsync_stop_io_fiber),
			host_id);
	}
	if (!local_leader)
		return;
	/* TODO : support hot recovery from send queue */
	if (do_switch) {
		bsync_send_data(&BSYNC_REMOTE,
				bsync_alloc_send(bsync_mtype_leader_submit));
		bsync_send_data(&BSYNC_REMOTE,
				bsync_alloc_send(bsync_mtype_iproto_switch));
	} else {
		bsync_send_data(&BSYNC_REMOTE,
				bsync_alloc_send(bsync_mtype_ready_switch));
		bsync_cleanup_follow(host_id, info->sign);
		if (!rlist_empty(&BSYNC_REMOTE.follow_queue)) {
			struct bsync_operation *oper = (struct bsync_operation *)
				rlist_first_entry(&BSYNC_REMOTE.follow_queue,
						struct bsync_send_elem, list)->arg;
			say_info("start to send follow queue from %d:%ld (%ld)",
				 LAST_ROW(oper->req)->server_id,
				 LAST_ROW(oper->req)->lsn, oper->sign);

		}
		while (!rlist_empty(&BSYNC_REMOTE.follow_queue)) {
			if (!BSYNC_REMOTE.fiber_out)
				return;
			struct bsync_send_elem *elem =
				rlist_shift_entry(&BSYNC_REMOTE.follow_queue,
						struct bsync_send_elem, list);
			bsync_send_data(&BSYNC_REMOTE, elem);
		}
		BSYNC_REMOTE.follow_queue_size = 0;
		BSYNC_REMOTE.state = bsync_host_connected;
	}
}

static void
bsync_process_disconnect(struct bsync_txn_info *info)
{
	STAILQ_REMOVE_HEAD(&bsync_state.bsync_proxy_input, fifo);
	int host_id = info->connection;
	if (BSYNC_REMOTE.state == bsync_host_disconnected)
		return;
	if (BSYNC_REMOTE.state == bsync_host_follow) {
		struct bsync_send_elem *elem = NULL;
		rlist_foreach_entry(elem, &BSYNC_REMOTE.follow_queue, list) {
			bsync_do_reject(host_id, elem);
		}
		rlist_create(&BSYNC_REMOTE.follow_queue);
		BSYNC_REMOTE.follow_queue_size = 0;
	}
	assert(bsync_state.num_connected > 0);
	assert(bsync_state.local_id == BSYNC_MAX_HOSTS ||
		bsync_state.num_connected > 1);
	--bsync_state.num_connected;
	BSYNC_REMOTE.state = bsync_host_disconnected;
	if ((bsync_state.local_id == bsync_state.leader_id &&
		2 * bsync_state.num_connected <= bsync_state.num_hosts) ||
		host_id == bsync_state.leader_id)
	{
		bsync_state.leader_id = BSYNC_MAX_HOSTS;
		bsync_state.accept_id = BSYNC_MAX_HOSTS;
		bsync_state.state = bsync_state_initial;
		BSYNC_REMOTE.flags = 0;
	}
}

static void
bsync_set_follow(struct bsync_txn_info *info)
{
	STAILQ_REMOVE_HEAD(&bsync_state.bsync_proxy_input, fifo);
	int host_id = info->connection;
	BSYNC_REMOTE.state = bsync_host_follow;
}

static void
bsync_process_follow(struct bsync_txn_info *info)
{
	STAILQ_REMOVE_HEAD(&bsync_state.bsync_proxy_input, fifo);
	int host_id = info->connection;
	uint64_t cleanup_sign = 0;
	tt_pthread_mutex_lock(&txn_state.mutex[host_id]);
	assert(rlist_empty(&BSYNC_REMOTE.send_queue));
	int64_t last_sign = info->sign;
	int64_t sign_bonus = bsync_state.max_host_queue -
			      BSYNC_REMOTE.follow_queue_size;
	if (BSYNC_REMOTE.follow_queue_size > 0) {
		assert(BSYNC_REMOTE.state == bsync_host_follow);
		void *arg = rlist_first_entry(&BSYNC_REMOTE.follow_queue,
					      struct bsync_send_elem, list)->arg;
		if (((struct bsync_operation *)arg)->sign <= info->sign) {
			arg = rlist_last_entry(&BSYNC_REMOTE.follow_queue,
					     struct bsync_send_elem, list)->arg;
			last_sign = ((struct bsync_operation *)arg)->sign;
		}
	}
	if (BSYNC_LOCAL.sign <= (last_sign + sign_bonus / 2)) {
		//info->proxy = (BSYNC_REMOTE.state == bsync_host_follow ||
		//		bsync_state.state < bsync_state_ready);
		info->proxy = true;
		BSYNC_REMOTE.state = bsync_host_follow;
	}
	say_debug("follow result for %s is %ld < %ld (%ld + %ld),"
		"queue size is %ld, state=%d, proxy=%d",
		BSYNC_REMOTE.name, info->sign, BSYNC_LOCAL.sign, last_sign,
		sign_bonus, BSYNC_REMOTE.follow_queue_size, BSYNC_REMOTE.state,
		info->proxy ? 1 : 0);
	cleanup_sign = info->sign;
	tt_pthread_cond_signal(&txn_state.cond[host_id]);
	tt_pthread_mutex_unlock(&txn_state.mutex[host_id]);
	bsync_cleanup_follow(host_id, cleanup_sign);
}

static void
bsync_process_loop(struct ev_loop */*loop*/, ev_async */*watcher*/, int /*event*/)
{
	tt_pthread_mutex_lock(&bsync_state.mutex);
	STAILQ_CONCAT(&bsync_state.bsync_proxy_input,
		      &bsync_state.bsync_proxy_queue);
	tt_pthread_mutex_unlock(&bsync_state.mutex);
	struct bsync_txn_info *info = NULL;
	while (!STAILQ_EMPTY(&bsync_state.bsync_proxy_input)) {
		info = STAILQ_FIRST(&bsync_state.bsync_proxy_input);
		if (info->oper) {
			assert(info->oper->status == bsync_op_status_txn ||
				info->oper->status == bsync_op_status_yield ||
				info->oper->status == bsync_op_status_proxy ||
				info->oper->status == bsync_op_status_finish_yield);
			STAILQ_REMOVE_HEAD(&bsync_state.bsync_proxy_input, fifo);
			if (bsync_state.state != bsync_state_ready)
				continue;
			fiber_call(info->oper->owner);
		} else {
			info->process(info);
		}
	}
}

/*
 * send leader ID to TXN
 * if we dont have snapshot start to generate and send it
 * if we have snapshot check for SLAVE for ready state
 * if SLAVE is ready switch it to ready state
 * else switch it to recovery state, send iproto_switch and switch to TXN
 */

static void
bsync_set_leader(uint8_t host_id)
{
	say_info("new leader are %s", BSYNC_REMOTE.name);
	bsync_state.leader_id = host_id;
	bsync_state.sysmsg.connection = host_id;
	SWITCH_TO_TXN(&bsync_state.sysmsg, txn_leader);
	if (BSYNC_LOCAL.sign == -1) {
		bsync_state.state = bsync_state_recovery;
		if (bsync_state.leader_id == bsync_state.local_id) {
			SWITCH_TO_TXN(&BSYNC_LOCAL.sysmsg, txn_join);
		}
		for (int i = 0; i < bsync_state.num_hosts; ++i) {
			if (bsync_index[i].state == bsync_host_disconnected ||
				i == host_id || i == bsync_state.local_id)
			{
				continue;
			}
			if (bsync_state.leader_id != bsync_state.local_id) {
				bsync_send_data(&bsync_index[i],
					bsync_alloc_send(bsync_mtype_close));
			} else {
				bsync_send_data(&bsync_index[i],
					bsync_alloc_send(bsync_mtype_iproto_switch));
			}
		}
		return;
	}
	if (host_id != bsync_state.local_id) {
		for (int i = 0; i < bsync_state.num_hosts; ++i) {
			if (bsync_index[i].state == bsync_host_disconnected ||
				i == host_id || i == bsync_state.local_id)
			{
				continue;
			}
			bsync_send_data(&bsync_index[i],
				bsync_alloc_send(bsync_mtype_close));
		}
		if (BSYNC_REMOTE.commit_sign > BSYNC_LOCAL.commit_sign ||
			BSYNC_LOCAL.sign == -1)
		{
			bsync_state.state = bsync_state_recovery;
		}
		return;
	}
	/* start recovery */
	uint8_t num_up = 1;
	for (uint8_t i = 0; i < bsync_state.num_hosts; ++i) {
		if (i == bsync_state.local_id ||
			bsync_index[i].state == bsync_host_disconnected)
			continue;
		if (bsync_index[i].sign == BSYNC_LOCAL.sign) {
			++num_up;
			bsync_send_data(&bsync_index[i],
				bsync_alloc_send(bsync_mtype_ready_switch));
			continue;
		}
		/* stop bsync io fibers and switch host to recovery state */
		bsync_send_data(&bsync_index[i],
			bsync_alloc_send(bsync_mtype_iproto_switch));
	}
	say_info("%s num_up = %d", __PRETTY_FUNCTION__, num_up);
	if (2 * num_up > bsync_state.num_hosts) {
		bsync_state.state = bsync_state_ready;
		SWITCH_TO_TXN(&BSYNC_REMOTE.sysmsg, txn_set_ready);
		bsync_election_ops();
	} else {
		bsync_state.state = bsync_state_recovery;
	}
}

/*
 * Leader election block
 */

static void
bsync_start_election()
{
	if (bsync_state.leader_id < BSYNC_MAX_HOSTS || !bsync_has_consensus()
		|| bsync_state.state == bsync_state_recovery)
		return;
	if (bsync_state.state == bsync_state_election) {
		if (bsync_state.num_connected == bsync_state.num_hosts)
			bsync_state.state = bsync_state_initial;
		else
			return;
	}
	assert(bsync_state.state < bsync_state_recovery);
	say_info("consensus connected");
	uint8_t max_host_id = bsync_max_host();
	say_info("next leader should be %s", bsync_index[max_host_id].name);
	if (max_host_id != bsync_state.local_id) {
		bsync_send_data(&bsync_index[max_host_id],
			bsync_alloc_send(bsync_mtype_leader_proposal));
		return;
	}
	bsync_state.num_accepted = 1;
	if (bsync_state.state == bsync_state_promise)
		return;
	else
		bsync_state.state = bsync_state_promise;
	for (uint8_t i = 0; i < bsync_state.num_hosts; ++i) {
		if (i == max_host_id ||
			bsync_index[i].state == bsync_host_disconnected)
			continue;
		bsync_send_data(&bsync_index[i],
			bsync_alloc_send(bsync_mtype_leader_promise));
	}
}

static void
bsync_check_consensus(uint8_t host_id)
{
	auto txn_reconnect = make_scoped_guard([host_id](){
		SWITCH_TO_TXN(&BSYNC_REMOTE.sysmsg, txn_process_reconnnect);
	});
	BSYNC_REMOTE.sysmsg.proxy = false;
	if ((bsync_state.leader_id == BSYNC_MAX_HOSTS ||
		host_id != bsync_state.leader_id) &&
	    (bsync_state.state >= bsync_state_recovery ||
		2 * bsync_state.num_connected > bsync_state.num_hosts) &&
	    (bsync_state.local_id != bsync_state.leader_id ||
		2 * bsync_state.num_connected > bsync_state.num_hosts))
	{
		return;
	}
	bool was_leader = (bsync_state.leader_id == bsync_state.local_id);
	bsync_state.leader_id = BSYNC_MAX_HOSTS;
	bsync_state.accept_id = BSYNC_MAX_HOSTS;
	bsync_state.state = bsync_state_initial;
	BSYNC_REMOTE.sysmsg.proxy = true;
	struct bsync_send_elem *elem = NULL;
	struct bsync_operation *oper = NULL;
	while (!rlist_empty(&bsync_state.wait_queue)) {
		oper = rlist_shift_entry(&bsync_state.wait_queue,
					 struct bsync_operation, list);
		say_debug("failed request %d:%ld(%ld), status is %d",
			  LAST_ROW(oper->req)->server_id,
			  LAST_ROW(oper->req)->lsn, oper->sign, oper->status);
		oper->txn_data->result = -1;
		bsync_status(oper, bsync_op_status_fail);
		fiber_call(oper->owner);
	}
	if (!was_leader)
		return;
	for (uint8_t i = 0; i < bsync_state.num_hosts; ++i) {
		if (i == bsync_state.local_id)
			continue;
		while (!rlist_empty(&bsync_index[i].follow_queue)) {
			elem = rlist_shift_entry(&bsync_index[i].follow_queue,
						 struct bsync_send_elem, list);
			bsync_do_reject(i, elem);
		}
		while (!rlist_empty(&bsync_index[i].op_queue)) {
			elem = rlist_shift_entry(&bsync_index[i].op_queue,
						 struct bsync_send_elem, list);
			bsync_do_reject(i, elem);
		}
		while (!rlist_empty(&bsync_index[i].send_queue)) {
			elem =rlist_shift_entry(&bsync_index[i].send_queue,
						struct bsync_send_elem, list);
			switch (elem->code) {
			case bsync_mtype_body:
			case bsync_mtype_proxy_accept:
				bsync_do_reject(i, elem);
				break;
			}
			if (elem->system)
				mempool_free(&bsync_state.system_send_pool, elem);
		}
		bsync_stop_io(i, false, true);
		bsync_index[i].op_queue_size = 0;
		bsync_index[i].send_queue_size = 0;
		bsync_index[i].follow_queue_size = 0;
	}
}

static void
bsync_disconnected(uint8_t host_id)
{
	if (BSYNC_REMOTE.state == bsync_host_disconnected ||
		bsync_state.state == bsync_state_shutdown)
		return;
	bsync_stop_io(host_id, false, false);
	if (BSYNC_REMOTE.proxy_rows) {
		for (size_t i = 0; i < BSYNC_REMOTE.proxy_cur; ++i) {
			if (BSYNC_REMOTE.proxy_rows[i]->bodycnt > 0) {
				void *data = BSYNC_REMOTE.proxy_rows[i]->body[0].iov_base;
				slab_put(&cord()->slabc, slab_from_data(data));
			}
			mempool_free(&BSYNC_REMOTE.proxy_pool, BSYNC_REMOTE.proxy_rows[i]);
		}
		BSYNC_REMOTE.proxy_cur = BSYNC_REMOTE.proxy_size =
			BSYNC_REMOTE.proxy_sign = 0;
		slab_put(&cord()->slabc, slab_from_data(BSYNC_REMOTE.proxy_rows));
		BSYNC_REMOTE.proxy_rows = NULL;

	}
	BSYNC_REMOTE.body_cur = 0;
	if (host_id == bsync_state.accept_id) {
		bsync_state.state = bsync_state_initial;
		bsync_state.accept_id = BSYNC_MAX_HOSTS;
	}
	{
		BSYNC_LOCK(bsync_state.active_ops_mutex);
		mh_bsync_clear(BSYNC_REMOTE.active_ops);
	}
	if (host_id == bsync_state.leader_id) {
		rlist_create(&bsync_state.accept_queue);
		struct bsync_txn_info *info;
		STAILQ_FOREACH(info, &bsync_state.bsync_proxy_input, fifo) {
			if (info->oper && info->oper->status == bsync_op_status_proxy &&
				info->oper->host_id == host_id)
			{
				bsync_status(info->oper, bsync_op_status_fail);
			}
		}
		say_warn("disconnecting master %s", BSYNC_REMOTE.name);
	} else
		say_warn("disconnecting slave %s", BSYNC_REMOTE.name);
	// TODO : cleanup more correctly
	struct bsync_send_elem *elem;
	while (!rlist_empty(&BSYNC_REMOTE.follow_queue)) {
		elem = rlist_shift_entry(&BSYNC_REMOTE.follow_queue,
					 struct bsync_send_elem, list);
		bsync_do_reject(host_id, elem);
	}
	while (!rlist_empty(&BSYNC_REMOTE.op_queue)) {
		elem = rlist_shift_entry(&BSYNC_REMOTE.op_queue,
					 struct bsync_send_elem, list);
		bsync_do_reject(host_id, elem);
	}
	while (!rlist_empty(&BSYNC_REMOTE.send_queue)) {
		elem = rlist_shift_entry(&BSYNC_REMOTE.send_queue,
					 struct bsync_send_elem, list);
		if (elem->code == bsync_mtype_body ||
			elem->code == bsync_mtype_proxy_accept)
		{
			bsync_do_reject(host_id, elem);
		}
		if (elem->system) {
			mempool_free(&bsync_state.system_send_pool, elem);
		}
	}
	rlist_create(&BSYNC_REMOTE.follow_queue);
	rlist_create(&BSYNC_REMOTE.op_queue);
	BSYNC_REMOTE.follow_queue_size = 0;
	BSYNC_REMOTE.op_queue_size = 0;
	BSYNC_REMOTE.send_queue_size = 0;
	bool was_leader = (bsync_state.leader_id == host_id);
	if (was_leader) {
		bsync_state.wal_commit_sign = 0;
		bsync_state.wal_rollback_sign = vclock_sum(&bsync_state.vclock);
		char *vclock = vclock_to_string(&bsync_state.vclock);
		say_info("rollback all ops from sign %ld, vclock is %s",
			 bsync_state.wal_rollback_sign, vclock);
		free(vclock);
		bsync_wal_system(true);
	}
	bsync_check_consensus(host_id);
	if (was_leader) {
		while (!rlist_empty(&bsync_state.submit_queue)) {
			struct bsync_operation *oper = rlist_shift_entry(
				&bsync_state.submit_queue,
				struct bsync_operation, list);
			say_debug("failed request %d:%ld(%ld), status is %d",
				  LAST_ROW(oper->req)->server_id,
				  LAST_ROW(oper->req)->lsn, oper->sign, oper->status);
			bool yield = (oper->status == bsync_op_status_yield);
			oper->txn_data->result = -1;
			bsync_status(oper, bsync_op_status_fail);
			if (yield)
				fiber_call(oper->owner);
		}
		while (!rlist_empty(&bsync_state.wal_queue)) {
			struct bsync_operation *oper = rlist_shift_entry(
				&bsync_state.wal_queue,
				struct bsync_operation, list);
			say_debug("failed request %d:%ld(%ld), status is %d",
				  LAST_ROW(oper->req)->server_id,
				  LAST_ROW(oper->req)->lsn, oper->sign, oper->status);
			oper->txn_data->result = -1;
			bsync_status(oper, bsync_op_status_fail);
		}
		if (!rlist_empty(&bsync_state.proxy_queue)) {
			struct bsync_operation *oper = rlist_shift_entry(
				&bsync_state.proxy_queue,
				struct bsync_operation, list);
			oper->txn_data->result = -1;
			fiber_call(oper->owner);
			rlist_create(&bsync_state.proxy_queue);
		}
	}
	bsync_start_election();
}

static void
bsync_leader_proposal(uint8_t host_id, const char **ipos, const char *iend)
{
	(void) iend;
	BSYNC_REMOTE.commit_sign = BSYNC_REMOTE.sign = mp_decode_uint(ipos);
	if (bsync_state.state != bsync_state_promise) {
		if (bsync_state.state == bsync_state_election)
			bsync_state.state = bsync_state_initial;
		bsync_start_election();
	} else if (bsync_state.state >= bsync_state_recovery &&
		bsync_state.state < bsync_state_shutdown &&
		bsync_state.leader_id == bsync_state.local_id)
	{
		bsync_send_data(&BSYNC_REMOTE,
			bsync_alloc_send(bsync_mtype_leader_submit));
	}
}

static void
bsync_leader_promise(uint8_t host_id, const char **ipos, const char *iend)
{
	(void) iend;
	BSYNC_REMOTE.commit_sign = BSYNC_REMOTE.sign = mp_decode_uint(ipos);
	uint8_t max_host_id = bsync_max_host();
	if (bsync_state.state >= bsync_state_recovery
		&& bsync_state.state < bsync_state_shutdown
		&& bsync_state.leader_id == bsync_state.local_id)
	{
		bsync_send_data(&BSYNC_REMOTE,
			bsync_alloc_send(bsync_mtype_leader_submit));
	} else if (host_id == max_host_id &&
			((bsync_state.state == bsync_state_accept &&
			  bsync_state.accept_id == host_id) ||
			bsync_state.state < bsync_state_accept))
	{
		say_info("accept leader promise from %s", BSYNC_REMOTE.name);
		bsync_send_data(&BSYNC_REMOTE,
			bsync_alloc_send(bsync_mtype_leader_accept));
		bsync_state.state = bsync_state_accept;
		bsync_state.accept_id = host_id;
	} else {
		say_warn("reject leader promise from %s(%ld),"
			"next leader should be %s(%ld), state=%d",
			BSYNC_REMOTE.name, BSYNC_REMOTE.commit_sign,
			bsync_index[max_host_id].name,
			bsync_index[max_host_id].commit_sign, bsync_state.state);
		bsync_send_data(&BSYNC_REMOTE,
			bsync_alloc_send(bsync_mtype_leader_reject));
	}
}

static void
bsync_election_ops()
{
	while (!rlist_empty(&bsync_state.election_ops)
		&& bsync_state.state == bsync_state_ready) {
		struct bsync_operation *oper = rlist_shift_entry(
			&bsync_state.election_ops, struct bsync_operation,
			list);
		fiber_call(oper->owner);
	}
}

static void
bsync_leader_accept(uint8_t host_id, const char **ipos, const char *iend)
{
	(void) iend;
	BSYNC_REMOTE.commit_sign = BSYNC_REMOTE.sign = mp_decode_uint(ipos);
	if (bsync_state.state != bsync_state_promise)
		return;
	if (2 * ++bsync_state.num_accepted <= bsync_state.num_hosts)
		return;
	for (uint8_t i = 0; i < bsync_state.num_hosts; ++i) {
		if (i == bsync_state.local_id ||
			bsync_index[i].state == bsync_host_disconnected)
			continue;
		bsync_send_data(&bsync_index[i],
			bsync_alloc_send(bsync_mtype_leader_submit));
	}
	bsync_set_leader(bsync_state.local_id);
}

static void
bsync_leader_submit(uint8_t host_id, const char **ipos, const char *iend)
{
	(void) iend;
	BSYNC_REMOTE.commit_sign = BSYNC_REMOTE.sign = mp_decode_uint(ipos);
	bsync_state.accept_id = BSYNC_MAX_HOSTS;
	if (host_id != bsync_state.leader_id)
		bsync_set_leader(host_id);
}

static void
bsync_ready_switch(uint8_t host_id, const char **ipos, const char *iend)
{
	bsync_leader_submit(host_id, ipos, iend);
	if (bsync_state.state == bsync_state_ready)
		return;
	bsync_state.state = bsync_state_ready;
	struct bsync_txn_info *info = &BSYNC_REMOTE.sysmsg;
	SWITCH_TO_TXN(info, txn_set_ready);
}

static void
bsync_leader_reject(uint8_t host_id, const char **ipos, const char *iend)
{
	(void) iend;
	BSYNC_REMOTE.commit_sign = BSYNC_REMOTE.sign = mp_decode_uint(ipos);
	bsync_state.state = bsync_state_initial;
	bsync_start_election();
}

static void
bsync_ping(uint8_t host_id, const char **ipos, const char *iend)
{
	(void) iend;
	BSYNC_REMOTE.sign = mp_decode_uint(ipos);
}

static void
bsync_iproto_switch(uint8_t host_id, const char **ipos, const char *iend)
{
	(void) ipos; (void) iend;
	if (bsync_state.leader_id != bsync_state.local_id) {
		bsync_send_data(&BSYNC_REMOTE,
			bsync_alloc_send(bsync_mtype_iproto_switch));
		if (BSYNC_REMOTE.fiber_out)
			fiber_join(BSYNC_REMOTE.fiber_out);
	}
	bsync_stop_io(host_id, false, true);
	if (BSYNC_REMOTE.fiber_out_fail) {
		bsync_check_consensus(host_id);
	} else {
		if (BSYNC_REMOTE.state != bsync_host_follow) {
			BSYNC_REMOTE.state = bsync_host_recovery;
			++bsync_state.num_connected;
		}
		SWITCH_TO_TXN(&BSYNC_REMOTE.sysmsg, txn_process_recovery);
	}
}

static void
bsync_close(uint8_t host_id, const char **ipos, const char *iend)
{
	(void) ipos; (void) iend;
	if (bsync_state.local_id == bsync_state.leader_id ||
		host_id == bsync_state.leader_id)
	{
		bsync_disconnected(host_id);
	}
	if (BSYNC_REMOTE.state != bsync_host_disconnected && BSYNC_REMOTE.fiber_out)
		bsync_send_data(&BSYNC_REMOTE, bsync_alloc_send(bsync_mtype_close));
	bsync_stop_io(host_id, false, true);
}

/*
 * Network block
 */

typedef void
(*bsync_handler_t)(uint8_t host_id, const char** ipos, const char* iend);

static bsync_handler_t bsync_handlers[] = {
	bsync_leader_proposal,
	bsync_leader_promise,
	bsync_leader_accept,
	bsync_leader_submit,
	bsync_leader_reject,
	bsync_ping,
	bsync_iproto_switch,
	NULL,
	bsync_ready_switch,
	bsync_close,
	bsync_rollback,
	bsync_proxy_reject,
	NULL,
	bsync_body,
	bsync_submit,
	bsync_reject,
	bsync_proxy_request,
	bsync_proxy_accept,
	bsync_proxy_join
};

static int
encode_sys_request(uint8_t host_id, struct bsync_send_elem *elem,
		   struct iovec *iov, size_t *packet_size);

static void
bsync_decode_extended_header(uint8_t host_id, const char **pos)
{
	uint32_t len = mp_decode_uint(pos);
	const char *end = *pos + len;
	if (!len)
		return;
	uint32_t flags = mp_decode_uint(pos);
	assert(flags);
	if (flags & bsync_iproto_commit_sign)
		bsync_commit(mp_decode_uint(pos));
	if (flags & bsync_iproto_txn_size) {
		assert(BSYNC_REMOTE.proxy_rows == NULL);
		BSYNC_REMOTE.proxy_size = mp_decode_uint(pos);
		BSYNC_REMOTE.proxy_cur = 0;
		struct slab *slab = slab_get(&cord()->slabc,
			BSYNC_REMOTE.proxy_size * sizeof(struct xrow_header *));
		if (slab == NULL)
			panic("cant alloc data for proxy op");
		say_debug("receive proxy header, size=%ld", BSYNC_REMOTE.proxy_size);
		BSYNC_REMOTE.proxy_rows = (struct xrow_header **)slab_data(slab);
		for (size_t i = 0; i < BSYNC_REMOTE.proxy_size; ++i) {
			BSYNC_REMOTE.proxy_rows[i] = (struct xrow_header *)
				mempool_alloc(&BSYNC_REMOTE.proxy_pool);
		}
	}
	/* ignore all unknown data from extended header */
	*pos = end;
}

static void
bsync_breadn(struct ev_io *coio, struct ibuf *in, size_t sz)
{
	coio_breadn_timeout(coio, in, sz, bsync_state.read_timeout);
//	if (errno == ETIMEDOUT) {
//		tnt_raise(SocketError, coio->fd, "timeout");
//	}
}

static void
bsync_readn(struct ev_io *coio, struct ibuf *in, size_t sz)
{
	ibuf_reserve(in, sz);
	in->wpos += coio_readn_ahead(coio, in->wpos, sz, sz);
}

static inline void
bsync_read(struct ev_io *coio, struct ibuf *in, size_t sz, bool buffered)
{
	if (buffered)
		bsync_breadn(coio, in, sz);
	else
		bsync_readn(coio, in, sz);
}

static uint32_t
bsync_read_package(struct ev_io *coio, struct ibuf *in, uint8_t host_id,
		   bool buffered)
{
	/* Read fixed header */
	if (ibuf_used(in) < 1)
		bsync_read(coio, in, 1, false);
	assert(ibuf_used(in) > 0);
	/* Read length */
	if (mp_typeof(*in->rpos) != MP_UINT) {
		tnt_raise(ClientError, ER_INVALID_MSGPACK,
			  "packet length");
	}
	ssize_t to_read = mp_check_uint(in->rpos, in->wpos);
	if (to_read > 0)
		bsync_read(coio, in, to_read, buffered);
	uint32_t len = mp_decode_uint((const char **) &in->rpos);
	if (len == 0)
		return 0;
	/* Read header and body */
	to_read = len - ibuf_used(in);
	assert(buffered || to_read > 0);
	if (to_read > 0)
		bsync_read(coio, in, to_read, buffered);
	const char *pos = (const char *)in->rpos;
	bsync_decode_extended_header(host_id, (const char **) &in->rpos);
	return len - (in->rpos - pos);
}

static void
bsync_incoming(struct ev_io *coio, struct iobuf *iobuf, uint8_t host_id)
{
	struct ibuf *in = &iobuf->in;
	while (BSYNC_REMOTE.state > bsync_host_disconnected &&
		bsync_state.state != bsync_state_shutdown)
	{
		/* cleanup buffer */
		iobuf_reset(iobuf);
		fiber_gc();
		uint32_t len = bsync_read_package(coio, in, host_id, true);
		/* proceed message */
		const char* iend = (const char *)in->rpos + len;
		const char **ipos = (const char **)&in->rpos;
		const char *backup = (const char *)in->rpos;
		(void)backup;
		if (BSYNC_REMOTE.fiber_in != fiber())
			return;
		if (BSYNC_REMOTE.state > bsync_host_disconnected) {
			uint32_t type = mp_decode_uint(ipos);
			assert(type < bsync_mtype_count);
			if ((type < bsync_mtype_sysend && type != bsync_mtype_ping) ||
				type == bsync_mtype_submit || type == bsync_mtype_reject)
				say_info("receive message from %s, type %s, length %d",
					 BSYNC_REMOTE.name, bsync_mtype_name[type], len);
			else
				say_debug("receive message from %s, type %s, length %d",
					  BSYNC_REMOTE.name, bsync_mtype_name[type], len);
			assert(type < sizeof(bsync_handlers));
			(*bsync_handlers[type])(host_id, ipos, iend);
			if (type == bsync_mtype_iproto_switch) {
				assert(in->wpos == iend);
				break;
			}
		} else {
			*ipos = iend;
			say_warn("receive message from disconnected host %s",
				 BSYNC_REMOTE.name);
		}
	}
}

static void
bsync_accept_handler(va_list ap)
{
	struct ev_io coio;
	coio_init(&coio);
	coio.fd = va_arg(ap, int);
	int host_id = va_arg(ap, int);
	struct iobuf *iobuf = iobuf_new();
	auto iobuf_guard = make_scoped_guard([&]() {
		iobuf_delete(iobuf);
	});
	BSYNC_REMOTE.fiber_in = fiber();
	fiber_set_joinable(fiber(), true);
	try {
		{
			struct bsync_send_elem *elem =
				bsync_alloc_send(bsync_mtype_bsync_switch);
			auto elem_guard = make_scoped_guard([&](){
				mempool_free(&bsync_state.system_send_pool, elem);
				fiber_gc();
			});
			struct iovec iov;
			encode_sys_request(host_id, elem, &iov, NULL);
			ssize_t total_sent =
				coio_writev_timeout(&coio, &iov, 1, -1,
						    bsync_state.write_timeout);
			say_info("sent to %s bsync_switch, num bytes is %ld",
				 BSYNC_REMOTE.name, total_sent);
		}
		bsync_incoming(&coio, iobuf, host_id);
	} catch (Exception *e) {
		if (fiber() == BSYNC_REMOTE.fiber_in) {
			e->log();
			bsync_disconnected(host_id);
		} else {
			say_warn("disconnected didnt called from accept handler, error: %s", e->errmsg());
		}
	}
	BSYNC_REMOTE.fiber_in = NULL;
	say_debug("stop incoming fiber from %s", BSYNC_REMOTE.name);
}

static int
bsync_extended_header_size(uint8_t host_id, bool sys, uint64_t body_size, uint8_t *flags)
{
	uint8_t tmp = 0;
	if (!flags)
		flags = &tmp;
	int result = 0;
	if (BSYNC_LOCAL.commit_sign > BSYNC_REMOTE.commit_sign && !sys &&
		bsync_state.leader_id == bsync_state.local_id) {
		*flags |= bsync_iproto_commit_sign;
		result += mp_sizeof_uint(BSYNC_LOCAL.commit_sign);
	}
	if (body_size) {
		*flags |= bsync_iproto_txn_size;
		result += mp_sizeof_uint(body_size);
	}
	if (*flags)
		result += mp_sizeof_uint(*flags);
	return result;
}

static char *
bsync_extended_header_encode(uint8_t host_id, char *pos, bool sys, uint64_t body_size)
{
	uint8_t flags = 0;
	pos = mp_encode_uint(pos, bsync_extended_header_size(host_id, sys, body_size, &flags));
	if (flags == 0)
		return pos;
	pos = mp_encode_uint(pos, flags);
	if (flags & bsync_iproto_commit_sign) {
		pos = mp_encode_uint(pos, BSYNC_LOCAL.commit_sign);
		BSYNC_REMOTE.commit_sign = BSYNC_LOCAL.commit_sign;
	}
	if (flags & bsync_iproto_txn_size)
		pos = mp_encode_uint(pos, body_size);
	return pos;
}

static uint64_t
bsync_mp_real_size(uint64_t size)
{
	return mp_sizeof_uint(size) + size;
}

static void
bsync_writev(struct ev_io *coio, struct iovec *iov, int iovcnt, uint8_t host_id)
{
	BSYNC_REMOTE.flags |= bsync_host_active_write;
	auto guard = make_scoped_guard([&]() {
		BSYNC_REMOTE.flags &= ~bsync_host_active_write;
	});
	coio_writev_timeout(coio, iov, iovcnt, -1, bsync_state.write_timeout);
}

static int
encode_sys_request(uint8_t host_id, struct bsync_send_elem *elem,
		   struct iovec *iov, size_t *packet_size)
{
	if (elem->code == bsync_mtype_none) {
		elem->code = bsync_mtype_ping;
	}
	ssize_t size = mp_sizeof_uint(elem->code)
		+ bsync_mp_real_size(bsync_extended_header_size(host_id,
					elem->code != bsync_mtype_ping, 0, 0));
	switch (elem->code) {
	case bsync_mtype_leader_reject:
	case bsync_mtype_ping:
	case bsync_mtype_leader_proposal:
	case bsync_mtype_leader_accept:
	case bsync_mtype_leader_promise:
	case bsync_mtype_leader_submit:
	case bsync_mtype_ready_switch:
		size += mp_sizeof_uint(BSYNC_LOCAL.commit_sign);
		break;
	case bsync_mtype_rollback:
		size += mp_sizeof_uint((uint64_t)elem->arg);
		/* no break */
	default:
		break;
	}
	iov->iov_len = mp_sizeof_uint(size) + size;
	iov->iov_base = region_alloc(&fiber()->gc, iov[0].iov_len);
	char *pos = (char *) iov[0].iov_base;
	pos = mp_encode_uint(pos, size);
	pos = bsync_extended_header_encode(host_id, pos,
					elem->code != bsync_mtype_ping, 0);
	pos = mp_encode_uint(pos, elem->code);
	if (packet_size)
		*packet_size += iov[0].iov_len;

	switch (elem->code) {
	case bsync_mtype_leader_reject:
	case bsync_mtype_ping:
	case bsync_mtype_leader_proposal:
	case bsync_mtype_leader_accept:
	case bsync_mtype_leader_promise:
	case bsync_mtype_leader_submit:
	case bsync_mtype_ready_switch:
		pos = mp_encode_uint(pos, BSYNC_LOCAL.commit_sign);
		break;
	case bsync_mtype_rollback:
		pos = mp_encode_uint(pos, (uint64_t)elem->arg);
		/* no break */
	default:
		break;
	}
	return 1;
}

static int
encode_request(uint8_t host_id, struct bsync_send_elem *elem,
		struct iovec *iov, size_t *packet_size)
{
	int iovcnt = 1;
	ssize_t bsize = mp_sizeof_uint(elem->code);
	struct bsync_operation *oper = (struct bsync_operation *)elem->arg;
	bool commit_ready = true;
	if (bsync_state.leader_id == bsync_state.local_id)
		commit_ready = (oper->sign <= BSYNC_LOCAL.commit_sign);
	uint64_t body_size = 0;
	if ((elem->code == bsync_mtype_body || elem->code == bsync_mtype_proxy_request) &&
		BSYNC_REMOTE.body_cur == 0)
	{
		body_size = oper->req->n_rows;
	}
	bsize +=
		bsync_mp_real_size(bsync_extended_header_size(
					host_id, commit_ready, body_size, 0
		));
	if (elem->code != bsync_mtype_proxy_request
		&& elem->code != bsync_mtype_proxy_reject
		&& elem->code != bsync_mtype_proxy_join
		&& (elem->code != bsync_mtype_body ||
			(BSYNC_REMOTE.body_cur + 1) == oper->req->n_rows))
	{
		bsize += mp_sizeof_uint(oper->sign);
	}
	ssize_t fsize = bsize;
	if (elem->code == bsync_mtype_body || elem->code == bsync_mtype_proxy_request) {
		iovcnt += xrow_header_encode(oper->req->rows[BSYNC_REMOTE.body_cur],
		if (++BSYNC_REMOTE.body_cur == oper->req->n_rows)
			BSYNC_REMOTE.body_cur = 0;
		else {
			rlist_del_entry(elem, list);
			rlist_add_entry(&BSYNC_REMOTE.send_queue, elem, list);
			++BSYNC_REMOTE.send_queue_size;
			--BSYNC_REMOTE.op_queue_size;
		}
		for (int i = 1; i < iovcnt; ++i) {
			bsize += iov[i].iov_len;
		}
	}
	iov[0].iov_len = mp_sizeof_uint(bsize) + fsize;
	iov[0].iov_base = region_alloc(&fiber()->gc, iov[0].iov_len);
	if (packet_size)
		*packet_size += mp_sizeof_uint(bsize) + bsize;
	char* pos = (char*) iov[0].iov_base;
	pos = mp_encode_uint(pos, bsize);
	pos = bsync_extended_header_encode(host_id, pos, commit_ready, body_size);
	pos = mp_encode_uint(pos, elem->code);
	if (elem->code != bsync_mtype_proxy_request
		&& elem->code != bsync_mtype_proxy_reject
		&& elem->code != bsync_mtype_proxy_join)
	{
		if (elem->code == bsync_mtype_body && BSYNC_REMOTE.body_cur > 0)
			return iovcnt;
		pos = mp_encode_uint(pos, oper->sign);
	}
	return iovcnt;
}

static bool
bsync_is_out_valid(uint8_t host_id)
{
	return BSYNC_REMOTE.state > bsync_host_disconnected &&
		bsync_state.state != bsync_state_shutdown &&
		fiber() == BSYNC_REMOTE.fiber_out;
}

#define BUFV_IOVMAX 256*XROW_IOVMAX
#define MAX_PACKET_SIZE 4*1024*1024

static void
bsync_send(struct ev_io *coio, uint8_t host_id)
{
	static struct iovec iov_cache[BSYNC_MAX_HOSTS][BUFV_IOVMAX];
	while (!rlist_empty(&BSYNC_REMOTE.send_queue) &&
		bsync_is_out_valid(host_id))
	{
		struct iovec *iov = iov_cache[host_id];
		size_t packet_size = 0;
		int iovcnt = 0;
		do {
			struct bsync_send_elem *elem =
				rlist_shift_entry(&BSYNC_REMOTE.send_queue,
						  struct bsync_send_elem, list);
			assert(BSYNC_REMOTE.send_queue_size > 0);
			--BSYNC_REMOTE.send_queue_size;
			assert(elem->code < bsync_mtype_count);
			if (elem->code == bsync_mtype_body ||
				elem->code == bsync_mtype_proxy_accept)
			{
				++BSYNC_REMOTE.op_queue_size;
				rlist_add_tail_entry(&BSYNC_REMOTE.op_queue,
						elem, list);
				struct bsync_operation *oper = ((bsync_operation *)elem->arg);
				say_debug("send to %s message with type %s, "
					  "lsn=%d:%ld(%ld)",
					  BSYNC_REMOTE.name,
					  bsync_mtype_name[elem->code],
					  LAST_ROW(oper->req)->server_id,
					  LAST_ROW(oper->req)->lsn,
					  oper->sign);
			} else if (elem->code == bsync_mtype_leader_reject ||
				   elem->code == bsync_mtype_leader_promise) {
				say_info("send to %s message with type %s. "
					 "local sign=%ld, remote_sign=%ld",
					 BSYNC_REMOTE.name,
					 bsync_mtype_name[elem->code],
					 BSYNC_LOCAL.commit_sign,
					 BSYNC_REMOTE.commit_sign);
			} else if (elem->code == bsync_mtype_proxy_request ||
				elem->code == bsync_mtype_submit ||
				elem->code == bsync_mtype_reject)
			{
				struct bsync_operation *oper = ((bsync_operation *)elem->arg);
				say_debug("send to %s message with type %s, lsn is %d:%ld (%ld)",
					BSYNC_REMOTE.name,
					bsync_mtype_name[elem->code],
					LAST_ROW(oper->req)->server_id,
					LAST_ROW(oper->req)->lsn, oper->sign);
			} else if (elem->code < bsync_mtype_sysend &&
				   elem->code != bsync_mtype_ping) {
				say_info("send to %s message with type %s",
					 BSYNC_REMOTE.name,
					 bsync_mtype_name[elem->code]);
			} else {
				say_debug("send to %s message with type %s",
					  BSYNC_REMOTE.name,
					  bsync_mtype_name[elem->code]);
			}
			if (elem->code < bsync_mtype_sysend)
				iovcnt += encode_sys_request(host_id, elem,
							     iov + iovcnt,
							     &packet_size);
			else
				iovcnt += encode_request(host_id, elem,
							 iov + iovcnt,
							 &packet_size);
			bool stop = (elem->code == bsync_mtype_iproto_switch);
			if (elem->system)
				mempool_free(&bsync_state.system_send_pool, elem);
			if (stop) {
				while (!rlist_empty(&BSYNC_REMOTE.send_queue)) {
					elem = rlist_shift_entry(
						&BSYNC_REMOTE.send_queue,
						struct bsync_send_elem, list);
					assert(elem->system);
					mempool_free(&bsync_state.system_send_pool, elem);
				}
				BSYNC_REMOTE.send_queue_size = 0;
				BSYNC_REMOTE.fiber_out = NULL;
				break;
			}
		} while (!rlist_empty(&BSYNC_REMOTE.send_queue) &&
			 (iovcnt + XROW_IOVMAX) < BUFV_IOVMAX &&
			 packet_size < MAX_PACKET_SIZE);
		bsync_writev(coio, iov, iovcnt, host_id);
		say_debug("wrote %ld bytes in %d parts to %s",
			  packet_size, iovcnt, BSYNC_REMOTE.name);
	}
}

static void
bsync_outgoing(va_list ap)
{
	struct ev_io coio;
	coio_init(&coio);
	coio.fd = va_arg(ap, int);
	int host_id = va_arg(ap, int);
	BSYNC_REMOTE.fiber_out = fiber();
	auto fiber_guard = make_scoped_guard([&](){
		if (fiber() == BSYNC_REMOTE.fiber_out)
			BSYNC_REMOTE.fiber_out = NULL;
	});
	fiber_set_joinable(fiber(), true);
	BSYNC_REMOTE.fiber_out_fail = false;
	say_debug("start outgoing to %s, fd=%d", BSYNC_REMOTE.name, coio.fd);
	try {
		{
			struct iobuf *iobuf = iobuf_new();
			BSYNC_REMOTE.flags |= bsync_host_active_write;
			auto iobuf_guard = make_scoped_guard([&]() {
				BSYNC_REMOTE.flags &= ~bsync_host_active_write;
				iobuf_delete(iobuf);
				fiber_gc();
			});
			bsync_read_package(&coio, &iobuf->in, host_id, false);
			uint32_t type = mp_decode_uint((const char **)&iobuf->in.rpos);
			assert(type == bsync_mtype_bsync_switch);
			say_info("receive from %s bsync_switch", BSYNC_REMOTE.name);
		}
		bool force = false;
		while (bsync_is_out_valid(host_id))
		{
			if (bsync_extended_header_size(host_id, false, 0, 0) > 0 &&
				rlist_empty(&BSYNC_REMOTE.send_queue) &&
				bsync_state.state != bsync_state_shutdown &&
				!force)
			{
				if (bsync_state.submit_timeout > 0.0001)
					fiber_yield_timeout(bsync_state.submit_timeout);
				if (BSYNC_REMOTE.send_queue_size == 0) {
					bsync_send_data(&BSYNC_REMOTE,
						&BSYNC_REMOTE.ping_msg);
				}
			}
			force = false;
			bsync_send(&coio, host_id);
			fiber_gc();
			if (bsync_extended_header_size(host_id, false, 0, 0) != 0)
				continue;
			if (bsync_is_out_valid(host_id) &&
				rlist_empty(&BSYNC_REMOTE.send_queue))
			{
				if (BSYNC_REMOTE.state == bsync_host_connected) {
					BSYNC_REMOTE.flags |= bsync_host_ping_sleep;
					bool ping_res = fiber_yield_timeout(bsync_state.ping_timeout);
					BSYNC_REMOTE.flags &= ~bsync_host_ping_sleep;
					if (BSYNC_REMOTE.send_queue_size > 0 || !ping_res)
						continue;
					assert(rlist_empty(&BSYNC_REMOTE.send_queue));
					bsync_send_data(&BSYNC_REMOTE, &BSYNC_REMOTE.ping_msg);
					force = true;
				} else {
					BSYNC_REMOTE.flags |= bsync_host_ping_sleep;
					fiber_yield();
					BSYNC_REMOTE.flags &= ~bsync_host_ping_sleep;
				}
			}
		}
	} catch (Exception *e) {
		if (bsync_is_out_valid(host_id)) {
			say_info("outgoing failed, remote state is %d, "
				 "local state is %d, reason: %s",
				 BSYNC_REMOTE.state, bsync_state.state,
				 e->errmsg());
			BSYNC_REMOTE.fiber_out_fail = true;
			bsync_disconnected(host_id);
		} else {
			say_info("outgoing failed, but host didnt disconnected."
				 " remote state is %d, local state is %d",
				 BSYNC_REMOTE.state, bsync_state.state);
		}
	}
}

/*
 * System block:
 * 1. initialize local variables;
 * 2. read cfg;
 * 3. start/stop cord and event loop
 */

static void
bsync_cfg_push_host(uint8_t host_id, const char *source)
{
	strncpy(BSYNC_REMOTE.name, source, 1024);
	BSYNC_REMOTE.fiber_in = NULL;
	BSYNC_REMOTE.fiber_out = NULL;
	BSYNC_REMOTE.sign = -1;
	BSYNC_REMOTE.commit_sign = -1;
	BSYNC_REMOTE.flags = 0;
	BSYNC_REMOTE.state = bsync_host_disconnected;
	rlist_create(&BSYNC_REMOTE.op_queue);
	rlist_create(&BSYNC_REMOTE.send_queue);
	rlist_create(&BSYNC_REMOTE.follow_queue);
	BSYNC_REMOTE.op_queue_size = 0;
	BSYNC_REMOTE.send_queue_size = 0;
	BSYNC_REMOTE.follow_queue_size = 0;
	BSYNC_REMOTE.active_ops = mh_bsync_new();
	BSYNC_REMOTE.body_cur = 0;
	BSYNC_REMOTE.proxy_rows = NULL;
	BSYNC_REMOTE.proxy_size = 0;
	BSYNC_REMOTE.proxy_cur = 0;
	BSYNC_REMOTE.proxy_sign = 0;

	if (BSYNC_REMOTE.active_ops == NULL)
		panic("out of memory");
	memset(&BSYNC_REMOTE.sysmsg, 0, sizeof(struct bsync_txn_info));
	BSYNC_REMOTE.sysmsg.connection = host_id;
	BSYNC_REMOTE.ping_msg.code = bsync_mtype_ping;
	BSYNC_REMOTE.ping_msg.arg = NULL;
	BSYNC_REMOTE.ping_msg.system = false;

	pthread_mutexattr_t errorcheck;
	(void) tt_pthread_mutexattr_init(&errorcheck);
#ifndef NDEBUG
	(void) tt_pthread_mutexattr_settype(&errorcheck, PTHREAD_MUTEX_ERRORCHECK);
#endif
	/* Initialize queue lock mutex. */
	(void) tt_pthread_mutex_init(&txn_state.mutex[host_id], &errorcheck);
	(void) tt_pthread_mutexattr_destroy(&errorcheck);
	(void) tt_pthread_cond_init(&txn_state.cond[host_id], NULL);
}

static void
bsync_cfg_read()
{
	bsync_state.num_connected = 0;
	bsync_state.state = bsync_state_election;
	bsync_state.local_id = BSYNC_MAX_HOSTS;
	bsync_state.leader_id = BSYNC_MAX_HOSTS;
	bsync_state.accept_id = BSYNC_MAX_HOSTS;
	txn_state.leader_id = BSYNC_MAX_HOSTS;
	txn_state.local_id = BSYNC_MAX_HOSTS;

	bsync_state.read_timeout = cfg_getd("replication.read_timeout");
	bsync_state.write_timeout = cfg_getd("replication.write_timeout");
	bsync_state.operation_timeout = cfg_getd(
		"replication.operation_timeout");
	bsync_state.ping_timeout = cfg_getd("replication.ping_timeout");
	bsync_state.election_timeout = cfg_getd("replication.election_timeout");
	bsync_state.submit_timeout = cfg_getd("replication.submit_timeout");
	bsync_state.num_hosts = cfg_getarr_size("replication.source");
	bsync_state.max_host_queue = cfg_geti("replication.max_host_queue");
}

void
bsync_init(struct recovery_state *r)
{
	local_state = r;
	if (!r->bsync_remote) {
		return;
	}
	bsync_state.proxy_host = BSYNC_MAX_HOSTS;
	bsync_state.wal_commit_sign = 0;
	bsync_state.wal_rollback_sign = 0;
	bsync_state.wal_push_fiber = NULL;
	memset(&bsync_state.sysmsg, 0, sizeof(struct bsync_txn_info));
	/* I. Initialize the state. */
	pthread_mutexattr_t errorcheck;

	(void) tt_pthread_mutexattr_init(&errorcheck);
#ifndef NDEBUG
	(void) tt_pthread_mutexattr_settype(&errorcheck,
		PTHREAD_MUTEX_ERRORCHECK);
#endif
	tt_pthread_mutexattr_settype(&errorcheck, PTHREAD_MUTEX_RECURSIVE);
	/* Initialize queue lock mutex. */
	(void) tt_pthread_mutex_init(&bsync_state.mutex, &errorcheck);
	(void) tt_pthread_mutex_init(&bsync_state.active_ops_mutex,
		&errorcheck);
	(void) tt_pthread_mutexattr_destroy(&errorcheck);

	(void) tt_pthread_cond_init(&bsync_state.cond, NULL);

	STAILQ_INIT(&bsync_state.txn_proxy_input);
	STAILQ_INIT(&bsync_state.txn_proxy_queue);
	STAILQ_INIT(&bsync_state.bsync_proxy_input);
	STAILQ_INIT(&bsync_state.bsync_proxy_queue);
	bsync_state.txn_fibers.size = 0;
	bsync_state.txn_fibers.active = 0;
	bsync_state.bsync_fibers.size = 0;
	bsync_state.bsync_fibers.active = 0;
	bsync_state.bsync_fibers.active_local = 0;
	bsync_state.bsync_fibers.active_proxy = 0;
	bsync_state.bsync_fibers.proxy_yield = 0;
	bsync_state.bsync_rollback = false;
	bsync_state.active_ops = 0;
	rlist_create(&txn_state.txn_queue);
	rlist_create(&txn_state.execute_queue);
	rlist_create(&bsync_state.txn_fibers.data);
	rlist_create(&bsync_state.bsync_fibers.data);
	rlist_create(&bsync_state.proxy_queue);
	rlist_create(&bsync_state.submit_queue);
	rlist_create(&bsync_state.wal_queue);
	rlist_create(&bsync_state.wait_queue);
	rlist_create(&bsync_state.election_ops);
	rlist_create(&bsync_state.region_free);
	rlist_create(&bsync_state.txn_queue);
	rlist_create(&bsync_state.commit_queue);
	rlist_create(&bsync_state.accept_queue);
	bsync_state.region_free_size = 0;
	rlist_create(&bsync_state.region_gc);
	rlist_create(&txn_state.incoming_connections);
	rlist_create(&txn_state.wait_start);
	if (recovery_has_data(r))
		txn_state.state = txn_state_subscribe;
	else
		txn_state.state = txn_state_join;

	mempool_create(&bsync_state.region_pool, &cord()->slabc,
		sizeof(struct bsync_region));

	txn_state.recovery = false;
	txn_state.snapshot_fiber = NULL;
	memset(txn_state.iproto, 0, sizeof(txn_state.iproto));
	memset(txn_state.join, 0, sizeof(txn_state.join));
	memset(txn_state.wait_local, 0, sizeof(txn_state.wait_local));
	memset(txn_state.id2index, BSYNC_MAX_HOSTS, sizeof(txn_state.id2index));

	ev_async_init(&txn_process_event, bsync_txn_process);
	ev_async_init(&bsync_process_event, bsync_process_loop);

	txn_loop = loop();

	ev_async_start(loop(), &txn_process_event);

	/* II. Start the thread. */
	bsync_cfg_read();
	for (int i = 0; i < r->remote_size; ++i) {
		bsync_cfg_push_host(i, r->remote[i].source);
	}
	tt_pthread_mutex_lock(&bsync_state.mutex);
	if (!cord_start(&bsync_state.cord, "bsync", bsync_thread, NULL)) {
		tt_pthread_cond_wait(&bsync_state.cond, &bsync_state.mutex);
		tt_pthread_mutex_unlock(&bsync_state.mutex);
	} else {
		local_state = NULL;
	}
}

void
bsync_start(struct recovery_state *r, int rows_per_wal)
{
	wal_writer_init(r, rows_per_wal);
	if (!r->bsync_remote) {
		wal_writer_start(r);
		return;
	}
	assert(local_state == r);
	txn_state.state = txn_state_subscribe;
	struct bsync_txn_info *info = &bsync_state.sysmsg;
	vclock_copy(&bsync_state.vclock, &r->vclock);
	info->result = rows_per_wal;
	SWITCH_TO_BSYNC(bsync_start_event);
	if (txn_state.leader_id < BSYNC_MAX_HOSTS &&
		!local_state->remote[txn_state.leader_id].localhost)
		return;

	for (int i = 0; i < local_state->remote_size; ++i) {
		if (!local_state->remote[i].connected ||
			local_state->remote[i].localhost ||
			local_state->remote[i].writer == NULL)
			continue;
		fiber_call(local_state->remote[i].writer);
	}
}

static void
bsync_election(va_list /* ap */)
{
	fiber_yield_timeout(bsync_state.election_timeout);
	if (bsync_state.state != bsync_state_election)
		return;
	bsync_state.state = bsync_state_initial;
	bsync_start_election();
}

static void
bsync_dump_statistic(va_list /* ap */)
{
	for(;;) {
		fiber_yield_timeout(1);
		say_debug("[statistic] [fibers] size=%ld, active=%ld,"
			  " local=%ld, proxy=%ld, proxy_yield=%ld",
			  bsync_state.bsync_fibers.size,
			  bsync_state.bsync_fibers.active,
			  bsync_state.bsync_fibers.active_local,
			  bsync_state.bsync_fibers.active_proxy,
			  bsync_state.bsync_fibers.proxy_yield);
	}
}

static void*
bsync_thread(void*)
{
	tt_pthread_mutex_lock(&bsync_state.mutex);
	iobuf_init();
	mempool_create(&bsync_state.system_send_pool, &cord()->slabc,
		sizeof(struct bsync_send_elem));
	for (int i = 0; i < bsync_state.num_hosts; ++i)
		mempool_create(&bsync_index[i].proxy_pool, &cord()->slabc,
			sizeof(struct xrow_header));
	if (bsync_state.num_hosts == 1) {
		bsync_state.leader_id = bsync_state.local_id = 0;
		bsync_index[0].state = bsync_host_connected;
		bsync_state.state = bsync_state_ready;
	}
	bsync_loop = loop();
	ev_async_start(loop(), &bsync_process_event);
	tt_pthread_cond_signal(&bsync_state.cond);
	tt_pthread_mutex_unlock(&bsync_state.mutex);
	fiber_start(fiber_new("bsync_election", bsync_election), false);
	fiber_start(fiber_new("bsync_statistic", bsync_dump_statistic), false);
	try {
		ev_run(loop(), 0);
	} catch (...) {
		say_crit("bsync_thread found unhandled exception");
		throw;
	}
	ev_async_stop(bsync_loop, &bsync_process_event);
	say_info("bsync stopped");
	return NULL;
}

void
bsync_writer_stop(struct recovery_state *r)
{
	assert(local_state == r);
	if (!local_state->bsync_remote) {
		recovery_exit(r);
		return;
	}
	if (cord_is_main()) {
		tt_pthread_mutex_lock(&bsync_state.mutex);
		if (bsync_state.bsync_rollback) {
			tt_pthread_cond_signal(&bsync_state.cond);
		}
		tt_pthread_mutex_unlock(&bsync_state.mutex);
		struct bsync_txn_info *info = &bsync_state.sysmsg;
		SWITCH_TO_BSYNC(bsync_shutdown);
		if (&bsync_state.cord != cord() && cord_join(&bsync_state.cord)) {
			panic_syserror("BSYNC writer: thread join failed");
		}
	}
	ev_async_stop(txn_loop, &txn_process_event);
}
