/*
 * Copyright 2010-2015, Tarantool AUTHORS, please see AUTHORS file.
 *
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
#include "box.h"
#include "cluster.h"
#include "recovery.h"
#include "applier.h"
#include "vclock.h" /* VCLOCK_MAX */

/**
 * Globally unique identifier of this cluster.
 * A cluster is a set of connected appliers.
 */
tt_uuid cluster_id;

/*
 * A set of replicas by server_id.
 * The set is implememted as static array to avoid handling out of memory
 * problems. Replication doesn't support more than VCLOCK_MAX anyway.
 * Records with replicaset[server_id].id == 0 is empty (not set).
 */
static struct replica replicaset[VCLOCK_MAX];

typedef rb_tree(struct applier) applierset_t;
rb_proto(, applierset_, applierset_t, struct applier)

static int
applier_compare_by_source(const struct applier *a, const struct applier *b)
{
	return strcmp(a->source, b->source);
}

rb_gen(, applierset_, applierset_t, struct applier, link,
       applier_compare_by_source);

static applierset_t applierset; /* zeroed by linker */

void
cluster_init(void)
{
	applierset_new(&applierset);
}

void
cluster_free(void)
{

}

extern "C" struct vclock *
cluster_clock()
{
        return &recovery->vclock;
}

struct replica *
cluster_get_server(uint32_t server_id)
{
	if (server_id >= VCLOCK_MAX || replicaset[server_id].id == 0)
		return NULL;
	return &replicaset[server_id];
}

static inline struct replica *
replicaset_skip_empty(struct replica *replica)
{
	for (; replica < replicaset + VCLOCK_MAX; ++replica) {
		if (replica->id != 0)
			return replica;
	}
	return NULL;
}

struct replica *
cluster_server_first(void)
{
	return replicaset_skip_empty(replicaset);
}

struct replica *
cluster_server_next(struct replica *replica)
{
	return replicaset_skip_empty(replica + 1);
}

void
cluster_set_server(const tt_uuid *server_uuid, uint32_t server_id)
{
	struct recovery *r = ::recovery;
	say_warn("set_server: %u uuid=%s", server_id, tt_uuid_str(server_uuid));
	say_warn("          : r->%u r->uuid=%s %s", r->server_id, tt_uuid_str(&r->server_uuid),
			vclock_to_string(&r->vclock));

	/** Checked in the before-commit trigger */
	assert(!tt_uuid_is_nil(server_uuid));
	assert(!cserver_id_is_reserved(server_id));

	/* Update replicaset */
	struct replica *replica = &replicaset[server_id];
	if (replica->id == 0) {
		/* Add server to replicaset */
		replica->id = server_id;

		/* Add server to vclock */
		vclock_add_server(&r->vclock, server_id);
	}

	memcpy(&replica->uuid, server_uuid, sizeof(*server_uuid));

	if (tt_uuid_is_equal(&r->server_uuid, server_uuid)) {
		/* Assign local server id */
		assert(r->server_id == 0);
		r->server_id = server_id;
		/*
		 * Leave read-only mode
		 * if this is a running server.
		 * Otherwise, read only is switched
		 * off after recovery_finalize().
		 */
		if (r->writer)
			box_set_ro(false);
	} else if (r->server_id == server_id) {
		say_warn("server UUID changed to %s",
			 tt_uuid_str(server_uuid));
		assert(vclock_has(&r->vclock, server_id));
		memcpy(&r->server_uuid, server_uuid, sizeof(*server_uuid));
	}
	say_warn("          : r->%u r->uuid=%s %s", r->server_id, tt_uuid_str(&r->server_uuid),
			vclock_to_string(&r->vclock));
}

void
cluster_del_server(uint32_t server_id)
{
	say_warn("del server: %u", server_id);

	/* Remove server from replicaset */
	struct replica *replica = cluster_get_server(server_id);
	assert(replica != NULL);
	replica->id = 0;

	/* Remove server from vclock */
	struct recovery *r = ::recovery;
	vclock_del_server(&r->vclock, server_id);
	if (r->server_id == server_id) {
		r->server_id = 0;
		box_set_ro(true);
	}
}

void
cluster_add_applier(struct applier *applier)
{
	applierset_insert(&applierset, applier);
}

void
cluster_del_applier(struct applier *applier)
{
	applierset_remove(&applierset, applier);
}

struct applier *
cluster_find_applier(const char *source)
{
	struct applier key;
	snprintf(key.source, sizeof(key.source), "%s", source);
	return applierset_search(&applierset, &key);
}

struct applier *
cluster_applier_first(void)
{
	return applierset_first(&applierset);
}

struct applier *
cluster_applier_next(struct applier *applier)
{
	return applierset_next(&applierset, applier);
}
