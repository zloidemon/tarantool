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
#include "relay.h"
#include <say.h>
#include "scoped_guard.h"

#include "recovery.h"
#include "xlog.h"
#include "iproto_constants.h"
#include "engine.h"
#include "cluster.h"
#include "schema.h"
#include "vclock.h"
#include "xrow.h"
#include "coeio.h"
#include "coio.h"
#include "cfg.h"
#include "trigger.h"
#include "errinj.h"

void
relay_send_row(struct recovery *r, void *param, struct xrow_header *packet);

static inline void
relay_create(struct relay *relay, int fd, uint64_t sync)
{
	memset(relay, 0, sizeof(*relay));
	relay->r = recovery_new(cfg_gets("snap_dir"), cfg_gets("wal_dir"),
			 relay_send_row, relay);
	recovery_setup_panic(relay->r, cfg_geti("panic_on_snap_error"),
			     cfg_geti("panic_on_wal_error"));

	coio_init(&relay->io, fd);
	relay->sync = sync;
	relay->wal_dir_rescan_delay = cfg_getd("wal_dir_rescan_delay");
}

static inline void
relay_destroy(struct relay *relay)
{
	recovery_delete(relay->r);
}

static inline void
relay_set_cord_name(int fd)
{
	char name[FIBER_NAME_MAX];
	struct sockaddr_storage peer;
	socklen_t addrlen = sizeof(peer);
	getpeername(fd, ((struct sockaddr*)&peer), &addrlen);
	snprintf(name, sizeof(name), "relay/%s",
		 sio_strfaddr((struct sockaddr *)&peer, addrlen));
	cord_set_name(name);
}

void
relay_join_f(va_list ap)
{
	struct relay *relay = va_arg(ap, struct relay *);

	relay_set_cord_name(relay->io.fd);
	iobuf_create(&relay->iobuf, &cord()->slabc);
	auto iobuf_guard = make_scoped_guard([=]{
		iobuf_destroy(&relay->iobuf);
	});

	/* Send snapshot */
	engine_join(relay);

	say_info("snapshot sent");
}

void
relay_join(int fd, struct xrow_header *packet,
	   uint32_t master_server_id,
	   void (*on_join)(const struct tt_uuid *))
{
	struct relay relay;
	relay_create(&relay, fd, packet->sync);
	auto scope_guard = make_scoped_guard([&]{
		relay_destroy(&relay);
	});
	struct recovery *r = relay.r;

	struct tt_uuid server_uuid = uuid_nil;
	xrow_decode_join(packet, &server_uuid);

	cord_costart(&relay.cord, "join", relay_join_f, &relay);
	cord_cojoin(&relay.cord);
	diag_raise();
	/**
	 * Call the server-side hook which stores the replica uuid
	 * in _cluster space after sending the last row but before
	 * sending OK - if the hook fails, the error reaches the
	 * client.
	 */
	on_join(&server_uuid);

	/*
	 * Send a response to JOIN request, an indicator of the
	 * end of the stream of snapshot rows.
	 */
	struct xrow_header row;
	xrow_encode_vclock(&row, vclockset_last(&r->snap_dir.index));
	/*
	 * Identify the message with the server id of this
	 * server, this is the only way for a replica to find
	 * out the id of the server it has connected to.
	 */
	row.server_id = master_server_id;
	row.sync = row.sync;
	coio_write_xrow(&relay.io, &row);
}

/**
 * A libev callback invoked when a relay client socket is ready
 * for read. This currently only happens when the client closes
 * its socket, and we get an EOF.
 */
static void
relay_subscribe_f(va_list ap)
{
	struct relay *relay = va_arg(ap, struct relay *);
	struct recovery *r = relay->r;
	relay_set_cord_name(relay->io.fd);
	iobuf_create(&relay->iobuf, &cord()->slabc);
	auto iobuf_guard = make_scoped_guard([=]{
		iobuf_destroy(&relay->iobuf);
	});
	recovery_follow_local(r, fiber_name(fiber()),
			      relay->wal_dir_rescan_delay);
	recovery_join_local(r);

	say_crit("exiting the relay loop");
}

/** Replication acceptor fiber handler. */
void
relay_subscribe(int fd, struct xrow_header *packet,
		uint32_t master_server_id,
		struct vclock *master_vclock)
{
	struct relay relay;
	relay_create(&relay, fd, packet->sync);
	auto scope_guard = make_scoped_guard([&]{
		relay_destroy(&relay);
	});

	struct tt_uuid uu = uuid_nil, server_uuid = uuid_nil;

	struct recovery *r = relay.r;
	xrow_decode_subscribe(packet, &uu, &server_uuid, &r->vclock);

	/**
	 * Check that the given UUID matches the UUID of the
	 * cluster this server belongs to. Used to handshake
	 * replica connect, and refuse a connection from a replica
	 * which belongs to a different cluster.
	 */
	if (!tt_uuid_is_equal(&uu, &cluster_id)) {
		tnt_raise(ClientError, ER_CLUSTER_ID_MISMATCH,
			  tt_uuid_str(&uu), tt_uuid_str(&cluster_id));
	}

	/* Check server uuid */
	r->server_id = schema_find_id(BOX_CLUSTER_ID, 1,
				      tt_uuid_str(&server_uuid), UUID_STR_LEN);
	if (r->server_id == BOX_ID_NIL) {
		tnt_raise(ClientError, ER_UNKNOWN_SERVER,
			  tt_uuid_str(&server_uuid));
	}
	/*
	 * Send a response to SUBSCRIBE request, tell
	 * the replica how many rows we have in stock for it,
	 * and identify ourselves with our own server id.
	 */
	struct xrow_header row;
	xrow_encode_vclock(&row, master_vclock);
	/*
	 * Identify the message with the server id of this
	 * server, this is the only way for a replica to find
	 * out the id of the server it has connected to.
	 */
	row.server_id = master_server_id;
	row.sync = row.sync;
	coio_write_xrow(&relay.io, &row);

	struct cord cord;
	cord_costart(&cord, "subscribe", relay_subscribe_f, &relay);
	cord_cojoin(&cord);
	diag_raise();
}

void
relay_send(struct relay *relay, struct xrow_header *packet)
{
	/* Send request to the remote peer */
	packet->sync = relay->sync;
	coio_write_xrow(&relay->io, packet);

	/* Wait for acknowledgement from the remote server */
	struct xrow_header resp;
	coio_read_xrow(&relay->io, &relay->iobuf.in, &resp);
	if (resp.sync != relay->sync) {
		tnt_raise(LoggedError, ER_UNEXPECTED_PACKET_SYNC,
			  (unsigned long long) resp.sync,
			  (unsigned long long) packet->sync);
	} else if (iproto_type_is_error(resp.type)) {
		xrow_decode_error(&resp);
		diag_raise();
	} else if (resp.type != IPROTO_OK) {
		tnt_raise(LoggedError, ER_UNEXPECTED_PACKET_TYPE,
			  resp.type);
	}

	iobuf_reset(&relay->iobuf);
}

/** Send a single row to the client. */
void
relay_send_row(struct recovery *r, void *param, struct xrow_header *packet)
{
	struct relay *relay = (struct relay *) param;
	assert(iproto_type_is_dml(packet->type));

	/*
	 * If packet->server_id == 0 this is a snapshot packet.
	 * (JOIN request). In this case, send every row.
	 * Otherwise, we're feeding a WAL, thus responding to
	 * SUBSCRIBE request. In that case, only send a row if
	 * it is not from the same server (i.e. don't send
	 * replica's own rows back).
	 */
	if (packet->server_id == 0 || packet->server_id != r->server_id) {
		relay_send(relay, packet);
		ERROR_INJECT(ERRINJ_RELAY,
		{
			fiber_sleep(1000.0);
		});
	}
	/*
	 * Update local vclock. During normal operation wal_write()
	 * updates local vclock. In relay mode we have to update
	 * it here.
	 */
	vclock_follow(&r->vclock, packet->server_id, packet->lsn);
}
