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
#include "applier.h"

#include <msgpuck.h>

#include "xlog.h"
#include "fiber.h"
#include "scoped_guard.h"
#include "coio.h"
#include "coio_buf.h"
#include "xstream.h"
#include "recovery.h"
#include "xrow.h"
#include "request.h"
#include "box/cluster.h"
#include "iproto_constants.h"
#include "version.h"
#include "trigger.h"
#include "xrow_io.h"
#include "cbus.h"
#include "schema.h"

#define APPLIER_IN_FLIGHT_LIMIT 1024

/* TODO: add configuration options */
static const int RECONNECT_DELAY = 1;
static const int CONNECT_TIMEOUT = 30;

extern struct cpipe tx_pipe;
extern struct cpipe net_pipe;
extern struct cbus net_tx_bus;

static unsigned int in_flight = 0;
static bool appliers_flushing = false;
static struct rlist flush_waiting_fibers =
	RLIST_HEAD_INITIALIZER(flush_waiting_fibers);

STRS(applier_state, applier_STATE);

static inline void
applier_set_state(struct applier *applier, enum applier_state state)
{
	applier->state = state;
	say_debug("=> %s", applier_state_strs[state] +
		  strlen("APPLIER_"));
	trigger_run(&applier->on_state, applier);
}

/**
 * Connect to a remote host and authenticate the client.
 */
void
applier_connect(struct applier *applier)
{
	struct ev_io *coio = &applier->io;
	struct iobuf *iobuf = applier->iobuf[applier->input_index];
	if (coio->fd >= 0)
		return;
	char greetingbuf[IPROTO_GREETING_SIZE];

	struct uri *uri = &applier->uri;
	/*
	 * coio_connect() stores resolved address to \a &applier->addr
	 * on success. &applier->addr_len is a value-result argument which
	 * must be initialized to the size of associated buffer (addrstorage)
	 * before calling coio_connect(). Since coio_connect() performs
	 * DNS resolution under the hood it is theoretically possible that
	 * applier->addr_len will be different even for same uri.
	 */
	applier->addr_len = sizeof(applier->addrstorage);
	applier_set_state(applier, APPLIER_CONNECT);
	coio_connect(coio, uri, &applier->addr, &applier->addr_len);
	assert(coio->fd >= 0);
	coio_readn(coio, greetingbuf, IPROTO_GREETING_SIZE);
	applier->last_row_time = ev_now(loop());

	/* Decode server version and name from greeting */
	struct greeting greeting;
	if (greeting_decode(greetingbuf, &greeting) != 0)
		tnt_raise(LoggedError, ER_PROTOCOL, "Invalid greeting");

	if (strcmp(greeting.protocol, "Binary") != 0) {
		tnt_raise(LoggedError, ER_PROTOCOL,
			  "Unsupported protocol for replication");
	}

	/*
	 * Forbid changing UUID dynamically on connect because
	 * applier is registered by UUID in cluster.h.
	 */
	if (!tt_uuid_is_nil(&applier->uuid) &&
	    !tt_uuid_is_equal(&applier->uuid, &greeting.uuid)) {
		Exception *e = tnt_error(ClientError, ER_SERVER_UUID_MISMATCH,
					 tt_uuid_str(&applier->uuid),
					 tt_uuid_str(&greeting.uuid));
		/* Log the error only once. */
		if (!applier->warning_said)
			e->log();
		e->raise();
	}

	/* Save the remote server version and UUID on connect. */
	applier->uuid = greeting.uuid;
	applier->version_id = greeting.version_id;

	say_info("connected to %u.%u.%u at %s\r\n",
		 version_id_major(greeting.version_id),
		 version_id_minor(greeting.version_id),
		 version_id_patch(greeting.version_id),
		 sio_strfaddr(&applier->addr, applier->addr_len));

	/* Don't display previous error messages in box.info.replication */
	diag_clear(&fiber()->diag);

	/* Perform authentication if user provided at least login */
	if (!uri->login) {
		applier_set_state(applier, APPLIER_CONNECTED);
		return;
	}

	/* Authenticate */
	applier_set_state(applier, APPLIER_AUTH);
	struct xrow_header row;
	xrow_encode_auth(&row, greeting.salt, greeting.salt_len, uri->login,
			 uri->login_len, uri->password,
			 uri->password_len);
	coio_write_xrow(coio, &row);
	coio_read_xrow(coio, &iobuf->in, &row);
	applier->last_row_time = ev_now(loop());
	if (row.type != IPROTO_OK)
		xrow_decode_error(&row); /* auth failed */
	/* auth successed */
	say_info("authenticated");
	applier_set_state(applier, APPLIER_CONNECTED);
}

struct recovery_apply_row_msg: public cmsg
{
	int len;
	struct iobuf *iobuf;
	struct applier *applier;
	struct xstream *stream;
	struct xrow_header row;
	struct request request;
};

static void
tx_recovery_do_apply_row(struct cmsg *m)
{
	struct recovery_apply_row_msg *msg = (struct recovery_apply_row_msg *)m;
	xstream_write(msg->stream, &msg->row);
}

static void
net_recovery_on_apply_row(struct cmsg *m)
{
	struct recovery_apply_row_msg *msg = (struct recovery_apply_row_msg *)m;
	msg->iobuf->in.rpos += msg->len;
	--in_flight;
	if (!in_flight) {
		/** No pending requests, we can wakeup all fibers */
		appliers_flushing = false;
		struct fiber *f;
		while (!rlist_empty(&flush_waiting_fibers)) {
			f = rlist_shift_entry(&flush_waiting_fibers,
				struct fiber, state);
			fiber_wakeup(f);
		}
	}
	if ((ibuf_used(&msg->iobuf->in) == msg->applier->input_unparsed)
		&& msg->applier->want_swap_buffers) {
		/** Buffer has no "hot" data and should be swapped */
		fiber_wakeup(msg->applier->reader);
	}
	mempool_free(&msg->applier->msg_pool, msg);
}

static void
net_recovery_apply_row(struct applier *applier, struct xstream *stream,
	struct xrow_header *row, int len)
{
	static const struct cmsg_hop apply_route[] = {
		{ tx_recovery_do_apply_row, &net_pipe },
		{ net_recovery_on_apply_row, NULL },
	};
	struct recovery_apply_row_msg *msg = (struct recovery_apply_row_msg *)
		mempool_alloc_xc(&applier->msg_pool);
	cmsg_init(msg, apply_route);
	msg->len = len;
	msg->iobuf = applier->iobuf[applier->input_index];
	msg->applier = applier;
	msg->stream = stream;
	msg->row = *row;
	request_create(&msg->request, msg->row.type);
	msg->request.header = &msg->row;
	request_decode(&msg->request,
		(const char *) msg->row.body[0].iov_base,
		msg->row.body[0].iov_len);

	while (appliers_flushing) {
		/** Wait for all pending request to be done */
		rlist_add_tail_entry(&flush_waiting_fibers, fiber(), state);
		fiber_yield();
	}

	++in_flight;
	if ((msg->request.space_id <= BOX_SYSTEM_ID_MAX) ||
		(in_flight > APPLIER_IN_FLIGHT_LIMIT)) {
		appliers_flushing = true;
	}

	cpipe_push(&tx_pipe, msg);
}

/**
 * Return amount of data needs to be readen, or negative
 * value if input buffer is busy.
 */
static ssize_t
applier_can_read(struct applier *applier)
{
	struct iobuf *oldbuf = applier->iobuf[applier->input_index];
	ssize_t to_read = 3;
	if (applier->input_unparsed) {
		const char *pos = oldbuf->in.wpos - applier->input_unparsed;
		if ((to_read = mp_check_uint(pos, oldbuf->in.wpos)) <= 0) {
			size_t len = mp_decode_uint(&pos);
			to_read = len - (oldbuf->in.wpos - pos);
		}
	}

	if (to_read <= (ssize_t)ibuf_unused(&oldbuf->in))
		return to_read;

	if (ibuf_used(&oldbuf->in) == applier->input_unparsed) {
		ibuf_reserve_xc(&oldbuf->in, to_read);
		return to_read;
	}

	struct iobuf *newbuf = applier->iobuf[1 - applier->input_index];
	if (ibuf_used(&newbuf->in)) {
		/** Buffer is busy */
		return -to_read;
	}

	/** Swap buffers */
	iobuf_reset_mt(newbuf);
	ibuf_reserve_xc(&newbuf->in, to_read + applier->input_unparsed);
	memmove(newbuf->in.wpos, oldbuf->in.wpos - applier->input_unparsed,
		applier->input_unparsed);
	newbuf->in.wpos += applier->input_unparsed;
	oldbuf->in.wpos -= applier->input_unparsed;
	applier->input_index = 1 - applier->input_index;

	return to_read;
}

/**
 * Read data or yield if input buffer is busy
 */
static void
applier_read(struct applier *applier)
{
	ssize_t to_read;
	while ((to_read = applier_can_read(applier)) < 0) {
		applier->want_swap_buffers = true;
		fiber_yield();
		applier->want_swap_buffers = false;
	}
	struct iobuf *iobuf = applier->iobuf[applier->input_index];
	struct ibuf *in = &iobuf->in;
	ssize_t readen = 0;
	if (to_read > 0) {
		readen = coio_readn_ahead(&applier->io, in->wpos,
			to_read, ibuf_unused(in));
	}
	in->wpos += readen;
	applier->input_unparsed += readen;
}

/**
 * Parse xrow on input buffer, return parsed len or
 * zero if we need to read more data
 */
static size_t
applier_parse_xrow(struct applier *applier, struct xrow_header *row)
{
	struct iobuf *iobuf = applier->iobuf[applier->input_index];
	struct ibuf *in = &iobuf->in;

	const char *reqstart = in->wpos - applier->input_unparsed;
	const char *pos = reqstart;

	if (mp_typeof(*pos) != MP_UINT) {
		tnt_raise(ClientError, ER_INVALID_MSGPACK,
			  "packet length");
	}
	if (mp_check_uint(pos, in->wpos) >= 0)
		return 0;
	uint32_t len = mp_decode_uint(&pos);
	const char *reqend = pos + len;
	if (reqend > in->wpos)
		return 0;
	xrow_header_decode(row, &pos, reqend);
	applier->input_unparsed -= reqend - reqstart;

	return reqend - reqstart;
}

/**
 * Execute and process JOIN request (bootstrap the server).
 */
static void
applier_join(struct applier *applier)
{
	assert(applier->join_stream != NULL);

	say_info("downloading a snapshot from %s",
		 sio_strfaddr(&applier->addr, applier->addr_len));

	/* Send JOIN request */
	struct ev_io *coio = &applier->io;
	struct xrow_header row;
	xrow_encode_join(&row, &SERVER_ID);
	coio_write_xrow(coio, &row);
	applier_set_state(applier, APPLIER_BOOTSTRAP);

	while (true) {
		applier_read(applier);
		struct iobuf *iobuf = applier->iobuf[applier->input_index];
		while (applier->input_unparsed) {
			size_t len = applier_parse_xrow(applier, &row);
			if (!len)
				break;
			applier->last_row_time = ev_now(loop());
			if (row.type == IPROTO_OK) {
				/* End of stream */
				say_info("done");
				iobuf->in.rpos += len;
				goto join_done;
			} else if (iproto_type_is_dml(row.type)) {
				/* Regular snapshot row  (IPROTO_INSERT) */
				net_recovery_apply_row(applier,
						       applier->join_stream,
						       &row, len);
			} else /* error or unexpected packet */ {
				iobuf->in.rpos += len;
				xrow_decode_error(&row);  /* rethrow error */
			}
		}
	}
join_done:
	/* Decode end of stream packet */
	vclock_create(&applier->vclock);
	assert(row.type == IPROTO_OK);
	xrow_decode_vclock(&row, &applier->vclock);

	/* Wait until all pending requests will be done */
	if (in_flight) {
		rlist_add_tail_entry(&flush_waiting_fibers, fiber(), state);
		fiber_yield();
	}
	applier_set_state(applier, APPLIER_CONNECTED);
}

/**
 * Execute and process SUBSCRIBE request (follow updates from a master).
 */
static void
applier_subscribe(struct applier *applier)
{
	assert(applier->subscribe_stream != NULL);

	/* Send SUBSCRIBE request */
	struct ev_io *coio = &applier->io;
	struct iobuf *iobuf = applier->iobuf[applier->input_index];
	struct xrow_header row;

	/* TODO: don't use struct recovery here */
	struct recovery *r = ::recovery;
	xrow_encode_subscribe(&row, &CLUSTER_ID, &SERVER_ID, &r->vclock);
	coio_write_xrow(coio, &row);
	applier_set_state(applier, APPLIER_FOLLOW);
	/* Re-enable warnings after successful execution of SUBSCRIBE */
	applier->warning_said = false;
	vclock_create(&applier->vclock);

	/*
	 * Read SUBSCRIBE response
	 */
	if (applier->version_id >= version_id(1, 6, 7)) {
		coio_read_xrow(coio, &iobuf->in, &row);
		if (iproto_type_is_error(row.type)) {
			return xrow_decode_error(&row);  /* error */
		} else if (row.type != IPROTO_OK) {
			tnt_raise(ClientError, ER_PROTOCOL,
				  "Invalid response to SUBSCRIBE");
		}

		/*
		 * Don't overwrite applier->vclock before performing
		 * sanity checks for a valid server id.
		 */
		struct vclock vclock;
		vclock_create(&vclock);
		xrow_decode_vclock(&row, &vclock);

		/* Forbid changing the server_id */
		if (applier->id != 0 && applier->id != row.server_id) {
			Exception *e = tnt_error(ClientError,
						 ER_SERVER_ID_MISMATCH,
						 tt_uuid_str(&applier->uuid),
						 applier->id, row.server_id);
			/* Log the error at most once. */
			if (!applier->warning_said)
				e->log();
			e->raise();
		}

		/* Save the received server_id and vclock */
		applier->id = row.server_id;
		vclock_copy(&applier->vclock, &vclock);
	}
	/**
	 * Tarantool < 1.6.7:
	 * If there is an error in subscribe, it's sent directly
	 * in response to subscribe.  If subscribe is successful,
	 * there is no "OK" response, but a stream of rows from
	 * the binary log.
	 */

	/*
	 * Process a stream of rows from the binary log.
	 */
	iobuf_reset(applier->iobuf[applier->input_index]);
	while (true) {
		applier_read(applier);
		struct iobuf *iobuf = applier->iobuf[applier->input_index];
		while (applier->input_unparsed) {
			size_t len = applier_parse_xrow(applier, &row);
			if (!len)
				break;
			applier->lag = ev_now(loop()) - row.tm;
			applier->last_row_time = ev_now(loop());

			if (iproto_type_is_error(row.type)) {
				iobuf->in.rpos += len;
				xrow_decode_error(&row);  /* error */
			}
			net_recovery_apply_row(applier,
					       applier->subscribe_stream,
					       &row, len);
		}
	}
}

/**
 * Write a nice error message to log file on SocketError or ClientError
 * in applier_f().
 */
static inline void
applier_log_error(struct applier *applier, struct error *e)
{
	if (type_cast(FiberIsCancelled, e))
		return;
	if (applier->warning_said)
		return;
	switch (applier->state) {
	case APPLIER_CONNECT:
		say_info("can't connect to master");
		break;
	case APPLIER_CONNECTED:
		say_info("can't join/subscribe");
		break;
	case APPLIER_AUTH:
		say_info("failed to authenticate");
		break;
	case APPLIER_FOLLOW:
	case APPLIER_BOOTSTRAP:
		say_info("can't read row");
		break;
	default:
		break;
	}
	error_log(e);
	if (type_cast(SocketError, e))
		say_info("will retry every %i second", RECONNECT_DELAY);
	applier->warning_said = true;
}

static inline void
applier_disconnect(struct applier *applier, struct error *e,
		   enum applier_state state)
{
	applier_log_error(applier, e);
	coio_close(loop(), &applier->io);
	iobuf_reset(applier->iobuf[0]);
	iobuf_reset(applier->iobuf[1]);
	applier_set_state(applier, state);
	fiber_gc();
}

static int
applier_f(va_list ap)
{
	struct applier *applier = va_arg(ap, struct applier *);

	/* Re-connect loop */
	while (!fiber_is_cancelled()) {
		try {
			applier_connect(applier);
			if (wal == NULL) {
				/*
				 * Execute JOIN if this is a bootstrap,
				 * and there is no snapshot. The
				 * join will pause the applier
				 * until WAL is created.
				 */
				applier_join(applier);
			}
			applier_subscribe(applier);
			/*
			 * subscribe() has an infinite loop which
			 * is stoppable only with fiber_cancel().
			 */
			assert(0);
			return 0;
		} catch (ClientError *e) {
			/* log logical error which caused replica to stop */
			e->log();
			applier_disconnect(applier, e, APPLIER_STOPPED);
			throw;
		} catch (FiberIsCancelled *e) {
			applier_disconnect(applier, e, APPLIER_OFF);
			throw;
		} catch (SocketError *e) {
			applier_disconnect(applier, e, APPLIER_DISCONNECTED);
			/* fall through */
		}
		/* Put fiber_sleep() out of catch block.
		 *
		 * This is done to avoid the case when two or more
		 * fibers yield inside their try/catch blocks and
		 * throw an exception. Seems like the exception unwinder
		 * uses global state inside the catch block.
		 *
		 * This could lead to incorrect exception processing
		 * and crash the server.
		 *
		 * See: https://github.com/tarantool/tarantool/issues/136
		*/
		fiber_sleep(RECONNECT_DELAY);
	}
	return 0;
}

/**
 * Start a client to a remote server using a background fiber.
 *
 * If recovery is finalized (i.e. r->writer != NULL) then the client
 * connect to a master and follow remote updates using SUBSCRIBE command.
 *
 * If recovery is not finalized (i.e. r->writer == NULL) then the client
 * connect to a master, download and process snapshot using JOIN command
 * and then switch to follow mode.
 *
 * \sa fiber_start()
 */
void
applier_start(struct applier *applier)
{
	char name[FIBER_NAME_MAX];
	assert(applier->reader == NULL);

	const char *uri = uri_format(&applier->uri);
	snprintf(name, sizeof(name), "applier/%s", uri);

	struct fiber *f = fiber_new_xc(name, applier_f);
	/**
	 * So that we can safely grab the status of the
	 * fiber any time we want.
	 */
	fiber_set_joinable(f, true);
	applier->reader = f;
	fiber_start(f, applier);
}

struct applier_stop_msg
{
	struct cbus_call_msg base;
	struct applier *applier;
};

static int
applier_do_stop(struct cbus_call_msg *m)
{
	struct applier_stop_msg *msg = (struct applier_stop_msg *)m;
	struct applier *applier = msg->applier;
	struct fiber *f = applier->reader;
	if (f == NULL)
		return 0;
	fiber_cancel(f);
	fiber_join(f);
	applier_set_state(applier, APPLIER_OFF);
	applier->reader = NULL;
	return 0;
}

void
tx_applier_stop(struct applier *applier)
{
	struct applier_stop_msg msg;
	msg.applier = applier;
	bool cancellable = fiber_set_cancellable(false);
	cbus_call(&net_tx_bus, &msg.base, applier_do_stop, NULL, TIMEOUT_INFINITY);
	fiber_set_cancellable(cancellable);
}

static void
applier_stop(struct applier *applier)
{
	struct applier_stop_msg msg;
	msg.applier = applier;
	(void) applier_do_stop(&msg.base);
}

struct applier_new_msg
{
	struct cbus_call_msg base;
	struct applier *applier;
	const char *uri;
	struct xstream *join_stream;
	struct xstream *subscribe_stream;
};

static int
applier_do_new(struct cbus_call_msg *m)
{
	struct applier_new_msg *msg = (struct applier_new_msg *)m;
	struct applier *applier = (struct applier *)
		calloc(1, sizeof(struct applier));
	if (applier == NULL) {
		diag_set(OutOfMemory, sizeof(*applier), "malloc",
			 "struct applier");
		return -1;
	}
	coio_init(&applier->io, -1);
	applier->iobuf[0] = iobuf_new();
	applier->iobuf[1] = iobuf_new();
	applier->input_index = 0;
	applier->input_unparsed = 0;
	applier->want_swap_buffers = false;
	mempool_create(&applier->msg_pool, &cord()->slabc,
		       sizeof(struct recovery_apply_row_msg));
	vclock_create(&applier->vclock);

	/* uri_parse() sets pointers to applier->source buffer */
	snprintf(applier->source, sizeof(applier->source), "%s", msg->uri);
	int rc = uri_parse(&applier->uri, applier->source);
	/* URI checked by box_check_replication_source() */
	assert(rc == 0 && applier->uri.service != NULL);
	(void) rc;

	applier->join_stream = msg->join_stream;
	applier->subscribe_stream = msg->subscribe_stream;
	applier->last_row_time = ev_now(loop());
	rlist_create(&applier->on_state);
	ipc_channel_create(&applier->pause, 0);
	msg->applier = applier;
	return 0;
}

struct applier *
tx_applier_new(const char *uri,
	       struct xstream *join_stream,
	       struct xstream *subscribe_stream)
{
	struct applier_new_msg msg;
	msg.applier = NULL;
	msg.uri = uri;
	msg.join_stream = join_stream;
	msg.subscribe_stream = subscribe_stream;
	bool cancellable = fiber_set_cancellable(false);
	cbus_call(&net_tx_bus, &msg.base, applier_do_new, NULL, TIMEOUT_INFINITY);
	fiber_set_cancellable(cancellable);
	return msg.applier;
}

struct applier_delete_msg
{
	struct cbus_call_msg base;
	struct applier *applier;
};

static int
applier_do_delete(struct cbus_call_msg *m)
{
	struct applier_delete_msg *msg = (applier_delete_msg *)m;
	struct applier *applier = msg->applier;
	assert(applier->reader == NULL);
	iobuf_delete(applier->iobuf[0]);
	iobuf_delete(applier->iobuf[1]);
	assert(applier->io.fd == -1);
	ipc_channel_destroy(&applier->pause);
	trigger_destroy(&applier->on_state);
	mempool_destroy(&applier->msg_pool);
	free(applier);
	return 0;
}

void
tx_applier_delete(struct applier *applier)
{
	struct applier_delete_msg msg;
	msg.applier = applier;
	bool cancellable = fiber_set_cancellable(false);
	cbus_call(&net_tx_bus, &msg.base, applier_do_delete, NULL,
		TIMEOUT_INFINITY);
	fiber_set_cancellable(cancellable);
}

struct applier_resume_msg
{
	struct cbus_call_msg base;
	struct applier *applier;
};

static int
applier_do_resume(struct cbus_call_msg *m)
{
	struct applier_resume_msg *msg = (struct applier_resume_msg *)m;
	struct applier *applier = msg->applier;
	assert(!fiber_is_dead(applier->reader));
	assert(applier->state == APPLIER_CONNECTED);
	void *data = NULL;
	ipc_channel_put_xc(&applier->pause, data);
	return 0;
}

void
tx_applier_resume(struct applier *applier)
{
	struct applier_resume_msg msg;
	msg.applier = applier;
	bool cancellable = fiber_set_cancellable(false);
	cbus_call(&net_tx_bus, &msg.base, applier_do_resume, NULL,
		TIMEOUT_INFINITY);
	fiber_set_cancellable(cancellable);
}

static inline void
applier_pause(struct applier *applier)
{
	/* Sleep until applier_resume() wake us up */
	void *data;
	ipc_channel_get_xc(&applier->pause, &data);
}

/** Used by applier_connect_all() */
static void
applier_on_connect(struct trigger *trigger, void *event)
{
	struct applier *applier = (struct applier *) event;
	if (applier->state != APPLIER_CONNECTED)
		return;

	/* Wake up applier_connect_all() fiber */
	struct ipc_channel *ch = (struct ipc_channel *) trigger->data;
	ipc_channel_put_xc(ch, applier);

	applier_pause(applier);
}

/* Used by applier_bootstrap() */
static void
applier_on_bootstrap(struct trigger *trigger, void *event)
{
	struct applier *applier = (struct applier *) event;
	if (applier->state == APPLIER_BOOTSTRAP)
		return;

	/* Wake up applier_bootstrap() fiber */
	struct ipc_channel *ch = (struct ipc_channel *) trigger->data;
	ipc_channel_put_xc(ch, applier);

	applier_pause(applier);
}

struct applier_connect_all_msg
{
	struct cbus_call_msg base;
	struct applier **appliers;
	int count;
	struct recovery *recovery;
};

static int
applier_do_connect_all(struct cbus_call_msg *m)
{
	/*
	 * Simultaneously connect to remote peers to receive their UUIDs
	 * and fill the resulting set:
	 *
	 * - create a single control channel;
	 * - register a trigger in each applier to wake up our
	 *   fiber via this channel when the remote peer becomes
	 *   connected and a UUID is received;
	 * - wait up to CONNECT_TIMEOUT seconds for `count` messages;
	 * - on timeout, raise a CFG error, cancel and destroy
	 *   the freshly created appliers (done in a guard);
	 * - an success, unregister the trigger, check the UUID set
	 *   for duplicates, fill the result set, return.
	 */
	struct applier_connect_all_msg *msg =
		(struct applier_connect_all_msg *)m;

	/* A channel from applier's on_state trigger is used to wake us up */
	IpcChannelGuard wakeup(msg->count);
	/* Memory for on_state triggers registered in appliers */
	struct trigger triggers[VCLOCK_MAX]; /* actually need `count' */
	/* Wait results until this time */
	double deadline = fiber_time() + CONNECT_TIMEOUT;

	/* Add triggers and start simulations connection to remote peers */
	for (int i = 0; i < msg->count; i++) {
		/* Register a trigger to wake us up when peer is connected */
		trigger_create(&triggers[i], applier_on_connect, wakeup.ch,
			       NULL);
		trigger_add(&msg->appliers[i]->on_state, &triggers[i]);

		/* Start background connection */
		applier_start(msg->appliers[i]);
	}

	/* Wait `count` messages from channel */
	for (int connected = 0; connected < msg->count; connected++) {
		void *data = NULL;
		double wait = deadline - fiber_time();
		/* Stop on timeout or channel error (doesn't matter here) */
		if (wait < 0.0 ||
		    ipc_channel_get_timeout(wakeup.ch, &data, wait) != 0) {
			tnt_error(ClientError, ER_CFG, "replication_source",
				  "failed to connect to one or more servers");
			goto error;
		}
	}

	for (int i = 0; i < msg->count; i++) {
		assert(msg->appliers[i]->state == APPLIER_CONNECTED);

		/* Unregister the temporary trigger used to wake us up */
		trigger_clear(&triggers[i]);
	}

	/* Now all the appliers are connected, finish. */
	return 0;
error:
	/*
	 * Preserve the original error which can be overwritten by
	 * fiber_join()
	 */
	struct diag diag;
	diag_create(&diag);
	diag_move(&fiber()->diag, &diag);
	assert(diag_last_error(&diag) != NULL);

	/* Destroy appliers */
	for (int i = 0; i < msg->count; i++) {
		trigger_clear(&triggers[i]);
		applier_stop(msg->appliers[i]);
	}

	/* Restore original error */
	diag_move(&diag, &fiber()->diag);
	diag_destroy(&diag);
	return -1;
}

void
tx_applier_connect_all(struct applier **appliers, int count)
{
	if (count == 0)
		return; /* nothing to do */
	struct applier_connect_all_msg msg;
	msg.appliers = appliers;
	msg.count = count;
	bool cancellable = fiber_set_cancellable(false);
	int res = cbus_call(&net_tx_bus, &msg.base, applier_do_connect_all, NULL,
		TIMEOUT_INFINITY);
	fiber_set_cancellable(cancellable);
	if (res)
		diag_raise();
}

struct applier_bootstrap_msg
{
	struct cbus_call_msg base;
	struct applier *applier;
};

/** Download and process the data snapshot from master. */
static int
applier_do_bootstrap(struct cbus_call_msg *m)
{
	struct applier_bootstrap_msg *msg = (struct applier_bootstrap_msg *)m;
	struct applier *master = msg->applier;
	/* cfg_get_replication_source() post condition */
	assert(master->state == APPLIER_CONNECTED);

	/*
	 * - create a channel to synchronize with the applier fiber;
	 * - register a trigger in the applier that puts a message to
	 *   this channel when the bootstrap is finished.
	 * - wait for a message from this channel and then check
	 *   applier state;
	 * - in case the applier state is not APPLIER_CONNECTED, as
	 *   we expect: re-throw the original exception and abort
	 *   the bootstrap process (panic() is called by box_cfg());
	 * - on success, remove the trigger and leave the
	 *   applier in CONNECTED state.
	 */

	/* A channel is used to wake us up from on_bootstrap trigger. */
	IpcChannelGuard wakeup(0);
	/* A trigger that wakes us up when the bootstrap is finished. */
	struct trigger on_bootstrap;
	trigger_create(&on_bootstrap, applier_on_bootstrap, wakeup.ch, NULL);
	trigger_add(&master->on_state, &on_bootstrap);

	/*
	 * Resume the applier and let it bootstrap (see
	 * cfg_get_replication_source().
	 */
	void *data = NULL;
	ipc_channel_put_xc(&master->pause, &data);

	/* Wait while the applier downloads and processes the snapshot. */
	ipc_channel_get_xc(wakeup.ch, &data);

	/* Unregister the temporary trigger */
	trigger_clear(&on_bootstrap);

	/*
	 * The trigger wakes us up in two cases:
	 *
	 * - the applier downloaded and processed the snapshot
	 *   and  then switched back to CONNECTED state;
	 * - the applier failed, and switched to STOPPED or
	 *   DISCONNECTED state.
	 */
	if (master->state != APPLIER_CONNECTED) {
		/* Re-throw the original error */
		assert(!diag_is_empty(&master->reader->diag));
		diag_move(&master->reader->diag, &fiber()->diag);
		diag_raise();
	}

	/* Leave the applier in CONNECTED state */
	assert(master->state == APPLIER_CONNECTED);
	return 0;
}

void
tx_applier_bootstrap(struct applier *master)
{
	struct applier_bootstrap_msg msg;
	msg.applier = master;
	bool cancellable = fiber_set_cancellable(false);
	cbus_call(&net_tx_bus, &msg.base, applier_do_bootstrap, NULL,
		TIMEOUT_INFINITY);
	fiber_set_cancellable(cancellable);
}
