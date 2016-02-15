#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include <sys/socket.h>
#include <small/ibuf.h>
#include <fiber.h>

#include <lua/utils.h>
#include <lua/fiber.h>

enum {
	SEND_LIMIT_DEFAULT = 0x100000,
	RECV_MIN_SIZE      = 0x1000
};

enum iomux_state {
	IOMUX_PASS, /* sender MAY queue data, subject to send_limit */
	IOMUX_HOLD, /* sender MUST wait for a state change - transient error */
	IOMUX_CLOSED/* sender MUST fail immediately */
};

struct iomux {
	/* ==== accessed directly from Lua ==== */
	ptrdiff_t       send_fast_thr;
	struct ibuf    *send_buf;
	struct ibuf    *recv_buf;
	int             inject_error;
	/* ==== private part ==== */
	struct rlist    waiters; /* the crowd waiting for send_buf space */
	struct fiber   *ls; /* a fiber currently in iomux_line_service */
	struct ev_io    r;
	struct ev_io    w;
	enum iomux_state s;
	int             revents;
	size_t          send_limit;
};

static void
iomux_io_cb(struct ev_loop *loop, struct ev_io *io, int revents)
{
	struct iomux *m = io->data;
	assert(loop == loop());
	assert(fiber() == &cord()->sched);
	m->revents |= revents;
	if (m->ls) {
		rlist_del(&m->ls->state);
		fiber_call(m->ls);
	} else {
		ev_io_stop(loop, io);
	}
}

static void
iomux_wake(struct iomux *m)
{
	while (m->s == IOMUX_PASS &&
	       ibuf_used(m->send_buf) < m->send_limit &&
	       !rlist_empty(&m->waiters)) {

		struct fiber *f  = rlist_shift_entry(
			       &m->waiters, struct fiber, state);
		fiber_call(f);
	}
}

/*
 * iomux_create(iomux-ptr) - initialize iomux structure
 */
static int
lbox_iomux_create(struct lua_State *L)
{
	struct iomux *m = (void *)lua_topointer(L, 1);
	assert(m);
	m->send_fast_thr = 0;
	m->recv_buf = m->send_buf = NULL;
	m->inject_error = 0;
	rlist_create(&m->waiters);
	m->ls = NULL;
	ev_io_init(&m->r, iomux_io_cb, -1, EV_READ);
	m->r.data = m;
	ev_io_init(&m->w, iomux_io_cb, -1, EV_WRITE);
	m->w.data = m;
	m->s = IOMUX_HOLD;
	m->send_limit = SEND_LIMIT_DEFAULT;
	m->revents = EV_READ | EV_WRITE;
	return 0;
}

/*
 * iomux_destroy(iomux-ptr) - finalize iomux structure
 */
static int
lbox_iomux_destroy(struct lua_State *L)
{
	struct iomux *m = (void *)lua_topointer(L, 1);
	assert(m);
	assert(m->ls == NULL);
	assert(rlist_empty(&m->waiters));
	ev_io_stop(loop(), &m->r);
	ev_io_stop(loop(), &m->w);
	return 0;
}

/*
 * iomux_configure(iomux-ptr, config-dict)
 */
static int
lbox_iomux_configure(struct lua_State *L)
{
	struct iomux *m = (void *)lua_topointer(L, 1);
	assert(m);
	if (lua_gettop(L) < 2 || !lua_istable(L, 2)) {
		luaL_error(L, "iomux_configure: bad argument");
		return 0;
	}
	m->send_fast_thr = 0;
	/* state ******/
	lua_getfield(L, 2, "state");
	if (!lua_isnil(L, -1)) {
		const char *ss = lua_tostring(L, -1);
		if (ss[0] && ss[1]) {
bad_state:
			luaL_error(L, "iomux_configure: bad state: %s", ss);
			return 0;
		}
		switch (*ss) {
		default:
			goto bad_state;
		case 'P': /* PASS */
			m->s = IOMUX_PASS;
			iomux_wake(m);
			break;
		case 'H': /* HOLD */
			m->s = IOMUX_HOLD;
			break;
		case 'X': /* CLOSED */
			m->s = IOMUX_CLOSED;
			while (! rlist_empty(&m->waiters)) {
				struct fiber *f;
				f = rlist_shift_entry(
					&m->waiters, struct fiber, state);
				fiber_wakeup(f);
			}
			break;
		}
	}
	lua_pop(L, 1);
	/* fd *********/
	lua_getfield(L, 2, "fd");
	if (!lua_isnil(L, -1)) {
		int fd = 0;
		if (!lua_isnumber(L, -1) || (fd = lua_tointeger(L, -1)) < -1) {
			luaL_error(L, "iomux_configure: bad fd: %d", fd);
			return 0;
		}
		if (m->ls != NULL) {
			luaL_error(L,
				   "iomux_configure: unable to change fd "
				   "while IO is in progress");
			return 0;
		}
		ev_io_stop(loop(), &m->r);
		ev_io_stop(loop(), &m->w);
		ev_io_set(&m->r, fd, EV_READ);
		ev_io_set(&m->w, fd, EV_WRITE);
		m->revents = EV_READ | EV_WRITE;
	}
	lua_pop(L, 1);
	return 0;
}

/*
 * Send is as simple as putting the data in send_buf,
 * HOWEVER, a well-behaved sender MUST perform the following steps first
 *  1) ensure that send_buf usage is below send_fast_thr(eshold);
 *  2) otherwise invoke iomux_sender_checkin().
 *
 * The purpose of checkin() is 3-fold:
 *  1) wait for send_buf usage to drop below send_limit;
 *  2) wait for IOMUX_PASS state;
 *  3) arrange for send_buf to get drained eventially.
 *
 * Returns: nil if sender MAY proceed; 'T' for timeout, 'X' if closed.
 */
static int
lbox_iomux_sender_checkin(struct lua_State *L /* iomux [, timeout] */ )
{
	ev_tstamp now = ev_now(loop());
	ev_tstamp deadline_ts;

	/* #1: iomux-ptr */
	struct iomux *m = (void *)lua_topointer(L, 1);
	assert(m);
	assert(m->send_buf);
	assert(m->recv_buf);

	/* #2: deadline */
	if (!lua_isnoneornil(L, 2)) {
		if (!lua_isnumber(L, 2)) {
			luaL_error(L, "iomux_sender_checkin: bad deadline");
			return 0;
		}
		deadline_ts = lua_tonumber(L, 2);
	} else {
		deadline_ts = now + TIMEOUT_INFINITY;
	}

	while (true) {
		if (m->s == IOMUX_CLOSED) {
			lua_pushstring(L, "X");
			return 1;
		}

		if (m->s == IOMUX_PASS &&
		    ibuf_used(m->send_buf) < m->send_limit) {

			if (m->ls != NULL && !ev_is_active(&m->w)) {
				assert(m->w.fd != -1);
				ev_io_start(loop(), &m->w);
			}
			/* enable fast path; enabled here ONLY */
			m->send_fast_thr = m->send_limit;
			return 0;
		}

		bool timed_out;

		if (deadline_ts <= now) {
			timed_out = true;
		} else {
			rlist_add_tail_entry(&m->waiters, fiber(), state);
			timed_out = fiber_yield_timeout(deadline_ts - now);
		}

		/* note: cancelation semantics consistent with fast path */
		luaL_testcancel(L);

		if (timed_out) {
			lua_pushstring(L, "T");
			return 1;
		}

		now = ev_now(loop());
	}
}

/*
 * Send queued data while receiving a response.
 *
 * Send+recv, return once at least recv-bytes are available:
 *     iomux_line_service(iomux-ptr, recv-bytes [, timeout])
 *
 * Send+recv, return once recv-boundary-str is encountered:
 *     iomux_line_service(iomux-ptr, recv-boundary-str [, timeout])
 *
 * Send, return once send_buf drained:
 *     iomux_line_service(iomux-ptr, nil or '' [, timeout])
 *
 * Returns:
 *  - nil:                   recv-bytes ready
 *  - nil, delimiter-offset: delimiter found
 *  - 'E', errno:            socket error
 *  - 'T':                   timeout exceeded
 *  - '.':                   peer closed (in recv mode)
 *  - '!':                   error injection
 */
static int
lbox_iomux_line_service(struct lua_State *L)
{
	int rc = 0;
	bool want_recv = false;
	size_t recv_bytes = 0;
	const char *recv_boundary_str = NULL;
	size_t recv_boundary_len;
	size_t recv_boundary_offset = 0;

	ev_tstamp now = ev_now(loop());
	ev_tstamp deadline_ts;

	/* #1: iomux-ptr */
	struct iomux *m = (void *)lua_topointer(L, 1);
	assert(m);
	assert(m->send_buf);
	assert(m->recv_buf);

	/* #2: recv-bytes | recv-boundary-str */
	if (!lua_isnoneornil(L, 2)) {
		if (lua_isnumber(L, 2)) {
			int v = lua_tointeger(L, 2);
			if (v < 0) {
				luaL_error(L,
					   "iomux_line_service: bad argument");
				return 0;
			}
			recv_bytes = v;
			want_recv = v != 0;
		} else {
			const char *s =
				lua_tolstring(L, 2, &recv_boundary_len);
			if (recv_boundary_len != 0) {
				recv_boundary_str = s;
				want_recv = true;
			}
		}
	}

	/* #3: deadline */
	if (!lua_isnoneornil(L, 3)) {
		if (!lua_isnumber(L, 3)) {
			luaL_error(L, "iomux_line_service: bad deadline");
			return 0;
		}
		deadline_ts = lua_tonumber(L, 3);
	} else {
		deadline_ts = now + TIMEOUT_INFINITY;
	}

	if (m->ls) {
		luaL_error(L, "iomux_line_service: sharing violation");
		return 0;
	}

	if (m->w.fd == -1) {
		luaL_error(L, "iomux_line_service: fd not set");
		return 0;
	}

	if (want_recv) {
		ev_io_start(loop(), &m->r);
	} else {
		ev_io_stop(loop(), &m->r);
	}

	m->ls = fiber();

	while (!fiber_is_cancelled()) {
		if (m->inject_error) {
			m->inject_error = 0;
			m->ls = NULL;
			lua_pushstring(L, "!");
			rc = 1;
			break;
		}

		/*** send *********/
		if (ibuf_used(m->send_buf) != 0 && (m->revents & EV_WRITE)) {
			ssize_t bytes_sent;
send_more:
			bytes_sent = send(m->w.fd, m->send_buf->rpos,
				  ibuf_used(m->send_buf), 0);

			assert(bytes_sent > 0 || bytes_sent == -1);

			if (bytes_sent > 0) {
				m->send_buf->rpos += bytes_sent;
				if (ibuf_used(m->send_buf) != 0)
					goto send_more;
			} else if (errno == EINTR) {
				goto send_more;
			} else if (errno == EAGAIN || errno == EWOULDBLOCK) {
				m->revents &= ~EV_WRITE;
			} else {
				m->ls = NULL;
				lua_pushstring(L, "E");
				lua_pushinteger(L, errno);
				rc = 2;
				break;
			}
		}

		/*** refill *******/
		iomux_wake(m);

		/*** recv *********/
		if (recv_boundary_offset > ibuf_used(m->recv_buf)) {
			/*
			 * Well-behaved user doesn't touch recv_buf
			 * while line_service() is active. If she does,
			 * funny things may happen. At least, don't crash.
			 */
			recv_boundary_offset = 0;
		}
recv_more:
		/*
		 * Check stop conditions before receiving more data:
		 * existing data may be ok.
		 */
		if (recv_bytes != 0 &&
		    ibuf_used(m->recv_buf) >= recv_bytes) {
			/* recv_bytes ready */
			m->ls = NULL;
			rc = 0;
			break;
		}

		if (recv_boundary_str != NULL &&
		    ibuf_used(m->recv_buf) >=
			recv_boundary_offset + recv_boundary_len) {

			char *p = memmem(
				m->recv_buf->rpos + recv_boundary_offset,
				ibuf_used(m->recv_buf) - recv_boundary_offset,
				recv_boundary_str,
				recv_boundary_len);

			if (p != NULL) {
				/* boundary matched */
				m->ls = NULL;
				lua_pushnil(L);
				lua_pushinteger(L, (int)(p - m->recv_buf->rpos));
				rc = 2;
				break;
			}

			/*
			 * Remember offset to make boundary scan
			 * incremental.
			 */
			recv_boundary_offset =
				ibuf_used(m->recv_buf) - recv_boundary_len;
		}

		if (want_recv && (m->revents & EV_READ)) {
			ssize_t bytes_recv;
			void *p = ibuf_reserve(m->recv_buf, RECV_MIN_SIZE);
			if (p == NULL) {
				m->ls = NULL;
				luaL_error(L, "out of memory");
				break;
			}
			bytes_recv = recv(
				m->w.fd, p, ibuf_unused(m->recv_buf), 0);
			if (bytes_recv > 0) {
				m->recv_buf->wpos += bytes_recv;
				goto recv_more;
			} else if (bytes_recv == 0) {
				/* eof */
				m->ls = NULL;
				lua_pushstring(L, ".");
				rc = 1;
				break;
			} else if (errno == EINTR) {
				goto recv_more;
			} else if (errno == EAGAIN || errno == EWOULDBLOCK) {
				m->revents &= ~EV_READ;
			} else {
				m->ls = NULL;
				lua_pushstring(L, "E");
				lua_pushinteger(L, errno);
				rc = 2;
				break;
			}
		}

		/*** wait ********/
		if (ibuf_used(m->send_buf) == 0) {
			ev_io_stop(loop(), &m->w);
			m->send_fast_thr = 0;
			if (!want_recv) {
				/* nothing to do */
				m->ls = NULL;
				break;
			}
		} else if (m->revents & EV_WRITE) {
			/* still writable, skip wait */
			assert(ibuf_used(m->send_buf) != 0);
			continue;
		} else {
			ev_io_start(loop(), &m->w);
		}

		bool timed_out =
			deadline_ts <= now ||
			fiber_yield_timeout(deadline_ts - now);

		if (timed_out) {
			m->ls = NULL;
			lua_pushstring(L, "T");
			rc = 1;
			break;
		}

		now = ev_now(loop());
	}

	m->ls = NULL;
	luaL_testcancel(L);
	return rc;
}

void
tarantool_lua_iomux_init(struct lua_State *L)
{
	static const struct luaL_reg iomux_lib[] = {
		{"imx_create",		lbox_iomux_create},
		{"imx_destroy",		lbox_iomux_destroy},
		{"imx_configure",	lbox_iomux_configure},
		{"imx_sender_checkin",	lbox_iomux_sender_checkin},
		{"imx_line_service",	lbox_iomux_line_service},
		{NULL, NULL}
	};

	luaL_register_module(L, "imxlib", iomux_lib);
	lua_pushinteger(L, (int)sizeof(struct iomux));
	lua_setfield(L, -2, "imx_size");
	lua_pop(L, 1);
}
