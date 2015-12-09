#include "cbus.h"

struct cpipe_call
{
	struct cpipe_call **callee_ref;
	int complete;
	void (*fn)(struct cpipe_call *, void *);
	void *userdata;
	struct fiber *caller_fiber;
	struct cpipe *response_pipe;
	struct cmsg request_msg;
	struct cmsg response_msg;
	struct diag diag;
};

static inline struct cpipe_call *
call_from_request(struct cmsg *m)
{
	return (struct cpipe_call *)(
		(uintptr_t)m - offsetof(struct cpipe_call, request_msg));
}

static inline struct cpipe_call *
call_from_response(struct cmsg *m)
{
	return (struct cpipe_call *)(
		(uintptr_t)m - offsetof(struct cpipe_call, response_msg));
}

static void
cpipe_do_call(struct cmsg *m)
{
	struct cpipe_call *call = call_from_request(m);
	say_debug("cpipe_do_call %p", call);
	/* once the call is declared done [call] variable is set to NULL */
	call->callee_ref = &call;
	try {
		call->fn(call, call->userdata);
		/* we are about to return, stop referencing a local var */
		if (call != NULL)
			call->callee_ref = NULL;
	} catch (Exception *e) {
		/* callee_ref exists to detect this particular case */
		if (call == NULL)
			panic("exception in cpipe_call but response already sent");
		diag_move(&fiber()->diag, &call->diag);
		cpipe_call_done(call);
	} catch (...) {
		panic("unexpected exception");
	}
}

static void
cpipe_did_call(struct cmsg *m)
{
	struct cpipe_call *call = call_from_response(m);
	say_debug("cpipe_did_call %p", call);
	call->complete = 1;
	fiber_wakeup(call->caller_fiber);
}

void
cpipe_call(struct cpipe *request_pipe, struct cpipe *response_pipe,
	   void(*fn)(struct cpipe_call *, void *),
	   void *userdata)
{
	struct cpipe_call call;

	say_debug("cpipe_call <begin> %p", &call);

	assert(fn != NULL);
	assert(response_pipe != NULL);

	static cmsg_hop request_route[] = {{ cpipe_do_call, NULL }};
	static cmsg_hop response_route[] = {{ cpipe_did_call, NULL }};

	call.callee_ref = NULL;
	call.complete = 0;
	call.fn = fn;
	call.userdata = userdata;
	call.caller_fiber = fiber();
	call.response_pipe = response_pipe;
	cmsg_init(&call.request_msg, request_route);
	cmsg_init(&call.response_msg, response_route);
	diag_create(&call.diag);
	cpipe_push(request_pipe, &call.request_msg);

	/* Things are allocated on the stack, don't let it go. */
	bool cancellable = fiber_set_cancellable(false);

	fiber_yield();
	/* Spurious wakeup indicates a severe BUG, fail early. */
	if (call.complete == 0)
		panic("Wrong fiber woken");

	fiber_set_cancellable(cancellable);

	say_debug("cpipe_call <end> %p", &call);

	if (! diag_is_empty(&call.diag)) {
		diag_move(&call.diag, &fiber()->diag);
		diag_raise();
	}
}

void
cpipe_call_done(struct cpipe_call *call)
{
	say_debug("cpipe_call_done %p", call);
	if (call->callee_ref != NULL)
		*call->callee_ref = NULL;
	cpipe_push(call->response_pipe, &call->response_msg);
}
