#ifndef TARANTOOL_JS_INIT_H_INCLUDED
#define TARANTOOL_JS_INIT_H_INCLUDED
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

/**
 * @file
 * @brief Tarantool JS bridge
 *
 * Please do not include this file from lib/ folder
 */

#include <stddef.h>

#if defined(__cplusplus)
extern "C" {
#endif /* defined(__cplusplus) */

struct tarantool_cfg;
struct tbuf;
struct fiber;

void
tarantool_js_init();

void
tarantool_js_free();

/**
 * Create an instance of JS interpreter and load it with Tarantool modules.
 * @retval new JS instance on success
 * @retval NULL on memory error
 */
struct tarantool_js *
tarantool_js_new(void);

void
tarantool_js_delete(struct tarantool_js *js);

/**
 * @brief Performs lazy initialization of JS engine on the current running fiber
 * @param js
 */
void
fiber_enable_js(struct tarantool_js *js);

/**
 * @brief Make a new configuration available in JS
 */
void
tarantool_js_load_cfg(struct tarantool_js *js,
		      struct tarantool_cfg *cfg);

/**
 * Initialize built-in JS library and load start-up module
 */
void
tarantool_js_init_library(struct tarantool_js *js);

void
tarantool_js_eval(struct tbuf *out, const void *source, size_t source_size,
		  const char *source_origin);

#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */

#endif /* TARANTOOL_JS_INIT_H_INCLUDED */
