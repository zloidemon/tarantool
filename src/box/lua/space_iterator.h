#ifndef INCLUDES_TARANTOOL_LUA_SPACE_ITERATOR_H
#define INCLUDES_TARANTOOL_LUA_SPACE_ITERATOR_H

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

#include "box/tuple.h"
#include "box/index.h"

//~~~~~~~~~~~~~~~~~~~~~~~~ S P A C E   I T E R A T O R ~~~~~~~~~~~~~~~~~~~~~~~~ 

#define WITHOUT_CALLBACK 0, NULL, NULL

class SpaceIterator
{
public:
	typedef int (*SIteratorCallback)(box_tuple_t *tpl, int argc, void **argv);

private:
	int argc;
	void **argv;
	SIteratorCallback callback;
	int space_id;
	int index_id;
	char *key;
	char *key_end;
	box_iterator_t *iter;
	iterator_type iter_tp;

	box_tuple_t *current_tuple;

	// Copying is forbidden
	SpaceIterator(const SpaceIterator &ob);
	SpaceIterator &operator=(const SpaceIterator &ob);

public:
	SpaceIterator(int argc, void **argv,
		SIteratorCallback callback, int space_id, int index_id,
		char *key, char *key_end, iterator_type it_tp = ITER_ALL);
	~SpaceIterator();
	SpaceIterator &operator=(SpaceIterator &&ob);
	SpaceIterator(SpaceIterator &&ob);

	bool IsEnd() const;
	bool IsOpen() const;
	void Close();
	bool Open();
	int Next();
	box_tuple_t *GetTuple();
	int IterateOver();
	bool InProcess() const;
	iterator_type GetIteratorType() const;
};

 #endif