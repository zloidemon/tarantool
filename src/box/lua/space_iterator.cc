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

#include "space_iterator.h"

//~~~~~~~~~~~~~~~~~~~~~~~~ S P A C E   I T E R A T O R ~~~~~~~~~~~~~~~~~~~~~~~~

SpaceIterator::SpaceIterator(const SpaceIterator &) { }

SpaceIterator &SpaceIterator::operator=(const SpaceIterator &)
{ return *this; }

SpaceIterator::SpaceIterator(int argc_, void **argv_,
	SIteratorCallback callback_, int space_id_, int index_id_,
	char *key_, char *key_end_, iterator_type it_tp)
	: argc(argc_), argv(argv_), callback(callback_), space_id(space_id_),
	index_id(index_id_), key(key_), key_end(key_end_), iter(NULL), iter_tp(it_tp),
	current_tuple(NULL)
{
	Open();
}

SpaceIterator::~SpaceIterator() {
	this->Close();
}

SpaceIterator &SpaceIterator::operator=(SpaceIterator &&ob) {
	this->argc = ob.argc;
	this->argv = ob.argv;
	this->callback = ob.callback;
	this->space_id = ob.space_id;
	this->index_id = ob.index_id;
	this->key = ob.key;
	this->key_end = ob.key_end;
	this->iter = ob.iter;
	this->current_tuple = ob.current_tuple;

	ob.current_tuple = NULL;
	ob.iter = NULL;
	return *this;
}

SpaceIterator::SpaceIterator(SpaceIterator &&ob) {
	*this = ob;
}

bool SpaceIterator::IsEnd() const {
	return IsOpen() && (!current_tuple);
}

bool SpaceIterator::IsOpen() const {
	return iter != NULL;
}

void SpaceIterator::Close() {
	if (IsOpen()) {
		box_iterator_free(iter);
		iter = NULL;
	}
}

bool SpaceIterator::Open() {
	iter = box_index_iterator(space_id, index_id, iter_tp, key, key_end);
	return (iter != NULL);
}

int SpaceIterator::Next() {
	if (!IsOpen()) return -1;
	return box_iterator_next(iter, &current_tuple);
}

box_tuple_t *SpaceIterator::GetTuple() {
	return current_tuple;
}

void SpaceIterator::IterateOver() {
	if (!callback) return;
	Next();
	while(IsOpen() && !IsEnd()) {
		if (callback(current_tuple, argc, argv)) {
			break;
		}
		Next();
	}
}

bool SpaceIterator::InProcess() const {
	return IsOpen() && !IsEnd();
}

iterator_type SpaceIterator::GetIteratorType() const {
	return iter_tp;
}