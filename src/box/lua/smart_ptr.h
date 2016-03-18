#ifndef INCLUDES_TARANTOOL_LUA_SMART_PTR_H
#define INCLUDES_TARANTOOL_LUA_SMART_PTR_H
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

template<typename T>
class SmartPtr {
private:
	T *ptr;
	mutable int *counter;
	void (*deleter)(T *);
	void decrease_counter();

public:
	SmartPtr(T *ptr = NULL, void (*deleter)(T *) = NULL);
	SmartPtr(const SmartPtr &ob);
	SmartPtr(SmartPtr &&ob);
	SmartPtr &operator=(const SmartPtr &ob);
	SmartPtr &operator=(SmartPtr &&ob);

	void reset(T *ptr = NULL);
	T *get();
	const T *get() const;
	~SmartPtr();
};

template<typename T>
void SmartPtr<T>::decrease_counter() {
	if (counter) {
		*counter -= 1;
		if (*counter == 0) {
			if (!deleter) delete ptr;
			else deleter(ptr);
			delete counter;
			counter = NULL;
			ptr = NULL;
		}
	}
}

template<typename T>
SmartPtr<T>::SmartPtr(T *ptr_, void (*deleter_)(T *))
	: ptr(ptr_), counter(new int), deleter(deleter_)
{
	*counter = 1;
}

template<typename T>
SmartPtr<T>::SmartPtr(const SmartPtr &ob)
	: ptr(NULL), counter(NULL), deleter(NULL)
{
	*this = ob;
}

template<typename T>
SmartPtr<T>::SmartPtr(SmartPtr &&ob)
	: ptr(NULL), counter(NULL), deleter(NULL)
{
	*this = ob;
}

template<typename T>
SmartPtr<T> &SmartPtr<T>::operator=(const SmartPtr &ob) {
	decrease_counter();
	counter = ob.counter;
	ptr = ob.ptr;
	deleter = ob.deleter;
	*counter += 1;
	return *this;
}

template<typename T>
SmartPtr<T> &SmartPtr<T>::operator=(SmartPtr &&ob) {
	decrease_counter();
	ptr = ob.ptr;
	counter = ob.counter;
	deleter = ob.deleter;
	ob.ptr = NULL;
	ob.counter = NULL;
	ob.deleter = NULL;
	return *this;
}

template<typename T>
void SmartPtr<T>::reset(T *ptr_) {
	decrease_counter();
	ptr = ptr_;
	counter = new int;
	*counter = 1;
}

template<typename T>
T *SmartPtr<T>::get() {
	return ptr;
}

template<typename T>
const T *SmartPtr<T>::get() const {
	return ptr;
}

template<typename T>
SmartPtr<T>::~SmartPtr() {
	decrease_counter();
}

#endif