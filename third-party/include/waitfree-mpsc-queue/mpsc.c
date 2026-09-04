/* 2015 Daniel Bittman <danielbittman1@gmail.com>: http://dbittman.github.io/ */

#include <stdatomic.h>
#include <stdbool.h>
#include <stdlib.h>
#include <assert.h>

#include "mpscq.h"

/* multi-producer, single consumer queue *
 * Requirements: max must be >= 2 */
struct mpscq *mpscq_create(struct mpscq *n, size_t capacity)
{
	if(capacity < 2 || (capacity & (capacity - 1)) != 0
			|| capacity > SIZE_MAX / sizeof(*n->buffer))
		return NULL;
	bool allocated = false;
	if(!n) {
		n = calloc(1, sizeof(*n));
		if(!n)
			return NULL;
		allocated = true;
		n->flags |= MPSCQ_MALLOC;
	} else {
		n->flags = 0;
	}
	atomic_init(&n->count, 0);
	atomic_init(&n->head, 0);
	n->tail = 0;
	n->buffer = malloc(capacity * sizeof(*n->buffer));
	if(!n->buffer) {
		if(allocated)
			free(n);
		return NULL;
	}
	for(size_t index = 0; index < capacity; ++index)
		atomic_init(&n->buffer[index], NULL);
	n->max = capacity;
	return n;
}

void mpscq_destroy(struct mpscq *q)
{
	if(!q)
		return;
	free(q->buffer);
	if(q->flags & MPSCQ_MALLOC)
		free(q);
}

bool mpscq_enqueue(struct mpscq *q, void *obj)
{
	if(!q || !obj)
		return false;
	size_t count = atomic_fetch_add_explicit(&q->count, 1, memory_order_acquire);
	if(count >= q->max) {
		/* back off, queue is full */
		atomic_fetch_sub_explicit(&q->count, 1, memory_order_release);
		return false;
	}

	/* increment the head, which gives us 'exclusive' access to that element */
	size_t head = atomic_fetch_add_explicit(&q->head, 1, memory_order_acquire);
	assert(q->buffer[head % q->max] == 0);
	void *rv = atomic_exchange_explicit(&q->buffer[head % q->max], obj, memory_order_release);
	assert(rv == NULL);
	return true;
}

void *mpscq_dequeue(struct mpscq *q)
{
	if(!q)
		return NULL;
	void *ret = atomic_exchange_explicit(&q->buffer[q->tail], NULL, memory_order_acquire);
	if(!ret) {
		/* a thread is adding to the queue, but hasn't done the atomic_exchange yet
		 * to actually put the item in. Act as if nothing is in the queue.
		 * Worst case, other producers write content to tail + 1..n and finish, but
		 * the producer that writes to tail doesn't do it in time, and we get here.
		 * But that's okay, because once it DOES finish, we can get at all the data
		 * that has been filled in. */
		return NULL;
	}
	if(++q->tail >= q->max)
		q->tail = 0;
	size_t r = atomic_fetch_sub_explicit(&q->count, 1, memory_order_release);
	assert(r > 0);
	return ret;
}

size_t mpscq_count(struct mpscq *q)
{
	if(!q)
		return 0;
	return atomic_load_explicit(&q->count, memory_order_relaxed);
}

size_t mpscq_capacity(struct mpscq *q)
{
	if(!q)
		return 0;
	return q->max;
}
