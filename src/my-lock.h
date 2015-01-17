/*
 * my-lock.h
 *
 *  Created on: 2012-8-3
 *      Author: BadBoy
 */

#ifndef MY_LOCK_H_
#define MY_LOCK_H_

#include <pthread.h>

struct my_lock
{
	pthread_mutex_t mutex;
	int sample;
	pthread_cond_t coun;
};

int init_mylock(struct my_lock * lock)
{
	pthread_mutex_init(&lock->mutex, NULL);
	pthread_cond_init(&lock->coun, NULL);
	lock->sample = 0;
	return 0;
}

int mylock(struct my_lock * lock)
{
	pthread_mutex_lock (&lock->mutex);
	while(1 == lock->sample)
	{
		pthread_cond_wait(&lock->coun, &lock->mutex);
	}
	lock->sample = 1;
	pthread_mutex_unlock (&lock->mutex); ;
	return 0;
}

int myunlock(struct my_lock * lock)
{
	pthread_mutex_lock (&lock->mutex);
	lock->sample = 0;
	pthread_mutex_unlock(&lock->mutex);
	pthread_cond_signal(&lock->coun);
	return 0;
}

#endif /* MY_LOCK_H_ */
