/*
 * data-pool.h
 *
 *  Created on: 2012-8-4
 *      Author: BadBoy
 */

#ifndef DATA_POOL_H_
#define DATA_POOL_H_

#include <semaphore.h>
#include "list.h"
#include "config.h"
#include <pthread.h>

struct node_pool
{
	pthread_cond_t coun;
	pthread_mutex_t mutex;
	struct list list;
	uint32_t len;
};

struct thread_header
{
	pthread_t id;
	int go_on;
};

struct data_node
{
	struct list list;
	uint32_t id;
	char calculate_type;
	char line_type;
	unsigned long calculate_time;
	char dup_sample[BATCH_NUM];
	unsigned int offset[BATCH_NUM + 1];
	int dup_num;
	int data_len;
	struct metadata metadata[BATCH_NUM];
	char data[MAX_CHUNK_LEN * BATCH_NUM];
	char compress_data[MAX_CHUNK_LEN * BATCH_NUM];
	uint64_t time_used;
};

int pool_init(struct node_pool * pool)
{
	pthread_mutex_init(&pool->mutex, NULL);
	pthread_cond_init(&pool->coun, NULL);
	list_init(&pool->list);
	pool->len = 0;
	return 0;
}

struct data_node * get_data_node(struct node_pool * pool, int * len)
{
	struct data_node * data_node;
	struct list * list;
	pthread_mutex_lock (&pool->mutex);
	while(0 >= pool->len)
	{
		pthread_cond_wait(&pool->coun, &pool->mutex);
	}
	list = list_first(&pool->list);
	list_del(list);
	pool->len --;
#ifdef DEBUG
	*len = pool->len;
#endif
	if(NULL == list)
		return NULL;
	data_node = list_item(list, struct data_node);
	pthread_mutex_unlock (&pool->mutex);
	return data_node;
}


int put_data_node(struct node_pool* pool, struct data_node * data_node)
{
	if(NULL == pool)
		return -1;
	pthread_mutex_lock (&pool->mutex);
	list_add(&pool->list, &data_node->list);
	pool->len ++;
	pthread_mutex_unlock (&pool->mutex);
	pthread_cond_signal(&pool->coun);
	return 0;
}


struct data_node * get_node_by_id(struct node_pool * pool, uint32_t id)
{
	struct data_node * data_node;
	struct list * list;
	pthread_mutex_lock (&pool->mutex);
	while(1)
	{
		while(0 >= pool->len)
		{
			pthread_cond_wait(&pool->coun, &pool->mutex);
		}
		list_uniterate(list, &pool->list, &pool->list)
		{
			data_node = list_item(list, struct data_node);
			if(id == data_node->id)
			{
				list_del(list);
				pool->len --;
				pthread_mutex_unlock (&pool->mutex);
				return data_node;
			}
		}
		pthread_cond_wait(&pool->coun, &pool->mutex);
	}
	return NULL;
}

#endif /* DATA_POOL_H_ */
