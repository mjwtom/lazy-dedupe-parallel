/*
 * chunk.c
 *
 *  Created on: 2012-7-13
 *      Author: BadBoy
 */
#ifndef CHUNK_H_
#define CHUNK_H_

#include <stdlib.h>
#include <stdint.h>
#include "config.h"
#include "thread.h"
#include <string.h>

struct chunk_node
{
	int len;
	char buf[TO_CHUNK_BUF_SIZE];
	struct list list;
};

struct chunk_pool
{
	pthread_cond_t coun;
	pthread_mutex_t mutex;
	struct list list;
	uint32_t len;
};

struct chunk_arg
{
	struct thread_header header;
	struct node_pool * from;
	struct node_pool * to;
	struct chunk_pool * from_rcv;
	struct chunk_pool * to_rcv;
	char chunk_buf[MAX_CHUNK_LEN];
};


int chunk_pool_init(struct chunk_pool * pool)
{
	pthread_mutex_init(&pool->mutex, NULL);
	pthread_cond_init(&pool->coun, NULL);
	list_init(&pool->list);
	pool->len = 0;
	return 0;
}


struct chunk_node * get_chunk_data_node(struct chunk_pool * pool, int *len)
{
	struct chunk_node * data_node;
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
	data_node = list_item(list, struct chunk_node);
	pthread_mutex_unlock (&pool->mutex);
	return data_node;
}



int put_chunk_data_node(struct chunk_pool* pool, struct chunk_node * data_node)
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


int chunk_batch(char * chunk_buf, int len, struct chunk_arg *chunk_arg, int end)
{
	static int batch_len = 0;
	static struct data_node * node = NULL;
	struct node_pool * from;
	struct node_pool * to;
#ifdef DEBUG
	int mylen;
	static int32_t id = 0;
#endif

	from = chunk_arg->from;
	to = chunk_arg->to;

	if(NULL == node)
	{
		node = get_data_node(from, &mylen);
#ifdef DEBUG
		node->id = id;
		id ++;
#endif
		node->calculate_type = CHUNK_THREAD;
		node->line_type = CUR_LINE_TYPE;
		batch_len = 0;
	}

	memcpy(node->data + batch_len * MAX_CHUNK_LEN, chunk_buf, len);
	node->metadata[batch_len].origin_len = len;
	batch_len ++;
	if((BATCH_NUM == batch_len) || end)
	{
		node->data_len = batch_len;
		put_data_node(to, node);
		node = NULL;
	}
	return 0;
}

void * fixed_chunk_thread(struct chunk_arg *chunk_arg)
{
	uint32_t chunk_num;

#ifdef DEBUG
	int mylen;
	int mybuflen;
	struct timeval start, end;
#endif

	int parsed_len;
	int cpy_len;
	int len;

	int i = 0;

	struct chunk_node * chunk_node, *tmp_chunk_node;
	struct data_node * node;

	while(chunk_arg->header.go_on)
	{
		tmp_chunk_node = chunk_node = get_chunk_data_node(chunk_arg->from_rcv, &mybuflen);
		printf("chunk thread from rec pool node is %d\n", mybuflen);
//		printf("get chunk buffer %d\n", i);
		printf("chunk node data len %d\n", chunk_node->len);
		printf("point %d\n", chunk_node);
		i ++;
		parsed_len = 0;
		len = chunk_node->len;
		while(parsed_len < len)
		{
			node = get_data_node(chunk_arg->from, &mylen);
			node->calculate_type = CHUNK_THREAD;
			node->line_type = CUR_LINE_TYPE;
#ifdef DEBUG
			gettimeofday(&start, NULL);
#endif
			chunk_num = 0;
			while((chunk_num < BATCH_NUM) && (parsed_len < len))
			{
				if(len != chunk_node->len)
				{
					printf("parsed len %d\n", parsed_len);
					printf("len != chunk_node len\n");
				}
				if(chunk_node != tmp_chunk_node)
					printf("tmp_chunk_node != chunk_node\n");
				if(len - parsed_len < MEAN_CHUNK_LEN)
				{
					cpy_len = len - parsed_len;
				}
				else
				{
					cpy_len = MEAN_CHUNK_LEN;
				}
				memcpy(node->data + chunk_num * MAX_CHUNK_LEN, chunk_node->buf + parsed_len, cpy_len);
				node->metadata[chunk_num].origin_len = cpy_len;
				parsed_len += cpy_len;
				chunk_num ++;
			}
			node->data_len = chunk_num;
#ifdef DEBUG
			gettimeofday(&end, NULL);
			node->time_used = td(&start, &end);
#endif
			put_data_node(chunk_arg->to, node);
		}
		put_chunk_data_node(chunk_arg->to_rcv, chunk_node);
	}
	return NULL;
}

void * chunk_thread(struct chunk_arg *chunk_arg)
{
	char first;
	int in_chunk_len;
	uint32_t abstract;
	char * win_start;
	char * data_start;
	char new;
	uint32_t chunk_num;
	int offset;

#ifdef DEBUG
	int mylen;
	int mybuflen;
#endif

	int parsed_len;
	int cpy_len;
	int len;

	struct chunk_node * chunk_node;
	struct data_node * node;

	while(chunk_arg->header.go_on)
	{
		chunk_node = get_chunk_data_node(chunk_arg->from_rcv, &mybuflen);
//		printf("chunk thread from rec pool node is %d\n", mybuflen);
		parsed_len = 0;
		len = chunk_node->len;
		while(parsed_len < len)
		{
			node = get_data_node(chunk_arg->from, &mylen);
			node->calculate_type = CHUNK_THREAD;
			node->line_type = CUR_LINE_TYPE;

			chunk_num = 0;
			offset = 0;
			while((chunk_num < BATCH_NUM) && (parsed_len < len))
			{
				node->offset[chunk_num] = offset;
				if(len - parsed_len < MIN_CHUNK_LEN)
				{
					cpy_len = len - parsed_len;
				}
				else
				{
					cpy_len = MIN_CHUNK_LEN;
				}
				memcpy(node->data + offset, chunk_node->buf + parsed_len, cpy_len);
				node->metadata[chunk_num].origin_len = cpy_len;
				parsed_len += cpy_len;
				in_chunk_len = cpy_len;
				if(MIN_CHUNK_LEN == cpy_len)
				{
					win_start = node->data + offset + in_chunk_len - WIN_LEN;
					data_start = chunk_node->buf + parsed_len;
					abstract = rabin_hash(win_start, WIN_LEN);
				}
				while((parsed_len < len) &&((abstract % MEAN_CHUNK_LEN) != CHUNK_CDC_R) && (in_chunk_len < MAX_CHUNK_LEN))
				{
					new = * data_start;
					node->data[offset + in_chunk_len] = chunk_node->buf[parsed_len];
					in_chunk_len ++;
					parsed_len ++;
					first = * win_start;
					abstract = rabin_karp(first, abstract, new);
					win_start ++;
					data_start ++;
				}
				node->metadata[chunk_num].origin_len = in_chunk_len;
				offset += in_chunk_len;
				chunk_num ++;
			}
			node->offset[chunk_num] = offset;
			node->data_len = chunk_num;
			put_data_node(chunk_arg->to, node);
		}
		put_chunk_data_node(chunk_arg->to_rcv, chunk_node);
	}
	return NULL;
}

int to_chunk_buf(struct chunk_arg *chunk_arg, char * buf, int len)
{
	static int in_buf_len = 0;
	static struct chunk_node * chunk_node = NULL;
#ifdef DEBUG
	int mybuflen;
#endif
	if(NULL == chunk_node)
	{
		chunk_node = get_chunk_data_node(chunk_arg->to_rcv, &mybuflen);
//		printf("receive from rec pool node is %d\n", mybuflen);
	}
	if(TO_CHUNK_BUF_SIZE - in_buf_len < len)
	{
		chunk_node->len = in_buf_len;
		put_chunk_data_node(chunk_arg->from_rcv, chunk_node);
		chunk_node = get_chunk_data_node(chunk_arg->to_rcv, &mybuflen);
//		printf("receive from rec pool node is %d\n", mybuflen);
		in_buf_len = 0;
	}
	memcpy(chunk_node->buf + in_buf_len, buf, len);
	in_buf_len += len;
	return 0;
}

#endif
