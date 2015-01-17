/*
 * thread.h
 *
 *  Created on: 2011-7-19
 *      Author: badboy
 */

#ifndef THREAD_H_
#define THREAD_H_

#include "config.h"
#include "sha1.h"
#include "dedup.h"
#include "aes.h"
#include "data-pool.h"
#include "lzjb.h"
#include "GPU-sha1.h"

#include "my-time.h"

#include "chunk.h"

struct compress_arg
{
	struct thread_header header;
	struct node_pool * from;
	struct node_pool * to;
};

struct hash_arg
{
	struct thread_header header;
	struct node_pool * from;
	struct node_pool * to;
};

struct identify_arg
{
	struct thread_header header;
	struct node_pool * from;
	struct node_pool * to;
	struct dedup_manager * manager;
};

struct encrypt_arg
{
	struct thread_header header;
	struct node_pool * from;
	struct node_pool * to;
};




struct schedule_arg
{
	struct thread_header header;
	struct node_pool * from;
	struct node_pool * free;
	struct node_pool * to_compress;
	struct node_pool * to_hash;
	struct node_pool * to_identify;
	struct node_pool * to_store;
	struct node_pool * to_iden_free;
};

struct pipe_dedup
{
	struct dedup_manager manager;
	struct schedule_arg schedule;
	struct hash_arg hash[HASH_THREAD_NUM];
	struct hash_arg gpu_hash;
	struct compress_arg compress[COMPRESS_THREAD_NUM];
	struct identify_arg identify;
	struct chunk_arg chunk[CHUNK_TRHEAD_NUM];
	struct data_node node[NODE_NUM];
	struct chunk_node chunk_node[CHUNK_NODE_NUM];

	struct node_pool to_compress;
	struct node_pool to_store;
	struct node_pool to_hash;
	struct node_pool to_identify;
	struct node_pool to_schedule;
	struct node_pool free;

	struct chunk_pool from_rcv;
	struct chunk_pool to_rcv;
};

int get_unique_data(char * input_data, char * output_data, struct metadata * mt, int * data_len, char *sample, int len, int block_size)
{
	int count;
	int i;
	count = 0;
	for(i = 0 ; i < len ; i ++)
	{
		if(0 == sample[i])
		{
			memcpy(output_data + count * block_size, input_data + i * block_size, block_size);
			data_len[count] = mt[i].origin_len;
			count ++;
		}
	}
	return count;
}

int put_compress_data(char * input_data, char * output_data, struct metadata * mt, int * compressed_len, char * sample, int len, int block_size)
{
	int count;
	int i;
	count = 0;
	for(i = 0 ; i < len ; i ++)
	{
		if(0 == sample[i])
		{
			memcpy(output_data + i * block_size, input_data + count * block_size, block_size);
			mt[i].len = compressed_len[count];
			count ++;
		}
	}
	return 0;
}

void * compress_thread(void * arg)
{
	struct compress_arg * compress_arg;
	int * go_on;
	int len;
	struct node_pool * from;
	struct node_pool * to;
	struct data_node * data_node;
	int i;

#ifdef DEBUG
	int mylen;
	struct timeval start, end;
#endif


#ifdef GPU
	int ret;
	int *data_len = malloc (sizeof(int) * BATCH_NUM);
	int *compressed_len = malloc(sizeof(int) * BATCH_NUM);
	GUP_compress_init(MAX_CHUNK_LEN, MAX_COMPRESS_LEN, BATCH_NUM);
	char * unique_data = malloc(BATCH_NUM * MAX_CHUNK_LEN);
	char * compress_data = malloc(BATCH_NUM * MAX_COMPRESS_LEN);
#endif

	compress_arg = (struct compress_arg *)arg;
	go_on = &(compress_arg->header.go_on);
	from = compress_arg->from;
	to = compress_arg->to;
	while(*go_on)
	{
		data_node = get_data_node(from, &mylen);
#ifdef DEBUG
//		printf("in compress from buf%d\n", mylen);
		gettimeofday(&start, NULL);
#endif
		data_node->calculate_type = COMPRESS_THREAD;
		switch(data_node->line_type)
		{
		case LINE_TYPE_CHIE:
#ifdef GPU
			for(i = 0 ; i < data_node->data_len; i ++)
			{
				data_len[i] = data_node->metadata[i].origin_len;
			}
	//		puts("before gpu compress");
			GPU_compress(data_node->data, data_node->compress_data, data_len, compressed_len, MAX_CHUNK_LEN, MAX_COMPRESS_LEN, data_node->data_len);
	//		puts("after gpu compress");
			for(i = 0 ; i < data_node->data_len; i ++)
			{
				data_node->metadata[i].len = compressed_len[i];
			}
#else
			len = data_node->data_len;
			for(i = 0 ; i < len ; i ++)
			{
				data_node->metadata[i].len = lzjb_compress(data_node->data + data_node->offset[i],
						data_node->compress_data + i * MAX_CHUNK_LEN,
						data_node->metadata[i].origin_len,
						MAX_CHUNK_LEN,
						0);
			}
#endif
			break;
		case LINE_TYPE_HICE:
#ifdef GPU
//			ret = statute(data_node->data, data_node->metadata, data_node->dup_sample, data_node->data_len, MAX_CHUNK_LEN);
			ret = get_unique_data(data_node->data, unique_data, data_node->metadata, data_len, data_node->dup_sample, data_node->data_len, MAX_CHUNK_LEN);
//			for(i = 0 ; i < ret; i ++)
//			{
//				data_len[i] = data_node->metadata[i].origin_len;
//			}
	//		puts("before gpu compress");
			GPU_compress(unique_data, compress_data, data_len, compressed_len, MAX_CHUNK_LEN, MAX_COMPRESS_LEN, ret);
	//		puts("after gpu compress");
//			for(i = 0 ; i < ret; i ++)
//			{
//				data_node->metadata[i].len = c_len[i];
//			}
			put_compress_data(compress_data, data_node->compress_data, data_node->metadata, compressed_len, data_node->dup_sample, data_node->data_len, MAX_COMPRESS_LEN);
#else
			len = data_node->data_len;
			for(i = 0 ; i < len ; i ++)
			{
				if(0 == data_node->dup_sample[i])
				{
					data_node->metadata[i].len = lzjb_compress(data_node->data + i * MAX_CHUNK_LEN,
							data_node->compress_data + i * MAX_CHUNK_LEN,
							data_node->metadata[i].origin_len,
							MAX_CHUNK_LEN,
							0);
				}
			}
#endif
			break;
		default:
			break;
		}
#ifdef DEBUG
		gettimeofday(&end, NULL);
		data_node->time_used = td(&start, &end);
#endif
		put_data_node(to, data_node);
//		puts("end compress");
	}
#ifdef GPU
	GUP_compress_destroy();
	free(data_len);
	free(compressed_len);
	free(unique_data);
	free(compress_data);
#endif
	return NULL;
}

void * gpu_hash_thread(void * arg)
{
	struct hash_arg * hash_arg;
	struct data_node * data_node;
	int * go_on;
	int len;
	struct node_pool * from;
	struct node_pool * to;
	int i;

#ifdef DEBUG
	int mylen;
	struct timeval start, end;
#endif

	hash_arg = (struct hash_arg *) arg;
	go_on = &(hash_arg->header.go_on);
	from = hash_arg->from;
	to = hash_arg->to;
	char * sha1_buf = malloc(BATCH_NUM * FINGERPRINT_LEN);
	int * data_len = malloc(BATCH_NUM * sizeof(int));

	GPU_sha1_init(MAX_CHUNK_LEN, BATCH_NUM);

	while(*go_on)
	{
		data_node = get_data_node(from, &mylen);
#ifdef DEBUG
		gettimeofday(&start, NULL);
#endif
		data_node->calculate_type = HASH_THREAD;
		GPU_sha1(data_node->data, sha1_buf,data_node->offset, data_node->data_len, data_node->offset[data_node->data_len]);
		for(i = 0 ; i < data_node->data_len ; i ++)
		{
			memcpy(data_node->metadata[i].fingerprint, sha1_buf + i * FINGERPRINT_LEN, FINGERPRINT_LEN);
		}

#ifdef DEBUG
		gettimeofday(&end, NULL);
		data_node->time_used = td(&start, &end);
#endif
		put_data_node(to, data_node);
	}
	GPU_sha1_destroy();
	free(sha1_buf);
	free(data_len);
	return NULL;
}

#ifdef FINGER_DEBUG
FILE * finger_file;
char end_finger_file = 0;
struct my_lock finger_status;
#endif

void * hash_thread(void * arg)
{
	struct hash_arg * hash_arg;
	struct data_node * data_node;
	int * go_on;
	int len;
	struct node_pool * from;
	struct node_pool * to;
	int i;

#ifdef DEBUG
	int mylen;
	struct timeval start, end;
#endif

	hash_arg = (struct hash_arg *) arg;
	go_on = &(hash_arg->header.go_on);
	from = hash_arg->from;
	to = hash_arg->to;
#ifdef GPU
	char * sha1_buf = malloc(BATCH_NUM * FINGERPRINT_LEN);
	int * data_len = malloc(BATCH_NUM * sizeof(int));
	GPU_sha1_init(MAX_COMPRESS_LEN, BATCH_NUM);
#endif

	while(*go_on)
	{
		data_node = get_data_node(from, &mylen);
#ifdef DEBUG
		gettimeofday(&start, NULL);
#endif
		data_node->calculate_type = HASH_THREAD;
		len = data_node->data_len;

#ifdef GPU
		for(i = 0 ; i < data_node->data_len ; i ++)
		{
			data_len[i] = data_node->metadata[i].origin_len;
		}
		GPU_sha1(data_node->data, sha1_buf, data_len, MAX_CHUNK_LEN, data_node->data_len);
		for(i = 0 ; i < data_node->data_len ; i ++)
		{
			memcpy(data_node->metadata[i].fingerprint, sha1_buf + i * FINGERPRINT_LEN, FINGERPRINT_LEN);
		}
#else
#ifdef FINGER_DEBUG
		for(i = 0 ; i < BATCH_NUM ; i ++)
		{
			if(fread(data_node->metadata[i].fingerprint, 1, FINGERPRINT_LEN, finger_file) < FINGERPRINT_LEN)
			{
				mylock(&finger_status);
				end_finger_file = 1;
				myunlock(&finger_status);
				break;
			}
		}
		data_node->data_len = i;
#else
		for(i = 0 ; i < len ; i ++)
		{
			sha1(data_node->data + data_node->offset[i],
					data_node->metadata[i].origin_len,
					data_node->metadata[i].fingerprint);

		}
#endif


#endif

#ifdef DEBUG
		gettimeofday(&end, NULL);
		data_node->time_used = td(&start, &end);
#endif
		put_data_node(to, data_node);
	}
#ifdef GPU
	GPU_sha1_destroy();
	free(sha1_buf);
	free(data_len);
#endif
	return NULL;
}

int after_identify(struct metadata * mt, char * data, struct dedup_manager * manager, char dup)
{
	struct disk_hash_node disk_hash_node;
	uint64_t mt_offset;
	/*static int id = 0;
	id ++;
	if(id > 1000000)
	{
		puts("");
		printf("identify time %f\n", identify_time / 1000000.0);
		printf("disk hash lookup time %f\n", disk_hash_look_time / 1000000.0);
		printf("disk hash fread times %d\n", disk_hash_fread_times);
		printf("disk hash fread time %fsec\n", disk_hash_fread_time / 1000000.0);
		printf("cache time is %f\n", cache_time / 1000000.0);
		printf("cache times is %d\n", cache_times);
		printf("load cache time %f\n", load_cache_time / 1000000.0);
		printf("load cache times %d\n", load_cache_times);
		printf("first cache hit time is %d\n", first_cache_hit);
		puts("");
		id = 0;
	}*/
	if(0 == dup)
	{
		mt->offset = add_data(data, mt->len, &manager->dt_seg);
		memcpy(disk_hash_node.fingerprint, mt->fingerprint, FINGERPRINT_LEN);
		disk_hash_node.data_len = mt->len;
		disk_hash_node.data_offset = mt->offset;
		mt_offset = add_metadata(*mt, &manager->mt_seg);
		disk_hash_node.mtdata_offset = mt_offset;
		add_2_disk_hash(&manager->disk_hash, disk_hash_node);
#ifdef DEBUG
		compress_len += mt->len;
		unique_len += mt->origin_len;
#endif
	}
	else
	{
		mt_offset = add_metadata(*mt, &manager->mt_seg);
#ifdef DEBUG
		dup_len += mt->origin_len;
		dup_num ++;
#endif
	}
	return 0;
}

int de_mh_frag(struct mem_hash * mh, struct dedup_manager * manager)
{
	int i;
	struct list *list, *safe, *result, *free, * inner_dup;
	struct mem_hash_node * mnode, *mnode_dup;
	for(i = 0 ; i < BUCKET_NUM ; i++)
	{
		result = &mh->mem_bucket[i].result;
		free = &mh->mem_bucket[i].free;
		inner_dup = &mh->mem_bucket[i].inner_dup;
		list_iterate_safe(list, safe, result)
		{
			mnode = list_item(list, struct mem_hash_node);
			after_identify(&mnode->mtdata, mnode->data, manager, mnode->dup);
			list_del(list);
			list_add(free, list);
		}
		list_iterate_safe(list, safe, inner_dup)
		{
			mnode = list_item(list, struct mem_hash_node);
			mnode_dup = mnode->innter_dup;
			if(1 == mnode_dup->result)
			{
				memcpy(&mnode->mtdata, &mnode_dup->mtdata, sizeof(struct metadata));
				after_identify(&mnode->mtdata, mnode->data, manager, 1);
				list_del(list);
				list_add(free, list);
			}
		}
	}
	return 0;
}

int cache_count[1024];

int line_cache_lookup(struct dedup_manager * dedup, struct mem_hash * mh, struct cache * cache, struct mem_hash_node * mhash_node, struct dedup_manager * manager)
{
	uint32_t first_int, index;
//	uint64_t hit_id;
	int ret, i = 0, j = 0;
	struct mem_hash_node * mnode, *next;
	struct timeval start, end;

//	printf("disk hit id id id id %d\n", mhash_node->mtdata.id);
	mnode = mhash_node->next;
	while((mnode != mhash_node) && (mnode != &mh->head))
	{
//		hit_id = mnode->mtdata.id;
		next = mnode->next;
		gettimeofday(&start, NULL);
		ret = lookup_in_cache(cache, &mnode->mtdata);
		mnode->cache_count ++;
		cache_times ++;
		this_cache_times ++;
		gettimeofday(&end, NULL);
		mem_hash_cache_look_time += td(&start, &end);
		this_mem_hash_cache_look_time += td(&start, &end);
		if(1 == ret)
		{
			memcpy((void *)&first_int, (void*)mnode->mtdata.fingerprint, sizeof(uint32_t));
			index = first_int & DISK_HASH_MASK;
			after_identify(&mnode->mtdata, mnode->data, manager, 1);
			list_del(&mnode->list);
			list_add(&mh->mem_bucket[index].free, &mnode->list);
			mnode->result = 1;
			mhnode_list_del(mnode);
			mhnode_list_init(mnode);
			j ++;
			second_cache_hit ++;
			this_second_cache_hit ++;
			cache_count[mnode->cache_count] ++;
//			printf("second cache hit metadata id %d\n", hit_id);
		}
		else
		{
			if(mnode->cache_count >= CACHE_THRESHOLD)
			{
				mhnode_list_del(mnode);
				mhnode_list_init(mnode);
			}
		}
		mnode = next;
		i ++;
	}
	if(mnode == &mh->head)
	{
		mnode = mh->head.next;
		while(mnode != mhash_node)
		{
//			hit_id = mnode->mtdata.id;
			next = mnode->next;
			gettimeofday(&start, NULL);
			ret = lookup_in_cache(cache, &mnode->mtdata);
			mnode->cache_count ++;
			cache_times ++;
			this_cache_times ++;
			gettimeofday(&end, NULL);
			mem_hash_cache_look_time += td(&start, &end);
			this_mem_hash_cache_look_time += td(&start, &end);
			if(1 == ret)
			{
				memcpy((void *)&first_int, (void*)mnode->mtdata.fingerprint, sizeof(uint32_t));
				index = first_int & DISK_HASH_MASK;
				after_identify(&mnode->mtdata, mnode->data, manager, 1);
				memcpy((void *)&first_int, (void*)mnode->mtdata.fingerprint, sizeof(uint32_t));
				index = first_int & DISK_HASH_MASK;
				list_del(&mnode->list);
				list_add(&mh->mem_bucket[index].free, &mnode->list);
				mnode->result = 1;
				mhnode_list_del(mnode);
				mhnode_list_init(mnode);
				j ++;
				second_cache_hit ++;
				this_second_cache_hit ++;
				cache_count[mnode->cache_count] ++;
//				printf("second cache hit metadata id %d\n", hit_id);
			}
			else
			{
				if(mnode->cache_count >= CACHE_THRESHOLD)
				{
					mhnode_list_del(mnode);
					mhnode_list_init(mnode);
				}
			}
			mnode = next;
			i ++;
		}

	}
//	printf("chain id %d, chain offset %d, chain len %d, dup len %d\n", mhash_node->chai_id, mhash_node->chain_offset, i, j);
	return 0;
}

int line_no_metadata_cache_lookup(struct dedup_manager * dedup, struct mtdata_seg * seg, struct metadata * mtdata, struct mem_hash * mh, struct cache * cache, struct mem_hash_node * mhash_node, uint64_t offset, uint32_t before_len, struct dedup_manager * manager)
{
	int len;
	before_len += LOAD_ADVANCE;
	len = get_inseg_metadata(seg, mtdata, offset, before_len, MAX_BEFORE_LEN - before_len + 2*LOAD_ADVANCE);

	add_metadata_in_cache(mtdata, len, cache);

	line_cache_lookup(dedup, mh, cache, mhash_node, manager);
	return 0;
}

int put_in_mem_hash(struct metadata * mt, char * data, struct mem_hash * mh, struct disk_hash * disk_hash, struct dedup_manager * dedup, int new_chain, int chain_off, struct dedup_manager * manager)
{
	static int chain_id = 0;
	uint32_t first_int, index;
	struct timeval start, end;
	struct mem_hash_node *mnode, *mnode_dup, * last_node;
	uint32_t last_node_index;
	struct merge_load_arg * merge_load;
	struct list * list, *safe, *free;
	int len, i;
	int need_lookup_cache = 0;
	int first_cache = 0;

	memcpy((void *)&first_int, (void*)mt->fingerprint, sizeof(uint32_t));
	index = first_int & DISK_HASH_MASK;
	list = list_first(&mh->mem_bucket[index].free);
	mnode = list_item(list, struct mem_hash_node);
	memcpy(&mnode->mtdata, mt, sizeof(struct metadata));
	memcpy(&mnode->data, data, mt->len);
	mnode->dup = 0;
	mnode->result = 0;
	mnode->chain_offset = chain_off;
	list_del(list);
	list_add(&mh->mem_bucket[index].head, list);
	if(1 == new_chain)
	{
//		printf("list len %d\n", mh_list_size(&mh->head));
		mhnode_list_del(&mh->head);
		mhnode_list_init(&mh->head);
		chain_id ++;
	}
	mnode->chai_id = chain_id;
	mnode->cache_count = 0;
	mhnode_list_add(&mh->head, mnode);
	if(list_empty(&mh->mem_bucket[index].free))
	{
		/*if(FH_LEN != list_size(&mh->mem_bucket[index].head))
		{
			printf("lost mh node\n");
			exit(0);
		}*/
		last_node = mnode;
		gettimeofday(&start, NULL);
		mem_hash_lookup(disk_hash, mh, index);
		gettimeofday(&end, NULL);
		disk_hash_look_time += td(&start, &end);
		this_disk_hash_look_time += td(&start, &end);
		free = &mh->mem_bucket[index].free;

#ifdef MERGE_LOAD_CACHE
		merge_load = &dedup->merge_load;
		merge_load->len = 0;
		need_lookup_cache = 0;

		i = 0;
		list = (&mh->mem_bucket[index].result)->n;
		while(list != &mh->mem_bucket[index].result)
		{
			mnode = list_item(list, struct mem_hash_node);
			if((1 == mnode->dup) && (mnode->next != mnode))
			{
				if(mnode == last_node)
				{
					last_node_index = i;
					first_cache = 1;
				}
//				printf("disk hit id %d\n", mnode->mtdata.id);
				merge_load->node[i].before = mnode->chain_offset + LOAD_ADVANCE;
				merge_load->node[i].offset = mnode->dnode.mtdata_offset;
				merge_load->node[i].after = MAX_BEFORE_LEN - mnode->chain_offset + 2 * LOAD_ADVANCE;
				mnode->load_cache_index = i;
				i ++;
				need_lookup_cache = 1;
			}
#ifndef LINE_LOOKUP
			after_identify(&mnode->mtdata, mnode->data, dedup, mnode->dup);
			safe = list->n;
			list_del(list);
			list_add(free, list);
			mhnode_list_del(mnode);
			list = safe;
#else
			list = list->n;
#endif
		}
		merge_load->len = i;

		if(1 == need_lookup_cache)
		{
			len = merge_get_metadata(&dedup->mt_seg, dedup->mtdata, merge_load);
//			len = merge_get_seg_metadata(&dedup->mt_seg, dedup->mtdata, merge_load);
		}
		gettimeofday(&start, NULL);
#ifdef LINE_LOOKUP
#ifdef SEPERATE_LOAD_CACHE
		list = (&mh->mem_bucket[index].result)->n;
		while(list != &mh->mem_bucket[index].result)
		{
			mnode = list_item(list, struct mem_hash_node);
			after_identify(&mnode->mtdata, mnode->data, dedup, mnode->dup);
			if(1 == mnode->dup)
			{
				if((1 == mnode->dup) && (mnode->next != mnode))
				{
					i = mnode->load_cache_index;
//					printf("disk hit id %d\n", mnode->mtdata.id);
					add_metadata_in_cache(dedup->mtdata + merge_load->index[i].mt_offset, merge_load->index[i].len, &dedup->cache);
					line_cache_lookup(dedup, mh, &dedup->cache, mnode);
				}
			}
			safe = list->n;
			list_del(list);
			list_add(free, list);
			mhnode_list_del(mnode);
			list = safe;
		}
#else
		add_metadata_in_cache(dedup->mtdata, len, &dedup->cache);

		list = (&mh->mem_bucket[index].result)->n;
		while(list != &mh->mem_bucket[index].result)
		{
			mnode = list_item(list, struct mem_hash_node);
			after_identify(&mnode->mtdata, mnode->data, dedup, mnode->dup);
			if((1 == mnode->dup) &&(mnode->next != mnode))
			{
				line_cache_lookup(dedup, mh, &dedup->cache, mnode);
			}
			safe = list->n;
			list_del(list);
			list_add(free, list);
			mhnode_list_del(mnode);
			list = safe;
		}
#endif
#else
		if(1 == need_lookup_cache)
		{
			clear_new_in_cache(&dedup->cache);
			add_metadata_in_cache(dedup->mtdata, len, &dedup->cache);
			lookup_mem_hash_in_cache(&dedup->cache, mh);
			gettimeofday(&end, NULL);
			mem_hash_cache_look_time += td(&start, &end);
			this_mem_hash_cache_look_time += td(&start, &end);
			gettimeofday(&start, NULL);
			de_mh_frag(mh, dedup);
			gettimeofday(&end, NULL);
			defrag_time += td(&start, &end);
			this_defrag_time += td(&start, &end);
		}
#endif
#else
		list = (&mh->mem_bucket[index].result)->n;
		while(list != &mh->mem_bucket[index].result)
		{
			mnode = list_item(list, struct mem_hash_node);
			after_identify(&mnode->mtdata, mnode->data, manager, mnode->dup);
			cache_count[mnode->cache_count] ++;
			if((1 == mnode->dup) && (mnode->next != mnode))
			{
//				printf("disk hit id %d\n", mnode->mtdata.id);
				len = line_no_metadata_cache_lookup(dedup, &dedup->mt_seg, dedup->mtdata, &dedup->mem_hash, &dedup->cache, mnode,mnode->dnode.mtdata_offset, mnode->chain_offset, manager);
				if((mnode == last_node) || ((NULL != last_node->innter_dup) && (mnode == last_node->innter_dup)))
				{
					first_cache = 1;
					memcpy(dedup->lastmtdata, dedup->mtdata, sizeof(struct metadata) * len);
				}
			}
			safe = list->n;
			list_del(list);
			list_add(free, list);
			mhnode_list_del(mnode);
			list = safe;
		}
#endif
//		exit(0);
		list_iterate_safe(list, safe, &mh->mem_bucket[index].inner_dup)
		{
			mnode = list_item(list, struct mem_hash_node);
			mnode_dup = mnode->innter_dup;
			if((1 == mnode_dup->result) && (1 == mnode_dup->dup))
			{
				memcpy(&mnode->mtdata, &mnode_dup->mtdata, sizeof(struct metadata));
				memcpy(&mnode->dnode, &mnode_dup->dnode, sizeof(struct disk_hash_node));
				after_identify(&mnode->mtdata, mnode->data, manager, 1);
				list_del(list);
				list_add(free, list);
				mhnode_list_del(mnode);
				mnode->result = 1;
				mnode->dup = 1;
			}
		}
		if(1 == first_cache)
		{
			gettimeofday(&start, NULL);
#ifdef MERGE_LOAD_CACHE
			add_metadata_in_cache(dedup->mtdata + merge_load->index[last_node_index].mt_offset, merge_load->index[last_node_index].len, &dedup->cache);
#else
			add_metadata_in_cache(dedup->lastmtdata, len, &dedup->cache);
#endif
			gettimeofday(&end, NULL);
			mh->first_cache = 1;
		}
		else
		{
			if((1 == last_node->result) && (1 == last_node->dup))
			{
//				len = get_metadata(&dedup->mt_seg, dedup->mtdata, last_node->dnode.mtdata_offset, last_node->chain_offset + LOAD_ADVANCE, TO_CACHE_LEN);
				len = get_seg_after_metadata(&dedup->mt_seg, dedup->mtdata, last_node->dnode.mtdata_offset);
				add_metadata_in_cache(dedup->mtdata, len, &dedup->cache);
				mh->first_cache = 1;
			}
		}
	}
	return 0;
}

void * identify_thread(void * arg)
{
#ifdef BATCH_DEDUP
	struct identify_arg * identify_arg;
	struct data_node * data_node;
	int * go_on;
	int len, mylen;
	struct node_pool * from;
	struct node_pool * to;
	struct node_pool * free;
	struct dedup_manager * dedup;
	int i;
	int ret;
	static int pre_unique = 0;
	static int chain_off = 0;
	static int unique_count = 0;
	static int dup_count = 0;
	static int new_chain = 1;
	struct disk_hash_node node;
	struct timeval start, end;

	identify_arg = (struct identify_arg *) arg;
	go_on = &(identify_arg->header.go_on);
	from = identify_arg->from;
	to = identify_arg->to;
	dedup = identify_arg->manager;

	while(*go_on)
	{
		data_node = get_data_node(from, &mylen);
#ifdef DEBUG
		gettimeofday(&start, NULL);
#endif
		data_node->calculate_type = IDENTIFY_THREAD;
		len = data_node->data_len;
		for(i = 0 ; i < len ; i ++)
		{
			ret = bloom_filter_lookup(dedup->bf, data_node->metadata[i].fingerprint);
			if(0 == ret)
			{
				after_identify(&data_node->metadata[i], data_node->data + data_node->offset[i], dedup, ret);
				pre_unique ++;
				if(pre_unique == PRE_UNIQUE_THREHOLD)
				{
					chain_off = 0;
					new_chain = 1;
					dedup->mem_hash.first_cache = 0;
				}
			}
			else
			{
				bf_hit ++;
				this_bf_hit ++;
				if(pre_unique <= PRE_UNIQUE_THREHOLD)
				{
					chain_off += pre_unique;
				}
				pre_unique = 0;
				if(1 == dedup->mem_hash.first_cache)
				{
					ret = lookup_in_cache(&dedup->cache, &data_node->metadata[i]);
					cache_times ++;
					this_cache_times ++;
					if(1 == ret)
					{
						this_first_cache_hit ++;
						first_cache_hit ++;
					}
				}
				else
				{
					ret = 0;
				}
				if(1 == ret)
				{
					after_identify(&data_node->metadata[i], data_node->data + data_node->offset[i], dedup, ret);
				}
				else
				{
					put_in_mem_hash(&data_node->metadata[i], data_node->data + data_node->offset[i], &dedup->mem_hash, &dedup->disk_hash, dedup, new_chain, chain_off, dedup);
					if(1 == new_chain)
					{
						new_chain = 0;
					}
				}
				chain_off ++;
				if(chain_off >= MAX_BEFORE_LEN)
				{
					chain_off = 0;
					new_chain = 1;
					dedup->mem_hash.first_cache = 0;
				}
			}
		}
		gettimeofday(&end, NULL);
		data_node->time_used = td(&start, &end);
		if(0 == mylen)
		{
			gettimeofday(&total_end, NULL);
		}
		put_data_node(to, data_node);
	}
#else
	struct identify_arg * identify_arg;
	struct data_node * data_node;
	int * go_on;
	int len;
	struct node_pool * from;
	struct node_pool * to;
	struct dedup_manager * manager;
	int i;

#ifdef DEBUG
	int mylen;
	struct timeval start, end;
#endif

	identify_arg = (struct identify_arg *) arg;
	go_on = &(identify_arg->header.go_on);
	from = identify_arg->from;
	to = identify_arg->to;
	manager = identify_arg->manager;

	while(*go_on)
	{
		data_node = get_data_node(from, &mylen);
#ifdef DEBUG
//		printf("in_identify_from buf %d\n", mylen);
		gettimeofday(&start, NULL);
#endif
		data_node->calculate_type = IDENTIFY_THREAD;
		len = data_node->data_len;
		for(i = 0 ; i < len ; i ++)
		{
			data_node->dup_sample[i] = is_dedup(&data_node->metadata[i], manager);
			after_identify(&data_node->metadata[i], data_node->data + data_node->offset[i], manager, data_node->dup_sample[i]);
		}
#ifdef DEBUG
		gettimeofday(&end, NULL);
		data_node->time_used = td(&start, &end);
#endif
		if(0 == mylen)
		{
			gettimeofday(&total_end, NULL);
		}
		put_data_node(to, data_node);
	}
#endif

	return NULL;
}

void * schedule_thread(void * arg)
{
	struct schedule_arg * schedule_arg;
	struct data_node * data_node;
	int * go_on;
	struct node_pool * from;
	struct node_pool * to_free;
	struct node_pool * to_iden_free;
	struct node_pool * to_compress;
	struct node_pool * to_hash;
	struct node_pool * to_identify;
	struct node_pool * to_store;

	schedule_arg = (struct schedule_arg *) arg;
	go_on = &(schedule_arg->header.go_on);
	from = schedule_arg->from;
	to_free = schedule_arg->free;
	to_compress = schedule_arg->to_compress;
	to_hash = schedule_arg->to_hash;
	to_identify = schedule_arg->to_identify;
	to_store = schedule_arg->to_store;
	to_iden_free = schedule_arg->to_iden_free;

#ifdef DEBUG
	int mylen;
#endif

	while(*go_on)
	{
		data_node = get_data_node(from, &mylen);
		switch(data_node->calculate_type)
		{
		case CHUNK_THREAD:
			chunk_time += data_node->time_used;
			put_data_node(to_hash, data_node);
//			put_data_node(to_free, data_node);
			break;
		case COMPRESS_THREAD:
			compress_time += data_node->time_used;
			put_data_node(from, data_node);
			break;
		case HASH_THREAD:
			hash_time += data_node->time_used;
			put_data_node(to_identify, data_node);
			break;
		case IDENTIFY_THREAD:
			identify_time += data_node->time_used;
			put_data_node(to_free, data_node);
			break;
		default:
			put_data_node(from, data_node);
			break;
		}
	}
	return NULL;
}

int pipe_dedup_init(struct pipe_dedup *p_d, char * name)
{
	int i;
	dedup_init(&p_d->manager, name);
	pool_init(&p_d->to_compress);
	pool_init(&p_d->to_hash);
	pool_init(&p_d->to_identify);
	pool_init(&p_d->to_store);
	pool_init(&p_d->to_schedule);
	pool_init(&p_d->free);

	chunk_pool_init(&p_d->from_rcv);
	chunk_pool_init(&p_d->to_rcv);

	for(i = 0 ; i < COMPRESS_THREAD_NUM ; i ++)
	{
		p_d->compress[i].from = &p_d->to_compress;
		p_d->compress[i].to = &p_d->to_schedule;
		p_d->compress[i].header.go_on = 1;
	}

	for(i = 0 ; i < HASH_THREAD_NUM ; i ++)
	{
#ifdef FINGER_DEBUG
		p_d->hash[i].from = &p_d->free;
#else
		p_d->hash[i].from = &p_d->to_hash;
#endif
		p_d->hash[i].to = &p_d->to_schedule;
		p_d->hash[i].header.go_on = 1;
	}

	p_d->gpu_hash.from = &p_d->to_hash;
	p_d->gpu_hash.to = &p_d->to_schedule;
	p_d->gpu_hash.header.go_on = 1;

	for(i = 0 ; i < CHUNK_TRHEAD_NUM ; i ++)
	{
		p_d->chunk[i].from = &p_d->free;
		p_d->chunk[i].to = &p_d->to_schedule;
		p_d->chunk[i].from_rcv = &p_d->from_rcv;
		p_d->chunk[i].to_rcv = &p_d->to_rcv;
		p_d->chunk[i].header.go_on = 1;

	}

	p_d->identify.from = &p_d->to_identify;
	p_d->identify.to = &p_d->to_schedule;

	p_d->schedule.from = &p_d->to_schedule;
	p_d->schedule.free = &p_d->free;
	p_d->schedule.to_compress = &p_d->to_compress;
	p_d->schedule.to_hash = &p_d->to_hash;
	p_d->schedule.to_identify = &p_d->to_identify;
	p_d->schedule.to_store = &p_d->to_store;

	p_d->identify.manager = &p_d->manager;

	p_d->schedule.header.go_on = 1;
	p_d->identify.header.go_on = 1;


	for(i = 0;  i < NODE_NUM ; i ++)
	{
		put_data_node(&p_d->free, &p_d->node[i]);
	}
	for(i = 0 ; i < CHUNK_NODE_NUM ; i ++)
	{
		put_chunk_data_node(&p_d->to_rcv, &p_d->chunk_node[i]);
	}

	return 0;
}

int start_threads(struct pipe_dedup * p_d)
{
	int i;
	for(i = 0 ; i < CHUNK_TRHEAD_NUM ; i ++)
	{
#ifdef FIXED_CHUNK
		pthread_create(&p_d->chunk[i].header.id, NULL, fixed_chunk_thread, &p_d->chunk[i]);
#else
		pthread_create(&p_d->chunk[i].header.id, NULL, chunk_thread, &p_d->chunk[i]);
#endif
	}
	for(i = 0 ; i < HASH_THREAD_NUM ; i ++)
	{
		pthread_create(&p_d->hash[i].header.id, NULL, hash_thread, &p_d->hash[i]);
	}
	pthread_create(&p_d->gpu_hash.header.id, NULL, gpu_hash_thread, &p_d->gpu_hash);
	pthread_create(&p_d->identify.header.id, NULL, identify_thread, &p_d->identify);
	pthread_create(&p_d->schedule.header.id, NULL, schedule_thread, &p_d->schedule);
	return 0;
}

#endif /* THREAD_H_ */
