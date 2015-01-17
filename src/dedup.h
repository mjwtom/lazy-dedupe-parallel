/*
 * dedup.h
 *
 *  Created on: 2012-7-12
 *      Author: BadBoy
 */

#ifndef DEDUP_H_
#define DEDUP_H_

#include "config.h"
#include "file.h"
#include "disk-hash.h"
#include "cache.h"
#include "data.h"
#include "metadata.h"
#include "sha1.h"
#include "storage-manager.h"
#include "bloom-filter.h"
#include "rabin-hash.h"
#ifdef COMPRESS
#include "lzjb.h"
#endif

struct dedup_manager
{
	//the file to store all the data produced by the de-dudplication program
	struct storage_manager manager;

	//the meta-data segment in memory, when it's full, it will be written into disk
	struct mtdata_seg mt_seg;

	//the data segment in memory, when it's full, it will be written into disk
	struct data_seg dt_seg;

	//the data structure to handle the current file to be de-dupliated
	struct file_seg f_seg;
	//the cache for finger-print lookup
	struct cache cache;
	//the disk hash for finger-print lookup
	struct disk_hash disk_hash;

	//the chunk buffer to hold the chunk data
	char chunk_buf[MAX_CHUNK_LEN];

#ifdef COMPRESS
	char compress_buf[MAX_COMPRESS_LEN];
#endif

	//the mem_hash for finger-print lookup
	struct mem_hash mem_hash;

	//the bloom-filter
	char * bf;//char bf[BF_LEN];

	//the struct to store metadata read from disk to cache
	struct metadata mtdata[(MAX_BEFORE_LEN * 2) * FH_LEN];
	struct metadata lastmtdata[MAX_BEFORE_LEN * 2];

	struct merge_load_arg merge_load;
};


int dedup_init(struct dedup_manager * dedup, char * data_file_name)
{
	if(NULL == dedup)
		return -1;
	init_power(WIN_LEN);
	storage_manager_init(&(dedup->manager), data_file_name);
	dedup->f_seg.manager = &(dedup->manager);
	dedup->disk_hash.manager = &(dedup->manager);
	dedup->dt_seg.manager = &(dedup->manager);
	dedup->mt_seg.manager = &(dedup->manager);
	if(file_init(&(dedup->f_seg)) < 0)
		return -1;
	if(cache_init(&(dedup->cache)) < 0)
		return -1;
	if(disk_hash_init(&(dedup->disk_hash)) < 0)
		return -1;
	if(mem_hash_init(&(dedup->mem_hash)) < 0)
		return -1;
	dedup->bf = malloc(BF_LEN);
	if(NULL == dedup->bf)
	{
		printf("bf too big");
		exit(0);
	}
	if(bloomfilter_init(dedup->bf) < 0)
		return -1;
	if(data_init(&(dedup->dt_seg)) < 0)
		return -1;
	if(metadata_init(&(dedup->mt_seg)) < 0)
		return -1;
	return 0;
}


int load_cache(struct mtdata_seg * seg, struct metadata * mtdata, uint64_t offset, int before_len, int after_len, struct cache * cache)
{
	int len;
	len = get_inseg_metadata(seg, mtdata, offset, before_len, after_len);
	add_metadata_in_cache(mtdata, len, cache);
	return 0;
}

int destroy(struct dedup_manager * dedup)
{
	fclose(dedup->manager.f);
	return 0;
}

int is_dedup(struct metadata * mtdata, struct dedup_manager * dedup)
{
	int ret = 0;
	struct disk_hash_node node;
	struct timeval start, end;
	gettimeofday(&start, NULL);
	ret = bloom_filter_lookup(dedup->bf, (unsigned int *) mtdata->fingerprint);
	gettimeofday(&end, NULL);
	bf_time += td(&start, &end);
	if(1 == ret)
	{
		gettimeofday(&start, NULL);
		cache_time ++;
		ret = lookup_in_cache(&dedup->cache, mtdata);
		cache_times ++;
		gettimeofday(&end, NULL);
		cache_time += td(&start, &end);
		if(0 == ret)
		{
			ret = lookup_fingerprint_in_disk_hash(&dedup->disk_hash, mtdata->fingerprint, &node);
			if(1 == ret)
			{
				mtdata->offset = node.data_offset;
				mtdata->len = node.data_len;
				gettimeofday(&start, NULL);
				load_cache(&(dedup->mt_seg), dedup->mtdata, node.mtdata_offset, 0, MAX_BEFORE_LEN, &dedup->cache);
				gettimeofday(&end, NULL);
				load_cache_time += td(&start, &end);
			}
		}
		else
		{
			first_cache_hit ++;
		}
	}
	return ret;
}

#endif /* DEDUP_H_ */
