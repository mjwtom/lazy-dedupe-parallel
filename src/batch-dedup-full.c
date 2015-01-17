#include<stdio.h>
#include<stdlib.h>
#include<stdint.h>
#include<string.h>
#include<time.h>
#include<errno.h>
#include<unistd.h>
#include<dirent.h>
#include<netinet/in.h>
#include<sys/stat.h>
#include<sys/types.h>
#include<sys/socket.h>
#include<sys/wait.h>


#include "file.h"
#include "thread.h"
#include "chunk.h"

char t[100];
int client_fd;

#define RFBLEN 20
char * read_file_buf[RFBLEN];
#define READ_FILE_BUF_SIZE (1024 * 1024 * 1024)

#define DEBUGFILE "/home/mjw/infofile.txt"

int filedebug(char *str)
{
	FILE *poutfile;
	if((poutfile = fopen(DEBUGFILE,"a+")) == NULL){
		printf("file open failed!\n");
		return -1;
		}
	fprintf(poutfile,str);
	fclose(poutfile);
	return 0;
}


struct file_head
{
	char name[MAX_NAME_LEN];
	int mtype;
	int msize;
};

int backup_file1(char * name, struct pipe_dedup * p_d)
{
	struct file_head head;
	struct stat s;
	uint64_t read_len = 0;
	FILE * fp;
	char read_buf[READ_BUF_SIZE];
	int ret;
	bzero(&head, sizeof(struct file_head));
	strncpy(head.name, name, MAX_NAME_LEN);
	if(lstat(name, &s) < 0)
	{
		printf("%s does not exist\r\n", name);
	}
	head.mtype = REG_FILE;
	head.msize = s.st_size;
//	to_chunk_buf(&p_d->chunk, (char *)&head, sizeof(struct file_head));
	if((fp = fopen(name,"r"))  == NULL )
	{
		fprintf(stderr,"Fail to open the file: %s\n", strerror(errno));
		return 0;
	}
	read_len = 0;
	while(read_len < s.st_size)
	{
		ret = fread(read_buf, 1, READ_BUF_SIZE, fp);
		to_chunk_buf(p_d->chunk, read_buf, ret);
		read_len += ret;
		read_data_len += ret;
	}
	fclose(fp);
	return 0;
}

int backup_file(char * name, struct pipe_dedup * p_d)
{
	struct stat s;
	uint64_t read_len = 0;
	char * read_buf;
	FILE * fp;
	size_t ret[RFBLEN];
	int i;
	int read_counter;
	if(lstat(name, &s) < 0)
	{
		printf("%s does not exist\r\n", name);
	}
	if((fp = fopen(name,"r"))  == NULL )
	{
		fprintf(stderr,"Fail to open the file: %s\n", strerror(errno));
		return 0;
	}

	while(read_len < s.st_size)
	{
		read_counter = 0;
		for(i = 0 ; i < RFBLEN ; i ++)
		{
			ret[i] = fread(read_file_buf[i], 1, READ_FILE_BUF_SIZE, fp);
			read_len += ret[i];
			data_len += ret[i];
			read_counter ++;
			if(ret[i] != READ_FILE_BUF_SIZE)
			{	i ++;
				break;
			}
		}
		gettimeofday(&total_start, NULL);
		for(i = 0 ; i < read_counter ; i ++)
		{
			read_buf = read_file_buf[i];
			while(ret[i] > 0)
			{
				if(ret[i] >= READ_BUF_SIZE)
				{
					to_chunk_buf(p_d->chunk, read_buf, READ_BUF_SIZE);
					read_buf += READ_BUF_SIZE;
					ret[i] -= READ_BUF_SIZE;
				}
				else
				{
					to_chunk_buf(p_d->chunk, read_buf, ret[i]);
					read_buf += ret[i];
					ret[i] -= ret[i];
				}
			}
		}
		sleep(60);
		chunk_time = td(&total_start, &total_end);
		printf("compress time %f\n", compress_time / 1000000.0);
		printf("hash time %f\n", hash_time / 1000000.0);
		printf("identify time %f\n", identify_time / 1000000.0);
		printf("store time %f\n", store_time / 1000000.0);
		printf("chunk time %f\n", chunk_time / 1000000.0);
		printf("chunk throughput %f\n", data_len / (1.0 *1024 * 1024) / chunk_time * 1000000.0);
		printf("data len %f MB\n", data_len / 1000000.0);
		printf("compress len %f MB\n", compress_len / 1000000.0);
		printf("unique len %f MB\n", unique_len / 1000000.0);
		printf("dup len %f MB\n", dup_len / 1000000.0);
		puts("");

		compress_time = 0;
		hash_time = 0;
		identify_time = 0;
		store_time = 0;
		chunk_time = 0;

//		return 0;
	}
	fclose(fp);
	return 0;
}

int backup_dir(char * name, struct pipe_dedup * p_d)
{
	DIR	*source = NULL;
	struct dirent *ent = NULL;
	char child_name[MAX_NAME_LEN];
	struct file_head head;
	bzero(&head, sizeof(struct file_head));
	strncpy(head.name, name, MAX_NAME_LEN);
	head.mtype = DIR_FILE;
//	to_chunk_buf(&p_d->chunk, (char *)&head, sizeof(struct file_head));
	source = opendir(name);
	if(source == NULL)
	{
		perror("Fail to opendir\n");
		return 0;
	}
	while((ent = readdir(source)) != NULL)
	{
		if(strcmp(ent->d_name,"..")  != 0 && strcmp(ent->d_name, ".") != 0)
		{
			strcpy(child_name,"\0");
			strcat(child_name, name);
			strcat(child_name,"/");
			strcat(child_name,ent->d_name);
			if(ent->d_type == 4)
				backup_dir(child_name, p_d);
			else if(ent->d_type == 8)
				backup_file(child_name, p_d);
		}
	}
	closedir(source);
	return 0;
}

int backup(char* name, struct pipe_dedup * p_d)
{
	static uint64_t mt_counter = 0;
	struct file backup_root;
	struct stat s;

	if(lstat(name, &s) < 0)
	{
		printf("lstat error for %s\r\n", name);
		return -1;
	}

	backup_root.chunk_num = 0;
//	create_file(&p_d->manager, mt_counter);
	strncpy(backup_root.name, name, 100);

	if(S_ISDIR(s.st_mode))
	{
		printf("%s is a directory\r\n", name);
		backup_dir(name, p_d);
	}
	else
	{
		if(S_ISREG(s.st_mode))
		{
			printf("%s is a regular file\r\n", name);
			backup_file(name, p_d);
		}
	}

//	ret = chunk(BUF, 0, 1, 1, &p_d->chunk);
//	mt_counter += ret;
//	backup_file.chunk_num += ret;
//	end_file(&p_d->manager, backup_root);

	return 0;
}

int main(int argc, char ** argv) {

	//prepare the threads for data deduplication
	struct pipe_dedup * p_d;
	char info[200];
	int i;
	argc--;
	argv++; //skip the program name itself

#ifdef DEBUG
	compress_time = 0;
	hash_time = 0;
	identify_time = 0;
	store_time = 0;
	data_len = 0;
	chunk_time = 0;
	compress_len = 0;
	unique_len = 0;
	dup_len = 0;

	printf("size of structure pipe_dedup %dMB\n", sizeof(struct pipe_dedup) / 1024 /1024);
	printf("size of struct dedup_manager %dMB\n", sizeof(struct dedup_manager) / 1024 /1024);
	printf("size of struct schedule_arg %d\n", sizeof(struct schedule_arg));
	printf("size of struct encrypt_arg %d\n", sizeof(struct encrypt_arg));
	printf("size of struct hash_arg %d\n", sizeof(struct hash_arg));
	printf("size of struct compress_arg %d\n", sizeof(struct compress_arg));
	printf("size of struct identify_arg %d\n", sizeof(struct identify_arg));
	printf("size of struct compress_arg %d\n", sizeof(struct compress_arg));
	printf("size of struct chunk_arg %d\n", sizeof(struct chunk_arg));
	printf("size of struct data_node %dMB\n", sizeof(struct data_node)/1024/1024);
	printf("size of struct chunk_node %dMB\n", sizeof(struct chunk_node)/1024/1024);
	puts("");
	printf("size of struct storage_manager %d\n", sizeof(struct storage_manager));
	printf("size of struct mtdata_seg %dMB\n", sizeof(struct mtdata_seg)/1024/1024);
	printf("size of struct data_seg %dMB\n", sizeof(struct data_seg)/1024/1024);
	printf("size of struct file_seg %dMB\n", sizeof(struct file_seg)/1024/1024);
	printf("size of struct cache %dMB\n", sizeof(struct cache)/1024/1024);
	printf("size of struct disk_hash %dMB\n", sizeof(struct disk_hash)/1024/1024);

	sprintf(info, "size of structure pipe_dedup %dMB\n", sizeof(struct pipe_dedup) / 1024 /1024);
	filedebug(info);
	sprintf(info, "size of struct dedup_manager %dMB\n", sizeof(struct dedup_manager) / 1024 /1024);
	filedebug(info);
	sprintf(info, "size of struct schedule_arg %d\n", sizeof(struct schedule_arg));
	filedebug(info);
	sprintf(info, "size of struct encrypt_arg %d\n", sizeof(struct encrypt_arg));
	filedebug(info);
	sprintf(info, "size of struct hash_arg %d\n", sizeof(struct hash_arg));
	filedebug(info);
	sprintf(info, "size of struct compress_arg %d\n", sizeof(struct compress_arg));
	filedebug(info);
	sprintf(info, "size of struct identify_arg %d\n", sizeof(struct identify_arg));
	filedebug(info);
	sprintf(info, "size of struct compress_arg %d\n", sizeof(struct compress_arg));
	filedebug(info);
	sprintf(info, "size of struct chunk_arg %d\n", sizeof(struct chunk_arg));
	filedebug(info);
	sprintf(info, "size of struct data_node %dMB\n", sizeof(struct data_node)/1024/1024);
	filedebug(info);
	sprintf(info, "size of struct chunk_node %dMB\n", sizeof(struct chunk_node)/1024/1024);
	filedebug(info);
	sprintf(info, "\n");
	filedebug(info);
	sprintf(info, "size of struct storage_manager %d\n", sizeof(struct storage_manager));
	filedebug(info);
	sprintf(info, "size of struct mtdata_seg %dMB\n", sizeof(struct mtdata_seg)/1024/1024);
	filedebug(info);
	sprintf(info, "size of struct data_seg %dMB\n", sizeof(struct data_seg)/1024/1024);
	filedebug(info);
	sprintf(info, "size of struct file_seg %dMB\n", sizeof(struct file_seg)/1024/1024);
	filedebug(info);
	sprintf(info, "size of struct cache %dMB\n", sizeof(struct cache)/1024/1024);
	filedebug(info);
	sprintf(info, "size of struct disk_hash %dMB\n", sizeof(struct disk_hash)/1024/1024);
	filedebug(info);
	//printf("size of struct chunk_node %d\n", sizeof(struct chunk_node));
#endif


	p_d = malloc(sizeof(struct pipe_dedup));
	if(0 == p_d)
	{
		puts("can't malloc so big memblock");
		return -1;
	}
	pipe_dedup_init(p_d, *argv);
#ifdef FINGER_DEBUG
	argv ++;
	argc --;
	finger_file = fopen(*argv, "rb");
	init_mylock(&finger_status);
	end_finger_file = 0;
#endif
	argv ++;argc --; //skip the metadatafile
	start_threads(p_d);
#ifdef FINGER_DEBUG
	while(1)
	{
		mylock(&finger_status);
		if(0 == end_finger_file)
		{
			myunlock(&finger_status);
			sleep(10);
		}
		else
		{
			myunlock(&finger_status);
			break;
		}
	}
#else
	for(i = 0 ; i < RFBLEN ; i ++)
	{
		read_file_buf[i] = malloc(READ_FILE_BUF_SIZE);
		if(NULL == read_file_buf[i])
		{
			puts("can not allocate so much memory");
			exit(0);
		}
	}

	while(argc > 0)
	{
		backup(*argv, p_d);
		argv ++; argc --;
	}
#endif

	sleep(10);

	chunk_time = td(&total_start, &total_end);

	printf("compress time %f\n", compress_time / 1000000.0);
	printf("hash time %f\n", hash_time / 1000000.0);
	printf("identify time %f\n", identify_time / 1000000.0);
	printf("store time %f\n", store_time / 1000000.0);
	printf("chunk time %f\n", chunk_time / 1000000.0);
	printf("chunk throughput %f\n", data_len / (1.0 *1024 * 1024) / chunk_time * 1000000.0);
	puts("");
	printf("data len %f MB\n", data_len / 1000000.0);
	printf("compress len %f MB\n", compress_len / 1000000.0);
	printf("unique len %f MB\n", unique_len / 1000000.0);
	printf("dup len %f MB\n", dup_len / 1000000.0);

	sprintf(info, "compress time %f\n", compress_time / 1000000.0);
	filedebug(info);
	sprintf(info, "hash time %f\n", hash_time / 1000000.0);
	filedebug(info);
	sprintf(info, "identify time %f\n", identify_time / 1000000.0);
	filedebug(info);
	sprintf(info, "store time %f\n", store_time / 1000000.0);
	filedebug(info);
	sprintf(info, "chunk time %f\n", chunk_time / 1000000.0);
	filedebug(info);
	sprintf(info, "\n");
	filedebug(info);
	sprintf(info, "data len %f MB\n", data_len / 1000000.0);
	filedebug(info);
	sprintf(info, "compress len %f MB\n", compress_len / 1000000.0);
	filedebug(info);
	sprintf(info, "unique len %f MB\n", unique_len / 1000000.0);
	filedebug(info);
	sprintf(info, "dup len %f MB\n", dup_len / 1000000.0);
	filedebug(info);

	printf("bloom filter time is %f\n", bf_time/1000000.0);

	sprintf(info, "cache time is %f\n", cache_time / 1000000.0);
	filedebug(info);
	sprintf(info, "load_cache_time is %f\n", load_cache_time / 1000000.0);
	filedebug(info);
	sprintf(info, "bloom filter time is %f\n", bf_time/1000000.0);
	filedebug(info);
	return 0;
}
