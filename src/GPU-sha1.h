/*
 * GPU-sha1.h
 *
 *  Created on: Oct 11, 2013
 *      Author: mjw
 */

#ifndef GPU_SHA1_H_
#define GPU_SHA1_H_


extern void GPU_sha1_init(unsigned int max_chunk_len,unsigned int num);
extern void GPU_sha1(unsigned char *input,unsigned char *output,unsigned int *offset,unsigned int num,unsigned int len);
extern void GPU_sha1_destroy(void);


#endif /* GPU_SHA1_H_ */
