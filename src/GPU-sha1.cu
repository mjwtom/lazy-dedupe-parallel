#include<cstdio>
#include<stdio.h>
#include<time.h>
#include<string.h>
#include<unistd.h>
#include<stdlib.h>


unsigned char *gpu_input_data_s,*gpu_output_data_s;
unsigned int *gpu_offset;
#define FINGERPRINT_LEN 20
#define MAX_CHUNK_LEN (16384)

#define MJW

#ifdef MJW

typedef struct
{
    unsigned long total[2];     /*!< number of bytes processed  */
    unsigned long state[5];     /*!< intermediate digest state  */
    unsigned char buffer[64];   /*!< data block being processed */

    unsigned char ipad[64];     /*!< HMAC: inner padding        */
    unsigned char opad[64];     /*!< HMAC: outer padding        */
}
sha1_context;

#ifndef GET_ULONG_BE
#define GET_ULONG_BE(n,b,i)                             \
{                                                       \
    (n) = ( (unsigned long) (b)[(i)    ] << 24 )        \
        | ( (unsigned long) (b)[(i) + 1] << 16 )        \
        | ( (unsigned long) (b)[(i) + 2] <<  8 )        \
        | ( (unsigned long) (b)[(i) + 3]       );       \
}
#endif

#ifndef PUT_ULONG_BE
#define PUT_ULONG_BE(n,b,i)                             \
{                                                       \
    (b)[(i)    ] = (unsigned char) ( (n) >> 24 );       \
    (b)[(i) + 1] = (unsigned char) ( (n) >> 16 );       \
    (b)[(i) + 2] = (unsigned char) ( (n) >>  8 );       \
    (b)[(i) + 3] = (unsigned char) ( (n)       );       \
}
#endif
/*
 * SHA-1 context setup
 */
__device__ void sha1_starts( sha1_context *ctx )
{
    ctx->total[0] = 0;
    ctx->total[1] = 0;

    ctx->state[0] = 0x67452301;
    ctx->state[1] = 0xEFCDAB89;
    ctx->state[2] = 0x98BADCFE;
    ctx->state[3] = 0x10325476;
    ctx->state[4] = 0xC3D2E1F0;
}

__device__ static void sha1_process( sha1_context *ctx, unsigned char data[64] )
{
    unsigned long temp, W[16], A, B, C, D, E;

    GET_ULONG_BE( W[ 0], data,  0 );
    GET_ULONG_BE( W[ 1], data,  4 );
    GET_ULONG_BE( W[ 2], data,  8 );
    GET_ULONG_BE( W[ 3], data, 12 );
    GET_ULONG_BE( W[ 4], data, 16 );
    GET_ULONG_BE( W[ 5], data, 20 );
    GET_ULONG_BE( W[ 6], data, 24 );
    GET_ULONG_BE( W[ 7], data, 28 );
    GET_ULONG_BE( W[ 8], data, 32 );
    GET_ULONG_BE( W[ 9], data, 36 );
    GET_ULONG_BE( W[10], data, 40 );
    GET_ULONG_BE( W[11], data, 44 );
    GET_ULONG_BE( W[12], data, 48 );
    GET_ULONG_BE( W[13], data, 52 );
    GET_ULONG_BE( W[14], data, 56 );
    GET_ULONG_BE( W[15], data, 60 );

#define S(x,n) ((x << n) | ((x & 0xFFFFFFFF) >> (32 - n)))

#define R(t)                                            \
(                                                       \
    temp = W[(t -  3) & 0x0F] ^ W[(t - 8) & 0x0F] ^     \
           W[(t - 14) & 0x0F] ^ W[ t      & 0x0F],      \
    ( W[t & 0x0F] = S(temp,1) )                         \
)

#define P(a,b,c,d,e,x)                                  \
{                                                       \
    e += S(a,5) + F(b,c,d) + K + x; b = S(b,30);        \
}

    A = ctx->state[0];
    B = ctx->state[1];
    C = ctx->state[2];
    D = ctx->state[3];
    E = ctx->state[4];

#define F(x,y,z) (z ^ (x & (y ^ z)))
#define K 0x5A827999

    P( A, B, C, D, E, W[0]  );
    P( E, A, B, C, D, W[1]  );
    P( D, E, A, B, C, W[2]  );
    P( C, D, E, A, B, W[3]  );
    P( B, C, D, E, A, W[4]  );
    P( A, B, C, D, E, W[5]  );
    P( E, A, B, C, D, W[6]  );
    P( D, E, A, B, C, W[7]  );
    P( C, D, E, A, B, W[8]  );
    P( B, C, D, E, A, W[9]  );
    P( A, B, C, D, E, W[10] );
    P( E, A, B, C, D, W[11] );
    P( D, E, A, B, C, W[12] );
    P( C, D, E, A, B, W[13] );
    P( B, C, D, E, A, W[14] );
    P( A, B, C, D, E, W[15] );
    P( E, A, B, C, D, R(16) );
    P( D, E, A, B, C, R(17) );
    P( C, D, E, A, B, R(18) );
    P( B, C, D, E, A, R(19) );

#undef K
#undef F

#define F(x,y,z) (x ^ y ^ z)
#define K 0x6ED9EBA1

    P( A, B, C, D, E, R(20) );
    P( E, A, B, C, D, R(21) );
    P( D, E, A, B, C, R(22) );
    P( C, D, E, A, B, R(23) );
    P( B, C, D, E, A, R(24) );
    P( A, B, C, D, E, R(25) );
    P( E, A, B, C, D, R(26) );
    P( D, E, A, B, C, R(27) );
    P( C, D, E, A, B, R(28) );
    P( B, C, D, E, A, R(29) );
    P( A, B, C, D, E, R(30) );
    P( E, A, B, C, D, R(31) );
    P( D, E, A, B, C, R(32) );
    P( C, D, E, A, B, R(33) );
    P( B, C, D, E, A, R(34) );
    P( A, B, C, D, E, R(35) );
    P( E, A, B, C, D, R(36) );
    P( D, E, A, B, C, R(37) );
    P( C, D, E, A, B, R(38) );
    P( B, C, D, E, A, R(39) );

#undef K
#undef F

#define F(x,y,z) ((x & y) | (z & (x | y)))
#define K 0x8F1BBCDC

    P( A, B, C, D, E, R(40) );
    P( E, A, B, C, D, R(41) );
    P( D, E, A, B, C, R(42) );
    P( C, D, E, A, B, R(43) );
    P( B, C, D, E, A, R(44) );
    P( A, B, C, D, E, R(45) );
    P( E, A, B, C, D, R(46) );
    P( D, E, A, B, C, R(47) );
    P( C, D, E, A, B, R(48) );
    P( B, C, D, E, A, R(49) );
    P( A, B, C, D, E, R(50) );
    P( E, A, B, C, D, R(51) );
    P( D, E, A, B, C, R(52) );
    P( C, D, E, A, B, R(53) );
    P( B, C, D, E, A, R(54) );
    P( A, B, C, D, E, R(55) );
    P( E, A, B, C, D, R(56) );
    P( D, E, A, B, C, R(57) );
    P( C, D, E, A, B, R(58) );
    P( B, C, D, E, A, R(59) );

#undef K
#undef F

#define F(x,y,z) (x ^ y ^ z)
#define K 0xCA62C1D6

    P( A, B, C, D, E, R(60) );
    P( E, A, B, C, D, R(61) );
    P( D, E, A, B, C, R(62) );
    P( C, D, E, A, B, R(63) );
    P( B, C, D, E, A, R(64) );
    P( A, B, C, D, E, R(65) );
    P( E, A, B, C, D, R(66) );
    P( D, E, A, B, C, R(67) );
    P( C, D, E, A, B, R(68) );
    P( B, C, D, E, A, R(69) );
    P( A, B, C, D, E, R(70) );
    P( E, A, B, C, D, R(71) );
    P( D, E, A, B, C, R(72) );
    P( C, D, E, A, B, R(73) );
    P( B, C, D, E, A, R(74) );
    P( A, B, C, D, E, R(75) );
    P( E, A, B, C, D, R(76) );
    P( D, E, A, B, C, R(77) );
    P( C, D, E, A, B, R(78) );
    P( B, C, D, E, A, R(79) );

#undef K
#undef F

    ctx->state[0] += A;
    ctx->state[1] += B;
    ctx->state[2] += C;
    ctx->state[3] += D;
    ctx->state[4] += E;
}

/*
 * SHA-1 process buffer
 */
__device__ void sha1_update( sha1_context *ctx, unsigned char *input, int ilen )
{
    int fill;
    unsigned long left;

    if( ilen <= 0 )
        return;

    left = ctx->total[0] & 0x3F;
    fill = 64 - left;

    ctx->total[0] += ilen;
    ctx->total[0] &= 0xFFFFFFFF;

    if( ctx->total[0] < (unsigned long) ilen )
        ctx->total[1]++;

    if( left && ilen >= fill )
    {
        memcpy( (void *) (ctx->buffer + left),
                (void *) input, fill );
        sha1_process( ctx, ctx->buffer );
        input += fill;
        ilen  -= fill;
        left = 0;
    }

    while( ilen >= 64 )
    {
        sha1_process( ctx, input );
        input += 64;
        ilen  -= 64;
    }

    if( ilen > 0 )
    {
        memcpy( (void *) (ctx->buffer + left),
                (void *) input, ilen );
    }
}

__device__ static const unsigned char sha1_padding[64] =
{
 0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
};

/*
 * SHA-1 final digest
 */
__device__ void sha1_finish( sha1_context *ctx, unsigned char output[20])
{
    unsigned long last, padn;
    unsigned long high, low;
    unsigned char msglen[8];

    high = ( ctx->total[0] >> 29 )
         | ( ctx->total[1] <<  3 );
    low  = ( ctx->total[0] <<  3 );

    PUT_ULONG_BE( high, msglen, 0 );
    PUT_ULONG_BE( low,  msglen, 4 );

    last = ctx->total[0] & 0x3F;
    padn = ( last < 56 ) ? ( 56 - last ) : ( 120 - last );

    sha1_update( ctx, (unsigned char *) sha1_padding, padn );
    sha1_update( ctx, msglen, 8 );

    PUT_ULONG_BE( ctx->state[0], output,  0 );
    PUT_ULONG_BE( ctx->state[1], output,  4 );
    PUT_ULONG_BE( ctx->state[2], output,  8 );
    PUT_ULONG_BE( ctx->state[3], output, 12 );
    PUT_ULONG_BE( ctx->state[4], output, 16 );
}

/*
 * output = SHA-1( input buffer )
 */
__device__ void sha1( unsigned char *input, int ilen, unsigned char *output )
{
    sha1_context ctx;
//    printf("hello");
    sha1_starts( &ctx );
    sha1_update( &ctx, input, ilen );
    sha1_finish( &ctx, output);
}

__global__ void sha1_kernel(unsigned char * input, unsigned char* output, unsigned int * offset, unsigned int num)
{
	const unsigned int index = blockIdx.x * blockDim.x + threadIdx.x;
	if(index < num)
	{
		sha1( input + offset[index], offset[index+1]-offset[index], output + index * FINGERPRINT_LEN);
	}

}

#else

#define FROM_BIG_ENDIAN(v)                                          \
 ((v & 0xff) << 24) | ((v & 0xff00) << 8) | ((v & 0xff0000) >> 8) |  \
		((v & 0xff000000) >> 24)                              \

#define LEFTROL(v, n)  (v << n) | (v >> (32 - n))

__device__ void GPU_sha1_kernel(unsigned char* data_tmp, unsigned int length_tmp,  unsigned int* md)
{

	unsigned int words[80];
	unsigned int H0 = 0x67452301,	H1 = 0xEFCDAB89, H2 = 0x98BADCFE, H3 = 0x10325476, H4 = 0xC3D2E1F0;
	unsigned int a, b, c, d, e, f, k, temp, temp2;
	unsigned int i, j;

	unsigned char add_data[MAX_CHUNK_LEN + 128];
	unsigned int kk;
	unsigned int tmp;
	unsigned long long long_tmp;
	memcpy(add_data, data_tmp, length_tmp);
	kk = length_tmp;
	if(length_tmp%64<56)
	{
		add_data[kk++]=0x80;
		int t=length_tmp%64+1;
		for(;t<56;t++)
		{
			add_data[kk++]=0x00;
		}
		tmp=length_tmp-(length_tmp%64)+64;
	}else if(length_tmp%64>56)
	{
		add_data[kk++]=0x80;
		int t=length_tmp%64+1;
		for(;t<64;t++)
		{
			add_data[kk++]=0x00;
		}
		for(t=0;t<56;t++)
		{
			add_data[kk++]=0x00;
		}
		tmp=length_tmp-(length_tmp%64)+128;
	}
	long_tmp = tmp;
	add_data[tmp-8]=(long_tmp & 0xFF00000000000000) >> 56;
	add_data[tmp-7]=(long_tmp & 0x00FF000000000000) >> 48;
	add_data[tmp-6]=(long_tmp & 0x0000FF0000000000) >> 40;
	add_data[tmp-5]=(long_tmp & 0x000000FF00000000) >> 32;
	add_data[tmp-4]=(long_tmp & 0x00000000FF000000) >> 24;
	add_data[tmp-3]=(long_tmp & 0x0000000000FF0000) >> 16;
	add_data[tmp-2]=(long_tmp & 0x000000000000FF00) >> 8;
	add_data[tmp-1]=(long_tmp & 0x00000000000000FF);

	unsigned int *data=(unsigned int*)add_data;
	unsigned int dataLen=tmp;

	for(j = 0; j < dataLen; j += 64)
	{
		a = H0;
		b = H1;
		c = H2;
		d = H3;
		e = H4;

		for (i=0; i<16; i++)
		{
			temp = *(( unsigned int*)(data + j/4+i));
			words[i] = FROM_BIG_ENDIAN(temp);

			f = (b & c) | ((~b) & d);
			k = 0x5A827999;
			temp = LEFTROL(a, 5);
			temp2 = f + e + k + words[i];
			temp = temp +temp2;
			e = d;
			d = c;
			c = LEFTROL(b, 30);
			b = a;
			a = temp;
		}


		for (i=16; i<20; i++)
		{
			temp = (words[i - 3] ^ words[i - 8] ^ words[i - 14] ^ words[i - 16]);
			words[i] = LEFTROL(temp, 1);
			f = (b & c) | ((~b) & d);
			temp = LEFTROL(a, 5);
			temp2 = f + e + k  + words[i];
			temp = temp + temp2;
		    e = d;
			d = c;
			c = LEFTROL(b, 30);
			b = a;
			a = temp;
		}

		for (i=20; i<40; i++)
		{
			temp = words[i - 3] ^ words[i - 8] ^ words[i - 14] ^ words[i - 16];
			words[i] = LEFTROL(temp, 1);
			f=b ^ c ^ d;
			k= 0x6ED9EBA1;
			temp = LEFTROL(a, 5);
			temp2 = f + e + k + words[i];
			temp = temp + temp2;
			e = d;
			d = c;
			c = LEFTROL(b, 30);
			b = a;
			a = temp;
		}

		for (i=40; i<60; i++)
		{
			temp =  (words[i - 3] ^ words[i - 8] ^ words[i - 14] ^ words[i - 16]);
		    words[i] = LEFTROL(temp, 1);
			f = (b & c) | (b & d) | (c & d);
			k = 0x8F1BBCDC;
			temp = LEFTROL(a, 5);
			temp2 = f + e + k+ words[i];
			temp = temp + temp2;
			e = d;
			d = c;
			c = LEFTROL(b, 30);
			b = a;
			a = temp;
		}

		for (i=60; i<80; i++)
		{
			temp = (words[i - 3] ^ words[i - 8] ^ words[i - 14] ^ words[i - 16]);
		    words[i] = LEFTROL(temp, 1);
			f = b ^ c ^ d;
			k = 0xCA62C1D6;
			temp = LEFTROL(a, 5);
			temp2 = f + e + k + words[i];
			temp = temp + temp2;
			e = d;
			d = c;
			c = LEFTROL(b, 30);
			b = a;
			a = temp;
		}

		H0 += a;
		H1 += b;
		H2 += c;
		H3 += d;
		H4 += e;
	}

	a = H0;
	b = H1;
	c = H2;
	d = H3;
	e = H4;

	words[0] = FROM_BIG_ENDIAN(128);
	f = (b & c) | ((~b) & d);
	k = 0x5A827999;
	temp = LEFTROL(a, 5);
	temp += f + e + k + words[0];
	e = d;
	d = c;
	c = LEFTROL(b, 30);
	b = a;
	a = temp;

	for (i=1; i<15; i++)
	{
		words[i] = 0;
		f = (b & c) | ((~b) & d);
		temp = LEFTROL(a, 5);
		temp += f + e + k + words[i];
		e = d;
		d = c;
		c = LEFTROL(b, 30);
		b = a;
		a = temp;
	}


	words[15] =  dataLen*8;
	f = (b & c) | ((~b) & d);
	temp = LEFTROL(a, 5);
	temp += f + e + k + words[15];
	e = d;
	d = c;
	c = LEFTROL(b, 30);
	b = a;
	a = temp;

	for (i=16; i<20; i++)
	{
		temp = (words[i - 3] ^ words[i - 8] ^ words[i - 14] ^ words[i - 16]);
		words[i] = LEFTROL(temp, 1);
		f = (b & c) | ((~b) & d);
		temp = LEFTROL(a, 5);
		temp += f + e + k + words[i];
		e = d;
		d = c;
		c = LEFTROL(b, 30);
		b = a;
		a = temp;
	}

	for (i=20; i<40; i++)
	{
		temp = (words[i - 3] ^ words[i - 8] ^ words[i - 14] ^ words[i - 16]);
		words[i] = LEFTROL(temp, 1);
		f=b ^ c ^ d;
		k = 0x6ED9EBA1;
		temp = LEFTROL(a, 5);
		temp += f + e + k + words[i];
		e = d;
		d = c;
		c = LEFTROL(b, 30);
		b = a;
		a = temp;
	}

	for (i=40; i<60; i++)
	{
		temp = (words[i - 3] ^ words[i - 8] ^ words[i - 14] ^ words[i - 16]);
		words[i] = LEFTROL(temp, 1);
		f = (b & c) | (b & d) | (c & d);
		k = 0x8F1BBCDC;
		temp = LEFTROL(a, 5);
		temp += f + e + k + words[i];
		e = d;
		d = c;
		c = LEFTROL(b, 30);
		b = a;
		a = temp;
	}

	for (i=60; i<80; i++)
	{
		temp = (words[i - 3] ^ words[i - 8] ^ words[i - 14] ^ words[i - 16]);
		words[i] = LEFTROL(temp, 1);
		f = b ^ c ^ d;
		k = 0xCA62C1D6;
		temp = LEFTROL(a, 5);
		temp += f + e + k + words[i];
		e = d;
		d = c;
		c = LEFTROL(b, 30);
		b = a;
		a = temp;
	}

	H0 += a;
	H1 += b;
	H2 += c;
	H3 += d;
	H4 += e;


	int ct=0;
	md[ct++] =FROM_BIG_ENDIAN( H0);
	md[ct++] =FROM_BIG_ENDIAN( H1);
	md[ct++] =FROM_BIG_ENDIAN( H2);
	md[ct++] =FROM_BIG_ENDIAN( H3);
	md[ct++] =FROM_BIG_ENDIAN( H4);

}

__global__ void sha1_kernel(unsigned int *offset, unsigned char *input, unsigned char *output, unsigned int num)
{
	unsigned int index=blockIdx.x*blockDim.x+threadIdx.x;
	if(index<num)
	{
		GPU_sha1_kernel(input+offset[index],offset[index+1]-offset[index],(unsigned int*)(output+index*FINGERPRINT_LEN));
	}
}
#endif

extern "C"
void GPU_sha1(unsigned char *input,unsigned char *output,unsigned int *offset,unsigned int num,unsigned int len)
{
	cudaMemcpy(gpu_input_data_s,input,len*sizeof(unsigned char),cudaMemcpyHostToDevice);
	cudaMemcpy(gpu_offset,offset,(num+1)*sizeof(unsigned int),cudaMemcpyHostToDevice);

	unsigned int threadNum=32;
	unsigned int blockNum=(unsigned int)(num+threadNum-1)/threadNum;
	dim3 grid(blockNum,1,1);
	dim3 threads(threadNum,1,1);
#ifdef MJW
	sha1_kernel<<<grid,threads>>>(gpu_input_data_s, gpu_output_data_s, gpu_offset, num);
#else
	sha1_kernel<<<grid,threads>>>(gpu_offset,gpu_input_data_s,gpu_output_data_s,num);
#endif
	cudaThreadSynchronize();

	cudaMemcpy(output,gpu_output_data_s,num*FINGERPRINT_LEN,cudaMemcpyDeviceToHost);
}

extern "C"
void GPU_sha1_init(unsigned int max_chunk_len,unsigned int num)
{
	cudaSetDevice(0);
	cudaMalloc((void**)&gpu_input_data_s, max_chunk_len*num);
	cudaMalloc((void**)&gpu_output_data_s, num*FINGERPRINT_LEN);
	cudaMalloc((void**)&gpu_offset, (num+1)*sizeof(unsigned int));
}

extern "C"
void GPU_sha1_destroy(void)
{
	cudaFree(gpu_input_data_s);
	cudaFree(gpu_output_data_s);
	cudaFree(gpu_offset);
}
