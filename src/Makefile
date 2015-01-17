OBJG=GPU-sha1.o GPU-compress.o
FLAG=-O3
SDK_HOME = /home/$(USER)/NVIDIA_GPU_Computing_SDK
INCLUDEDIR= $(SDK_HOME)/C/common/inc
CUDALIB=/usr/local/cuda/lib64/
SDK_LIB= $(SDK_HOME)/C/lib/
PTHREAD_LIB = pthread
STDCPP =-lstdc++ -lm

file:$(OBJG) dedup-server.c
	gcc  dedup-server.c $(STDCPP) -L $(CUDALIB) -lcudart -L $(SDK_LIB) -l $(PTHREAD_LIB) -lcutil_x86_64 -DCUDA -fPIC -Wall $(OBJG) -o dedup-server
	
GPU-sha1.o: GPU-sha1.cu
	nvcc -c  -I $(INCLUDEDIR) GPU-sha1.cu

GPU-compress.o: GPU-compress.cu
	nvcc -c -I $(INCLUDEDIR) GPU-compress.cu
clean:
	rm *.o dedup-server -rf
