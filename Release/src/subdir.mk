################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/batch-dedup-full.c 

CU_SRCS += \
../src/GPU-sha1.cu 

CU_DEPS += \
./src/GPU-sha1.d 

OBJS += \
./src/GPU-sha1.o \
./src/batch-dedup-full.o 

C_DEPS += \
./src/batch-dedup-full.d 


# Each subdirectory must supply rules for building sources it contributes
src/%.o: ../src/%.cu
	@echo 'Building file: $<'
	@echo 'Invoking: NVCC Compiler'
	nvcc -D_GNU_SOURCE -O3 -gencode arch=compute_35,code=sm_35 -odir "src" -M -o "$(@:%.o=%.d)" "$<"
	nvcc --compile -D_GNU_SOURCE -O3 -gencode arch=compute_35,code=compute_35 -gencode arch=compute_35,code=sm_35  -x cu -o  "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '

src/%.o: ../src/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: NVCC Compiler'
	nvcc -D_GNU_SOURCE -O3 -gencode arch=compute_35,code=sm_35 -odir "src" -M -o "$(@:%.o=%.d)" "$<"
	nvcc -D_GNU_SOURCE -O3 --compile  -x c -o  "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


