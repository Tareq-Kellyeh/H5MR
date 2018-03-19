#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <time.h>
#include <mpi.h>

#define BILLION  1E9
#define COUNT 100000

#include "../h5mr/h5mr.h"

int main(int argc, char *argv[])
{
    h5mr_init(1);
    int rank;
    int i;
    MPI_Status status;
    
    struct timespec program_start, program_stop;
    double total_program_time;
    
    MPI_Init(&argc,&argv);
    
    struct timespec start, stop;
    double total_time;
    
    if( clock_gettime( CLOCK_MONOTONIC , &program_start) == -1 ) {
            exit( EXIT_FAILURE );
    }
    
    MPI_Comm_rank(MPI_COMM_WORLD,&rank);

    const int count       = 2;
    const int blocklength = 3;
    const int stride      = 4;

    MPI_Datatype c_datatype, v_datatype;
    MPI_Type_vector(count, blocklength, stride, MPI_INT, &v_datatype);
    MPI_Type_commit(&v_datatype);
    MPI_Type_contiguous(3, v_datatype, &c_datatype);
    MPI_Type_commit(&c_datatype);

    if(rank % 2 == 0){

        int buffer[63];
        for (i=0; i<63; i++)
            buffer[i] = i;
        
        if( clock_gettime( CLOCK_MONOTONIC , &start) == -1 ) {
            exit( EXIT_FAILURE );
        }

        for(int i=0; i < COUNT; i++){
            MPI_Send(buffer, 3, c_datatype, rank + 1, 52, MPI_COMM_WORLD);
        }
        
        if( clock_gettime( CLOCK_MONOTONIC , &stop) == -1 ) {
            exit( EXIT_FAILURE );
        }
        
        total_time = (( stop.tv_sec - start.tv_sec ) + ( stop.tv_nsec - start.tv_nsec ) / BILLION)/ COUNT;
        printf( "Run time of send operation call: %lf\n", total_time );
        
    } else if(rank % 2 == 1) {
        int bufferr[63];
        for (i=0; i<63; i++)
            bufferr[i] = -1;
        
        if( clock_gettime( CLOCK_MONOTONIC , &start) == -1 ) {
            exit( EXIT_FAILURE );
        }
        
        for(int i=0; i < COUNT; i++){
            MPI_Recv(bufferr, 3, c_datatype, rank -1, 52, MPI_COMM_WORLD, &status);
        }
        
        if( clock_gettime( CLOCK_MONOTONIC , &stop) == -1 ) {
            exit( EXIT_FAILURE );
        }

        total_time = (( stop.tv_sec - start.tv_sec ) + ( stop.tv_nsec - start.tv_nsec ) / BILLION)/ COUNT;
        printf( "Run time of receive operation call: %lf\n", total_time );

    }
    
    MPI_Type_free(&v_datatype);
    MPI_Type_free(&c_datatype);

    if( clock_gettime( CLOCK_MONOTONIC , &program_stop) == -1 ) {
            exit( EXIT_FAILURE );
    }
    total_program_time = (( program_stop.tv_sec - program_start.tv_sec ) + ( program_stop.tv_nsec - program_start.tv_nsec ) / BILLION);
    printf( "Run time of process %d: %lf\n", rank, total_program_time );
    
    MPI_Finalize();
    return 0;
}