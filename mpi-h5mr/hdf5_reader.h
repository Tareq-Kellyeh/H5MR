#ifndef HDF5_READER_H
#define HDF5_READER_H


#include <stdlib.h>

#include <glib.h>
#include <mpi.h>
#include "hdf5.h"
#include "hdf5_hl.h"
//#include "../mh5r/hdf5_builder.h"
//#define free_resources() free_resources()

//void free_resources();



GHashTable* datatype_ds_htbl;

typedef struct {
    int                     number_of_rows;
    process_communicator_t* ptr_data;
} communicator_ds_t;

typedef struct {
    int                     number_of_rows;
    datatype_description_t* ptr_data;
} description_ds_t;

// To cache RECV_DATASET_NAME
typedef struct {
    int     number_of_rows;
    recv_t* ptr_data;
    int     row_to_be_read;
} recv_ds_t;

// To cache RECORD_LOG_DATASET_NAME
typedef struct {
    int             number_of_rows;
    record_log_t*   ptr_data;
    int             row_to_be_read;
} record_log_ds_t;

// To cache the received data
typedef struct {
    int         number_of_rows;
    void *      ptr_data;
    int         row_to_be_read;   
} data_ds_t;


char *tracefile;
int world_rank;


void h5mr_initilize_param(char * param);
void h5mr_initilize_hashtables();
 int h5mr_find_comm_size(MPI_Comm comm, int *size);
 int h5mr_find_rank_in_comm( MPI_Comm comm, int *rank );
 int h5mr_fetch_transmitted_data( int receiving_type, void *buf, MPI_Datatype typ, int source, int tag, MPI_Comm comm );
void h5mr_free_resourcess();
 int h5mr_fetch_captured_data( const char * unique_key, MPI_Datatype datatype, int count, void * buf );

#endif