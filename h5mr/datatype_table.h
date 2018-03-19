#ifndef DATATYPE_TABLE_H
#define DATATYPE_TABLE_H

#include <hdf5.h>
#include <mpi.h>
#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <stdlib.h>


#ifdef DEBUG
  #define debug(...) printf("[DT] "__VA_ARGS__);
#else
  #define debug(...)
#endif

#define CHECK_RET(ret) if(ret != MPI_SUCCESS){ printf("Critical error decoding datatype in %d\n", __LINE__); MPI_Abort(MPI_COMM_WORLD, 1);}

void create_datatype_table(MPI_Datatype typ, hid_t * file_id, char * DATASET_NAME, hid_t * dataset_id, hid_t * memtype, hid_t * filetype, size_t total_size);


#endif