#ifndef DATATYPE_H
#define DATATYPE_H

#include <mpi.h>
#include <stdio.h>
#include <assert.h>
#include <string.h>


#ifdef DEBUG
  #define debug(...) printf("[DT] "__VA_ARGS__);
#else
  #define debug(...)
#endif

#define printType(...) NULL;
#define CHECK_RET(ret) if(ret != MPI_SUCCESS){ printf("Critical error decoding datatype in %d\n", __LINE__); MPI_Abort(MPI_COMM_WORLD, 1);}

char * get_datatype_description(MPI_Datatype typ);


#endif
