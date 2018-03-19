#ifndef H5MR_SERIALIZER_H
#define H5MR_SERIALIZER_H

#include <mpi.h>

/*
 * Manage auxilliary data like global variables with a similar approach to MPI
 * Read scans for the key and returns the data according to the datatype and count
 */
void h5mr_record_data(const char * unique_key, MPI_Datatype datatype, int count, void * buf);
int h5mr_read_data(const char * unique_key, MPI_Datatype datatype, int count, void * buf);

#endif
