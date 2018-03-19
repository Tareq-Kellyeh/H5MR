#ifndef HDF5_BUILDER_H
#define HDF5_BUILDER_H

#include <stdlib.h>
#include <mpi.h>
#include <glib.h>
#include <string.h>
#include <assert.h>

#include "hdf5.h"
#include "hdf5_hl.h"

/* hashtable: string => int
 ex: "contingiuous(.....)"  => 1
 ex: "vector(.....)"        => 2
*/
GHashTable* datatype_htbl;


/* hashtable: string => * ds_info_t
 ex: "DS_SEND_<datatype_id>"  => 1  :datatype_id = get from the hashtable 'datatype_htbl'
 ex: "DS_RECV_<datatype_id>"  => 2
*/
GHashTable* mpi_datatype_ds_htbl; // All the items have to free at the end


#define FILE_NAME               "mpi-hdf5-recorder"  //.h5
#define SEND_DATASET_NAME       "DS_SEND"
#define I_SEND_DATASET_NAME     "DS_ISEND"


typedef struct {
    int             ret;
    int             count;
    int             position;
    int             datatype_id;
    int             destination;
    int             tag;
    int             communicator_id;
} send_t;

typedef struct {
    hid_t           dataset_id;
    hid_t           filetype;
    hid_t           memtype;
    int             count_of_rows;
} ds_info_t;


ds_info_t * send_operation;
ds_info_t * recv_operation;
ds_info_t * i_send_operation;
ds_info_t * i_recv_operation;

ds_info_t * datatype_description;
ds_info_t * process_communicator;


int h5mr_consist_communicators ();
int h5mr_consist_descriptions();
void h5mr_free_resources ();

hid_t create_datatype_description_t_memorytype ();

int h5mr_write_send_log (int sending_type, int ret, int count, int position, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm);
int h5mr_write_recv_log (int ret, int count, int actually_received, int position, MPI_Datatype datatype, int source, int tag, MPI_Comm comm);
int h5mr_append_buffer_to_datatype_ds ( MPI_Datatype datatype, const void * buf, int count );

/* hashtable: MPI_Request* req => i_recv_t
 ex: =>
 ex: =>
*/
GHashTable* i_recv_request_htbl;

typedef struct {
    void*   buf;
    recv_t* ptr_metadata;
    MPI_Datatype datatype;
} i_recv_t;

int h5mr_register_receive_request (void *buf, int ret, int count, MPI_Datatype datatype, int source, int tag, MPI_Comm comm, MPI_Request* request);
int h5mr_commit_i_recv (MPI_Request* request);

//*********************************************************************************


int h5mr_write_record_log (const char* unique_key, MPI_Datatype datatype, int count, int position);

#endif
