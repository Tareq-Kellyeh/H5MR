#ifndef STRUCTURES_H
#define STRUCTURES_H

#include <glib.h>
#include "hdf5.h"
#include "hdf5_hl.h"
#include <string.h>
#include <assert.h>

/* hashtable: MEMORY location => int
 ex: "MPI_COMM_SELF"  => 0
 ex: "MPI_COMM_World" => 1
*/
GHashTable* comm_htbl; // has to be inserted in HDF5_Table at the end?
GHashTable* dummy_comm_htbl;
GHashTable* persistent_comm_htbl;

int last_comm_id;

#define DATATYPE_DESCRIPTION_DATASET_NAME   "DS_DATATYPE_DESCRIPTION"
#define PROCESS_COMMUNICATOR_DATASET_NAME   "DS_PROCESS_COMMUNICATOR"

#define DATATYPE_DATASET_NAME   "DS_DATATYPE"
#define RECV_DATASET_NAME       "DS_RECV"
#define I_RECV_DATASET_NAME     "DS_IRECV"

#define RECORD_LOG_DATASET_NAME "DS_RECORD_LOG"


#define RET                 "Ret"
#define COUNT               "Count"
#define POSITION            "Position"
#define ACTUALLY_RECEIVED   "ActuallyReceived"
#define DATATYPE_ID         "DatatypeID"
#define DESTINATION         "Destination"
#define SOURCE              "Source"
#define TAG                 "Tag"
#define COMMUNICATOR_ID     "CommunicatorID"
#define FETCHED_UP          "FettchedUp"
#define STATUS              "Status"
#define ID                  "ID"
#define DESCRIPTION         "Description"
#define RANK                "Rank"
#define SIZE                "Size"
#define UNIQUE_KEY          "UniqueKey"


typedef struct {
    int     mpi_rank;
    char *  file_name;
    hid_t   file_id;
} file_info_t;

file_info_t * file_info_ptr;

typedef struct {
    int     communicator_id;
    int     rank;
    int     size;
} process_communicator_t;


typedef struct {
    int             id;
    char *          description;
} datatype_description_t;


typedef struct {
    int             ret;
    int             count;
    int             actually_received;
    int             position;
    int             datatype_id;
    int             source;
    int             tag;
    int             communicator_id;
    int             fetched_up;
} recv_t;


typedef struct {
    const char*     unique_key;
    int             datatype_id;
    int             count;
    int             position;
} record_log_t;



int mpi_size, mpi_rank;

void h5mr_intitial_dummy_data_structures ();
hid_t create_process_communicator_t_memorytype ();
 void h5mr_free_dummy_data_structures ();
int find_comm_id (MPI_Comm comm);
hid_t create_record_log_t_memorytype();
hid_t create_recv_t_memorytype ();
int h5mr_register_communicator ( MPI_Comm newcomm );
int h5mr_comm_change_name (MPI_Comm comm, const char *new_name);

#endif