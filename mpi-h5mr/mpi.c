#include <mpi.h>

#include "../h5mr/structures.h"
#include "hdf5_reader.h"


int MPI_Init(int * argc, char *** argv)
{
    // the last parameter of the program is expected to be the trace file
    if (*argc > 1){
        (*argc)--;
        h5mr_initilize_param((*argv)[*argc]);

        printf("MPI-MH5R: Replaying the trace file %s by the process %d\n", tracefile, world_rank);
    } else {
        printf("MPI-MH5R: Please use the trace file as last parameter\n");
        MPI_Abort(MPI_COMM_WORLD, 2);
    }

    PMPI_Init(argc, argv);
    h5mr_initilize_hashtables();

    return MPI_SUCCESS;
}

int MPI_Finalize()
{
    // close HDF5 trace file etc.
    h5mr_free_resourcess();
    PMPI_Finalize();

    return MPI_SUCCESS;
}

int MPI_Comm_rank(MPI_Comm comm, int *rank)
{
    int ret = h5mr_find_rank_in_comm( comm, rank );
    assert(ret == 0);

    return MPI_SUCCESS;
}

int MPI_Comm_size(MPI_Comm comm, int *size)
{
    int ret = h5mr_find_comm_size( comm, size );
    assert(ret == 0);

    return MPI_SUCCESS;
}

int MPI_Recv(void *buf, int count, MPI_Datatype datatype, int source, int tag, MPI_Comm comm, MPI_Status *stat_out)
{
    // read the next message matching to datatype and tag from the trace file

    /*
     * Read the next rows of the received data
     */
    int ret = h5mr_fetch_transmitted_data( 0, buf, datatype, source, tag, comm );
    assert(ret == 0);

    return MPI_SUCCESS;
}

int MPI_Send(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm)
{
    // MPI_Send does basically nothing, might check if the send data is identical to the data in the trace file
    return MPI_SUCCESS;
}

int MPI_Isend(const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm, MPI_Request *request)
{
  return MPI_SUCCESS;
}

int MPI_Irecv(void *buf, int count, MPI_Datatype datatype, int source, int tag, MPI_Comm comm, MPI_Request *request)
{
    int ret = h5mr_fetch_transmitted_data( 1, buf, datatype, source, tag, comm );
    assert(ret == 0);

    return MPI_SUCCESS;
    // like MPI_Recv, immediately receive data....
}

int MPI_Wait (MPI_Request *request, MPI_Status *status)
{
    return MPI_SUCCESS;
}

int MPI_Waitall(int count, MPI_Request * array_of_requests, MPI_Status * array_of_statuses)
{
    return MPI_SUCCESS;
}

int MPI_Request_get_status( MPI_Request request, int *flag,  MPI_Status *status )
{
    *flag = 1;
    return MPI_SUCCESS;
}

int MPI_Comm_split(MPI_Comm comm, int color, int key, MPI_Comm *newcomm)
{
    int ret = PMPI_Comm_split(comm, color, key, newcomm);

    if (ret == MPI_SUCCESS){
        h5mr_register_communicator( *newcomm );
    }

    return ret;
}

int MPI_Comm_set_name(MPI_Comm comm, const char *comm_name)
{
    int ret = h5mr_comm_change_name(comm, comm_name);
    assert (ret == 0);

    return ret;
}

int MPI_Comm_get_name(MPI_Comm comm, char *comm_name, int *resultlen)
{
    // This function should always intercepted (When capturing and when not)

    // ToDo
//    int ret = get_comm_name(comm, comm_name);
//    assert (ret == 0);
    int ret = 0;
    return ret;
}

int MPI_Comm_free(MPI_Comm *comm)
{
  return MPI_SUCCESS;
}