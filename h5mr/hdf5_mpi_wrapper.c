#include "structures.h"
#include "hdf5_builder.h"
#include "hdf5_mpi_wrapper.h"

int MPI_Init (int * argc, char *** args)
{
    PMPI_Init(argc, args);
    h5mr_intitial_dummy_data_structures();

    return 0;
}

int MPI_Finalize ()
{
    int communicators_consisted = h5mr_consist_communicators();
    assert( communicators_consisted == 0 );

    int descriptions_consisted = h5mr_consist_descriptions();
    assert( descriptions_consisted == 0 );
    
    if ( h5mr_is_enabled() ){    
        h5mr_free_resources();
    }

    h5mr_free_dummy_data_structures();
    PMPI_Finalize();

    return 0;
}

int MPI_Comm_split (MPI_Comm comm, int color, int key, MPI_Comm *newcomm)
{
    int ret = PMPI_Comm_split(comm, color, key, newcomm);

    if (ret == MPI_SUCCESS){
        h5mr_register_communicator( *newcomm );
    }

    return ret;
}

int MPI_Comm_set_name (MPI_Comm comm, const char *comm_name)
{
    // This function should always intercepted (When capturing and when not)

    int ret = h5mr_comm_change_name(comm, comm_name);
    assert (ret == 0);

    return ret;
}

int MPI_Comm_get_name (MPI_Comm comm, char *comm_name, int *resultlen)
{
    // This function should always intercepted (When capturing and when not)

    // ToDo
//    int ret = get_comm_name(comm, comm_name);
//    assert (ret == 0);
    int ret = 0;
    return ret;
}

int MPI_Send (const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm)
{
    int ret = PMPI_Send(buf, count, datatype, dest, tag, comm);

    if ( ! h5mr_is_enabled() ){
        return ret;
    }
    
    int position = -1;
    if (ret == MPI_SUCCESS){
        position = h5mr_append_buffer_to_datatype_ds( datatype, buf, count );
        assert ( position >= 0 );
    }

    int success = h5mr_write_send_log(0, ret, count, position, datatype, dest, tag, comm);
    assert (success == 0);

    return ret;
}

int MPI_Isend (const void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm, MPI_Request *request)
{
    int ret = PMPI_Isend (buf, count, datatype, dest, tag, comm, request);
    
    if ( ! h5mr_is_enabled() ){
        return ret;
    }

    int position = -1;
    if (ret == MPI_SUCCESS){
        position = h5mr_append_buffer_to_datatype_ds (datatype, buf, count);
        assert ( position >= 0 );
    }
    
    int success = h5mr_write_send_log (2, ret, count, position, datatype, dest, tag, comm);
    assert (success == 0);
    
    return ret;
}


int MPI_Recv (void *buf, int count, MPI_Datatype datatype, int source, int tag, MPI_Comm comm, MPI_Status *stat_out)
{
    int ret;

    if ( ! h5mr_is_enabled() ){
        ret = PMPI_Recv(buf, count, datatype, source, tag, comm, stat_out);
        return ret;
    }

    MPI_Status * stat = stat_out;

    if (stat_out == MPI_STATUS_IGNORE) {
        // use a fake status
        MPI_Status actual_status;
        stat = & actual_status;
    }

    ret = PMPI_Recv(buf, count, datatype, source, tag, comm, stat);

    int actually_received;
    MPI_Get_count(stat, datatype, & actually_received);

    if (ret != MPI_SUCCESS){
        actually_received = 0;
    }
    
    int position = -1;
    if (actually_received > 0){
        position = h5mr_append_buffer_to_datatype_ds( datatype, buf, actually_received );
        assert ( position >= 0 );
    }

    int success = h5mr_write_recv_log(ret, count, actually_received, position, datatype, source, tag, comm);
    assert (success == 0);

    return ret;
}

int MPI_Irecv (void *buf, int count, MPI_Datatype datatype, int source, int tag, MPI_Comm comm, MPI_Request *request)
{
    int ret;
    
    if ( ! h5mr_is_enabled() ){
        ret = PMPI_Irecv(buf, count, datatype, source, tag, comm, request);
        return ret;
    }
    
    ret = PMPI_Irecv(buf, count, datatype, source, tag, comm, request);
    
    // except no data, BUT pointer of request
    // cached in memory using g_hash_map with pointer
    
    int success = h5mr_register_receive_request(buf, ret, count, datatype, source, tag, comm, request);
    assert (success == 0);
    
    return ret;
}

int MPI_Wait (MPI_Request *request, MPI_Status *status)
{   
    if ( h5mr_is_enabled() ){
        int flag = 0;
        while (flag == 0){
            MPI_Request_get_status(*request, &flag, status);
        }

        int is_consistent = h5mr_commit_i_recv (request);
        assert( is_consistent == 0 );
    }
    
    int ret = PMPI_Wait (request, status);
    
    return ret;
}

int MPI_Waitall (int count, MPI_Request* array_of_requests, MPI_Status* array_of_statuses)
{
    if ( h5mr_is_enabled() ){
        int flag;
        for(int i=0; i < count; i++){
            flag = 0;
            while (flag == 0){
                MPI_Request_get_status(array_of_requests[i], &flag, &array_of_statuses[i]);
            }
            int is_consistent = h5mr_commit_i_recv (&array_of_requests[i]);
            assert( is_consistent == 0 );
        }
    }
    
    int ret = PMPI_Waitall(count, array_of_requests, array_of_statuses);

    return ret;
}