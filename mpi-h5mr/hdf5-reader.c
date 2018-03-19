#include "../h5mr/structures.h"

#include "../h5mr/datatype.h"
#include "../h5mr/datatype-table.h"

#include "hdf5-reader.h"


communicator_ds_t * communicator_ds = NULL;
description_ds_t *  description_ds  = NULL;
recv_ds_t *         recv_ds         = NULL;
recv_ds_t *         i_recv_ds       = NULL;
record_log_ds_t *   record_log_ds   = NULL;


int _get_rank_from_filename(char * filename)
{
    const char delimiter = '_';
    char *rank_char = strrchr(filename, delimiter);
    return (int) *(++rank_char) - 48;
}

void h5mr_initilize_param(char * param)
{
    tracefile   = param;
    world_rank  = _get_rank_from_filename(param);
}

void h5mr_initilize_hashtables()
{
    datatype_ds_htbl = g_hash_table_new(g_str_hash, g_str_equal);

    /* hashtable: MEMORY location => id
     ex: "MPI_COMM_SELF"  => 0
     ex: "MPI_COMM_World" => 1
    */

//    comm_htbl = g_hash_table_new(g_direct_hash, g_direct_equal);
//
//    g_hash_table_insert(comm_htbl, (void *) MPI_COMM_SELF, (void*) ++last_comm_id);
//    g_hash_table_insert(comm_htbl, (void *) MPI_COMM_WORLD, (void*) ++last_comm_id);

    h5mr_intitial_dummy_data_structures();
}

hid_t _get_or_open_trace_file()
{
    if (file_info_ptr != NULL) {
        return file_info_ptr->file_id;
    }

    file_info_t *file_info = malloc(sizeof(file_info_t));

    file_info->mpi_rank = mpi_rank;
    file_info->file_name = tracefile;

    /*
    * Open the trace file using the default properties.
    */
//    herr_t status;
    hid_t file_id = H5Fopen( tracefile, H5F_ACC_RDONLY, H5P_DEFAULT );
//    assert(status >= 0);

    file_info->file_id = file_id;
    file_info_ptr = file_info;

    return file_info_ptr->file_id;
}

static int _find_comm_size_in_ds(int comm_id, int *size)
{
    if ( communicator_ds == NULL ){

        hid_t file      = _get_or_open_trace_file();

        communicator_ds_t *ds = malloc(sizeof(communicator_ds_t));

        hid_t dataset_id    = H5Dopen(file, PROCESS_COMMUNICATOR_DATASET_NAME, H5P_DEFAULT);
        assert(dataset_id >= 0);
        
        hid_t memtype       = create_process_communicator_t_memorytype();

        hid_t dspace    = H5Dget_space(dataset_id);
        int   ndims     = H5Sget_simple_extent_ndims(dspace);

        hsize_t     dims[ndims];
        H5Sget_simple_extent_dims(dspace, dims, NULL);

        process_communicator_t * ptr_data = (process_communicator_t *) malloc (dims[0] * sizeof (process_communicator_t));

        herr_t status;
        status = H5Dread(dataset_id, memtype, H5S_ALL, H5S_ALL, H5P_DEFAULT, ptr_data);
        assert(status >= 0);

        ds->number_of_rows = dims[0];
        ds->ptr_data = ptr_data;

        communicator_ds = ds;

        status = H5Dclose (dataset_id);
        assert(status >= 0);
        status = H5Tclose (memtype);
        assert(status >= 0);
    }

    /*
     * Find the rank according to the communicator
     */
    int i;
    for( i = 0; i < communicator_ds->number_of_rows; i++) {
        if (communicator_ds->ptr_data[i].communicator_id == comm_id){
            *size =  communicator_ds->ptr_data[i].size;
        }
    }

    return 0;
}

static int _find_rank_in_ds(int comm_id, int *rank)
{
    if ( communicator_ds == NULL ){

        hid_t file      = _get_or_open_trace_file();

        communicator_ds_t *ds = malloc(sizeof(communicator_ds_t));

        hid_t dataset_id    = H5Dopen(file, PROCESS_COMMUNICATOR_DATASET_NAME, H5P_DEFAULT);
        assert(dataset_id >= 0);
        
        hid_t memtype       = create_process_communicator_t_memorytype();

        hid_t dspace    = H5Dget_space(dataset_id);
        int   ndims     = H5Sget_simple_extent_ndims(dspace);

        hsize_t     dims[ndims];
        H5Sget_simple_extent_dims(dspace, dims, NULL);

        process_communicator_t * ptr_data = (process_communicator_t *) malloc (dims[0] * sizeof (process_communicator_t));

        herr_t status;
        status = H5Dread(dataset_id, memtype, H5S_ALL, H5S_ALL, H5P_DEFAULT, ptr_data);
        assert(status >= 0);

        ds->number_of_rows = dims[0];
        ds->ptr_data = ptr_data;

        communicator_ds = ds;

        status = H5Dclose (dataset_id);
        assert(status >= 0);
        status = H5Tclose (memtype);
        assert(status >= 0);
    }

    /*
     * Find the rank according to the communicator
     */
    int i;
    for( i = 0; i < communicator_ds->number_of_rows; i++) {
        if (communicator_ds->ptr_data[i].communicator_id == comm_id){
            *rank =  communicator_ds->ptr_data[i].rank;
        }
    }

    return 0;
}

int h5mr_find_comm_size(MPI_Comm comm, int *size)
{
    /*
     * 1- Find in comm hashtable the CommunicatorID related to comm
     */
    int comm_id = find_comm_id(comm);

    /*
     * 2- Find in the DS_PROCESS_COMMUNICATOR dataset the rank of the process related to CommunicatorID
     */
    int ret = _find_comm_size_in_ds(comm_id, size);
    assert (ret == 0);

    return 0;
}

int h5mr_find_rank_in_comm(MPI_Comm comm, int *rank)
{
    /*
     * 1- Find in comm hashtable the CommunicatorID related to comm
     */
    int comm_id = find_comm_id(comm);
    assert ( comm_id > 0 );
    /*
     * 2- Find in the DS_PROCESS_COMMUNICATOR dataset the rank of the process related to CommunicatorID
     */
    int ret = _find_rank_in_ds(comm_id, rank);
    assert (ret == 0);

    return 0;
}

int _find_datatype_id_in_ds(MPI_Datatype datatype)
{
    if ( description_ds == NULL ){

        hid_t file      = _get_or_open_trace_file();

        description_ds_t *ds = malloc(sizeof(description_ds_t));

        hid_t dataset_id    = H5Dopen(file, DATATYPE_DESCRIPTION_DATASET_NAME, H5P_DEFAULT);
        assert(dataset_id >= 0);
        
        hid_t memtype       = H5Dget_type( dataset_id );
//        hid_t memtype       = create_datatype_description_t_memorytype();

        hid_t dspace    = H5Dget_space(dataset_id);
        int   ndims     = H5Sget_simple_extent_ndims(dspace);

        hsize_t     dims[ndims];
        H5Sget_simple_extent_dims(dspace, dims, NULL);

        datatype_description_t * ptr_data = (datatype_description_t *) malloc (dims[0] * sizeof (datatype_description_t));

        herr_t status;
        status = H5Dread(dataset_id, memtype, H5S_ALL, H5S_ALL, H5P_DEFAULT, ptr_data);
        assert(status >= 0);

        ds->number_of_rows = dims[0];
        ds->ptr_data = ptr_data;

        description_ds = ds;

        status = H5Dclose (dataset_id);
        assert(status >= 0);
        status = H5Tclose (memtype);
        assert(status >= 0);
    }

    char * datatype_desc = get_datatype_description(datatype);
    /*
     * Find the descriptionID according to the datatype_description
     */
    int i, description_id;
    for( i = 0; i < description_ds->number_of_rows; i++) {
        if (strcmp(datatype_desc, description_ds->ptr_data[i].description) == 0){
            description_id =  description_ds->ptr_data[i].id;
        }
    }

    return description_id;
}

data_ds_t *_get_or_create_received_data_ds( MPI_Datatype datatype, int datatype_id )
{
    char dataset_name[1024];
    sprintf(dataset_name, "%s_%d", DATATYPE_DATASET_NAME, datatype_id);

    data_ds_t *datatype_ds = g_hash_table_lookup(datatype_ds_htbl, dataset_name);

    if(datatype_ds != NULL){
        return datatype_ds;
    }

    data_ds_t *ds = malloc(sizeof(data_ds_t));

    hid_t file          = _get_or_open_trace_file();
    hid_t dataset_id    = H5Dopen(file, dataset_name, H5P_DEFAULT);
    assert(dataset_id >= 0);
    
    hid_t memtype       = H5Dget_type(dataset_id);
    hid_t space         = H5Dget_space (dataset_id);
    int   ndims         = H5Sget_simple_extent_ndims(space);

    hsize_t     dims[ndims];
    H5Sget_simple_extent_dims(space, dims, NULL);

    void * rdata = malloc (H5Dget_storage_size(dataset_id));

    /*
    * Read the data.
    */
    herr_t status = H5Dread (dataset_id, memtype, H5S_ALL, H5S_ALL, H5P_DEFAULT, rdata);
    assert(status >= 0);

    ds->row_to_be_read  = 0;
    ds->number_of_rows  = dims[0];
    ds->ptr_data        = rdata;

    datatype_ds = ds;

    g_hash_table_insert(datatype_ds_htbl, g_strdup(dataset_name), datatype_ds);

    H5Tclose(memtype);
    assert(status >= 0);
    H5Dclose(dataset_id);
    assert(status >= 0);

    return datatype_ds;
}

recv_ds_t* _prepare_recv_ds( void )
{
    if ( recv_ds != NULL ){
        return recv_ds;
    }

    hid_t file      = _get_or_open_trace_file();

    recv_ds_t *ds = malloc(sizeof(recv_ds_t));

    hid_t dataset_id    = H5Dopen(file, RECV_DATASET_NAME, H5P_DEFAULT);
    assert(dataset_id >= 0);
//    hid_t memtype       = H5Dget_type( dataset_id );
    hid_t memtype       = create_recv_t_memorytype();
    hid_t dspace        = H5Dget_space(dataset_id);
    int   ndims         = H5Sget_simple_extent_ndims(dspace);

    hsize_t     dims[ndims];
    H5Sget_simple_extent_dims(dspace, dims, NULL);

    recv_t * ptr_data = (recv_t *) malloc (dims[0] * sizeof (recv_t));

    herr_t status;
    status = H5Dread(dataset_id, memtype, H5S_ALL, H5S_ALL, H5P_DEFAULT, ptr_data);
    assert(status >= 0);

    ds->number_of_rows      = dims[0];
    ds->ptr_data            = ptr_data;
    ds->row_to_be_read = 0;

    recv_ds = ds;

    status = H5Tclose (memtype);
    assert(status >= 0);
    status = H5Dclose (dataset_id);
    assert(status >= 0);

    return recv_ds;
}

recv_ds_t* _prepare_i_recv_ds( void )
{
    if ( i_recv_ds != NULL ){
        return i_recv_ds;
    }

    hid_t file      = _get_or_open_trace_file();

    recv_ds_t *ds = malloc(sizeof(recv_ds_t));

    hid_t dataset_id    = H5Dopen(file, I_RECV_DATASET_NAME, H5P_DEFAULT);
    assert(dataset_id >= 0);
    
//    hid_t memtype       = H5Dget_type( dataset_id );
    hid_t memtype       = create_recv_t_memorytype();
    hid_t dspace        = H5Dget_space(dataset_id);
    int   ndims         = H5Sget_simple_extent_ndims(dspace);

    hsize_t     dims[ndims];
    H5Sget_simple_extent_dims(dspace, dims, NULL);

    recv_t * ptr_data = (recv_t *) malloc (dims[0] * sizeof (recv_t));

    herr_t status;
    status = H5Dread(dataset_id, memtype, H5S_ALL, H5S_ALL, H5P_DEFAULT, ptr_data);
    assert(status >= 0);

    ds->number_of_rows      = dims[0];
    ds->ptr_data            = ptr_data;
    ds->row_to_be_read = 0;

    i_recv_ds = ds;

    status = H5Tclose (memtype);
    assert(status >= 0);
    status = H5Dclose (dataset_id);
    assert(status >= 0);

    return i_recv_ds;
}


int h5mr_fetch_transmitted_data( int receiving_type, void *buf, MPI_Datatype typ, int source, int tag, MPI_Comm comm )
{
    recv_ds_t* ds;
    if (receiving_type == 0) {
        ds = _prepare_recv_ds();
    } else if ( receiving_type == 1 ) {
        ds = _prepare_i_recv_ds();
    }
    assert( ds != NULL );

    /*
     * Find the CommunicatorID related to comm
     */
    int comm_id = find_comm_id(comm);
    assert ( comm_id > 0 );

    /*
     * If the (rank, tag, communicator) tuple can be found in the dataset, the tuble (position, actually_received) will be delivered.
     */
    int position = -1;
    int num_to_read = -1;
    int datatype_id = -1;

    for( int i = 0; i < ds->number_of_rows; i++) {
        if ( ds->ptr_data[i].source == source && ds->ptr_data[i].tag == tag && ds->ptr_data[i].communicator_id == comm_id && ds->ptr_data[i].fetched_up == 0){
            position                    = ds->ptr_data[i].position;
            num_to_read                 = ds->ptr_data[i].actually_received;
            datatype_id                 = ds->ptr_data[i].datatype_id;
            ds->ptr_data[i].fetched_up  = 1;

            break;
        }
    }

    assert ( position >= 0 );
    assert ( num_to_read >= 0 );
    assert ( datatype_id > 0 );

    /*
     * The number of read rows will be detected depending on the actually_received count of data.
     * Or the parameter count ??
     */

    if (num_to_read > 0){
        data_ds_t *datatype_ds = _get_or_create_received_data_ds( typ, datatype_id ); // To Fix

        MPI_Aint extent;
        MPI_Type_extent(typ, &extent);

        /*
        * The user has to allocate memory for the buffer, that's why no buffer allocation is needed
        */
        void * start_address    = datatype_ds->ptr_data + (extent * position);
        memcpy(buf, start_address, extent * num_to_read);
    }

    return 0;
}

void h5mr_free_resourcess()
{
    free(communicator_ds);
    free(description_ds);
    free(recv_ds);
    free(i_recv_ds);
    free(record_log_ds);

    g_hash_table_destroy(datatype_ds_htbl); // The included array must be destoyed too.

    h5mr_free_dummy_data_structures();
    herr_t status;
    if (file_info_ptr != NULL) {
        H5Fclose(file_info_ptr->file_id);
        assert(status >= 0);
    }
}

//******************************************************************************************************************************
//******************************************************************************************************************************
//******************************************************************************************************************************

record_log_ds_t* _prepare_record_log_ds( void )
{
    if ( record_log_ds != NULL ){
        return record_log_ds;
    }

    hid_t file      = _get_or_open_trace_file();

    record_log_ds_t *ds = malloc(sizeof(record_log_ds_t));

    hid_t dataset_id    = H5Dopen(file, RECORD_LOG_DATASET_NAME, H5P_DEFAULT);
    assert(dataset_id >= 0);
//    hid_t memtype       = H5Dget_type( dataset_id );
    hid_t memtype       = create_record_log_t_memorytype();
    hid_t dspace        = H5Dget_space(dataset_id);
    int   ndims         = H5Sget_simple_extent_ndims(dspace);

    hsize_t     dims[ndims];
    H5Sget_simple_extent_dims(dspace, dims, NULL);

    record_log_t * ptr_data = (record_log_t *) malloc (dims[0] * sizeof (record_log_t));

    herr_t status;
    status = H5Dread(dataset_id, memtype, H5S_ALL, H5S_ALL, H5P_DEFAULT, ptr_data);
    assert(status >= 0);

    ds->number_of_rows  = dims[0];
    ds->ptr_data        = ptr_data;
    ds->row_to_be_read  = 0;

    record_log_ds = ds;

    status = H5Tclose (memtype);
    assert(status >= 0);
    status = H5Dclose (dataset_id);
    assert(status >= 0);

    return record_log_ds;
}

int h5mr_fetch_captured_data( const char * unique_key, MPI_Datatype datatype, int count, void * buf )
{
    record_log_ds_t* ds = _prepare_record_log_ds();
    assert( ds != NULL );

    /*
    * If the (rank, tag, communicator) tuple can be found in the dataset, the tuble (position, actually_received) will be delivered.
    */
    int position    = -1;
    int num_to_read = -1;
    int datatype_id = -1;

    for( int i = 0; i < ds->number_of_rows; i++) {
//        if ( ds->ptr_data[i].source == source && ds->ptr_data[i].datatype_id == datatype_id && ds->ptr_data[i].count == count ){
        if (strcmp( ds->ptr_data[i].unique_key, unique_key ) == 0 ) {
            position    = ds->ptr_data[i].position;
            num_to_read = ds->ptr_data[i].count;
            datatype_id = ds->ptr_data[i].datatype_id;

            break;
        }
    }

    assert ( position >= 0 );
    assert ( num_to_read >= 0 );
    assert ( datatype_id > 0 );

    if (num_to_read > 0){
        /*
        * The number of read rows will be detected depending on the parameter "count"
        */

        data_ds_t *datatype_ds = _get_or_create_received_data_ds( datatype, datatype_id ); // To Fix

        MPI_Aint extent;
        MPI_Type_extent(datatype, &extent);

        /*
        * The user has to allocate memory for the buffer, that's why no buffer allocation is needed
        */
        void * start_address    = datatype_ds->ptr_data + (extent * position);
        memcpy(buf, start_address, extent * num_to_read);
    }

    return 0;
}
