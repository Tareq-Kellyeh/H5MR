#include "structures.h"
#include "datatype.h"
#include "datatype_table.h"
#include "hdf5_builder.h"

int is_data_structures_initialized  = 0;
int last_comm_id                    = 0;

GHashTable* datatype_htbl           = NULL;
GHashTable* mpi_datatype_ds_htbl    = NULL;
GHashTable* i_recv_request_htbl     = NULL;

file_info_t * file_info_ptr         = NULL;

ds_info_t * send_operation          = NULL;
ds_info_t * recv_operation          = NULL;
ds_info_t * i_send_operation        = NULL;
ds_info_t * i_recv_operation        = NULL;
ds_info_t * datatype_description    = NULL;
ds_info_t * process_communicator    = NULL;
ds_info_t * record_log              = NULL;

hid_t _get_or_create_file_id()
{
    if (file_info_ptr != NULL) {
        return file_info_ptr->file_id;
    }

    char file_name[1024];
    sprintf(file_name, "%s_%d.h5", FILE_NAME, mpi_rank);
    file_info_t *new_file = (file_info_t *) malloc ( sizeof(file_info_t) );
        
    new_file->mpi_rank = mpi_rank;
    new_file->file_name = file_name;
    /*
    * Create a new file using the default properties.
    */
    hid_t file_id = H5Fcreate (file_name, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
    new_file->file_id = file_id;
    file_info_ptr = new_file;
    
    return file_info_ptr->file_id;
}

// ************************************** Communicator: Dataset + insertion ***********************************************/

int _create_communicator_ds()
{
    if ( process_communicator != NULL ){
        return 0;
    }
    
    hid_t file_id = _get_or_create_file_id();
    // *************************************************
    hid_t      filetype, memtype, dset;
    herr_t      status;
    /*
    * Create the compound datatype for memory.
    */
    memtype = create_process_communicator_t_memorytype();
    
    filetype = H5Tcreate (H5T_COMPOUND, 8 + 8 + 8);
    status = H5Tinsert (filetype, COMMUNICATOR_ID, 0, H5T_STD_I64BE);
    assert(status >= 0);
    status = H5Tinsert (filetype, RANK, 0 + 8, H5T_STD_I64BE);
    assert(status >= 0);
    status = H5Tinsert (filetype, SIZE, 0 + 8 + 8, H5T_STD_I64BE);
    assert(status >= 0);
    
    hid_t   dcpl_id;
    /* Create the data space with unlimited dimensions. */
    hsize_t     dims[1] = {0};
    hsize_t     dims_e[1] = {H5S_UNLIMITED};
    hid_t memspace = H5Screate_simple(1, dims, dims_e);

    /* Modify dataset creation properties, i.e. enable chunking  */
    dcpl_id = H5Pcreate(H5P_DATASET_CREATE);
    hsize_t chunk_dims[1] = {1}; // DIM0
    H5Pset_chunk(dcpl_id, 1, chunk_dims);
    
    dset = H5Dcreate2(file_id, PROCESS_COMMUNICATOR_DATASET_NAME, filetype, memspace, H5P_DEFAULT, dcpl_id, H5P_DEFAULT);

    ds_info_t *ds = malloc(sizeof(ds_info_t));
    
    ds->dataset_id      = dset;
    ds->filetype        = filetype;
    ds->memtype         = memtype;
    ds->count_of_rows   = 0;
    
    // *************************************************
    process_communicator = ds;
    
    status = H5Sclose (memspace);
    assert(status >= 0);
    
    return 0;
}

void _add_communicator_to_communicator_ds(process_communicator_t * comm_data)
{
    if ( process_communicator == NULL ){
        int ret = _create_communicator_ds();
        assert (ret == 0);
    }
    
    ds_info_t * ds = process_communicator;
        
    // ********************** write_description_to_datatype_description_ds *********************************
    herr_t      status;
    /* Extend the dataset. Dataset becomes 2 x 19  = row * column
    * row has to be added by 1(or count) every time
    */
    //    hsize_t     size[2] = {2 ,19};
    //    hsize_t     size[1] = {1};
    hsize_t     size[1] ;
    
    size[0] = ds->count_of_rows + 1;
    //    size[0] = dims[0]+ dimsext[0];
    //    size[1] = dims[1];
    status = H5Dset_extent (ds->dataset_id, size);
    assert(status >= 0);
    
    hid_t filespace     = H5Dget_space(ds->dataset_id);
    hsize_t offset[1]   = {ds->count_of_rows};
    hsize_t dimsext[1]  = {1};
    status = H5Sselect_hyperslab(filespace, H5S_SELECT_SET, offset, NULL, dimsext, NULL);
    assert(status >= 0);
    
    /* Define memory space
    */        
    hsize_t  dimsr[1] = {1};
    hid_t memspace = H5Screate_simple(1, dimsr, NULL);
    
    /* Write the data to the extended portion of dataset  */
    process_communicator_t    wdata[1];                /* Write buffer */
    /*
    * Initialize data.
    */
    wdata[0].communicator_id    = comm_data->communicator_id;
    wdata[0].rank               = comm_data->rank;
    wdata[0].size               = comm_data->size;
    
    //    return ret;
    
    status = H5Dwrite(ds->dataset_id, ds->memtype, memspace, filespace, H5P_DEFAULT, wdata);
    assert(status >= 0);

    status = H5Sclose (filespace);
    assert(status >= 0);
    status = H5Sclose (memspace);
    assert(status >= 0);
    
    ds->count_of_rows = ds->count_of_rows + 1;
}

// *************************************************************************************************************************/


int _init_data_structures()
{
    if ( is_data_structures_initialized == 1 ){
        return 0;
    }
    
    MPI_Comm comm  = MPI_COMM_WORLD;
    MPI_Comm_size(comm, &mpi_size);
    MPI_Comm_rank(comm, &mpi_rank);

    if ( datatype_htbl == NULL) {
        datatype_htbl = g_hash_table_new(g_str_hash, g_str_equal);
    }

    if ( mpi_datatype_ds_htbl == NULL) {
        mpi_datatype_ds_htbl = g_hash_table_new(g_str_hash, g_str_equal);
    }
    
    is_data_structures_initialized = 1;
    
    return 0;
}


void _iterator_communicators(gpointer key, gpointer value, gpointer user_data)
{
    _add_communicator_to_communicator_ds( (process_communicator_t *) value);
}

int h5mr_consist_communicators()
{
    if ( persistent_comm_htbl == NULL) {
        return 0;
    }

    g_hash_table_foreach( persistent_comm_htbl, (GHFunc)_iterator_communicators, NULL);
    
    return 0;
}

void _commit_i_recvs(MPI_Request* key, ds_info_t * value)
{ 
    int is_committed = h5mr_commit_i_recv(key);
    assert(is_committed == 0);
}

void _free_datatype_ds_resources(gpointer key, ds_info_t * value)
{ 
    herr_t status = H5Dclose (value->dataset_id);
    assert(status >= 0);
    status = H5Tclose ( value->filetype);
    assert(status >= 0);
    status = H5Tclose (value->memtype);
    assert(status >= 0);
}

void h5mr_free_resources()
{    
    herr_t status;
    
    if(send_operation != NULL){
        status = H5Dclose (send_operation->dataset_id);
        assert(status >= 0);
        status = H5Tclose (send_operation->filetype);
        assert(status >= 0);
        status = H5Tclose (send_operation->memtype);
        assert(status >= 0);
        
        free(send_operation);
    }
    
    if(record_log != NULL){
        status = H5Dclose (record_log->dataset_id);
        assert(status >= 0);
        status = H5Tclose (record_log->filetype);
        assert(status >= 0);
        status = H5Tclose (record_log->memtype);
        assert(status >= 0);
        
        free(record_log);
    }
    
    if(i_send_operation != NULL){
        status = H5Dclose (i_send_operation->dataset_id);
        assert(status >= 0);
        status = H5Tclose (i_send_operation->filetype);
        assert(status >= 0);
        status = H5Tclose (i_send_operation->memtype);
        assert(status >= 0);   
        
        free(i_send_operation);
    }

    if(recv_operation != NULL){
        status = H5Dclose (recv_operation->dataset_id);
        assert(status >= 0);
        status = H5Tclose (recv_operation->filetype);
        assert(status >= 0);
        status = H5Tclose (recv_operation->memtype);
        assert(status >= 0);    
        
        free(recv_operation);
    }
    
    if(i_recv_operation != NULL){
        status = H5Dclose (i_recv_operation->dataset_id);
        assert(status >= 0);
        status = H5Tclose (i_recv_operation->filetype);
        assert(status >= 0);
        status = H5Tclose (i_recv_operation->memtype);
        assert(status >= 0);   
        
        free(i_recv_operation);
    }
    
    if(datatype_description != NULL){
        status = H5Dclose (datatype_description->dataset_id);
        assert(status >= 0);
        status = H5Tclose (datatype_description->filetype);
        assert(status >= 0);
        status = H5Tclose (datatype_description->memtype);
        assert(status >= 0);
        
        free(datatype_description);
    }
    
    if(process_communicator != NULL){
        status = H5Dclose (process_communicator->dataset_id);
        assert(status >= 0);
        status = H5Tclose (process_communicator->filetype);
        assert(status >= 0);
        status = H5Tclose (process_communicator->memtype);
        assert(status >= 0);
        
        free(process_communicator);
    }
    
    if(file_info_ptr != NULL){
        status = H5Fclose(file_info_ptr->file_id);
        assert(status >= 0);
    }

    if (datatype_htbl != NULL){
        g_hash_table_destroy( datatype_htbl );
    }
    
    if (persistent_comm_htbl != NULL){
        g_hash_table_destroy( persistent_comm_htbl );
    }
    
    if (mpi_datatype_ds_htbl != NULL){
        g_hash_table_foreach( mpi_datatype_ds_htbl, (GHFunc)_free_datatype_ds_resources, NULL);
        g_hash_table_destroy( mpi_datatype_ds_htbl );
    }
    
    if (i_recv_request_htbl != NULL){
        g_hash_table_foreach( i_recv_request_htbl, (GHFunc)_commit_i_recvs, NULL);
        g_hash_table_destroy( i_recv_request_htbl );
    }
}

/**************************************************************************************************************************/

// **************************************** Datatype: Dataset + insertion **************************************************/

hid_t create_datatype_description_t_memorytype()
{
    herr_t status;
    /*
    * Create variable-length string datatype.
    */
    hid_t strtype = H5Tcopy (H5T_C_S1);
    status = H5Tset_size (strtype, H5T_VARIABLE);
    assert(status >= 0);
    
    hid_t  memtype = H5Tcreate (H5T_COMPOUND, sizeof (datatype_description_t));
    status = H5Tinsert (memtype, ID, HOFFSET (datatype_description_t, id), H5T_NATIVE_INT);
    assert(status >= 0);
    status = H5Tinsert (memtype, DESCRIPTION, HOFFSET (datatype_description_t, description), strtype);
    assert(status >= 0);
    
    return memtype;
}

/*
* Create the dataset that will contain the datatypes(ID, Description)
*/
int _create_datatype_description_ds()
{
    if ( datatype_description != NULL ){
        return 0;
    }
    
    hid_t file_id = _get_or_create_file_id();

    // *************************************************
    hid_t      filetype, memtype, strtype, dset;
    herr_t      status;
    
    /*
    * Create variable-length string datatype.
    */
    strtype = H5Tcopy (H5T_C_S1);
    status = H5Tset_size (strtype, H5T_VARIABLE);
    assert(status >= 0);
    
    /*
    * Create the compound datatype for memory.
    */
    
    memtype = create_datatype_description_t_memorytype();
    
    /*
    * Create the compound datatype for the file.  Because the standard
    * types we are using for the file may have different sizes than
    * the corresponding native types, we must manually calculate the
    * offset of each member.
    */
    
    filetype = H5Tcreate (H5T_COMPOUND, 8 + sizeof (hvl_t));
    status = H5Tinsert (filetype, ID, 0, H5T_STD_I64BE);
    assert(status >= 0);
    status = H5Tinsert (filetype, DESCRIPTION, 8, strtype);
    assert(status >= 0);
    
    hid_t   dcpl_id;
    /* Create the data space with unlimited dimensions. */
    hsize_t     dims[1] = {0};
    hsize_t     dims_e[1] = {H5S_UNLIMITED};
    hid_t space = H5Screate_simple(1, dims, dims_e);
    
    /* Modify dataset creation properties, i.e. enable chunking  */
    dcpl_id = H5Pcreate(H5P_DATASET_CREATE);
    hsize_t chunk_dims[1] = {1}; // DIM0
    H5Pset_chunk(dcpl_id, 1, chunk_dims);
    
    dset = H5Dcreate2(file_id, DATATYPE_DESCRIPTION_DATASET_NAME, filetype, space, H5P_DEFAULT, dcpl_id, H5P_DEFAULT);
    
    ds_info_t *ds = malloc(sizeof(ds_info_t));
    
    ds->dataset_id      = dset;
    ds->filetype        = filetype;
    ds->memtype         = memtype;
    ds->count_of_rows   = 0;
    // *************************************************
    datatype_description = ds;
    
    status = H5Sclose (space);
    assert(status >= 0);
    
    return 0;
}

void _add_description_to_datatype_description_ds(char * datatype_desc, int id)
{
    if ( datatype_description == NULL ){
        int ret = _create_datatype_description_ds();
        assert (ret == 0);
    }
        
    ds_info_t * ds = datatype_description;
    
    // ********************** write_description_to_datatype_description_ds *********************************
    herr_t      status;
    /* Extend the dataset. Dataset becomes 2 x 19  = row * column
    * row has to be added by 1(or count) every time
    */
    //    hsize_t     size[2] = {2 ,19};
    //    hsize_t     size[1] = {1};
    hsize_t     size[1] ;
    
    size[0] = ds->count_of_rows + 1;
    //    size[0] = dims[0]+ dimsext[0];
    //    size[1] = dims[1];
    status = H5Dset_extent (ds->dataset_id, size);
    assert(status >= 0);
    
    hid_t filespace     = H5Dget_space(ds->dataset_id);
    hsize_t offset[1]   = {ds->count_of_rows};
    hsize_t dimsext[1]  = {1};
    status = H5Sselect_hyperslab(filespace, H5S_SELECT_SET, offset, NULL, dimsext, NULL);
    assert(status >= 0);
    
    // Define memory space        
    hsize_t  dimsr[1] = {1};
    hid_t memspace = H5Screate_simple(1, dimsr, NULL);
    
    /* Write the data to the extended portion of dataset  */
    datatype_description_t    wdata[1];
    /*
    * Initialize data.
    */
    wdata[0].id             = id;
    wdata[0].description    = datatype_desc;
    
    status = H5Dwrite(ds->dataset_id, ds->memtype, memspace, filespace, H5P_DEFAULT, wdata);
    assert(status >= 0);
    ds->count_of_rows = ds->count_of_rows + 1;
    // ***********************************************************************
    
    status = H5Sclose (filespace);
    assert(status >= 0);
    
    status = H5Sclose (memspace);
    assert(status >= 0);
}

void _iterator_descriptions(gpointer key, gpointer value, gpointer user_data)
{
    _add_description_to_datatype_description_ds( (char *) key, *(gint*) value);
}

int h5mr_consist_descriptions()
{
    if ( datatype_htbl == NULL) {
        return 0;
    }

    g_hash_table_foreach( datatype_htbl, (GHFunc)_iterator_descriptions, NULL);
    
    return 0;
}

int _get_or_create_datatype_id(MPI_Datatype datatype)
{ 
    char * datatype_desc = get_datatype_description(datatype);
    int * datatype_id    = g_hash_table_lookup(datatype_htbl, datatype_desc);

    if(datatype_id != NULL){
        return *datatype_id;
    }
    
    int new_id = g_hash_table_size(datatype_htbl) + 1;
    gint* v_id = g_new(gint, 1);
    *v_id = new_id;
    if (g_hash_table_insert(datatype_htbl, g_strdup (datatype_desc), v_id)) {
        return new_id;
    }
    
    fprintf(stderr, "trace_helper: A new description could not be created!\n");
    
    return -1;
}


/**********************************************************************************************************/

int _commit_communicator( MPI_Comm comm, int comm_id )
{
    if ( persistent_comm_htbl == NULL) {
        persistent_comm_htbl = g_hash_table_new(g_str_hash, g_str_equal);
    }

    int rlen;
    char comm_name[MPI_MAX_OBJECT_NAME];
    strcpy(comm_name, "");
    
    PMPI_Comm_get_name( comm, comm_name, &rlen );
    
    if(strcmp(comm_name, "" ) == 0 ){
 
        return -1;
    }
    
    int rank, size;
    MPI_Comm_size(comm, &size);
    MPI_Comm_rank(comm, &rank);
    
    process_communicator_t *comm_data = malloc(sizeof(process_communicator_t));
    comm_data->communicator_id  = comm_id;
    comm_data->rank             = rank;
    comm_data->size             = size;
    g_hash_table_replace ( persistent_comm_htbl, g_strdup (comm_name), comm_data );
    
    return 0;
}

hid_t _create_send_t_memorytype()
{
    herr_t status;

    hid_t memtype = H5Tcreate (H5T_COMPOUND, sizeof (send_t));
    status = H5Tinsert (memtype, RET, HOFFSET (send_t, ret), H5T_NATIVE_INT);
    assert(status >= 0);
    status = H5Tinsert (memtype, COUNT, HOFFSET (send_t, count), H5T_NATIVE_INT);
    assert(status >= 0);
    status = H5Tinsert (memtype, POSITION, HOFFSET (send_t, position), H5T_NATIVE_INT);
    assert(status >= 0);
    status = H5Tinsert (memtype, DATATYPE_ID, HOFFSET (send_t, datatype_id), H5T_NATIVE_INT);
    assert(status >= 0);
    status = H5Tinsert (memtype, DESTINATION, HOFFSET (send_t, destination), H5T_NATIVE_INT);
    assert(status >= 0);
    status = H5Tinsert (memtype, TAG, HOFFSET (send_t, tag), H5T_NATIVE_INT);
    assert(status >= 0);
    status = H5Tinsert (memtype, COMMUNICATOR_ID, HOFFSET (send_t, communicator_id), H5T_NATIVE_INT);
    assert(status >= 0);
    
    return memtype;
}

hid_t _create_send_t_filetype()
{
    herr_t status;

    hid_t filetype = H5Tcreate (H5T_COMPOUND, 8 + 8 + 8 + 8 + 8 + 8 + 8);
    status = H5Tinsert (filetype, RET, 0, H5T_STD_I64BE);
    assert(status >= 0);
    status = H5Tinsert (filetype, COUNT, 0 + 8, H5T_STD_I64BE);
    assert(status >= 0);
    status = H5Tinsert (filetype, POSITION, 0 + 8 + 8, H5T_STD_I64BE);
    assert(status >= 0);
    status = H5Tinsert (filetype, DATATYPE_ID, 0 + 8 + 8 + 8, H5T_STD_I64BE);
    assert(status >= 0);
    status = H5Tinsert (filetype, DESTINATION, 0 + 8 + 8 + 8 + 8, H5T_IEEE_F64BE);
    assert(status >= 0);
    status = H5Tinsert (filetype, TAG, 0 + 8 + 8 + 8 + 8 + 8, H5T_IEEE_F64BE);
    assert(status >= 0);
    status = H5Tinsert (filetype, COMMUNICATOR_ID, 0 + 8 + 8 + 8 + 8 + 8 + 8, H5T_IEEE_F64BE);
    assert(status >= 0);
    
    return filetype;
}

ds_info_t* _get_or_create_i_send_ds()
{
    if( i_send_operation != NULL ){
        return i_send_operation;
    }
    
    hid_t      filetype, memtype, dset;

    /*
    * Create the compound datatype for memory.      
    */
    memtype = _create_send_t_memorytype();
    
    /*
    * Create the compound datatype for the file.  Because the standard
    * types we are using for the file may have different sizes than
    * the corresponding native types, we must manually calculate the
    * offset of each member.
    */
    filetype = _create_send_t_filetype();
    
    hid_t   dcpl_id;
    /* Create the data space with unlimited dimensions. */
    hsize_t     dims[1] = {0};
    hsize_t     dims_e[1] = {H5S_UNLIMITED};
    hid_t space = H5Screate_simple(1, dims, dims_e);
    
    /* Modify dataset creation properties, i.e. enable chunking  */
    dcpl_id = H5Pcreate(H5P_DATASET_CREATE);
    hsize_t chunk_dims[1] = {1}; // DIM0
    H5Pset_chunk(dcpl_id, 1, chunk_dims);
    
    hid_t file_id = _get_or_create_file_id();
    
    dset = H5Dcreate2(file_id, I_SEND_DATASET_NAME, filetype, space, H5P_DEFAULT, dcpl_id, H5P_DEFAULT);
    
    i_send_operation = malloc(sizeof(ds_info_t));
    
    i_send_operation->dataset_id      = dset;
    i_send_operation->filetype        = filetype;
    i_send_operation->memtype         = memtype;
    i_send_operation->count_of_rows   = 0;
    // *************************************************
    
    herr_t status = H5Sclose ( space );
    assert(status >= 0);
    
    return i_send_operation;
}
/*
 * sending_type = 0 send
 * sending_type = 2 ISend
 */
ds_info_t* _get_or_create_send_ds()
{
   if( send_operation != NULL ){
        return send_operation;
    }
    
    // *************************************************
    hid_t      filetype, memtype, dset;
    
    /*
    * Create the compound datatype for memory.      
    */
    memtype = _create_send_t_memorytype();
    
    /*
    * Create the compound datatype for the file.  Because the standard
    * types we are using for the file may have different sizes than
    * the corresponding native types, we must manually calculate the
    * offset of each member.
    */
    filetype = _create_send_t_filetype();
    
    hid_t   dcpl_id;
    /* Create the data space with unlimited dimensions. */
    hsize_t     dims[1] = {0};
    hsize_t     dims_e[1] = {H5S_UNLIMITED};
    hid_t space = H5Screate_simple(1, dims, dims_e);
    
    /* Modify dataset creation properties, i.e. enable chunking  */
    dcpl_id = H5Pcreate(H5P_DATASET_CREATE);
    hsize_t chunk_dims[1] = {1}; // DIM0
    H5Pset_chunk(dcpl_id, 1, chunk_dims);
    
    hid_t file_id = _get_or_create_file_id();
    
    dset = H5Dcreate2(file_id, SEND_DATASET_NAME, filetype, space, H5P_DEFAULT, dcpl_id, H5P_DEFAULT);
    
    send_operation = malloc(sizeof(ds_info_t));
    
    send_operation->dataset_id      = dset;
    send_operation->filetype        = filetype;
    send_operation->memtype         = memtype;
    send_operation->count_of_rows   = 0;
    // *************************************************
    herr_t status = H5Sclose ( space );
    assert(status >= 0);

    return send_operation;
}


ds_info_t* _get_or_create_recv_ds()
{
    ds_info_t * ds_info_ptr = recv_operation;

    if(ds_info_ptr == NULL){
        // *************************************************
        hid_t      filetype, memtype, dset;
        herr_t      status;

        /*
        * Create the compound datatype for memory.      
        */
        memtype = create_recv_t_memorytype();
        
        /*
        * Create the compound datatype for the file.  Because the standard
        * types we are using for the file may have different sizes than
        * the corresponding native types, we must manually calculate the
        * offset of each member.
        */
        filetype = H5Tcreate (H5T_COMPOUND, 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8);
        status = H5Tinsert (filetype, RET, 0, H5T_STD_I64BE);
        assert(status >= 0);
        status = H5Tinsert (filetype, COUNT, 0 + 8, H5T_STD_I64BE);
        assert(status >= 0);
        status = H5Tinsert (filetype, ACTUALLY_RECEIVED, 0 + 8 + 8, H5T_STD_I64BE);
        assert(status >= 0);
        status = H5Tinsert (filetype, POSITION, 0 + 8 + 8 + 8, H5T_STD_I64BE);
        assert(status >= 0);
        status = H5Tinsert (filetype, DATATYPE_ID, 0 + 8 + 8 + 8 + 8, H5T_STD_I64BE);
        assert(status >= 0);
        status = H5Tinsert (filetype, SOURCE, 0 + 8 + 8 + 8 + 8 + 8, H5T_IEEE_F64BE);
        assert(status >= 0);
        status = H5Tinsert (filetype, TAG, 0 + 8 + 8 + 8 + 8 + 8 + 8, H5T_IEEE_F64BE);
        assert(status >= 0);
        status = H5Tinsert (filetype, COMMUNICATOR_ID, 0 + 8 + 8 + 8 + 8 + 8 + 8 + 8, H5T_IEEE_F64BE);
        assert(status >= 0);
        status = H5Tinsert (filetype, FETCHED_UP, 0 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8, H5T_IEEE_F64BE);
        assert(status >= 0);

        hid_t   dcpl_id;
        /* Create the data space with unlimited dimensions. */
        hsize_t     dims[1] = {0};
        hsize_t     dims_e[1] = {H5S_UNLIMITED};
        hid_t space = H5Screate_simple(1, dims, dims_e);

        /* Modify dataset creation properties, i.e. enable chunking  */
        dcpl_id = H5Pcreate(H5P_DATASET_CREATE);
        hsize_t chunk_dims[1] = {1}; // DIM0
        H5Pset_chunk(dcpl_id, 1, chunk_dims);

        hid_t file_id = _get_or_create_file_id();
        
        dset = H5Dcreate2(file_id, RECV_DATASET_NAME, filetype, space, H5P_DEFAULT, dcpl_id, H5P_DEFAULT);

        ds_info_t *ds = malloc(sizeof(ds_info_t));

        ds->dataset_id      = dset;
        ds->filetype        = filetype;
        ds->memtype         = memtype;
        ds->count_of_rows   = 0;
        // *************************************************
        recv_operation = ds;
        
        status = H5Sclose ( space );
        assert(status >= 0);
        
        return recv_operation;
    }

    return ds_info_ptr;
}

ds_info_t *make_datatype_table(MPI_Datatype typ, char * ds_name)
{
    MPI_Aint extent;
    MPI_Type_extent(typ, &extent);

    /*
    * We need the same memtype to read the dataset because the one used in the creation of the dataset contains the type info(s)
    * and as a result can convert between scr and dest datatype
    */
    hid_t  dataset_id;
    hid_t  memtype;
    hid_t  filetype;

    hid_t file_id = _get_or_create_file_id();
    create_datatype_table(typ, &file_id, ds_name, &dataset_id, &memtype, &filetype, extent);
    ds_info_t *dt_ds = malloc(sizeof(ds_info_t));

    // Must allocate memory for contents of pointers.  Here, strdup()
    // creates a new copy of name.  Another option:
    // wrap->file_name = malloc(strlen(FILE_NAME)+1);
    // strcpy(wrap->file_name, FILE_NAME);
    dt_ds->dataset_id       = dataset_id;
    dt_ds->memtype          = memtype;
    dt_ds->filetype         = filetype;
    dt_ds->count_of_rows    = 0;

    return (ds_info_t *) dt_ds;
}

/*
* @return: int position: number of the row where the data committed
*/
int h5mr_append_buffer_to_datatype_ds( MPI_Datatype datatype, const void * buf, int count )
{
    if ( ! is_data_structures_initialized ){
        int ret = _init_data_structures();
        assert (ret == 0);
    }
    
    // Find the dataset name that corresponded to the datatype
    
    int dID = _get_or_create_datatype_id(datatype);
    assert (dID > 0);
    // int cID       = find_comm_id(comm);
    
    char datatype_ds[1024] = "";
    sprintf(datatype_ds, "%s_%d", DATATYPE_DATASET_NAME, dID);

    ds_info_t * dt_ds_ptr = g_hash_table_lookup(mpi_datatype_ds_htbl, datatype_ds);

    if ( dt_ds_ptr == NULL ){
        dt_ds_ptr = make_datatype_table(datatype, datatype_ds);
        // insert the values into the hashtable
        g_hash_table_insert(mpi_datatype_ds_htbl, g_strdup(datatype_ds), dt_ds_ptr);
    }
    assert (dt_ds_ptr != NULL);
    
    herr_t      status;

    hsize_t size[1];
    hsize_t offset[1];
    hsize_t dimsext[1];
    hsize_t dimsr[1];

    int size_of_buffer = H5Tget_size (dt_ds_ptr->memtype);

    int position = dt_ds_ptr->count_of_rows;
    
    if ( buf == NULL ){
        return -1;
    }
    
    for(int i = 0; i < count; i++){
        /* Extend the dataset. Dataset becomes 2 x 19  = row * column
        * row has to be added by 1(or count) every time
        */
        //    hsize_t     size[2] = {2 ,19};
        //    hsize_t     size[1] = {1};
        //    hsize_t     size[1] ;
        size[0] = dt_ds_ptr->count_of_rows + 1;
        //    size[0] = dims[0]+ dimsext[0];
        //    size[1] = dims[1];
        status = H5Dset_extent (dt_ds_ptr->dataset_id, size);
        assert(status >= 0);

        hid_t filespace = H5Dget_space(dt_ds_ptr->dataset_id);

        offset[0]   = dt_ds_ptr->count_of_rows;
        dimsext[0]  = 1;
        status = H5Sselect_hyperslab(filespace, H5S_SELECT_SET, offset, NULL, dimsext, NULL);
        assert(status >= 0);

        /* Define memory space
        */
        dimsr[0] = 1;
        hid_t memspace = H5Screate_simple(1, dimsr, NULL);

        /* Write the data to the extended portion of dataset  */

        status = H5Dwrite(dt_ds_ptr->dataset_id, dt_ds_ptr->memtype, memspace, filespace, H5P_DEFAULT, buf+(i*size_of_buffer));
        assert(status >= 0);
        dt_ds_ptr->count_of_rows = dt_ds_ptr->count_of_rows + 1;
        
        status = H5Sclose (filespace);
        assert(status >= 0);
        status = H5Sclose (memspace);
        assert(status >= 0);
    }
    
    return position;
}

//******************************************************************************************************************************

/*
* sending_operation = 0 Send Dataset
* sending_operation = 2 Isend Dataset
*/
int h5mr_write_send_log(int sending_operation, int ret, int count, int position, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm)
{
    if ( ! is_data_structures_initialized ){
        int ret = _init_data_structures();
        assert (ret == 0);
    }
    
    int dID = _get_or_create_datatype_id(datatype);
    assert (dID > 0);
    int cID = find_comm_id(comm);
    assert (cID > 0);
    
    ret = _commit_communicator( comm, cID );
    assert (ret == 0);
   
    ds_info_t *ds = NULL;
    if (sending_operation == 0) {
        ds = _get_or_create_send_ds();
    } else if (sending_operation == 2) {
        ds = _get_or_create_i_send_ds();   
    }
    assert (ds != NULL);
    
    
    // ********************** write_send_log *********************************
    herr_t      status;
    /* Extend the dataset. Dataset becomes 2 x 19  = row * column
    * row has to be added by 1(or count) every time
    */
    //    hsize_t     size[2] = {2 ,19};
    //    hsize_t     size[1] = {1};
    hsize_t     size[1] ;

    size[0] = ds->count_of_rows + 1;
    //    size[0] = dims[0]+ dimsext[0];
    //    size[1] = dims[1];
    status = H5Dset_extent (ds->dataset_id, size);
    assert(status >= 0);

    hid_t filespace     = H5Dget_space(ds->dataset_id);
    hsize_t offset[1]   = {ds->count_of_rows};
    hsize_t dimsext[1]  = {1};
    status = H5Sselect_hyperslab(filespace, H5S_SELECT_SET, offset, NULL, dimsext, NULL);
    assert(status >= 0);

    /* Define memory space
    */

    hsize_t  dimsr[1] = {1};
    hid_t memspace = H5Screate_simple(1, dimsr, NULL);

    /* Write the data to the extended portion of dataset  */
    send_t    wdata[1];                /* Write buffer */
    /*
    * Initialize data.
    */
    wdata[0].ret                = ret;
    wdata[0].count              = count;
    wdata[0].position           = position;
    wdata[0].datatype_id        = dID;
    wdata[0].destination        = dest;
    wdata[0].tag                = tag;
    wdata[0].communicator_id    = cID;
  
    status = H5Dwrite(ds->dataset_id, ds->memtype, memspace, filespace, H5P_DEFAULT, wdata);
    assert(status >= 0);
    
    ds->count_of_rows = ds->count_of_rows + 1;


    status = H5Sclose (filespace);
    assert(status >= 0);
    status = H5Sclose (memspace);
    assert(status >= 0);
    
    return 0;
}

int h5mr_write_recv_log(int ret, int count, int actually_received, int position, MPI_Datatype datatype, int source, int tag, MPI_Comm comm)
{
    if ( ! is_data_structures_initialized ){
        int ret = _init_data_structures();
        assert (ret == 0);
    }
    
    int dID = _get_or_create_datatype_id(datatype);
    assert (dID > 0);
    int cID = find_comm_id(comm);
    assert (cID > 0);
    ret = _commit_communicator( comm, cID );
    assert (ret == 0);
    
    ds_info_t * ds = _get_or_create_recv_ds();

    // ********************** write_recv_log *********************************
    herr_t      status;
    /* Extend the dataset. Dataset becomes 2 x 19  = row * column
    * row has to be added by 1(or count) every time
    */
    //    hsize_t     size[2] = {2 ,19};
    //    hsize_t     size[1] = {1};
    hsize_t     size[1] ;

    size[0] = ds->count_of_rows + 1;
    //    size[0] = dims[0]+ dimsext[0];
    //    size[1] = dims[1];
    status = H5Dset_extent (ds->dataset_id, size);
    assert(status >= 0);

    hid_t filespace     = H5Dget_space(ds->dataset_id);
    hsize_t offset[1]   = {ds->count_of_rows};
    hsize_t dimsext[1]  = {1};
    status = H5Sselect_hyperslab(filespace, H5S_SELECT_SET, offset, NULL, dimsext, NULL);
    assert(status >= 0);

    /* Define memory space
    */

    hsize_t  dimsr[1] = {1};
    hid_t memspace = H5Screate_simple(1, dimsr, NULL);

    /* Write the data to the extended portion of dataset  */
    recv_t    wdata[1];                /* Write buffer */
    /*
    * Initialize data.
    */
    wdata[0].ret                = ret;
    wdata[0].count              = count;
    wdata[0].actually_received  = actually_received;
    wdata[0].position           = position;
    wdata[0].datatype_id        = dID;
    wdata[0].source             = source;
    wdata[0].tag                = tag;
    wdata[0].communicator_id    = cID;
    wdata[0].fetched_up         = 0;
      
    status = H5Dwrite(ds->dataset_id, ds->memtype, memspace, filespace, H5P_DEFAULT, wdata);
    assert(status >= 0);
    ds->count_of_rows = ds->count_of_rows + 1;
    
    status = H5Sclose (filespace);
    assert(status >= 0);
    status = H5Sclose (memspace);
    assert(status >= 0);
    
    return 0;
}

//******************************************************************************************************************************
//******************************************************************************************************************************
//******************************************************************************************************************************

int h5mr_register_receive_request(void *buf, int ret, int count, MPI_Datatype datatype, int source, int tag, MPI_Comm comm, MPI_Request* request)
{
    if ( ! is_data_structures_initialized ){
        int ret = _init_data_structures();
        assert (ret == 0);
    }
    
    if ( i_recv_request_htbl == NULL) {
        i_recv_request_htbl = g_hash_table_new(g_direct_hash, g_direct_equal);
    }
    
    i_recv_t *ds = malloc(sizeof(i_recv_t));
    recv_t * ptr_metadata = (recv_t *) malloc (sizeof (recv_t));
    ptr_metadata->ret               = ret;
    ptr_metadata->count             = count;
    ptr_metadata->actually_received = -1;
    ptr_metadata->position          = -1;
    ptr_metadata->datatype_id       = _get_or_create_datatype_id(datatype);
    ptr_metadata->source            = source;
    ptr_metadata->tag               = tag;
    ptr_metadata->communicator_id   = find_comm_id(comm);
    ptr_metadata->fetched_up        = 0;
    
    ds->ptr_metadata = ptr_metadata;
    
    //    MPI_Aint extent;
    //    MPI_Type_extent(datatype, &extent);
    //    ds->buf = malloc (extent * count);

    // Allocating space for this buffer is not necessary, because this is the user responsibility
    ds->buf = buf;
    
    ds->datatype = (MPI_Datatype) malloc(sizeof(datatype));
    ds->datatype = datatype;
    
    if (g_hash_table_insert( i_recv_request_htbl, (void *) request, ds)){
        return 0;
    }
    
    return -1;
}

ds_info_t* _get_or_create_i_recv_ds()
{
    if (i_recv_operation != NULL){
        return i_recv_operation;
    }
    
    /*
    * Create the compound datatype for memory.      
    */
    hid_t memtype = create_recv_t_memorytype();
    
    /*
    * Create the compound datatype for the file.  Because the standard
    * types we are using for the file may have different sizes than
    * the corresponding native types, we must manually calculate the
    * offset of each member.
    */
    hid_t filetype  = H5Tcreate (H5T_COMPOUND, 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8);
    herr_t status   = H5Tinsert (filetype, RET, 0, H5T_STD_I64BE);
    assert(status >= 0);
    status = H5Tinsert (filetype, COUNT, 0 + 8, H5T_STD_I64BE);
    assert(status >= 0);
    status = H5Tinsert (filetype, ACTUALLY_RECEIVED, 0 + 8 + 8, H5T_STD_I64BE);
    assert(status >= 0);
    status = H5Tinsert (filetype, POSITION, 0 + 8 + 8 + 8, H5T_STD_I64BE);
    assert(status >= 0);
    status = H5Tinsert (filetype, DATATYPE_ID, 0 + 8 + 8 + 8 + 8, H5T_STD_I64BE);
    assert(status >= 0);
    status = H5Tinsert (filetype, SOURCE, 0 + 8 + 8 + 8 + 8 + 8, H5T_IEEE_F64BE);
    assert(status >= 0);
    status = H5Tinsert (filetype, TAG, 0 + 8 + 8 + 8 + 8 + 8 + 8, H5T_IEEE_F64BE);
    assert(status >= 0);
    status = H5Tinsert (filetype, COMMUNICATOR_ID, 0 + 8 + 8 + 8 + 8 + 8 + 8 + 8, H5T_IEEE_F64BE);
    assert(status >= 0);
    status = H5Tinsert (filetype, FETCHED_UP, 0 + 8 + 8 + 8 + 8 + 8 + 8 + 8 + 8, H5T_IEEE_F64BE);
    assert(status >= 0);
    
    hid_t   dcpl_id;
    /* Create the data space with unlimited dimensions. */
    hsize_t     dims[1] = {0};
    hsize_t     dims_e[1] = {H5S_UNLIMITED};
    hid_t space = H5Screate_simple(1, dims, dims_e);
    
    /* Modify dataset creation properties, i.e. enable chunking  */
    dcpl_id = H5Pcreate(H5P_DATASET_CREATE);
    hsize_t chunk_dims[1] = {1}; // DIM0
    H5Pset_chunk(dcpl_id, 1, chunk_dims);
    
    hid_t file_id = _get_or_create_file_id();
    
    hid_t dset = H5Dcreate2(file_id, I_RECV_DATASET_NAME, filetype, space, H5P_DEFAULT, dcpl_id, H5P_DEFAULT);
    
    i_recv_operation = malloc(sizeof(ds_info_t));
    
    i_recv_operation->dataset_id      = dset;
    i_recv_operation->filetype        = filetype;
    i_recv_operation->memtype         = memtype;
    i_recv_operation->count_of_rows   = 0;
    // *************************************************
    
    status = H5Sclose ( space );
    assert(status >= 0);
    
    return i_recv_operation;
}

int _write_i_recv_log (i_recv_t * i_recv_data)
{
    ds_info_t * ds = _get_or_create_i_recv_ds();

    herr_t      status;
    /* Extend the dataset. Dataset becomes 2 x 19  = row * column
    * row has to be added by 1(or count) every time
    */
    //    hsize_t     size[2] = {2 ,19};
    //    hsize_t     size[1] = {1};
    hsize_t     size[1] ;

    size[0] = ds->count_of_rows + 1;
    //    size[0] = dims[0]+ dimsext[0];
    //    size[1] = dims[1];
    status = H5Dset_extent (ds->dataset_id, size);
    assert(status >= 0);

    hid_t filespace     = H5Dget_space(ds->dataset_id);
    hsize_t offset[1]   = {ds->count_of_rows};
    hsize_t dimsext[1]  = {1};
    status = H5Sselect_hyperslab(filespace, H5S_SELECT_SET, offset, NULL, dimsext, NULL);
    assert(status >= 0);

    /* Define memory space
    */

    hsize_t  dimsr[1] = {1};
    hid_t memspace = H5Screate_simple(1, dimsr, NULL);

    /* Write the data to the extended portion of dataset  */
    recv_t    wdata[1];                /* Write buffer */
    /*
    * Initialize data.
    */
    wdata[0].ret                = i_recv_data->ptr_metadata->ret;
    wdata[0].count              = i_recv_data->ptr_metadata->count;
    wdata[0].actually_received  = i_recv_data->ptr_metadata->actually_received;
    wdata[0].position           = i_recv_data->ptr_metadata->position;
    wdata[0].datatype_id        = i_recv_data->ptr_metadata->datatype_id;
    wdata[0].source             = i_recv_data->ptr_metadata->source;
    wdata[0].tag                = i_recv_data->ptr_metadata->tag;
    wdata[0].communicator_id    = i_recv_data->ptr_metadata->communicator_id;
    wdata[0].fetched_up         = i_recv_data->ptr_metadata->fetched_up;
      
    status = H5Dwrite(ds->dataset_id, ds->memtype, memspace, filespace, H5P_DEFAULT, wdata);
    assert(status >= 0);
    ds->count_of_rows = ds->count_of_rows + 1;
    
    status = H5Sclose (filespace);
    assert(status >= 0);
    status = H5Sclose (memspace);
    assert(status >= 0);
    
    return 0;
}

int h5mr_commit_i_recv(MPI_Request* request)
{
    if ( i_recv_request_htbl == NULL) {
        return 0;
    }
    
    i_recv_t* value = g_hash_table_lookup(i_recv_request_htbl, request);
    if (value == NULL){
        return 0;
    }
    
    int flag = 0;    
    MPI_Status status;
    MPI_Request_get_status( *request, &flag, &status );
    
    int actually_received = 0;
    MPI_Get_count(&status, ((i_recv_t *) value)->datatype, & actually_received);

    int position = -1;
    if (actually_received > 0){
        void* buf = ((i_recv_t *) value)->buf;        
        position = h5mr_append_buffer_to_datatype_ds( ((i_recv_t *) value)->datatype, buf, actually_received );
        assert (position >= 0);
    }
    
    ((i_recv_t *) value)->ptr_metadata->ret                 = 0;
    ((i_recv_t *) value)->ptr_metadata->actually_received   = actually_received;
    ((i_recv_t *) value)->ptr_metadata->position            = position;
    ((i_recv_t *) value)->ptr_metadata->fetched_up          = 0;
    
    int success = _write_i_recv_log( (i_recv_t *) value);
    assert (success == 0);
    
    if(g_hash_table_remove (i_recv_request_htbl, request)){
        return 0;        
    }
    
    return -1;
}

//******************************************************************************************************************************
//******************************************************************************************************************************
//******************************************************************************************************************************

hid_t _create_record_log_t_filetype()
{
    herr_t status;

    hid_t strtype = H5Tcopy (H5T_C_S1);
    status = H5Tset_size (strtype, H5T_VARIABLE);
    assert(status >= 0);
    
    hid_t filetype = H5Tcreate (H5T_COMPOUND, sizeof (hvl_t) + 8 + 8 + 8);
    status = H5Tinsert (filetype, UNIQUE_KEY, 0, strtype);
    assert(status >= 0);
    status = H5Tinsert (filetype, DATATYPE_ID, 0 + sizeof (hvl_t), H5T_STD_I64BE);
    assert(status >= 0);
    status = H5Tinsert (filetype, COUNT, 0 + sizeof (hvl_t) + 8, H5T_STD_I64BE);
    assert(status >= 0);
    status = H5Tinsert (filetype, POSITION, 0 + sizeof (hvl_t) + 8 + 8, H5T_STD_I64BE);
    assert(status >= 0);
    
    return filetype;
}

ds_info_t* _get_or_create_record_log_ds()
{
   if( record_log != NULL ){
        return record_log;
    }
    
    // *************************************************
    hid_t      filetype, memtype, dset;
    
    /*
    * Create the compound datatype for memory.      
    */
    memtype = create_record_log_t_memorytype();
    
    /*
    * Create the compound datatype for the file.  Because the standard
    * types we are using for the file may have different sizes than
    * the corresponding native types, we must manually calculate the
    * offset of each member.
    */
    filetype = _create_record_log_t_filetype();
    
    hid_t   dcpl_id;
    /* Create the data space with unlimited dimensions. */
    hsize_t     dims[1] = {0};
    hsize_t     dims_e[1] = {H5S_UNLIMITED};
    hid_t space = H5Screate_simple(1, dims, dims_e);
    
    /* Modify dataset creation properties, i.e. enable chunking  */
    dcpl_id = H5Pcreate(H5P_DATASET_CREATE);
    hsize_t chunk_dims[1] = {1}; // DIM0
    H5Pset_chunk(dcpl_id, 1, chunk_dims);
    
    hid_t file_id = _get_or_create_file_id();
    
    dset = H5Dcreate2(file_id, RECORD_LOG_DATASET_NAME, filetype, space, H5P_DEFAULT, dcpl_id, H5P_DEFAULT);
    
    record_log = malloc(sizeof(ds_info_t));
    
    record_log->dataset_id      = dset;
    record_log->filetype        = filetype;
    record_log->memtype         = memtype;
    record_log->count_of_rows   = 0;
    // *************************************************
    herr_t status = H5Sclose ( space );
    assert(status >= 0);

    return record_log;
}


int h5mr_write_record_log (const char* unique_key, MPI_Datatype datatype, int count, int position)
{
    int dID = _get_or_create_datatype_id(datatype);
    assert (dID > 0);
    
    ds_info_t *ds = _get_or_create_record_log_ds();
    assert (ds != NULL);
    
    herr_t      status;
    /* Extend the dataset. Dataset becomes 2 x 19  = row * column
    * row has to be added by 1(or count) every time
    */
    //    hsize_t     size[2] = {2 ,19};
    //    hsize_t     size[1] = {1};
    hsize_t     size[1] ;

    size[0] = ds->count_of_rows + 1;
    //    size[0] = dims[0]+ dimsext[0];
    //    size[1] = dims[1];
    status = H5Dset_extent (ds->dataset_id, size);
    assert(status >= 0);

    hid_t filespace     = H5Dget_space(ds->dataset_id);
    hsize_t offset[1]   = {ds->count_of_rows};
    hsize_t dimsext[1]  = {1};
    status = H5Sselect_hyperslab(filespace, H5S_SELECT_SET, offset, NULL, dimsext, NULL);
    assert(status >= 0);

    /* Define memory space
    */

    hsize_t  dimsr[1] = {1};
    hid_t memspace = H5Screate_simple(1, dimsr, NULL);

    /* Write the data to the extended portion of dataset  */
    record_log_t    wdata[1];                /* Write buffer */
    /*
    * Initialize data.
    */
    wdata[0].unique_key     = malloc(sizeof(unique_key));
    wdata[0].unique_key     = unique_key;
    wdata[0].datatype_id    = dID;
    wdata[0].count          = count;
    wdata[0].position       = position;
  
    status = H5Dwrite(ds->dataset_id, ds->memtype, memspace, filespace, H5P_DEFAULT, wdata);
    assert(status >= 0);
    
    ds->count_of_rows = ds->count_of_rows + 1;


    status = H5Sclose (filespace);
    assert(status >= 0);
    status = H5Sclose (memspace);
    assert(status >= 0);
    
    return 0;
}