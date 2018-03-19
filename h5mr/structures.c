#include "structures.h"

GHashTable* comm_htbl               = NULL;
GHashTable* dummy_comm_htbl         = NULL;
GHashTable* persistent_comm_htbl    = NULL;

void h5mr_intitial_dummy_data_structures()
{
    if ( comm_htbl == NULL) {
        comm_htbl = g_hash_table_new(g_str_hash, g_str_equal);
    }
    
    if ( dummy_comm_htbl == NULL) {
        dummy_comm_htbl = g_hash_table_new(g_str_hash, g_str_equal);
    }
    
    MPI_Comm    default_communicators[2] = { MPI_COMM_SELF, MPI_COMM_WORLD };
    const char * default_communicator_names[2] = { "MPI_COMM_SELF", "MPI_COMM_WORLD" };
    for (int i = 0; i < 2; i++){
        char name[MPI_MAX_OBJECT_NAME];
        strcpy(name, "");

        sprintf( name, "comm-%d", ++last_comm_id );
        int ret = PMPI_Comm_set_name( default_communicators[i], name );
        assert ( ret == MPI_SUCCESS );
              
        g_hash_table_insert( comm_htbl, g_strdup (name), (void *) last_comm_id);
        g_hash_table_insert( dummy_comm_htbl, g_strdup (name), g_strdup (default_communicator_names[i]));
    }
    
}

hid_t create_process_communicator_t_memorytype()
{
    herr_t status;
    hid_t  memtype = H5Tcreate (H5T_COMPOUND, sizeof (process_communicator_t));
    status = H5Tinsert (memtype, COMMUNICATOR_ID, HOFFSET (process_communicator_t, communicator_id), H5T_NATIVE_INT);
    assert(status >= 0);
    status = H5Tinsert (memtype, RANK, HOFFSET (process_communicator_t, rank), H5T_NATIVE_INT);
    assert(status >= 0);
    status = H5Tinsert (memtype, SIZE, HOFFSET (process_communicator_t, size), H5T_NATIVE_INT);
    assert(status >= 0);
    
    return memtype;
}

void h5mr_free_dummy_data_structures()
{
    g_hash_table_destroy( comm_htbl );
    g_hash_table_destroy( dummy_comm_htbl );
}

// return communicator ID from the hash table
int find_comm_id(MPI_Comm comm)
{
    if ( comm_htbl == NULL) {
        return -1;
    }
    
    int rlen;
    char name[MPI_MAX_OBJECT_NAME];
    strcpy(name, "");
    
    PMPI_Comm_get_name( comm, name, &rlen );
    
    if(strcmp(name, "" ) == 0 ){
 
        return -1;
    }
    
    int * comm_id = g_hash_table_lookup(comm_htbl, name);
    
    return (int) ((size_t) comm_id);
}


hid_t create_record_log_t_memorytype()
{
    herr_t status;
    /*
     * Create variable-length string datatype.
     */
    hid_t strtype = H5Tcopy (H5T_C_S1);
    status = H5Tset_size (strtype, H5T_VARIABLE);
    assert(status >= 0);
    
    hid_t memtype = H5Tcreate (H5T_COMPOUND, sizeof (record_log_t));
    status = H5Tinsert (memtype, UNIQUE_KEY, HOFFSET (record_log_t, unique_key), strtype);
    assert(status >= 0);
    status = H5Tinsert (memtype, DATATYPE_ID, HOFFSET (record_log_t, datatype_id), H5T_NATIVE_INT);
    assert(status >= 0);
    status = H5Tinsert (memtype, COUNT, HOFFSET (record_log_t, count), H5T_NATIVE_INT);
    assert(status >= 0);
    status = H5Tinsert (memtype, POSITION, HOFFSET (record_log_t, position), H5T_NATIVE_INT);
    assert(status >= 0);
    
    return memtype;
}

hid_t create_recv_t_memorytype()
{
    herr_t status;
    
    hid_t  memtype = H5Tcreate (H5T_COMPOUND, sizeof (recv_t));
    status = H5Tinsert (memtype, RET, HOFFSET (recv_t, ret), H5T_NATIVE_INT);
    assert(status >= 0);
    status = H5Tinsert (memtype, COUNT, HOFFSET (recv_t, count), H5T_NATIVE_INT);
    assert(status >= 0);
    status = H5Tinsert (memtype, ACTUALLY_RECEIVED, HOFFSET (recv_t, actually_received), H5T_NATIVE_INT);
    assert(status >= 0);
    status = H5Tinsert (memtype, POSITION, HOFFSET (recv_t, position), H5T_NATIVE_INT);
    assert(status >= 0);
    status = H5Tinsert (memtype, DATATYPE_ID, HOFFSET (recv_t, datatype_id), H5T_NATIVE_INT);
    assert(status >= 0);
    status = H5Tinsert (memtype, SOURCE, HOFFSET (recv_t, source), H5T_NATIVE_INT);
    assert(status >= 0);
    status = H5Tinsert (memtype, TAG, HOFFSET (recv_t, tag), H5T_NATIVE_INT);
    assert(status >= 0);
    status = H5Tinsert (memtype, COMMUNICATOR_ID, HOFFSET (recv_t, communicator_id), H5T_NATIVE_INT);
    assert(status >= 0);
    status = H5Tinsert (memtype, FETCHED_UP, HOFFSET (recv_t, fetched_up), H5T_NATIVE_INT);
    assert(status >= 0);
    
    return memtype;
}


/*
* Add communicator to the both hashtables: comm_ht and dummy_ht
* @newcomm is always a new created communicator.
*/
int h5mr_register_communicator( MPI_Comm newcomm )
{
    if ( comm_htbl == NULL) {
        return -1;
    }
    
    if ( dummy_comm_htbl == NULL) {
        return -1;
    }
    
    char certified_comm_name[MPI_MAX_OBJECT_NAME];
    strcpy( certified_comm_name, "" );
        
    if (strcmp( certified_comm_name, "" ) == 0 ) {
        sprintf( certified_comm_name, "comm-%d", ++last_comm_id );
    }

    if (! g_hash_table_insert( dummy_comm_htbl, g_strdup (certified_comm_name), "") ){
        return -1;
    }
    
    PMPI_Comm_set_name( newcomm, certified_comm_name );
    
    if (! g_hash_table_insert(comm_htbl, g_strdup (certified_comm_name), (void*) last_comm_id) ){
        return -1;
    }
    
    return 0;
}


int h5mr_comm_change_name(MPI_Comm comm, const char *new_name)
{
    if ( comm_htbl == NULL) {
        return -1;
    }
    
    if ( dummy_comm_htbl == NULL) {
        return -1;
    } 
       
    int rlen;
    char certified_name[MPI_MAX_OBJECT_NAME];
    strcpy(certified_name, "");
    
    PMPI_Comm_get_name( comm, certified_name, &rlen );
    
    int * comm_id = g_hash_table_lookup(comm_htbl, certified_name);
    if( comm_id == NULL ){
        return -1;
    }
    
    g_hash_table_replace( dummy_comm_htbl, g_strdup (certified_name), g_strdup (new_name));
        
    return 0;
}

