#include "datatype_table.h"

static void _mpi_hdf5_mapper(MPI_Datatype typ, int* type_serial_no, hid_t *memtype, size_t *offset, hid_t *filetype)
{
    hid_t   hdf5_datatype;
    size_t  hdf5_datatype_size;
    
    int mpi_datatype_size;
    MPI_Type_size(typ, &mpi_datatype_size);
    
    char temp[1000];
    char type_name[50];
    
    if(typ == MPI_INT){
        sprintf(type_name, "INT");
        hdf5_datatype_size = H5Tget_size(H5T_NATIVE_INT);
        hdf5_datatype = H5T_NATIVE_INT;
    }else if(typ == MPI_FLOAT){
        sprintf(type_name, "FLOAT");
        hdf5_datatype_size = H5Tget_size(H5T_NATIVE_FLOAT);
        hdf5_datatype = H5T_NATIVE_FLOAT;
    }else if(typ == MPI_DOUBLE){
        sprintf(type_name, "DOUBLE");
        hdf5_datatype_size = H5Tget_size(H5T_NATIVE_DOUBLE);
        hdf5_datatype = H5T_NATIVE_DOUBLE;
    }else if(typ == MPI_CHAR){
        sprintf(type_name, "CHAR");
        hdf5_datatype_size = H5Tget_size(H5T_NATIVE_CHAR);
        hdf5_datatype = H5T_NATIVE_CHAR;
    }else if(typ == MPI_BYTE){
        sprintf(type_name, "BYTE");
        hdf5_datatype_size = H5Tget_size(H5T_NATIVE_B8);
        hdf5_datatype = H5T_NATIVE_B8;
    }else{
        printf("Error: unsupported basic data type\n");
    }

    // **************************************************************            
    (*type_serial_no)++;
    sprintf(temp, "%s_%d", type_name, *type_serial_no);
    herr_t      status;
    status = H5Tinsert (*memtype, temp, *offset, hdf5_datatype);
    assert(status >= 0);
    status = H5Tinsert (*filetype, temp, *offset, hdf5_datatype);
    assert(status >= 0);
    *offset = *offset + hdf5_datatype_size;
    // **************************************************************
}

static void _get_datatype_details_i(MPI_Datatype typ, int* type_serial_no, hid_t *memtype, hid_t *filetype, size_t *offset)
{
    int ret;
    int num_integers, num_addresses, num_datatypes, combiner;
    ret = MPI_Type_get_envelope(typ, & num_integers, & num_addresses, & num_datatypes, & combiner);

    CHECK_RET(ret)
        debug("%d %d %d %d", num_integers, num_addresses, num_datatypes, combiner);

    if( combiner == MPI_COMBINER_NAMED ){
        _mpi_hdf5_mapper(typ, type_serial_no, memtype, offset, filetype);
        return;
    }

    int integers[num_integers];
    MPI_Aint addresses[num_addresses];
    MPI_Datatype datatypes[num_datatypes];

    ret = MPI_Type_get_contents(typ, num_integers, num_addresses, num_datatypes, integers, addresses, datatypes);
    CHECK_RET(ret)

        for(int i=0; i < num_integers; i++){
            debug("Count: %d", integers[i]);
        }

        for(int i=0; i < num_addresses; i++){
            debug("Address: %zu", (size_t) addresses[i]);
        }

    switch(combiner){
            
        case(MPI_COMBINER_CONTIGUOUS):
            {
                for(int i = integers[0]; i > 0; i--){
                    _get_datatype_details_i(datatypes[0], type_serial_no, memtype, filetype, offset);            
                }
                break;
            }

        case(MPI_COMBINER_VECTOR):
            {
                /**
                vector  = N * Block
                        = N * (block_len + stride)
                        = N * (block_len + stride_len)
                */
           
                for(int i = 1; i <= integers[0]; i++){ // count of blocks
                    for(int j = 1; j <= integers[1]; j++){ // Blocklength
                        _get_datatype_details_i(datatypes[0], type_serial_no, memtype, filetype, offset);                    
                    }
                
                    // stride_len = Stride - Blocklength
                    // if it were the last block there is no stride
                    // It could give a failure because the difference between mpi_datatypes size and HDF5 datatypes size
                    if (i < integers[0]){
                        int mpi_datatype_size;
                        MPI_Type_size(datatypes[0], &mpi_datatype_size);
                        for(int j = 1; j <= integers[2] - integers[1]; j++){ // stride_len = Stride - Blocklength
                            *offset = *offset + mpi_datatype_size;
                        }
                    }
                }
                break;
            }

        case(MPI_COMBINER_STRUCT):
        case(MPI_COMBINER_STRUCT_INTEGER):
            {
                //**********************************//
                // The displacements are byte-sized //
                //**********************************//
             
                // Save the initial offset
                int initial_offset = *offset;
                    
                for(int i=0; i < integers[0]; i++){ // count of blocks
                    *offset = initial_offset + addresses[i];
                    int block_size = 0;
                    int mpi_datatype_size;
                    for(int j = 0; j < integers[i+1]; j++){ // Blocklength
                        MPI_Type_size(datatypes[i], &mpi_datatype_size);
                        _get_datatype_details_i(datatypes[i], type_serial_no, memtype, filetype, offset);
                        block_size = block_size + mpi_datatype_size;
                    }
                }
                break;
            }
            
            //**********************************************************************************//
            //**********************************************************************************//
            
        default:
            {
                printf("ERROR Unsupported combiner: %d\n", combiner);
            }
    }
}


void create_datatype_table(MPI_Datatype typ, hid_t * file_id, char * DATASET_NAME, hid_t * dataset_id, hid_t * memtype, hid_t * filetype, size_t total_size)
{
    size_t      offset = 0;

    /*
     * Create the compound datatype for memory.
     */
    *memtype = H5Tcreate (H5T_COMPOUND, total_size);
       
    /*
     * Create the compound datatype for the file.  Because the standard
     * types we are using for the file may have different sizes than
     * the corresponding native types, we must manually calculate the
     * offset of each member.
     */
    *filetype = H5Tcreate (H5T_COMPOUND, total_size);
    
    int type_serial_no = 0;

    _get_datatype_details_i(typ, &type_serial_no, memtype, filetype, &offset);

    /* Create the data space with unlimited dimensions. */
    hsize_t dims[1]     = {0};
    hsize_t dims_e[1]   = {H5S_UNLIMITED};
    
    hid_t space = H5Screate_simple(1, dims, dims_e);    
    
    /* Modify dataset creation properties, i.e. enable chunking  */
    hid_t dcpl_id = H5Pcreate(H5P_DATASET_CREATE);
    hsize_t chunk_dims[1] = {1};
    H5Pset_chunk(dcpl_id, 1, chunk_dims);

    /*
     * Create the dataset and write the compound data to it.
     */
    *dataset_id = H5Dcreate2(*file_id, DATASET_NAME, *filetype, space, H5P_DEFAULT, dcpl_id, H5P_DEFAULT);
    herr_t status = H5Dwrite (*dataset_id, *memtype, H5S_ALL, H5S_ALL, H5P_DEFAULT, NULL); // wdata = NULL
    if( status < 0 )
        printf("datatype_ds: %s\n", DATASET_NAME);
    assert(status >= 0);
    /*
     * Close and release resources.
     */
    status = H5Sclose (space);
    assert(status >= 0);
    // status = H5Tclose (*memtype); It is not allowed because we need this memtype to read the dataset (function: read_datatype_table)
    // status = H5Tclose (*filetype);
     

}