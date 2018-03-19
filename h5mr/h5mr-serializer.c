#include "h5mr-serializer.h"

#include "h5mr.h"
#include "structures.h"
#include "hdf5-builder.h"
#include "../mpi-h5mr/hdf5-reader.h"

void h5mr_record_data(const char * unique_key, MPI_Datatype datatype, int count, void * buf)
{
    if ( ! h5mr_is_enabled() ){
        return;
    }

    int position = -1;
    position = h5mr_append_buffer_to_datatype_ds (datatype, buf, count);
    assert ( position >= 0 );

    int success = h5mr_write_record_log (unique_key, datatype, count, position);
    assert (success == 0);

    return;
}

int h5mr_read_data(const char * unique_key, MPI_Datatype datatype, int count, void * buf)
{
    int ret = h5mr_fetch_captured_data( unique_key, datatype, count, buf );
    return ret;
}
