#include <stdio.h>
#include <mpi.h>
#include <string.h>

#include "../h5mr/h5mr.h"

int main( int argc, char *argv[] )
{
    h5mr_init(1);
    int i;

    MPI_Init(&argc,&argv);

    const int count       = 2;
    const int blocklength = 2;
    const int stride      = 4;


    MPI_Datatype c_datatype, v_datatype;

    MPI_Type_vector(count, blocklength, stride, MPI_INT, &v_datatype);
    MPI_Type_commit(&v_datatype);
    MPI_Type_contiguous(2, v_datatype, &c_datatype);
    MPI_Type_commit(&c_datatype);


    int world_rank, world_size;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
h5mr_start_recording();
    if(world_rank==0){

        int buffer[63];
        for (i=0; i<63; i++)
            buffer[i] = i;
        MPI_Send(buffer, 3, c_datatype, 1, 1147, MPI_COMM_WORLD);
        MPI_Send(buffer, 2, v_datatype, 1, 1148, MPI_COMM_WORLD);

    } else if(world_rank==1) {
        int bufferr[63];
        for (i=0; i<63; i++)
            bufferr[i] = -1;
        MPI_Recv(bufferr, 3, c_datatype, 0, 1147, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        MPI_Recv(bufferr, 3, v_datatype, 0, 1148, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        for (i=0; i<63; i++)
            printf("buffer[%d] = %d\n", i, bufferr[i]);

        fflush(stdout);
    }
h5mr_stop_recording();

    int color = world_rank / 4; // Determine color based on row

    // Split the communicator based on the color and use the
    // original rank for ordering
    MPI_Comm row_comm;
    MPI_Comm_split(MPI_COMM_WORLD, color, world_rank, &row_comm);


    int row_rank, row_size;
    MPI_Comm_rank(row_comm, &row_rank);
    MPI_Comm_size(row_comm, &row_size);

    char name[MPI_MAX_OBJECT_NAME];
    strcpy( name, "comm-0" );
    MPI_Comm_set_name( MPI_COMM_WORLD, name );
    MPI_Comm_set_name( row_comm, name );
    
    
    if(row_rank==0){

        int buffer[63];
        for (i=0; i<63; i++)
            buffer[i] = i;
        MPI_Send(buffer, 3, c_datatype, 1, 1149, row_comm);
        MPI_Send(buffer, 2, v_datatype, 1, 1150, row_comm);

    } else if(row_rank==1) {
        int bufferr[63];
        for (i=0; i<63; i++)
            bufferr[i] = -1;
        MPI_Recv(bufferr, 3, c_datatype, 0, 1149, row_comm, MPI_STATUS_IGNORE);
        MPI_Recv(bufferr, 3, v_datatype, 0, 1150, row_comm, MPI_STATUS_IGNORE);

        for (i=0; i<63; i++)
            printf("buffer[%d] = %d\n", i, bufferr[i]);

        fflush(stdout);
    }


    int color_2 = row_rank / 2; // Determine color based on row

    MPI_Comm row_comm_2;
    MPI_Comm_split(row_comm, color_2, row_rank, &row_comm_2);

    int row_rank_2, row_size_2;
    MPI_Comm_rank(row_comm_2, &row_rank_2);
    MPI_Comm_size(row_comm_2, &row_size_2);

    
    if(row_rank_2==0){

        int buffer[63];
        for (i=0; i<63; i++)
            buffer[i] = i;
        MPI_Send(buffer, 3, c_datatype, 1, 1151, row_comm_2);
        MPI_Send(buffer, 2, v_datatype, 1, 1152, row_comm_2);

    } else if(row_rank_2==1) {
        int bufferr[63];
        for (i=0; i<63; i++)
            bufferr[i] = -1;
        MPI_Recv(bufferr, 3, c_datatype, 0, 1151, row_comm_2, MPI_STATUS_IGNORE);
        MPI_Recv(bufferr, 3, v_datatype, 0, 1152, row_comm_2, MPI_STATUS_IGNORE);

        for (i=0; i<63; i++)
            printf("buffer[%d] = %d\n", i, bufferr[i]);

        fflush(stdout);
    }

    
    MPI_Comm_free(&row_comm);
    MPI_Comm_free(&row_comm_2);

    MPI_Type_free(&v_datatype);
    MPI_Type_free(&c_datatype);

    MPI_Finalize();
}