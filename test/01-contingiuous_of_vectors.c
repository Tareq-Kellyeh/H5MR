#include <stdio.h>
#include <mpi.h>

/* Run with four processes */

int main(int argc, char *argv[]) {
    int rank;
    int i;


    MPI_Status status;

    MPI_Init(&argc,&argv);
    MPI_Comm_rank(MPI_COMM_WORLD,&rank);

    const int count       = 2;
    const int blocklength = 3;
    const int stride      = 4;


    MPI_Datatype c_datatype, v_datatype;
    MPI_Type_vector(count, blocklength, stride, MPI_INT, &v_datatype);
    MPI_Type_commit(&v_datatype);
    MPI_Type_contiguous(3, v_datatype, &c_datatype);
    MPI_Type_commit(&c_datatype);

    if(rank % 2 == 0){

        int buffer[63];
        for (i=0; i<63; i++)
            buffer[i] = i;
//        MPI_Send(buffer, 3, c_datatype, rank + 1, 52, MPI_COMM_WORLD);
        MPI_Send(buffer, 3, c_datatype, rank + 1, 52, MPI_COMM_WORLD);

    } else if(rank % 2 == 1) {
        int bufferr[63];
        for (i=0; i<63; i++)
            bufferr[i] = -1;
//        MPI_Recv(&bufferr, 3, c_datatype, rank -1, 52, MPI_COMM_WORLD, &status);
        MPI_Recv(bufferr, 3, c_datatype, rank -1, 52, MPI_COMM_WORLD, &status);

        //*********************************************************************************************

        for (i=0; i<63; i++)
            printf("buffer[%d] = %d\n", i, bufferr[i]);

        fflush(stdout);

    }
    MPI_Type_free(&v_datatype);
    MPI_Type_free(&c_datatype);

    MPI_Finalize();
}
