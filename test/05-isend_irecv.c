/****************************************************************************
* FILE: persist2.c
* DESCRIPTION:
*  This code conducts timing tests on messages sent between two processes
*  using a non-blocking receive and a non-blocking send - for comparison with
*  persist.c
* AUTHOR:  01/09/99 Blaise Barney
* LAST REVISED:
******************************************************************************/

#include "mpi.h"
#include <stdio.h>

/* Modify these to change timing scenario */
#define TRIALS          10
#define STEPS           20
#define MAX_MSGSIZE     1048576    /* 2^STEPS */
#define REPS            1000
#define MAXPOINTS       10000

int    numtasks, rank, tag=999, n, i, j, k, msgsizes[MAXPOINTS];
double  mbytes, tbytes, results[MAXPOINTS], ttime, t1, t2;
char   sbuff[MAX_MSGSIZE], rbuff[MAX_MSGSIZE];
MPI_Status stats[2];
MPI_Request reqs[2];

int main(argc,argv)
int argc;
char *argv[];  {

MPI_Init(&argc,&argv);
MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
MPI_Comm_rank(MPI_COMM_WORLD, &rank);

/**************************** task 0 ***********************************/
if (rank == 0) {

  /* Initializations */
  n=1;
  for (i=0; i<=STEPS; i++) {
    msgsizes[i] = n;
    results[i] = 0.0; 
    n=n*2;
    }
  for (i=0; i<MAX_MSGSIZE; i++)
    sbuff[i] = 'x';

  /* Greetings */
  printf("\n****** MPI_Irecv with MPI_Isend ******\n");
  printf("Trials=      %8d\n",TRIALS);
  printf("Reps/trial=  %8d\n",REPS);
  printf("Message Size   Bandwidth (bytes/sec)\n");

  /* Begin timings */
  for (k=0; k<TRIALS; k++) {

    n=1;
    for (j=0; j<=STEPS; j++) {
      t1 = MPI_Wtime();
      for (i=1; i<=REPS; i++){
        MPI_Irecv(&rbuff, n, MPI_CHAR, 1, tag, MPI_COMM_WORLD, &reqs[0]);
        MPI_Isend(&sbuff, n, MPI_CHAR, 1, tag, MPI_COMM_WORLD, &reqs[1]);
        MPI_Waitall(2, reqs, stats);
        }
      t2 = MPI_Wtime();

      /* Compute bandwidth and save best result over all TRIALS */
      ttime = t2 - t1;
      tbytes = sizeof(char) * n * 2.0 * (float)REPS;
      mbytes = tbytes/ttime;
      if (results[j] < mbytes)
         results[j] = mbytes;

      n=n*2;
      }   /* end j loop */
    }     /* end k loop */

  /* Print results */
  for (j=0; j<=STEPS; j++) {
    printf("%9d %16d\n", msgsizes[j], (int)results[j]);
    }

  }       /* end of task 0 */



/****************************  task 1 ************************************/
if (rank == 1) {

  /* Begin timing tests */
  for (k=0; k<TRIALS; k++) {

    n=1;
    for (j=0; j<=STEPS; j++) {
      for (i=1; i<=REPS; i++){
        MPI_Irecv(&rbuff, n, MPI_CHAR, 0, tag, MPI_COMM_WORLD, &reqs[0]);
        MPI_Isend(&sbuff, n, MPI_CHAR, 0, tag, MPI_COMM_WORLD, &reqs[1]);
        MPI_Waitall(2, reqs, stats);
        }
      n=n*2;
      }   /* end j loop */
    }     /* end k loop */
  }       /* end task 1 */


MPI_Finalize();

}  /* end of main */
