# H5MR
an HDF5-MPI Recorder for capture and replay executions of MPI programs written in _C_.

## Motivation
To write test units for MPI subroutines that contain MPI-calls. H5MR captures the execution of MPI applications by recording the MPI-data exchanged by each rank in rank-specific HDF5 file. It uses this data to replay the execution of a specific rank in isolation.

This work was a part of a master thesis at the University of Hamburg.

## Limitations
Capture and replay is limited to the following MPI-calls: ```MPI_Send```, ```MPI_Recv```, ```MPI_Isend```, ```MPI_Irecv```, ```MPI_Wait```, ```MPI_Waitall```

## Requirements
* GNU C v6.0 or newer.
* An MPI v3.1 compliant implementation with default compiler wrappers (```mpicc```).
* [HDF5](https://www.hdfgroup.org/)
* [Glib](https://developer.gnome.org/glib/stable/)

## Setup
1. ```make interface``` to create ```libh5mr.so``` and ```libmpi-h5mr.so```.
2. ```#include "h5mr.h"``` to select the code under test _CUT_.
3. ```#include "h5mr-serializer.h"``` to record user data on demand.
4. Run by ```mpiexec``` and the desired number of processes.

## License
The source code is subject to the MIT license.
