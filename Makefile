CFLAGS=-O0 -g3 -Wall # -Werror

SRC=$(shell ls test/*)
EXE=$(subst test/,, $(subst .c,.exe, $(SRC)))

all: libh5mr.so libmpi-h5mr.so $(EXE)

#  LD_PRELOAD=./libh5mr.so mpiexec -np 4 ./01.exe

libh5mr.so: Makefile h5mr/hdf5_builder.c h5mr/hdf5_mpi_wrapper.c h5mr/h5mr.c h5mr/datatype.c h5mr/datatype_table.c mpi-h5mr/hdf5_reader.c h5mr/h5mr-serializer.c h5mr/structures.c
	mpicc ${CFLAGS} -fPIC -shared -o libh5mr.so h5mr/hdf5_builder.c h5mr/hdf5_mpi_wrapper.c h5mr/h5mr.c h5mr/datatype.c h5mr/datatype_table.c mpi-h5mr/hdf5_reader.c h5mr/h5mr-serializer.c h5mr/structures.c -l hdf5 -l hdf5_hl -L /usr/lib/x86_64-linux-gnu/hdf5/openmpi  -I /usr/include/hdf5/openmpi/ -L /usr/lib/x86_64-linux-gnu/hdf5_hl/openmpi -I /usr/include/hdf5_hl/openmpi/ ${shell pkg-config glib-2.0 --libs} ${shell pkg-config glib-2.0 --cflags}

libmpi-h5mr.so: Makefile h5mr/hdf5_builder.c h5mr/datatype.c h5mr/h5mr.c mpi-h5mr/hdf5_reader.c mpi-h5mr/mpi.c h5mr/datatype_table.c h5mr/h5mr-serializer.c h5mr/structures.c
	mpicc ${CFLAGS} -fPIC -shared -o libmpi-h5mr.so h5mr/hdf5_builder.c h5mr/h5mr.c h5mr/datatype.c mpi-h5mr/hdf5_reader.c h5mr/datatype_table.c mpi-h5mr/mpi.c h5mr/h5mr-serializer.c h5mr/structures.c -l hdf5 -l hdf5_hl -L /usr/lib/x86_64-linux-gnu/hdf5/openmpi  -I /usr/include/hdf5/openmpi/ -L /usr/lib/x86_64-linux-gnu/hdf5_hl/openmpi -I /usr/include/hdf5_hl/openmpi/ ${shell pkg-config glib-2.0 --libs} ${shell pkg-config glib-2.0 --cflags}

%.exe : test/%.c Makefile libh5mr.so libmpi-h5mr.so
	mpicc ${CFLAGS} $< -L. -l h5mr -o $@
	mpicc ${CFLAGS} $< -L. -l h5mr -o $@-trace -Wl,--rpath="./"
	mpicc ${CFLAGS} $< -L. -l mpi-h5mr -o $@-replay -Wl,--rpath="./"
	#gcc  ${CFLAGS} -I /usr/lib/openmpi/include/ $< -L. -lmpi-h5mr -o $@-replay-no-mpicc -Wl,--rpath="./"

clean:
	rm *.o *.exe* *.so
