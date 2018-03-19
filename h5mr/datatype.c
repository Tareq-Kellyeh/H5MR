#include "datatype.h"

char datatype_desc[1000] = "";
char temp[100];

static void mpix_decode_primitive(MPI_Datatype typ){
    
    printType("NAMED(");
    strcat(datatype_desc, "NAMED(");
    int size;
    MPI_Type_size( typ, &size );
  
    printType("size=%d,", size);
    if(typ == MPI_BYTE){
        printType("BYTE)");
        strcat(datatype_desc, "BYTE)");
        return;
    }
    if(typ == MPI_LB){
        printType("LB)");
        strcat(datatype_desc, "LB)");
        return;
    }
    if(typ == MPI_UB){
        printType("UB)");
        strcat(datatype_desc, "UB)");
        return;
    }
    if(typ == MPI_CHAR){
        printType("CHAR)");
        strcat(datatype_desc, "CHAR)");
        return;
    }
    if(typ == MPI_SHORT){
        printType("SHORT)");
        strcat(datatype_desc, "SHORT)");
        return;
    }
    if(typ == MPI_INT){
        printType("INT)");
        strcat(datatype_desc, "INT)");
        return;
    }
    if(typ == MPI_LONG){
        printType("LONG)");
        strcat(datatype_desc, "LONG)");
        return;
    }
    if(typ == MPI_UNSIGNED_CHAR){
        printType("UNSIGNED_CHAR)");
        strcat(datatype_desc, "UNSIGNED_CHAR)");
        return;
    }
    if(typ == MPI_UNSIGNED_SHORT){
        printType("UNSIGNED_SHORT)");
        strcat(datatype_desc, "UNSIGNED_SHORT)");
        return;
    }
    if(typ == MPI_UNSIGNED){
        printType("UNSIGNED)");
        strcat(datatype_desc, "UNSIGNED)");
        return;
    }
    if(typ == MPI_UNSIGNED_LONG){
        printType("UNSIGNED_LONG)");
        strcat(datatype_desc, "UNSIGNED_LONG)");
        return;
    }
    if(typ == MPI_FLOAT){
        printType("FLOAT)");
        strcat(datatype_desc, "FLOAT)");
        return;
    }
    if(typ == MPI_DOUBLE){
        printType("DOUBLE)");
        strcat(datatype_desc, "DOUBLE)");
        return;
    }
    if(typ == MPI_LONG_DOUBLE){
        printType("LONG_DOUBLE)");
        strcat(datatype_desc, "LONG_DOUBLE)");
        return;
    }
    if(typ == MPI_PACKED){
        printType("PACKED)");
        strcat(datatype_desc, "PACKED)");
        return;
    }
    if(typ == MPI_INTEGER){
        printType("INTEGER)");
        strcat(datatype_desc, "INTEGER)");
        return;
    }
    if(typ == MPI_REAL){
        printType("REAL)");
        strcat(datatype_desc, "REAL)");
        return;
    }
    if(typ == MPI_DOUBLE_PRECISION){
        printType("DOUBLE_PRECISION)");
        strcat(datatype_desc, "DOUBLE_PRECISION)");
        return;
    }
    if(typ == MPI_COMPLEX){
        printType("COMPLEX)");
        strcat(datatype_desc, "COMPLEX)");
        return;
    }
    if(typ == MPI_LOGICAL){
        printType("LOGICAL)");
        strcat(datatype_desc, "LOGICAL)");
        return;
    }
    if(typ == MPI_CHARACTER){
        printType("CHARACTER)");
        strcat(datatype_desc, "CHARACTER)");
        return;
    }
    if(typ == MPI_PACKED){
        printType("PACKED)");
        strcat(datatype_desc, "PACKED)");
        return;
    }
    #ifdef MPI_INTEGER1
    if(typ == MPI_INTEGER1){
        printType("INTEGER1)");
        strcat(datatype_desc, "INTEGER1)");
        return;
    }
    #endif
    #ifdef MPI_INTEGER2
    if(typ == MPI_INTEGER2){
        printType("INTEGER2)");
        strcat(datatype_desc, "INTEGER2)");
        return;
    }
    #endif
    #ifdef MPI_INTEGER4
    if(typ == MPI_INTEGER4){
        printType("INTEGER4)");
        strcat(datatype_desc, "INTEGER4)");
        return;
    }
    #endif
    #ifdef MPI_REAL2
    if(typ == MPI_REAL2){
        printType("REAL2)");
        strcat(datatype_desc, "REAL2)");
        return;
    }
    #endif
    #ifdef MPI_REAL4
    if(typ == MPI_REAL4){
        printType("REAL4)");
        strcat(datatype_desc, "REAL4)");
        return;
    }
    #endif
    #ifdef MPI_REAL8
    if(typ == MPI_REAL8){
        printType("REAL8)");
        strcat(datatype_desc, "REAL8)");
        return;
    }
    #endif
    #ifdef MPI_LONG_LONG_INT
    if(typ == MPI_LONG_LONG_INT){
        printType("LONG_LONG_INT)");
        strcat(datatype_desc, "LONG_LONG_INT)");
        return;
    }
    #endif
    printf("Error: unsupported basic data type\n");
}

static void printSizeExt(MPI_Datatype typ){
    MPI_Aint extent;
    int size;
    MPI_Type_extent( typ, &extent);
    MPI_Type_size( typ, &size );
    
    printType(",size=%d,extent=%zu)", size, (size_t) extent);
    sprintf(temp, ",size=%d,extent=%zu)", size, (size_t) extent);
    strcat(datatype_desc, temp);
}

static void mpix_decode_datatype_i(MPI_Datatype typ){
    
    int ret;
    int num_integers, num_addresses, num_datatypes, combiner;
    ret = MPI_Type_get_envelope(typ, & num_integers, & num_addresses, & num_datatypes, & combiner);
    CHECK_RET(ret)
        debug("%d %d %d %d", num_integers, num_addresses, num_datatypes, combiner);
        
    if( combiner == MPI_COMBINER_NAMED ){
        mpix_decode_primitive(typ);
    
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
        case(MPI_COMBINER_DUP):
            {
                printType("DUP(typ=");
                mpix_decode_datatype_i(datatypes[0]);
                break;
            }
            
        case(MPI_COMBINER_CONTIGUOUS):
            {
                printType("CONTIGUOUS(count=%d,typ=", integers[0]); 
                
                sprintf(temp, "CONTIGUOUS(count=%d,typ=", integers[0]);
                strcat(datatype_desc, temp);
                
                mpix_decode_datatype_i(datatypes[0]);
                break;
            }
            
        case(MPI_COMBINER_VECTOR):
            {
                printType("VECTOR(count=%d,blocklength=%d,stride=%d,typ=", integers[0], integers[1], integers[2]);
               
                sprintf(temp, "VECTOR(count=%d,blocklength=%d,stride=%d,typ=", integers[0], integers[1], integers[2]);
                strcat(datatype_desc, temp);
                
                mpix_decode_datatype_i(datatypes[0]);
                break;
            }
            
        case(MPI_COMBINER_HVECTOR_INTEGER):
        case(MPI_COMBINER_HVECTOR):
            {
                printType("HVECTOR(count=%d,blocklength=%d,stride=%ld,typ=", integers[0], integers[1], addresses[0]);
                mpix_decode_datatype_i(datatypes[0]);
                break;
            }
            
        case(MPI_COMBINER_INDEXED):
            {
                printType("INDEXED(count=%d,blocklength=[", integers[0]);
                for(int i=1; i <= integers[0]; i++){
                    if( i != 1) printType(";");
                    printType("%d", integers[i]);
                }
                printType("], displacement=[");
                for(int i=integers[0]+1; i <= 2*integers[0]; i++){
                    if( i != 1) printType(";");
                    printType("%d", integers[i]);
                }
                printType("],typ=");
                mpix_decode_datatype_i(datatypes[0]);
                break;
            }  
          
        case(MPI_COMBINER_HINDEXED_INTEGER):
        case(MPI_COMBINER_HINDEXED):
            {
                printType("HINDEXED(count=%d,blocklength=[", integers[0]);
                
                for(int i=1; i <= integers[0]; i++){
                    if( i != 1) printType(";");
                    printType("%d", integers[i]);
                }
                printType("],displacement=[");
                for(int i=0; i < integers[0]; i++){
                    if( i != 0) printType(";");
                    printType("%ld", addresses[i]);
                }
                printType("],typ=");
                mpix_decode_datatype_i(datatypes[0]);
                break;
            }
            
        case(MPI_COMBINER_INDEXED_BLOCK):
            {
                printType("INDEXED_BLOCK(count=%d,blocklength=%d,displacement=[", integers[0], integers[1]);
                for(int i=2; i <= integers[0] + 1; i++){
                    if( i != 2) printType(";");
                    printType("%ld", addresses[i]);
                }
                printType("],typ=");
                mpix_decode_datatype_i(datatypes[0]);
                break;
            }
            
            
        case(MPI_COMBINER_STRUCT):
        case(MPI_COMBINER_STRUCT_INTEGER):
            {          
                printType("STRUCT(count=%d,blocklength=[", integers[0]);
                sprintf(temp, "STRUCT(count=%d,blocklength=[", integers[0]);
                strcat(datatype_desc, temp);
                
                for(int i=1; i <= integers[0]; i++){
                    if( i != 1) {
                        printType(";");
                        strcat(datatype_desc, ";");
                    }
                    printType("%d", integers[i]);
                    sprintf(temp, "%d", integers[i]);
                    strcat(datatype_desc, temp);

                }
                printType("],displacement=[");
                strcat(datatype_desc, "],displacement=[");
                for(int i=0; i < integers[0]; i++){
                    if( i != 0) {
                        printType(";");
                        strcat(datatype_desc, ";");
                    }
                    printType("%ld", addresses[i]);
                    sprintf(temp, "%ld", addresses[i]);
                    strcat(datatype_desc, temp);
                    
                }
                printType("],typ=[");
                strcat(datatype_desc, "],typ=[");
                for(int i=0; i < integers[0]; i++){
                    if( i != 0) {
                        printType(";");
                        strcat(datatype_desc, ";");
                    }
                    mpix_decode_datatype_i(datatypes[i]);
                }
                printType("]");
                strcat(datatype_desc, "]");
                break;
            }
            //**********************************************************************************//
            //**********************************************************************************//
            
        case(MPI_COMBINER_SUBARRAY):
            {
                printType("SUBARRAY(ndims=%d,size=[", integers[0]);
                for(int i=1; i <= integers[0]; i++){
                    if( i != 1) printType(";");
                    printType("%d", integers[i]);
                }
                printType("],subsize=[");
                for(int i=integers[0] + 1; i <= 2*integers[0]; i++){
                    if( i != integers[0] + 1) printType(";");
                    printType("%d", integers[i]);
                }
              printType("],starts=[");
              for(int i=2*integers[0] + 1; i <= 3*integers[0]; i++){
                  if( i != 2*integers[0] + 1) printType(";");
                  printType("%d", integers[i]);
              }
              printType("],order=%d,typ=", integers[3*integers[0]+1]);
              mpix_decode_datatype_i(datatypes[0]);
              break;
          }

        case(MPI_COMBINER_DARRAY):
            {
              int ndims = integers[2];
              printType("DARRAY(size=%d,rank=%d,ndims=%d,gsizes=[", integers[0], integers[1], ndims);
              int start;
              start = 3;
              for(int i=start; i < ndims + start; i++){
                  if( i != start) printType(";");
                  printType("%d", integers[i]);
              }
              printType("],distribs=[");
              start += ndims;
              for(int i=start; i < ndims + start; i++){
                  if( i != start) printType(";");
                  printType("%d", integers[i]);
              }
              printType("],dargs=[");
              start += ndims;
              for(int i=start; i < ndims + start; i++){
                  if( i != start) printType(";");
                  printType("%d", integers[i]);
              }
              printType("],psizes=[");
              start += ndims;
              for(int i=start; i < ndims + start; i++){
                  if( i != start) printType(";");
                  printType("%d", integers[i]);
              }
              printType("],order=%d,typ=", integers[4*ndims+3]);
              mpix_decode_datatype_i(datatypes[0]);
              break;
            }
      
        case(MPI_COMBINER_F90_REAL):
            {
                printType("F90_REAL(p=%d,r=%d)", integers[0], integers[1]);
                break;
            }
    
        case(MPI_COMBINER_F90_COMPLEX):
            {
                printType("F90_COMPLEX(p=%d,r=%d)", integers[0], integers[1]);
                break;
            }
          
        case(MPI_COMBINER_F90_INTEGER):
            {
                printType("F90_INTEGER(r=%d)", integers[0]);
                break;
            }
          
        case(MPI_COMBINER_RESIZED):
            {
                printType("RESIZED(lb=%ld,typ=", addresses[0]);
                mpix_decode_datatype_i(datatypes[0]);
                break;
            }
          
        default:
            {
                printf("ERROR Unsupported combiner: %d\n", combiner);
            }
    }
    
    printf("\n");
    printSizeExt(typ);
}

char * get_datatype_description(MPI_Datatype typ){
    strcpy(datatype_desc, ""); // Reset the description string
    mpix_decode_datatype_i(typ);    
    char * desc = datatype_desc;
    
    return desc;
}