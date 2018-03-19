#include "h5mr.h"

/*
* 1 => capture completely
* 0 => capture partialy
*/
static int h5mr_full_capture    = 0;
int h5mr_trace_enabled          = 0;

int h5mr_init(int full_capture){
  if (full_capture){
    h5mr_full_capture   = 1;
    h5mr_trace_enabled  = 0;
  }
  return 0;
}

int h5mr_start_recording(){
    if (h5mr_full_capture) return 0;
    h5mr_trace_enabled = 1;

    return 0;
}

int h5mr_stop_recording(){
    if (h5mr_full_capture) return 0;
    h5mr_trace_enabled = 0;
    return 0;
}

int h5mr_is_enabled(){
    if (h5mr_full_capture) return 1;
    return h5mr_trace_enabled;
}
