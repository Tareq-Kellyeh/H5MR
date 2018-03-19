#ifndef H5MR_H
#define H5MR_H


/*
 * @brief  This function configures the default / automatic recording.
 * @param manual_control if set to 1, then the library will not automatically capture anything, but one has to use h5mr_start_recording()
 * @return  0 if successfull, otherwise an error happened.
 */
int h5mr_init(int manual_control);

/*
 * @brief: This function starts the recording of MPI calls
 * @return: 0 if successfull, otherwise an error happened.
 */
int h5mr_start_recording();

/*
 * @brief: This function stops the recording of MPI calls (if previously started)
 * @return: 0 if successfull, otherwise an error happened.
 */
int h5mr_stop_recording();

int h5mr_is_enabled();

#endif