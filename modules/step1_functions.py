import NBM2_functions as nbmf
import pandas as pd 
import numpy as np 



def speeds_vectorized(dfgroup, val_arr, column_list, speed_type):
    """
    Calculates the population above the relevant down/up-load speeds for a given 
    hoconum & transtech combination. Population is multiplied by the boolean
    (max_download_speed >= download_speed_threshold) then summed over all 
    block FIPS for a given hoconum and transtech.  This routine is only 
    called by the apply function in pandas.

    Arguments In:
        dfgroup:        an implicit variable passed in by the datafram 
                        calling the function
        val_arr:        a list of float upload/download speeds that are used
                        to aggregate the reported provider speeds
        column_list:    a list of strings that provide the column names for
                        the upload and download speeds
        speed_type:     a string variable that indicates whether the call
                        to the function is for upload or download speeds.  
                        Can only take the value of "up" or "down"

    Arguments Out:
        result:         a numpy vector of the aggregated results 
    """
    pop_mat=np.array(dfgroup['pop'].repeat(len(val_arr))).\
                reshape((len(dfgroup),len(val_arr)))
    speed_mat=eval("np.array(dfgroup.max_%sload_speed.repeat(len(val_arr))).\
                reshape((len(dfgroup),len(val_arr)))" % speed_type)
    result = pd.Series(list(np.sum(np.multiply(speed_mat>=val_arr,pop_mat),\
                        axis=0)),index=column_list) 
    return result