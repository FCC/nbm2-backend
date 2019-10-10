import NBM2_functions as nbmf
import step1_functions as s1f
import pandas as pd
import numpy as np
import traceback
import time 

def consolidateUsersByTech(merged_df, config, start_time):
    """
    Consolidates the users by technology and holding company

    Arguments In:
        merged_df:      pandas dataframe that consists of data from both the
                        fixed broadband data and the blockmaster data
        config:         a dictionary variable that contains all configuration
                        information for the procedures run in this module
        start_time:     a time.structure variable that contains the starting
                        time for when the data processing began

    Arguments Out:
        continue_run:   boolean variable that indicates whether the run was
                        successfully completed and if the processing should
                        continue
        df_hocotrans:   pandas dataframe that contains the aggregate of 
                        users broken out by technology and holding company
                        name
    """
    # supress warnings about chained assignments
    pd.options.mode.chained_assignment = None

    my_message = """
            INFO - STEP 1 (MASTER): TASK 3 OF 7 - STARTING TO CONSOLIDATE BY 
            TECHNOLOGIES
            """
    print(' '.join(my_message.split()))

    try:
        temp_time = time.localtime()
        df_hocotrans=merged_df[['HocoNum','TechCode','pop','max_download_speed']].\
                    groupby(['HocoNum','TechCode']).\
                    apply(  s1f.speeds_vectorized, 
                            val_arr=np.array(config['d_val_arr']), 
                            column_list=config['d_column_list'],
                            speed_type='down')

        my_message = """
            INFO - STEP 1 (MASTER): TASK 3 OF 7 - COMPLETED AGGREGATING USERS 
            AND TECHNOLOGIES FOR DOWNLOAD SPEEDS
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message,temp_time, time.localtime(),
                time.mktime(time.localtime())-time.mktime(start_time)))
    except:
        my_message = """
            ERROR - STEP 1 (MASTER): TASK 3 OF 7 - FAILED AGGREGATING USERS AND 
            TECHNOLOGIES FOR DOWNLOAD SPEEDS
            """
        my_message = ' '.join(my_message.split())
        my_message += '\n'+traceback.format_exc()
        print(nbmf.logMessage(my_message,temp_time, time.localtime(),
                time.mktime(time.localtime())-time.mktime(start_time)))
        return False, None
    try:
        df_i=merged_df[['HocoNum','TechCode','pop','max_upload_speed']].\
                groupby(['HocoNum','TechCode']).\
                apply(s1f.speeds_vectorized, 
                val_arr=np.array(config['u_val_arr']), 
                column_list=config['u_column_list'],
                speed_type='up')
        my_message = """
            INFO - STEP 1 (MASTER): TASK 3 OF 7 - COMPLETED AGGREGATING USERS 
            AND TECHNOLOGIES FOR UPLOAD SPEEDS
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message,temp_time, time.localtime(),
                time.mktime(time.localtime())-time.mktime(start_time)))
    except:
        my_message = """
            ERROR - STEP 1 (MASTER): TASK 3 OF 7 - FAILED AGGREGATING USERS AND 
            TECHNOLOGIES FOR UPLOAD SPEEDS
            """
        my_message = ' '.join(my_message.split())
        my_message += '\n'+traceback.format_exc()
        print(nbmf.logMessage(my_message,temp_time, time.localtime(),
                time.mktime(time.localtime())-time.mktime(start_time)))
        return False, None
    try:
        df_hocotrans=df_hocotrans.merge(df_i,left_index=True,right_index=True)
        df_hocotrans = df_hocotrans.reset_index(drop=False) 
        my_message = """
            INFO - STEP 1 (MASTER): TASK 3 OF 7 - COMPLETED MERGING AGGREGATION 
            OF USERS AND TECHNOLOGIES FOR BOTH DIRECTIONS
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message,temp_time, time.localtime(),
                time.mktime(time.localtime())-time.mktime(start_time)))
        return True, df_hocotrans
    except:
        my_message = """
            ERROR - STEP 1 (MASTER): TASK 3 OF 7 - FAILED MERGING AGGREGATION OF 
            USERS AND TECHNOLOGIES FOR BOTH DIRECTIONS
            """
        my_message = ' '.join(my_message.split())
        my_message += '\n'+traceback.format_exc()
        print(nbmf.logMessage(my_message,temp_time, time.localtime(),
                time.mktime(time.localtime())-time.mktime(start_time)))
        return False, None