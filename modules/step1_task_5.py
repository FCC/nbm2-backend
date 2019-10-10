import NBM2_functions as nbmf
import step1_functions as s1f
import pandas as pd
import numpy as np
import traceback
import time 
import gc

def aggregateHoCoNum(merged_df, config, start_time):
    """
    Consolidates the users by holding company

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
        df_hoco:        pandas dataframe that contains the aggregated users
                        for each holding company
    """
    my_message = """
            INFO - STEP 1 (MASTER): TASK 5 OF 7 - STARTING TO AGGREGATE AT THE 
            HOCONUM LEVEL
            """
    print(' '.join(my_message.split()))

    try:
        temp_time = time.localtime()
        # Aggregating on hoconum
        merged_df['max_download_speed']=merged_df[['BlockCode','HocoNum','max_download_speed']].\
                    groupby(['BlockCode','HocoNum']).transform(np.amax)
        merged_df['max_upload_speed']=merged_df[['BlockCode','HocoNum','max_upload_speed']].\
                    groupby(['BlockCode','HocoNum']).transform(np.amax)
        merged_df.drop_duplicates(subset=['BlockCode','HocoNum'],inplace=True)
        merged_df.drop(['BlockCode','TechCode'],axis=1,inplace=True)

        # Calculate the population above a given download/upload speed for
        # a given hoconum
        df_hoco=merged_df[['HocoNum','pop','max_download_speed']].\
                groupby(['HocoNum']).apply(s1f.speeds_vectorized,
                            val_arr=np.array(config['d_val_arr']), 
                            column_list=config['d_column_list'], 
                            speed_type='down')
        df_i=merged_df[['HocoNum','pop','max_upload_speed']].\
                groupby(['HocoNum']).apply(s1f.speeds_vectorized,
                            val_arr=np.array(config['u_val_arr']), 
                            column_list=config['u_column_list'], 
                            speed_type='up')
        df_hoco=df_hoco.merge(df_i,left_index=True,right_index=True)

        # Create a column with transtech = 'all'
        df_hoco['TechCode'] = "all"

        # Reset the index for the two dataframes and clean up 
        df_hoco = df_hoco.reset_index(drop=False)
        del df_i
        gc.collect()

        my_message = """
            INFO - STEP 1 (MASTER): TASK 5 OF 7 - COMPLETED AGGREGATING AT THE 
            HOCONUM LEVEL
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
                time.mktime(time.localtime())-time.mktime(start_time)))
        return True, df_hoco
    except:
        my_message = """
            ERROR - STEP 1 (MASTER): TASK 5 OF 7 - FAILED AGGREGATING AT THE 
            HOCONUM LEVEL
            """
        my_message = ' '.join(my_message.split())
        my_message += '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
                time.mktime(time.localtime())-time.mktime(start_time)))
        return False, None