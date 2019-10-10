import NBM2_functions as nbmf
import pandas as pd
import numpy as np
import traceback
import time 

def mergeFrames(fbd_df, bm_df, start_time):
    """
    Merges fixed broadband data and the blockmaster data into a single 
    dataframe

    Arguments In:
        fbd_df:         pandas dataframe that contains fixed broadband data 
        bm_df:          pandas dataframe that contains the block master data
    	start_time:	the clock time that the step began using the 
                        time.clock() format

    Arguments Out:
        continue_run:   boolean variable that indicates whether the run was
                        successfully completed and if the processing should
                        continue
        merged_df:      pandas dataframe that consists of data from both the
                        fixed broadband data and the blockmaster data
    """
    # supress warnings about chained assignments
    pd.options.mode.chained_assignment = None

    my_message = """
            INFO - STEP 1 (MASTER): TASK 2 OF 7 - STARTING TO MERGE DATA SETS
            """
    print(' '.join(my_message.split()))

    try:
        temp_time = time.localtime()
        # keep all records where there are consumers present - discard the rest
        fbd_df = fbd_df.loc[fbd_df.Consumer == 1]
        fbd_df.drop(['Consumer'],axis=1,inplace=True)

        # merge the block master and fbd data frames
        merged_df = fbd_df.merge(bm_df, on='BlockCode', how="left")

        # find the max download speed for the block, holding company, and tech
        merged_df['max_download_speed']=\
                merged_df[['BlockCode','HocoNum','TechCode','MaxAdDown']].\
                groupby(['BlockCode','HocoNum','TechCode']).transform(np.amax)
        merged_df.drop(['MaxAdDown'],axis=1,inplace=True)

        # find the max upload speed for the block, holding company and tech
        merged_df['max_upload_speed']=\
                merged_df[['BlockCode','HocoNum','TechCode','MaxAdUp']].\
                groupby(['BlockCode','HocoNum','TechCode']).transform(np.amax)
        merged_df.drop(['MaxAdUp'],axis=1,inplace=True)

        # drop duplicates
        merged_df.drop_duplicates(subset=['BlockCode','HocoNum','TechCode'],
                                inplace=True)

        my_message = """
            INFO - STEP 1 (MASTER): TASK 2 OF 7 - COMPLETED MERGING FBD AND 
            BLOCK MASTER DATAFRAMES
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
                time.mktime(time.localtime())-time.mktime(start_time)))
        return True, merged_df
    except:
        my_message = """
            ERROR - STEP 1 (MASTER): TASK 2 OF 7 - COULD NOT MERGE FBD AND BLOCK
            MASTER DATAFRAMES - TERMINATING ALL PROCESSES
            """
        my_message = ' '.join(my_message.split())
        my_message += '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
                time.mktime(time.localtime())-time.mktime(start_time)))
        return False, None