import NBM2_functions as nbmf
import pandas as pd 
import numpy as np
import traceback 
import time 
import gc 

def formatDataFrames(fbd_df, blockm_df, config, start_time):
    """
    formats the fixed broadband data and merges it with some of the data 
    from the blockmaster table

    Arguments In:
        fbd_df:         a pandas dataframe that holds all of the fixed
                        broadband data
        blockm_df:      a pandas dataframe that holds all of the blockmaster
                        data
        config:         a dictionary that contains all of the 
                        configuration parameters for the entire 
                        process
        start_time:     a time structure that holds when the step 
                        started

    Arguments Out:
        continue_run:   a boolean that indicates whether the process 
                        was successfully completed
        fbd_df:         a pandas data frame that contains the fixed
                        broadband data
    """
    # supress warnings about chained assignments
    pd.options.mode.chained_assignment = None
    
    try:
        temp_time = time.localtime()
        #Keep Form 477 records where consumer = 1 
        fbd_df = fbd_df.loc[fbd_df.Consumer == 1]
        fbd_df.drop(['Consumer'],axis=1,inplace=True)
        gc.collect()
    
        #Outer join to include blocks without any broadband access that aren't 
        # in the Form 477 data
        fbd_df = pd.merge(fbd_df, blockm_df["BlockCode"]\
                    .to_frame(), on="BlockCode", how='outer')
    
        # Aggregate on the HocoNum and TechCode - required to remove hoconum 
        # subsidiaries
        fbd_df['max_download_speed']=fbd_df[['BlockCode','HocoNum',
                                            'TechCode','MaxAdDown']]\
                                        .groupby(['BlockCode','HocoNum',
                                                'TechCode'])\
                                        .transform(np.amax)
        fbd_df.drop(['MaxAdDown'],axis=1,inplace=True)
        fbd_df['max_upload_speed']=fbd_df[['BlockCode','HocoNum',
                                            'TechCode','MaxAdUp']]\
                                    .groupby(['BlockCode','HocoNum',
                                            'TechCode'])\
                                    .transform(np.amax)
        fbd_df.drop(['MaxAdUp'],axis=1,inplace=True)
        fbd_df.drop_duplicates(subset=['BlockCode','HocoNum','TechCode'],
                                inplace=True)
    
        #Create index on block FIPS
        fbd_df.set_index('BlockCode',inplace=True)
        gc.collect()

        my_message = """
            INFO - STEP 2 (MASTER): TASK 2 of 5 - COMPLETED MERGING FBD AND 
            BLOCKMASTER
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
                time.mktime(time.localtime())-time.mktime(start_time)))
        return True, fbd_df
    except:
        my_message = """
            ERROR - STEP 2 (MASTER): TASK 2 of 5 - FAILED MERGING FBD AND 
            BLOCKMASTER
            """
        my_message = ' '.join(my_message.split())
        my_message += '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
                time.mktime(time.localtime())-time.mktime(start_time)))
        return False, None        