import NBM2_functions as nbmf
import pandas as pd 
import traceback 
import time 

def loadMainDataFiles(config, start_time):
    """
    Reads in the csv's that contain the fixed broadband data and the 
    block master data.

    Arguments In:
        config:                 a dictionary that contains all of the 
                                configuration parameters for the entire 
                                process
        start_time:             a time structure that holds when the step 
                                started
    
    Arguments Out:
        continue_run:           a boolean that indicates whether the process 
                                was successfully completed
        fbd_df:                 a pandas data frame that contains the fixed
                                broadband data
        blockm_df:              a pandas data frome that contains the 
                                blockmaster data
    """
    # supress warnings about chained assignments
    pd.options.mode.chained_assignment = None

    continue_run = True
    fbd_file = config['input_csvs_path']+config['fbData']
    blockmaster_data_file   = config['input_csvs_path']+\
                                            config['blockmaster_data_file']

    try:
        #Load fbData (Form 477 data)
        temp_time = time.localtime()
        fbd_columns = list(pd.read_csv(fbd_file, nrows=0))  # get the headers

        # check to see if all the expected headers are in the file
        column_count = 0
        for column_name in config['fbd_data_columns']:
            if column_name in fbd_columns:
                column_count += 1

        # if all of the expected headers were not in the file, then check
        # to confirm that we have properly prepared the change names config param
        if column_count != len(config['fbd_data_columns']):
            data_count = 0
            for column_name in config['fbd_rename_columns']:
                if column_name in fbd_columns:
                    data_count += 1

        # if headers are as we anticipated, then load the data into the dataframe
        if column_count == len(config['fbd_data_columns']):    
            fbd_df = pd.read_csv(fbd_file, usecols=config['fbd_data_columns'], 
                                dtype=config['fbd_data_types'])
        
        elif data_count == len(config['fbd_data_columns']):
            old_names = config['fbd_rename_columns'].keys()
            fbd_df = pd.read_csv(fbd_file, usecols=old_names, 
                                dtype=config['fbd_data_types_old'])\
                                .rename(columns=config['fbd_rename_columns'])

        # headers are unexpected and we cannot proceed
        else:
            my_message = """
                ERROR - STEP 2 (MASTER): TASK 1 of 5 - FAILED READING IN FIXED 
                BROADBAND DATA SET - CHANGE CONFIG FILE
                """
            my_message = ' '.join(my_message.split())
            print(nbmf.logMessage(my_message, temp_time, time.localtime(),
                time.mktime(time.localtime())-time.mktime(start_time)))
            return False, None, None 

        my_message = """
            INFO - STEP 2 (MASTER): TASK 1 of 5 - COMPLETED READING IN FIXED 
            BROADBAND DATA SET
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))

    except:
        my_message = """
            ERROR - STEP 2 (MASTER): TASK 1 of 5 - FAILED READING IN FIXED 
            BROADBAND DATA SET
            """
        my_message = ' '.join(my_message.split())
        my_message += '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
                time.mktime(time.localtime())-time.mktime(start_time)))
        return False, None, None

    try:
        #Load Blockmaster data
        temp_time = time.localtime()
        blockm_df = pd.read_csv(blockmaster_data_file, 
                                dtype = config['blockm_dtype'])\
                                .rename(columns=config['bm_rename_columns'])
        blockm_df['country']=0 # add a column to group by to get national figures 
        my_message = """
            INFO - STEP 2 (MASTER): TASK 1 OF 5 - COMPLETED READING IN BLOCK 
            MASTER DATA SET
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
                time.mktime(time.localtime())-time.mktime(start_time)))
        return continue_run, fbd_df, blockm_df
    except:
        my_message = """
            ERROR - STEP 2 (MASTER): TASK 1 of 5 - FAILED READING IN BLOCK 
            MASTER DATA SET
            """
        my_message = ' '.join(my_message.split())
        my_message += '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
                time.mktime(time.localtime())-time.mktime(start_time)))
        return False, None, None