import NBM2_functions as nbmf
import pandas as pd
import traceback
import time 


def loadFBData(config, start_time):
    """
    Gets data file, validates headers, and loads the data into the pandas 
    data from

    Arguments In:
        config:         a dictionary variable that contains all configuration
                        information for the procedures run in this module
        start_time:     a time.structure variable that contains the starting
                        time for when the data processing began

    Arguments Out:
        continue_run:   a boolean variable that indicates whether the routine
                        successfully completed its processing 
        fbd_df:         a pandas dataframe that contains the fixed broadband
                        data
    """
    # supress warnings about chained assignments
    pd.options.mode.chained_assignment = None

    try:
        temp_time = time.localtime()
        fbd_file = config['input_csvs_path']+config['fbData']
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
                ERROR - STEP 1 (MASTER): TASK 1 OF 7 - UNEXPECTED COLUMN HEADERS
                IN FBD FBD DATA FILE
                """
            my_message = ' '.join(my_message.split())
            print(nbmf.logMessage(my_message, temp_time, time.localtime(),
                time.mktime(time.localtime())-time.mktime(start_time)))
            return False, None 

        # successfully loaded the fixed broadband data
        my_message = """
            INFO - STEP 1 (MASTER): TASK 1 OF 7 - FBD DATA INGESTED
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return True, fbd_df
    except:
        my_message = """
            ERROR - STEP 1 (MASTER): TASK 1 OF 7 - COULD NOT PROCESS THE FBD 
            FILE
            """
        my_message = ' '.join(my_message.split())
        my_message += '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time,time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return False, None

def loadBlockMasterData(config, start_time):
    """
    Gets data file, validates headers, and loads the data into the pandas 
    data frame

    Arguments In:
        config:         a dictionary variable that contains all configuration
                        information for the procedures run in this module
        start_time:     a time.structure variable that contains the starting
                        time for when the data processing began

    Arguments Out:
        continue_run:   a boolean variable that indicates whether the routine
                        successfully completed its processing 
        fbd_df:         a pandas dataframe that contains the fixed broadband
                        data
    """
    try:
        temp_time = time.localtime()
        bm_file = config['input_csvs_path']+config['blockmaster_data_file']
        bm_df = pd.read_csv(bm_file, usecols=config['bm_data_columns'], 
                            dtype=config['bm_data_types']).\
                            rename(columns=config['bm_rename_columns'])
        my_message = """
            INFO - STEP 1 (MASTER): TASK 1 OF 7 - BLOCK MASTER DATA INGESTED
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return True, bm_df
    except:
        my_message = """
            ERROR - STEP 1 (MASTER): TASK 1 OF 7 - COULD NOT PROCESS THE BLOCK 
            MASTER FILE
            """
        my_message = ' '.join(my_message.split())
        my_message += '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time,time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return False, None

def loadInputFiles(config, start_time):
    temp_time = time.localtime()
    my_message = """
        INFO - STEP 1 (MASTER): TASK 1 OF 7 - PREPARING TO INGEST FBD AND BLOCK 
        MASTER DATA
        """
    print(' '.join(my_message.split()))
    continue_run, fbd_df = loadFBData(config, start_time)

    if continue_run:
        continue_run, bm_df = loadBlockMasterData(config, start_time)

    if continue_run:
        my_message = """
            INFO - STEP 1 (MASTER): TASK 1 OF 7 - COMPLETED INGESTING FBD AND 
            BLOCK MASTER DATA
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time,time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return True, fbd_df, bm_df
    else:
        my_message = """
            ERROR - STEP 1 (MASTER): TASK 1 OF 7 - FAILED INGESTING FBD AND 
            BLOCK MASTER DATA - TERMINATING ALL PROCESSES
            """       
        my_message = ' '.join(my_message.split()) 
        print(nbmf.logMessage(my_message, temp_time,time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return False, None, None