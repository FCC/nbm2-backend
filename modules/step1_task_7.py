import NBM2_functions as nbmf
import pandas as pd
import traceback
import time

def writeProviderTable(all_df, config, start_time):
    """
    writes the provider table out as a CSV file

    Arguments In:
        all_df:         a pandas dataframe that contains all of the data 
                        required for the provider table file
        config:	        the json variable that contains all configration
    				    data required for the data processing        
    	start_time:	    the clock time that the step began using the 
    				    time.clock() format

    Arguments Out:
        continue_run:   a boolean variable that indicates if the routine
                        successfully completed and whether the next steps
                        should be executed
    
    """
    my_message = """
        INFO - STEP 1 (MASTER): TASK 7 OF 7 - STARTING TO WRITE PROVIDER DATA TO 
        A CSV FILE
        """
    print(' '.join(my_message.split()))
    try:
        temp_time = time.localtime()
        destination = config['output_dir_path'] 
        destination += 'provider_table_{}.csv'.format(config['fbd_vintage'])
        csv_columns = ['hoconum','tech']
        csv_columns.extend(config['d_column_list'])
        csv_columns.extend(config['u_column_list'])
        all_df[csv_columns].to_csv(destination,index=False, encoding="utf-8")

        my_message = """
            INFO - STEP 1 (MASTER): TASK 7 OF 7 - COMPLETED CREATING PROVIDER 
            TABLE CSV
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return True
    except:
        my_message = """
            ERROR - STEP 1 (MASTER): TASK 7 OF 7 FAILED CREATING PROVIDER 
            TABLE CSV - TERMINATING ALL PROCESSES
            """
        my_message = ' '.join(my_message.split())
        my_message += '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return False