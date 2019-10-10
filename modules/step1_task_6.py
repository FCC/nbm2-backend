import NBM2_functions as nbmf
import pandas as pd
import traceback
import time 
import gc


def mergeAggregatedData(df_stack, start_time):
    """
    Consolidates the data from the five dataframes created throughout the 
    step 1 process and merges them into a single dataframe so they can be
    written to a single csv file.

    Arguments In:
        df_stack:       a list that contains the following pandas dataframes:
                        df_hoco, df_asdl, df_cable, df_other, df_hocotrans 
        start_time:     a time.structure variable that contains the starting
                        time for when the data processing began

    Arguments Out:
        continue_run:   boolean variable that indicates whether the run was
                        successfully completed and if the processing should
                        continue
        merged_df:      pandas dataframe that contains the final set of data
                        to be written to a csv file
    """
    try:
        temp_time = time.localtime()
        #Merge the dataframes 
        merged_df = pd.concat(df_stack,sort=False)
        merged_df.reset_index(drop=True,inplace=True)

        #Rename column "transtech" to "tech"
        merged_df=merged_df.rename(columns = {'TechCode':'tech','HocoNum':'hoconum'})
        gc.collect()

        my_message = """
            INFO - STEP 1 (MASTER): TASK 6 OF 7 - COMPLETED CONSOLODATING DATA 
            INTO A SINGLE DATA FRAME
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
                time.mktime(time.localtime())-time.mktime(start_time)))
        return True, merged_df

    except:
        my_message = """
            ERROR - STEP 1 (MASTER): TASK 6 OF 7 - FAILED CONSOLODATING DATA 
            INTO A SINGLE DATA FRAME - TERMINATING ALL PROCESSES
            """
        my_message = ' '.join(my_message.split())
        my_message += '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
                time.mktime(time.localtime())-time.mktime(start_time)))
        return False, None


def consolidateData(df_stack, df_hoco, df_hocotrans, start_time):
    """

    Arguments In:
        df_stack:       a list of pnadas dictionaries that contains the 
                        various technologies and populations throughout the 
                        country     
        df_hoco:        pandas dataframe that contains served population 
                        aggregated by holding company across all technologies
        df_hocotrans:   pandas dataframe that contains served population 
                        aggregated by holding company and each specific 
                        service offered by the providers
        start_time:     a time.structure variable that contains the starting
                        time for when the data processing began

    Arguments Out:
        continue_run:   boolean variable that indicates whether the run was
                        successfully completed and if the processing should
                        continue
        all_df:         a pandas dataframe that contains all of the data 
                        required for the provider table file
    """
    my_message = """
        INFO - STEP 1 (MASTER): TASK 6 OF 7 - STARTING TO CONSOLIDATE 
        INFORMATION INTO A SINGLE DATAFRAME
        """
    print(' '.join(my_message.split()))

    try:
        df_stack.insert(0,df_hoco)
        df_stack.append(df_hocotrans)
        continue_run, all_df = mergeAggregatedData(df_stack, start_time)
    except:
        my_message = """
            ERROR - STEP 1 (MASTER): TASK 6 OF 7 - UNHANDLED ERROR - TERMINATING 
            ALL PROCESSES
            """
        my_message = ' '.join(my_message.strip()) + '\n' + traceback.format_exc()
        print(my_message)
    
    return continue_run, all_df
