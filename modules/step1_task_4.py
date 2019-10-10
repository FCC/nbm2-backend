import NBM2_functions as nbmf
import step1_functions as s1f
import pandas as pd
import numpy as np
import traceback
import time 
import gc

def aggregate_tech_collection(merged_df,techlist,code_string, config, start_time):
    """
    Consolidates the users by a specific technology category and holding 
    company

    Arguments In:
        merged_df:      pandas dataframe that consists of data from both the
                        fixed broadband data and the blockmaster data
        techlist:       a list of integers that represent the 4 to 5 
                        technology codes from the fbd data that are specific
                        to the technology being aggregated over
        code_string:    a string variabel that contains the label that will
                        be used to characterize the technology being 
                        aggregated over.  
        config:         a dictionary variable that contains all configuration
                        information for the procedures run in this module
        start_time:     a time.structure variable that contains the starting
                        time for when the data processing began
    Arguments Out:
        continue_run:   boolean variable that indicates whether the run was
                        successfully completed and if the processing should
                        continue
        df_i:           pandas dataframe that contains the aggregated users
                        for each combination of holding company and the 
                        specific technology
    """
    try:
        temp_time = time.localtime()
        # Aggregating on hoconum for a category of techcodes
        df_i=merged_df.loc[merged_df.TechCode.isin(techlist)]
        df_i['max_download_speed']=df_i[['BlockCode','HocoNum','max_download_speed']].\
                    groupby(['BlockCode','HocoNum']).transform(np.amax)
        df_i['max_upload_speed']=df_i[['BlockCode','HocoNum','max_upload_speed']].\
                    groupby(['BlockCode','HocoNum']).transform(np.amax)
        df_i.drop_duplicates(subset=['BlockCode','HocoNum'],inplace=True)
        df_i.drop(['BlockCode','TechCode'],axis=1,inplace=True)

        # Calculate the population above a given download/upload speed 
        # for a given hoconum
        df_down=df_i[['HocoNum','pop','max_download_speed']].\
                    groupby(['HocoNum']).\
                    apply(  s1f.speeds_vectorized, 
                            val_arr=np.array(config['d_val_arr']), 
                            column_list=config['d_column_list'], 
                            speed_type='down')
        df_up=df_i[['HocoNum','pop','max_upload_speed']].\
                    groupby(['HocoNum']).\
                    apply(  s1f.speeds_vectorized, 
                            val_arr=np.array(config['u_val_arr']), 
                            column_list=config['u_column_list'], 
                            speed_type='up')
        df_i=df_down.merge(df_up,left_index=True,right_index=True)

        # Create a column with name of the technology being processed
        df_i['TechCode'] = code_string

        # Reset the index for the two dataframes 
        df_i = df_i.reset_index(drop=False)
        gc.collect()

        my_message = """
            INFO - STEP 1 (MASTER): TASK 4 OF 7 - COMPLETED AGGREGATING BY {0} TECHNOLOGY 
            """.format(code_string)
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
                time.mktime(time.localtime())-time.mktime(start_time)))
        return True, df_i
    except:
        my_message = """
            ERROR - STEP 1 (MASTER): TASK 4 OF 7 - FAILED AGGREGATING BY {0} TECHNOLOGY 
            """.format(code_string)
        my_message = ' '.join(my_message.split())
        my_message += '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
                time.mktime(time.localtime())-time.mktime(start_time)))
        return False, None

def aggPopAtTechLevels(config, merged_df, start_time):
    """
    aggregates the populations at each technology level

    Arguments In:
        config:			the json variable that contains all configration
    					data required for the data processing
        merged_df       pandas dataframe with the information that combines
                        fbd data and population data
        start_time:     a time.structure variable that contains the starting
                        time for when the data processing began
    Arguments Out:
        continue_run:   boolean variable that indicates whether the run was
                        successfully completed and if the processing should
                        continue
        df_stack:       a list of pnadas dictionaries that contains the 
                        various technologies and populations throughout the 
                        country

    """
    my_message = """
            INFO - STEP 1 (MASTER): TASK 4 OF 7 - STARTING TO AGGREGATE 
            POPULATION AT THE TECH CATEGORY LEVEL
            """
    my_message = ' '.join(my_message.split())
    print(my_message)
    continue_run = True
    temp_time = time.localtime()

    try:
        tech_stack = config['tech_stack']
        df_stack = []
        for code, code_string in tech_stack:
            if continue_run:
                continue_run, df_i=aggregate_tech_collection(merged_df, 
                            config['tech_dict'][code], code_string, 
                            config, start_time)
                df_stack.append(df_i)
    except:
        my_message = """
            ERROR - STEP 1 (MASTER): TASK 4 OF 7 - UNHANDLED ERROR WHILE 
            AGGREGATING POPULATION AT TECH LEVELS
            """
        my_message = ' '.join(my_message.split()) + '\n' + traceback.format_exc()
        print(my_message)

    if continue_run:
        my_message = """
            INFO - STEP 1 (MASTER): TASK 4 OF 7 - COMPLETED AGGREGATING AT THE 
            TECH CATEGORY LEVEL
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
                time.mktime(time.localtime())-time.mktime(start_time)))
        return True, df_stack
    else:
        my_message = """
            INFO - STEP 1 (MASTER): TASK 4 OF 7 - FAILED AGGREGATING AT THE 
            TECH CATEGORY LEVEL - TERMINATING ALL PROCESSES
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
                time.mktime(time.localtime())-time.mktime(start_time)))
        return False, None