import NBM2_functions as nbmf 
import pandas as pd 
import traceback 
import time 

def formatBlockMaster(config, start_time):
    """
    makes modifications to the block master pdf so it conforms to the 
    specifications of the county and tract numprov files

    Arguments In:
        config:			    the json variable that contains all configration
    					    data required for the data processing
    	start_time:	        the clock time that the step began using the 
    					    time.clock() format
    Arguments Out:
        continue_run:       a boolean variable that indicates if the routine
                            successfully completed and whether the next steps
                            should be executed   
        blockm_df:          a pandas dataframe that holds all of the blockmaster
                            data
        tract_df:           a pandas dataframe that holds all of the data
                            required for the tract numprov file
        county_df:          a pandas dataframe that holds all of the data
                            required for the county numprov file      
    """
    try:
        temp_time = time.localtime()
        blockmaster_data_file = config['input_csvs_path'] +\
                            config['blockmaster_data_file']
        geom = 'geoid%s' % config['census_vintage'][2:]
        blockm_df = pd.read_csv(blockmaster_data_file,
                                usecols=[geom,'pop'],
                                dtype={geom:'object','pop':'float'})
        
        # create the templates for the tract numprovs and county numprovs
        blockm_df ['tract_id'] = blockm_df[geom].str[0:11]
        blockm_df ['county_id'] = blockm_df[geom].str[0:5]
        tract_df = blockm_df.groupby('tract_id')['pop'].sum().\
                    to_frame('tract_pop') 
        county_df = blockm_df.groupby('county_id')['pop'].sum().\
                    to_frame('county_pop') 

        my_message = """
            INFO - STEP 3 (MASTER): COMPLETED READING BLOCKMASTER FILE AND MAKING 
            TRACT AND COUNTY TEMPLATES
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return True, blockm_df, tract_df, county_df
    except:
        my_message = """
            ERROR - STEP 3 (MASTER): FAILED READING BLOCKMASTER FILE AND MAKING 
            TRACT AND COUNTY TEMPLATES
            """
        print(nbmf.logMessage(' '.join(my_message.split())+ '\n'+\
            traceback.format_exc(), temp_time, time.localtime(), 
            time.mktime(time.localtime())-time.mktime(start_time)))
        return False, None, None, None
