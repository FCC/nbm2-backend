import NBM2_functions as nbmf 
import step5_functions as s5f
import geopandas as gpd 
import pandas as pd
import traceback
import time

def createH2OnlyUndev(block_df, config, start_time):
    """
    creates the csv that contains the blocks that only have water in them

    Arguments In:
        block_df:           a pandas dataframe containing the block level
                            geometries
        config:             a dictionary that contains the configuration
                            information of various steps of NMB2 data 
                            processing
        start_time:         a time structure variable that indicates when 
                            the current step started

    Arguments Out:
        continue_run:       a boolean variable that indicates if the routine
                            successfully completed and whether the next 
                            steps should be exectuted 
    """
    # consolidate data to make the h2only_dev file
    try:
        temp_time = time.localtime()
        df1 = block_df[['geoid%s' % config['census_vintage'][2:],'ALAND%s' %\
                config['census_vintage'][2:]]].\
                loc[block_df["ALAND%s" % config['census_vintage'][2:]]==0].\
                sort_values('geoid%s' % config['census_vintage'][2:],ascending=True)
        df2 = pd.read_csv(config['input_csvs_path']+config['hu_hh_pop'][0], 
                dtype={'block_fips':object}, usecols=config['households_columns'])
        df2['block_fips']=df2['block_fips'].apply('{:0>15}'.format)
        df_out = pd.merge(df2, df1, how='left', left_on='block_fips', 
                right_on='geoid%s' % config['census_vintage'][2:])
    except:
        my_message = """
            ERROR - STEP 5 (MASTER):  FAILED TO CREATE DF_OUT
            """
        my_message = ' '.join(my_message.split()) + '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return False

    try:
        df_out['h2only_undev']=0
        df_out['h2only_undev'].loc[df_out["ALAND%s" % \
                config['census_vintage'][2:]]==0]=1
        df_out['h2only_undev'].loc[(df_out["ALAND%s" % \
                config['census_vintage'][2:]]!=0) & \
                (df_out['hu%s' % config['household_vintage']]==0) & \
                (df_out['pop%s' % config['household_vintage']]==0)]=2
    except:
        my_message = """
            ERROR - STEP 5 (MASTER):  TASK 3 of 13 - FAILED TO MAKE H2ONLY_UNDEV 
            IN DF_OUT
            """
        my_message = ' '.join(my_message.split()) + '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return False

    try:
        df_out.to_csv(config['temp_csvs_dir_path']+'h2only_undev_{0}.csv'\
                    .format(config['census_vintage']), \
                columns=['block_fips','h2only_undev'], index=False)
        my_message = """
            INFO - STEP 5 (MASTER):  TASK 3 OF 13 - COMPLETED WRITING 
            H2ONLY_UNDEV.CSV
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return True
    except:
        my_message = """
            ERROR - STEP 5 (MASTER):  TASK 3 OF 13 - FAILED TO WRITE 
            H2ONLY_UNDEV.CSV
            """
        my_message = ' '.join(my_message.split()) + '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return False