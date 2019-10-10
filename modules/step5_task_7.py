import NBM2_functions as nbmf 
import step5_functions as s5f
import geopandas as gpd 
import pandas as pd 
import traceback
import time

def createTractArea(config,tract_df,start_time):
    """
    creates the tract area file (csv) that records how much area each tract
    contains

    Arguments In:
        config:             a dictionary that contains the configuration
                            information of various steps of NMB2 data 
                            processing
        db_config:          a dictionary that contains the configuration
                            information for the database and queue
        start_time:         a time structure variable that indicates when 
                            the current step started

    Arguments Out:
        continue_run:       a boolean variable that indicates if the routine
                            successfully completed and whether the next 
                            steps should be exectuted
        tract_df:           a pandas dataframe that contains the dissolved
                            tract level information used by later routines
    """
    temp_time = time.localtime()
    tract_df['tract_id']=tract_df.index
    tract_df['area']=tract_df.to_crs({'init':'epsg:3857'}).geometry.area

    if config['step5']['tracts_area.csv']:
        try:
            tract_df.sort_index(inplace=True)
            tract_df[['tract_id','area']].to_csv(config['temp_csvs_dir_path']+\
                    'tract_area_m_2_%s.csv' % config['geometry_vintage'], 
                    index=False)

            my_message = """
                INFO - STEP 5 (MASTER): TASK 7 OF 13 - CREATED TRACT AREA FILE
                """
            my_message = ' '.join(my_message.split())
            print(nbmf.logMessage(my_message, temp_time, time.localtime(),
                time.mktime(time.localtime())-time.mktime(start_time)))
            return True, tract_df
        except:
            my_message = """
                ERROR - STEP 5 (MASTER): TASK 7 OF 13 - FAILED TO WRITE TRACT AREA FILE
                """
            my_message = ' '.join(my_message.split()) + '\n' + traceback.format_exc()
            print(nbmf.logMessage(my_message, temp_time, time.localtime(),
                time.mktime(time.localtime())-time.mktime(start_time)))
            return False, None
    else:
        return True, tract_df