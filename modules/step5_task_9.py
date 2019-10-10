import NBM2_functions as nbmf 
import step5_functions as s5f
import geopandas as gpd
import pandas as pd 
import traceback
import time

def createLandTracts(config, db_config, tract_df,start_time):
    """
    creates the geometry file that contains tract level details for tracts that
    contain only land

    Arguments In:
        config:         a dictionary that contains the configuration
                        information of various steps of NMB2 data 
                        processing
        db_config:      a dictionary that contains the configuration
                        information for the database and queue
        tract_df:       a pandas dataframe that contains the dissolved
                        tract level information used by later routines
        start_time:     a time structure variable that indicates when 
                        the current step started

    Arguments Out:
        continue_run:   a boolean variable that indicates if the routine
                        successfully completed and whether the next 
                        steps should be exectuted

    """ 
    try:
        temp_time = time.localtime()
        file_name = config['temp_speed_geojson_path']+\
                'tracts_land_%s_%s.sort.geojson' %\
                (config['census_vintage'], db_config['SRID'])
        nbmf.silentDelete(file_name)

        tract_df.reset_index(drop=True, inplace = True)
        tract_df.sort_values('tract_id',ascending=True)\
                .to_file(file_name,driver='GeoJSON')
        tract_df.set_index('tract_id',inplace=True, drop=False)

        my_message = """
            INFO - STEP 5 (MASTER): TASK 9 OF 13 - CREATED LAND TRACTS GEOJSON
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return True
    except:
        my_message = """
            ERROR - STEP 5 (MASTER): TASK 9 OF 13 - FAILED WRITING TRACTS_LAND
            GEOJSONS
            """
        my_message = ' '.join(my_message.split()) + '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return False