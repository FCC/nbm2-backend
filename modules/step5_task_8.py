import NBM2_functions as nbmf 
import step5_functions as sf5
import geopandas as gpd
import pandas as pd 
import traceback
import time


def writeNotLargeTracts(config, db_config, tract_df, start_time):
    """
    creates the geometry file of tracts that are not considered "large"

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
        # Write out tracts and geometry for not-large tracts (<=5.e8 square meters)
        temp_time = time.localtime()
        file_name = config['temp_csvs_dir_path']+'tract_area_m_2_%s.csv' %\
                    config['geometry_vintage']
        tract_area=pd.read_csv(file_name, dtype={'tract_id':str})
        file_name = config['temp_speed_geojson_path']+\
                'notbig_tracts_5e8_%s_%s.sort.geojson' %\
                (config['census_vintage'], db_config['SRID'])
        nbmf.silentDelete(file_name)
        tract_df.sort_index(inplace=True)
        tract_df[['tract_id','geometry']].loc[tract_df.tract_id\
                .isin(tract_area['tract_id'].loc[tract_area.area<=5.e8])]\
                .to_file(file_name, driver='GeoJSON')

        my_message = """
            INFO - STEP 5 (MASTER): TASK 8 OF 13 - CREATED NOT BIG TRACTS GEOJSON
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return True
    except: 
        my_message = """
            ERROR - STEP 5 (MASTER): TASK 8 OF 13 - FAILED TO CREATE 
            NOT_BIG_TRACTS GEOJSON
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)), 
            '\n', traceback.format_exc())
        return False
