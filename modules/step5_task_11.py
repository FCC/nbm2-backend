import NBM2_functions as nbmf 
import step5_functions as s5f
import geopandas as gpd 
import pandas as pd 
import traceback
import time

def createCountyGeojson(config, db_config, tract_df, start_time):
    """
    creates the county geometry file that includes the county level details
    fo all use counties and territories

    Arguments In:
        config:             a dictionary that contains the configuration
                            information of various steps of NMB2 data 
                            processing
        db_config:          a dictionary that contains the configuration
                            information for the database and queue
        tract_df:           a pandas dataframe that contains the dissolved
                            tract level information used by later routines
        start_time:         a time structure variable that indicates when 
                            the current step started

    Arguments Out:
        continue_run:       a boolean variable that indicates if the routine
                            successfully completed and whether the next 
                            steps should be exectuted

    """
    try:
        temp_time = time.localtime()

        # read in US county cartographic boundary file, keep only the necessary 
        # columns and change GEOID to county_id so the data can be cleanly 
        # mapped between county_numprov and geometry
        gzm_file = config['shape_files_path'] + config['county_gzm_shape_file_name']
        county_out = gpd.read_file(gzm_file)
        county_out.rename(columns={'GEOID':'county_id'}, inplace=True)
        county_out=county_out[['county_id','geometry']]

        # write the file
        file_name = config['temp_speed_geojson_path']+\
                'county_%s_500k_%s.sort.geojson' %\
                (config['census_vintage'], db_config['SRID'])
        nbmf.silentDelete(file_name)
        county_out.sort_values('county_id',ascending=True)\
                .to_file(file_name, driver='GeoJSON')

        my_message = """
            INFO - STEP 5 (MASTER): TASK 11 OF 13 - COMPLETED WRITING COUNTY
            LEVEL CARTOGRPAHIC BASED GEOJSON
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))           
        return True            
    except:
        my_message = """
            ERROR - STEP 5 (MASTER): TASK 11 OF 13 - FAILED WRITING COUNTY LEVEL
            CARTOGRPAHIC BASED GEOJSON
            """
        my_message = ' '.join(my_message.split()) + '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return False