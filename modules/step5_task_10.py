import NBM2_functions as nbmf 
import step5_functions as s5f
import geopandas as gpd 
import pandas as pd 
import traceback 
import time


def createTractCarto(config, db_config, start_time):
    """
    creates the trimmed down version of the nbm2_tracts file that includes
    the required information that allows tract_numprov data to be mapped
    to the appropriate geometries

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
    """
    try:
        temp_time = time.localtime()
        # Read in tract level geojson, rename GEOID to tract_id so that tract 
        # geometry can be linked to tract_numprov data, and keep only the 
        # necessary columns 
        carto_tract_out=gpd.read_file(config['temp_geog_geojson_path']+\
                'nbm2_tract_%s.geojson' % (config['geometry_vintage']))
        carto_tract_out.rename(columns={'GEOID':'tract_id'},inplace=True)
        carto_tract_out=carto_tract_out[['tract_id','geometry']]

        # write the file
        file_name = config['temp_speed_geojson_path']+\
                'tract_%s_500k_%s.sort.geojson' %\
                (config['census_vintage'], db_config['SRID'])
        nbmf.silentDelete(file_name)
        carto_tract_out.sort_values('tract_id',ascending=True)\
                .to_file(file_name,driver='GeoJSON')

        my_message = """
            INFO - STEP 5 (MASTER): TASK 10 OF 13 - COMPLETED WRITING TRACT LEVEL
            CARTOGRAPHIC BASED GEOJSON
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))           
        return True
    except:
        my_message = """
            ERROR - STEP 5 (MASTER): TASK 10 OF 13 - FAILED WRITING TRACT LEVEL 
            CARTOGRAPHIC BASED GEOJSON
            """
        my_message = ' '.join(my_message.split()) + '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return False
