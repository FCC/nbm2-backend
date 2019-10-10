import NBM2_functions as nbmf 
import step5_functions as sf5
import geopandas as gpd 
import traceback
import time

def createBlockUSGeojson(block_df, config, start_time):
    """
    creates the block_us_20X0.sort.geojson dataframe

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
        block_df:           a pandas dataframe containing the block level
                            geometries
    """
    try:
        temp_time = time.localtime()
        file_name = config['temp_speed_geojson_path']+\
                    'block_us_{0}.sort.geojson'.format(config['census_vintage'])
        nbmf.silentDelete(file_name)
        block_df[['geoid%s' % config['census_vintage'][2:],'geom']]\
                .sort_values('geoid%s' % config['census_vintage'][2:],ascending=True)\
                .to_file(file_name, driver='GeoJSON')
        my_message = """
            INFO - STEP 5 (MASTER): TASK 2 OF 13 - COMPLETED WRITING 
            BLOCK_US_SORT.GEOJSON
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return True, block_df
    except:
        my_message = """
            ERROR - STEP 5 (MASTER): TASK 2 OF 13 - FAILED TO WRITE 
            BLOCK_US_SORT.GEOJSON
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message + '\n' + traceback.format_exc(), 
            temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return False, None
