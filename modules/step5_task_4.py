import NBM2_functions as nbmf 
import step5_functions as s5f
import geopandas as gpd
import pandas as pd
import traceback
import time

def createBigTractBlocksGeojson(block_df, config, db_config, start_time):
    """
    creates the geojson that contains all of the tracts with more than 50 M
    sq meters area

    Arguments In:
        block_df:           a pandas dataframe containing the block level
                            geometries
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
        block_df:           a pandas dataframe containing the block level
                            geometries
    """
    try:
        temp_time = time.localtime()
        tract_area=pd.read_csv(config['input_csvs_path']+\
                    config['tract_area_file'],dtype={'tract_fips':object})
        file_name = config['temp_speed_geojson_path']\
                    +'bigtract_blocks_5e8_%s_%s.sort.geojson'\
                    % (config['census_vintage'],db_config['SRID'])
        nbmf.silentDelete(file_name)
        block_df[['geoid%s' % config['census_vintage'][2:],'geom']].loc[block_df.tract_id\
                    .isin(tract_area['tract_fips']\
                    .loc[tract_area.land_area_m>=5.e8])]\
                    .sort_values('geoid%s' % config['census_vintage'][2:],ascending=True)\
                    .to_file(file_name, driver='GeoJSON')
        my_message = """
            INFO - STEP 5 (MASTER):  TASK 4 OF 13 - COMPLETED WRITING BIGTRACTS 
            GEOJSON
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return True, block_df
    except:
        my_message = """
            ERROR - STEP 5 (MASTER):  TASK 4 OF 13 - FAILED TO WRITE BIGTRACTS 
            GEOJSON
            """
        my_message = ' '.join(my_message.split())+'\n'+traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return False, None