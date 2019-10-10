import NBM2_functions as nbmf 
import step5_functions as s5f
import geopandas as gpd
import pandas as pd
import traceback
import time 

def createLargeBlocksGeojson(block_df, config, start_time):
    """
    creates the file that contains the geometries for all blocks that are 
    considered "large".  This is used to inform zoom-levels later

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
        list_df=pd.read_csv(config['input_csvs_path']+config['large_blocks_file'], 
                sep='|', quotechar="'", dtype={'block_fips':object})
        list_df['min_zoom']=list_df[['x_zoom','y_zoom']].min(axis=1)
        block_large_df=block_df.loc[block_df['geoid%s' % config['census_vintage'][2:]].
                isin(list_df['block_fips'])]
        block_large_df=block_large_df.merge(list_df[['block_fips','min_zoom']], 
                left_on='geoid%s' % config['census_vintage'][2:], right_on='block_fips')  
        file_name = config['temp_speed_geojson_path']+'xlarge_blocks_{0}.geojson'\
                .format(config['census_vintage'])
        nbmf.silentDelete(file_name)
        block_large_df[['geoid%s' % config['census_vintage'][2:],'min_zoom','geom']].\
                sort_values('geoid%s' % config['census_vintage'][2:],ascending=True).\
                to_file(file_name, driver='GeoJSON')
        my_message = """
            INFO - STEP 5 (MASTER):  TASK 5 OF 13 - COMPLETED WRITING LARGE 
            BLOCKS GEOJSON
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return True, block_df
    except:
        my_message = """
            ERROR - STEP 5 (MASTER):  TASK 5 OF 13 - FAILED TO WRITE LARGE 
            BLOCKS GEOJSON
            """
        my_message = ' '.join(my_message.split())+'\n'+traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time))) 
        return False, None
