import NBM2_functions as nbmf
import step4_functions as s4f 
import geopandas as gpd
import pandas as pd
import traceback
import time 
import glob

def createBoundingBoxesComplex(config, db_config, start_time):
    """
    creates the bounding boxes for the geometries whose data comes in 
    multiple files (blocks, tracts, places)

    Arguments In:
        config:         a dictionary variabel that contains the 
                        configuration parameters for the step
        start_time:     a time.structure variable that indicates when the 
                        entire step started 

    Arguments Out:
        continue_run:   a boolean variable that indicates whether the run
                        was a success
    """  
    continue_run = True
    starting_crs={'init':'epsg:%s' % db_config['SRID']}
    
    for fldr in config['iter_geojson']:
        try:
            files = glob.glob(config['shape_files_path']+'%s/*.shp' % fldr)
            big_df = []
            for f in files:
                territory_finder = f.split('_')
                if territory_finder[3] in ['60','66','69','78']:
                    big_df.append(gpd.read_file(f))
                else:
                    temp_df = gpd.read_file(f)
                    temp_df.rename(columns={'GEO_ID':'GEOID'},inplace=True)
                    temp_df=temp_df[['GEOID','NAME','geometry']]
                    temp_df['GEOID']=temp_df['GEOID'].str[-11:] 
                    big_df.append(temp_df)

            temp_time = time.localtime()
            big_df = pd.concat([df for df in big_df],sort=True)
            big_df.rename(columns={'geometry':'GEOMETRY'},inplace=True)
            big_df = gpd.GeoDataFrame(big_df, geometry='GEOMETRY')
            big_df.crs=starting_crs

            my_message = """
                INFO - STEP 4 (MASTER): COMPLETED READING IN ALL FILES FOR %s
                THERE ARE %s ROWS
                """ % (fldr.upper(), len(big_df))
            print(nbmf.logMessage(' '.join(my_message.split()), 
                temp_time, time.localtime(), 
                time.mktime(time.localtime())-time.mktime(start_time)))

        except:
            my_message = """
                ERROR - STEP 4 (MASTER): FAILED READING IN ALL FILES FOR %s
                """ % (fldr.upper())
            my_message += '\n'+ traceback.format_exc()
            print(nbmf.logMessage(' '.join(my_message.split()), 
                temp_time, time.localtime(), 
                time.mktime(time.localtime())-time.mktime(start_time)))
            return False

        if continue_run:
            continue_run, new_df = s4f.makeBasicGeoJson(big_df, fldr, config, 
                                                    start_time)

        if continue_run:
            continue_run, new_df = s4f.dropColumns(new_df, fldr, config, start_time)

        if continue_run:
            continue_run = s4f.writeGeoJson(new_df, config, fldr, start_time)

    return continue_run

