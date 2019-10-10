import NBM2_functions as nbmf
import step4_functions as s4f 
import geopandas as gpd
import sqlalchemy as sal 
import pandas as pd
import traceback
import time 
import glob 


def changeBBox(new_df, shape_type, config, start_time):
    """
    changes the bounding box values based off of the config file for very 
    specific locations

    Arguments In:
        new_df:         a pandas dataframe containing the bounding box, 
                        several attributes and the geometry for a given 
                        shape file
        shape_type:     a string variables that contains the type of 
                        shape (state, county...) being processed
        config:         a dictionary variabel that contains the 
                        configuration parameters for the step
        start_time:     a time.structure variable that indicates when the 
                        entire step started 

    Arguments Out:
        continue_run:   a boolean variable that indicates whether the run
                        was a success
        new_df:         a pandas dataframe containing the bounding box, 
                        several attributes and the geometry for a given 
                        shape file
    """
    for geo in config['modify_bbox']:
        try:
            temp_time = time.localtime()
            if shape_type == geo[0]:
                idx = new_df[new_df['GEOID'] == geo[1]].index
                new_df.at[idx, 'bbox_arr'] = geo[2]
        except:
            my_message = """
                ERROR - STEP 4 (MASTER): COULD NOT CHANGE BOUNDING BOX DATA FOR
                %s
                """ % geo[0]
            print(nbmf.logMessage(' '.join(my_message.split()) + '\n' +\
                traceback.format_exc(),
                temp_time, time.localtime(), 
                time.mktime(time.localtime())-time.mktime(start_time)))            
            return False, None
    return True, new_df

def createBasicBoundingBoxGeoJson(config, db_config, start_time):  
    """
    creates the bounding boxes for each geometry for the single file
    geographies (county, tribe, state...)

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

    try:
        temp_time = time.localtime()
        # make the connection string
        connection_string = 'postgresql://%s:%s@%s:%s/%s' %\
                (db_config['db_user'], db_config['db_password'], 
                db_config['db_host'], db_config['db_port'], db_config['db'])   
        engine = sal.create_engine(connection_string)

    except:
        print("ERROR CONNECTING TO DATABASE")
        return False

    for shape_type in config['basic_geojson']:
        run_loop = True 
        if shape_type == 'block' and config['step4']['block_bounding_box'] == False:
            run_loop = False
        
        if run_loop and continue_run:
            try:
                temp_time = time.localtime()
                if shape_type == 'block':
                    vintage = config['census_vintage']
                else:
                    vintage = config['geometry_vintage']
                sql_string = """
                    SELECT * FROM %s.nbm2_%s_%s
                    """ % (db_config['db_schema'], shape_type, vintage)
                shape_df = gpd.read_postgis(sql_string, engine, 
                            geom_col='GEOMETRY', crs=starting_crs)

            except:
                print(traceback.format_exc())
                return False

            continue_run, new_df = s4f.makeBasicGeoJson(shape_df,shape_type, 
                                                    config, start_time)

            if continue_run:
                continue_run, new_df = changeBBox(new_df, shape_type, 
                                                    config, start_time)

            if continue_run:
                continue_run, new_df = s4f.dropColumns(new_df, shape_type, 
                                                    config, start_time)

            if continue_run:
                continue_run = s4f.writeGeoJson(new_df, config, shape_type, start_time)
            
    return continue_run