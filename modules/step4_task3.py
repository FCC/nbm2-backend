import NBM2_functions as nbmf
import step4_functions as s4f 
import geopandas as gpd
import pandas as pd
import traceback 
import time 
import glob 
import os


def makeMapBoxTile(file_name, config, start_time):
    """
    creates the mapbox tile used for later steps in the process

    Arguments In:
        file_name:      a string variable containing the full path to the 
                        geojson file being converted to a map box tile
        config:         a dictionary variable that contains the 
                        configuration parameters for the step
        start_time:     a time.structure variable that indicates when the 
                        entire step started 

    Arguments Out:
        continue_run:   a boolean variable that indicates whether the run
                        was a success
    """
    # build the file data used to create the tippe command
    temp_time = time.localtime()
    root_file = file_name.split('/')[-1].split('.')[0] 
    output_file_path = config['output_mbtiles_path'] + root_file + '.mbtiles '
    try:

        # build and run the tippe command
        zoom_level = config['zoom_params'][root_file.split('_')[1]]
        tippe_command = """
            tippecanoe -o %s %s --force --coalesce-smallest-as-needed 
            --drop-densest-as-needed --minimum-zoom=%s --maximum-zoom=%s
            """ % (output_file_path, file_name, zoom_level[0], zoom_level[1])
        tippe_command = " ".join(tippe_command.split())
        os.system(tippe_command)

        # output runtime message
        my_message = """
            INFO - STEP 4 (MASTER): %s CREATED
            """ % root_file
        print(nbmf.logMessage(' '.join(my_message.split()), 
        temp_time, time.localtime(), 
        time.mktime(time.localtime())-time.mktime(start_time)))
        return True
    except:
        my_message = """
            ERROR - STEP 4 (MASTER): %s MAPBOX TILE COULD NOT BE CREATED
            """ % root_file
        print(nbmf.logMessage(' '.join(my_message.split()) + '\n' +\
            traceback.format_exc(), temp_time, time.localtime(), 
            time.mktime(time.localtime())-time.mktime(start_time)))
        return False

def createMBTiles(config, start_time):
    """
    converts the geojsons created in the previous steps into Map Box Tiles

    Arguments In:
        config:         a dictionary variable that contains the 
                        configuration parameters for the step
        start_time:     a time.structure variable that indicates when the 
                        entire step started 

    Arguments Out:
        continue_run:   a boolean variable that indicates whether the run
                        was a success
    """  
    continue_run = True
    all_shapes = config['basic_geojson'][:]
    all_shapes.extend(config['iter_geojson'])
    for shape_type in all_shapes:
        run_loop = True 
        if shape_type == 'block' and config['step4']['block_bounding_box'] == False:
            run_loop = False
        if run_loop and continue_run:
            file_name = config['temp_geog_geojson_path'] + '/nbm2_%s_%s.geojson' % (shape_type, config["geometry_vintage"])
            continue_run = makeMapBoxTile(file_name, config, start_time)
    
    return continue_run