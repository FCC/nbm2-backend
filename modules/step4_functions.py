import NBM2_functions as nbmf
import geopandas as gpd
import pandas as pd
import traceback
import time 

# helper function that gets called by geopandas in the lambda call
def concat_list(minx,miny,maxx,maxy):
    """
    converts the bounding box to a string

    Arguments In:   
        minx:  a float variable that contains the minimum lat coordinate
        miny:  a float variable that contains the minimum lon coordinate
        maxx:  a float variable that contains the maximum lat coordinate
        maxy:  a float variable that contains the maximum lon coordinate

    Arguments Out:
        l:      a string variable that contains the bounding box
    """
    l = []
    l.append(minx)
    l.append(miny)
    l.append(maxx)
    l.append(maxy)
    l = str(l)
    return l

def makeBasicGeoJson(shape_df, shape_type, config, start_time): 
    """
    processes a basic shape file to prepare it for writing out as a geojson

    Arguments In:
        shape_df:       a pandas dataframe that contains the full contents 
                        of a shape file (or set of shapefiles)
        shape_type:     a string variable that indicates the type of shape
                        being processed
        config:         a dictionary variable that contains the 
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
    try:
        temp_time = time.localtime()
        # create a dataframe of minx, miny, maxx, and maxy values based on the 
        # geometry column
        bounds_df = shape_df['GEOMETRY'].bounds

        # create a new column to populate lists of [minx,miny,maxx,maxy]
        # the first assignment of '' is to ensure that the column is of data 
        # type 'object'
        bounds_df['bbox_arr'] = ''
        bounds_df['bbox_arr'] = bounds_df.apply(lambda row: concat_list(row['minx'],
                                    row['miny'],row['maxx'],row['maxy']),axis=1)
        # concatenate the attribute columns in shape_df and the bounds dataframes 
        # into one dataframe
        new_df = pd.concat([shape_df, bounds_df], axis=1)
        if shape_type != 'block':
            sort_column = 'GEOID'
        else:
            sort_column = 'BLOCK_FIPS'
        new_df.sort_values(sort_column, inplace=True)
        return True, new_df
    except:
        my_message = """
            ERROR - STEP 4 (MASTER): COULD NOT PREPARE GEODATAFRAME FOR %s
            """ % shape_type
        print(nbmf.logMessage(' '.join(my_message.split()) + '\n' +\
            traceback.format_exc(),
            temp_time, time.localtime(), 
            time.mktime(time.localtime())-time.mktime(start_time)))
        return False, None

def dropColumns(new_df, shape_type, config, start_time):
    """
    drops the unnecessary columns from the dataframe so that we save memory
    and only the essential data points are preserved

    Arguments In:
        new_df:         a pandas dataframe containing the bounding box, 
                        several attributes and the geometry for a given 
                        shape file         
        shape_type:     a string variable that indicates the type of shape
                        being processed
        config:         a dictionary variable that contains the 
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
    try:
        temp_time = time.localtime()
        unneeded_cols = []
        if shape_type == 'block':
            compare_list = config['keep_columns_block']
        elif shape_type == 'tribe':
            compare_list = config['keep_columns_tribe']
        else:
            compare_list = config['keep_columns']

        for col in new_df.columns:
            if col not in compare_list:
                unneeded_cols.append(col)
        new_df.drop(unneeded_cols,axis=1,inplace=True)

        return True, new_df
    except:
        my_message = """
            ERROR - STEP 4 (MASTER): COULD NOT DROP COLUMNS FOR THE DATAFRAME
            """
        print(nbmf.logMessage(' '.join(my_message.split()) + '\n' +\
            traceback.format_exc(),
            temp_time, time.localtime(), time.mktime(time.localtime())-\
            time.mktime(start_time)))   
        return False, None

def writeGeoJson(new_df, config, shape_type, start_time):
    """
    writes the geojson file that will be used to create the mapbox file

    Arguments In:
        new_df:         a pandas dataframe containing the bounding box, 
                        several attributes and the geometry for a given 
                        shape file
        config:         a dictionary variable that contains the 
                        configuration parameters for the step
        shape_type:     a string variable that indicates the type of shape
                        being processed
        start_time:     a time.structure variable that indicates when the 
                        entire step started 

    Arguments Out:
        continue_run:   a boolean variable that indicates whether the run
                        was a success
    """
    try:
        temp_time = time.localtime()
        temp_geo_json_file_name = config['temp_geog_geojson_path']+\
                'nbm2_%s_%s.geojson' % (shape_type, config['geometry_vintage'])
        nbmf.silentDelete(temp_geo_json_file_name)
        new_df.to_file(temp_geo_json_file_name, driver="GeoJSON")  
        my_message = """
            INFO - STEP 4 (MASTER):  COMPLETED WRITING GEOJSON FILE - CREATED %s
            """ % (shape_type.upper())
        print(nbmf.logMessage(' '.join(my_message.split()), temp_time, 
            time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return True
    except:
        my_message = """
            ERROR - STEP 4 (MASTER):  FAILED TO WRITE GEOJSON FILE FOR %s
            """ % (shape_type.upper())
        print(nbmf.logMessage(' '.join(my_message.split()) + '\n' +
            traceback.format_exc(), temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return False