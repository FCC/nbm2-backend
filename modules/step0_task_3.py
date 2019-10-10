import NBM2_functions as nbmf 
import geopandas as gpd 
import pandas as pd 
import numpy as np 
import traceback 
import time



def createTerritoryGeometries(config, start_time):
    """
    Creates geometries at the tract and county level for US territories.  
    They are not included in the US Cartographic boundary files so we have
    to create them.  This routine makes for gz files for each territory at
    the tract level and then updates the county level gz file to include data
    from the territories.  This will need to be reviewed when census vintage 
    changes to 2020 since the standard naming convention for carto files 
    changed in 2013

    Arguments In:
        config:         the json variable that contains all configration
                        data required for the data processing

        start_time:     the clock time that the step began using the 
                        time.clock() format

    Arguments Out:
        continue_run:   a boolean variable that indicates if the routine
                        successfully completed and whether the next steps
                        should be executed

    """
    # get the correct names for all of the provinces within each territory
    file_name = config['shape_files_path'] + config['county_shape_file_name']
    names_df = gpd.read_file(file_name)
    names_df.rename(columns={'NAMELSAD':'NAME'})
    names_df = names_df[['GEOID', 'NAME']]

    df_holder = []
    # read in block files for the 4 excluded US territories
    for territory in ['60','66','69','78']:
        try:
            temp_time = time.localtime()
            # open the appropriate block file for the given territory
            file_name = config['shape_files_path'] +\
                        "block/tl_%s_%s_tabblock%s.shp" %\
                        (config['census_vintage'],territory,config['census_vintage'][2:])
            temp_df = gpd.read_file(file_name)
            # modify the column names so they match what we expect in the tract and 
            # county geojson files
            change_columns = {  'STATEFP%s' % config['census_vintage'][2:]:'state_fips', 
                                'COUNTYFP%s' % config['census_vintage'][2:]: 'county_fips',
                                'GEOID%s' % config['census_vintage'][2:]:'block_fips',
                                'ALAND%s' % config['census_vintage'][2:]:'aland'}
            temp_df.rename(columns=change_columns, inplace=True)

            # create the tract file for the given territory
            tract_df = temp_df[['block_fips', 'aland', 'geometry']]
            tract_df['GEOID'] = tract_df['block_fips'].str[:11]
            tract_df['NAME']=tract_df['GEOID'].str[5:11]
            tract_df['NAME'] = np.where(tract_df['NAME'].str[4:6] != '00', 
                                        tract_df['NAME'].str[:4] + "." + tract_df['NAME'].str[4:6], 
                                        tract_df['NAME'].str[:4])

            # dissolve the blocks into tract level detail
            tract_df=tract_df[['GEOID', 'NAME', 'geometry']].loc[tract_df['aland']>0].dissolve(by='GEOID')
            tract_df.reset_index(inplace=True)

            # save the newly created tracts for the territory into a shape file
            # for later use by processes
            file_name = config['shape_files_path'] +\
                        "tract/gz_%s_%s_140_00_500k.shp" %\
                        (config['census_vintage'],territory)
            tract_df.to_file(file_name)

            # provide status or data processing
            my_message = """
                INFO - STEP 0 (MASTER): TASK 3 OF 13 - FINISHED WRITING TRACT SHAPE FILE
                FOR US TERRITORY %s
                """ % territory
            my_message = ' '.join(my_message.split()) 
            print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        except:
            # there was an error in processing.  Capture the error and output the
            # stacktrace to the screen
            my_message = """
                ERROR - STEP 0 (MASTER): TASK 3 OF 13 - FAILED WRITING TRACT SHAPE FILE
                FOR US TERRITORY %s
                """ % territory        
            my_message += "\n" + traceback.format_exc()
            print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
            return False

        try:
            # create the dataframe for capturing county level data
            temp_time = time.localtime()
            county_df = temp_df[['state_fips', 'county_fips', 'aland', 'geometry']]
            county_df['GEOID'] = county_df['state_fips'] + county_df['county_fips']

            # merge the block level data at the county level to get the geometry
            county_df=county_df[['GEOID', 'geometry']].loc[county_df['aland']>0].dissolve(by='GEOID')

            # the county records for US states include names.  The names cannot
            # be easily constructed following a set of rules, so instead we just
            # merge the names of the territories that are listed in the tiger line
            # files with the geometries we just calculated.  This ends up giving
            # us the information we need to create the equivalent of a fully 
            # populated 2010 county cartographic file that includes territories
            county_df = county_df.merge(names_df, left_on='GEOID', right_on='GEOID')
            county_df = county_df[['GEOID', 'NAME', 'geometry']]

            # append the information to a list that we will process later
            df_holder.append(county_df)

            # provide the status on the data processing for this task
            my_message = """
                INFO - STEP 0 (MASTER): TASK 3 OF 13 - PROCESSED COUNTY DATA FOR
                US TERRITORY %s
                """ % territory
            my_message = ' '.join(my_message.split()) 
            print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        except:
            # there was an error in processing.  Capture the error and output the
            # stacktrace to the screen        
            my_message = """
                ERROR - STEP 0 (MASTER): TASK 3 OF 13 - FAILED PROCESSING COUNTY DATA
                FOR US TERRITORY %s
                """ % territory        
            my_message += "\n" + traceback.format_exc()
            print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
            return False        

    try:
        # now that we have the county level data for the territories, we need to merge
        # it with the US county data and create a single file for subsequent processing
        # open the county cartographic bounday file
        file_name = config['shape_files_path'] + config['county_cb_shape_file_name']
        county = gpd.read_file(file_name)

        # the cartographic boundary files do not have full names, so concatenate the 
        # name and lsad columns and overwrite the original name
        county['NAME']=county['NAME'] + ' ' + county['LSAD']

        # extract the county fips from the non-standard county fips identifier in the
        # 2010 cartographic boundary file and then preserve only the necessary columns
        county['GEOID']=county['GEO_ID'].str[9:]
        county = county[['GEOID', 'NAME','geometry']]

        # append the county data to the list to be used to build the single file
        df_holder.append(county)

        # merge all of the dataframes into a single dataframe, sort it, and then 
        # write the file out as a shape file so it can be used later for subsequent
        # data processing
        counties = pd.concat([x for x in df_holder])
        counties.sort_values(by='GEOID',inplace=True)
        file_name = config['shape_files_path'] + config['county_gzm_shape_file_name']
        counties.to_file(file_name)
    
        # provide the status on the data processing for this task
        my_message = """
            INFO - STEP 0 (MASTER): TASK 3 OF 13 - COMPLETED UPDATING COUNTY 
            CARTOGRAPHIC SHAPE FILE
            """ 
        my_message = ' '.join(my_message.split()) 
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
        time.mktime(time.localtime())-time.mktime(start_time)))  
        return True 

    except:
        # there was an error in processing.  Capture the error and output the
        # stacktrace to the screen        
        my_message = """
            ERROR - STEP 0 (MASTER): TASK 3 OF 13 - FAILED UPDATING COUNTY 
            CARTOGRAPHIC SHAPE FILE
            """         
        my_message += "\n" + traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
        time.mktime(time.localtime())-time.mktime(start_time)))
        return False    

