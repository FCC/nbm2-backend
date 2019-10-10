import NBM2_functions as nbmf 
import step5_functions as s5f
import geopandas as gpd 
import pandas as pd 
import traceback 
import time
import glob


def concat_list(minx,miny,maxx,maxy):
    """
    converts four values in separate fields in a geopandas dataframe into the
    specific format required to create a bounding box for the geometry
    Arguments In:
        minx    The southern boundary of a geometry's bounding box
        miny    The western boundary (for North America) of a geometry's 
                bounding box
        maxx    The northern boundary of a geometry's bounding box 
        maxy    The eastern bounday (for North America) of a geometry's
                bounding box
    Arguments Out:
        l       a string that contains a geometry's formatted bounding box 
    """
    l = []
    l.append(minx)
    l.append(miny)
    l.append(maxx)
    l.append(maxy)
    l = "{" + ','.join([str(x) for x in l]) + '}'
    return l

def makeProviderBBoxFile(config, large_provider_df, other_provider_df, start_time):
    """
    finds the bounding box that includes all separate geometries for each 
    provider

    Arguments In:
        config:             a dictionary that contains the configuration
                            information of various steps of NMB2 data 
                            processing
        large_provider_df:  a pandas dataframe containing aggregated data for
                            large broadband providers (geographic coverage)
        other_provider_df:  a pandas dataframe containing aggretaged data for 
                            all other broadband providers that dont qualify
                            for "large"
        start_time:         a time structure variable that indicates when 
                            the current step started
    Arguments Out:
        continue_run:       a boolean variable that indicates if the routine
                            successfully completed and whether the next 
                            steps should be exectuted
    """
    df_holder = []
    counter = 1
    for df in [other_provider_df, large_provider_df]:
        try:
            temp_time = time.localtime()
            # find the max and min (x,y) coordinates for each hoconum and 
            # write them down for each record
            df.set_index('hoconum',inplace=True)
            bounds_df = df["geometry"].bounds
            bounds_df['h_minx'] = bounds_df.groupby(['hoconum'])['minx'].min() 
            bounds_df['h_miny'] = bounds_df.groupby(['hoconum'])['miny'].min()    
            bounds_df['h_maxx'] = bounds_df.groupby(['hoconum'])['maxx'].max()
            bounds_df['h_maxy'] = bounds_df.groupby(['hoconum'])['maxy'].max()

            # remove the excess columns and rows that are no longer needed  
            # end state is the reduced dataframe that only contains the terms
            # that will eventually become the bounding box
            bounds_df=bounds_df[['h_minx', 'h_miny','h_maxx', 'h_maxy']]
            bounds_df.reset_index(inplace=True)
            bounds_df.sort_values('hoconum',inplace=True)
            bounds_df.drop_duplicates(inplace=True)

            # create the bounding box and then format it so that the term is in 
            # the correct format before writing
            bounds_df['bbox_arr'] = ''
            bounds_df['bbox_arr'] = bounds_df.apply(lambda row: concat_list(row['h_minx'],row['h_miny'],row['h_maxx'],row['h_maxy']),axis=1)  

            # add the fbd_vintage to the dataframe
            bounds_df['year']     = config['fbd_vintage']

            # retain only the columns that are necessary for the provider look up
            bounds_df = bounds_df[["year", "hoconum", "bbox_arr"]]

            # add the dataframe to df_holder so we can concatenate everything 
            # together when we are done loading data
            df_holder.append(bounds_df)
            my_message = """
            INFO - STEP 5 (MASTER): TASK 12 OF 12 - SUCCESSFULLY PROCESSED %s OF
            2 PROVIDER GEOJSON FILES
            """ % str(counter)
            my_message = ' '.join(my_message.split())
            print(nbmf.logMessage(my_message, temp_time, time.localtime(),
                time.mktime(time.localtime())-time.mktime(start_time)))
            counter += 1
        except:
            my_message = """
            ERROR - STEP 5 (MASTER): TASK 12 OF 12 - FAILED TO PROCESS %s OF
            2 PROVIDER GEOJSON FILES
            """ % str(counter)
            my_message = ' '.join(my_message.split())
            my_message += '\n' + traceback.format_exc()
            print(nbmf.logMessage(my_message, temp_time, time.localtime(),
                time.mktime(time.localtime())-time.mktime(start_time)))
            return False

    # ingest the previous provider look up table
    try:
        temp_time = time.localtime()
        old_df = pd.read_csv(config['input_csvs_path']+config['previous_provider_lookup_table'])
        df_holder.append(old_df)
        my_message = """
        INFO - STEP 5 (MASTER): TASK 12 OF 12 - SUCCESSFULLY INGESTED PREVIOUS
        FBD_VINTAGE PROVIDER BOUNDING BOX LOOK UP FILE
        """ 
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
    except:
        my_message = """
        ERROR - STEP 5 (MASTER): TASK 12 OF 12 - FAILED TO INGEST PREVIOUS
        FBD_VINTAGE PROVIDER BOUNDING BOX LOOK UP FILE 
        """ 
        my_message = ' '.join(my_message.split())
        my_message += '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return False

    try:
        # concatenate the dataframes together
        temp_time = time.localtime()
        provider_df = pd.concat(df_holder)
        my_message = """
        INFO - STEP 5 (MASTER): TASK 12 OF 12 - SUCCESSFULLY CONCATENATED 
        DATAFRAMES TO MAKE SINGLE PROVIDER BOUNDING BOX LOOK UP DATA FRAME
        """ 
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))        
    except:
        my_message = """
        ERROR - STEP 5 (MASTER): TASK 12 OF 12 - FAILED TO CONCATENATE DATAFRAMES
        TO MAKE SINGLE PROVIDER BOUNDING BOX LOOK UP DATA FRAME 
        """ 
        my_message = ' '.join(my_message.split())
        my_message += '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return False


    # write out the csv
    try:
        temp_time = time.localtime()        
        destination = config['output_dir_path'] + "provider_lookup_table_%s.csv" % config['fbd_vintage']
        provider_df.to_csv(destination, encoding='utf-8',index=False)
        my_message = """
        INFO - STEP 5 (MASTER): TASK 12 OF 12 - SUCCESSFULLY WROTE OUT PROVIDER
        BOUNDING BOX LOOK UP CSV FILE
        """ 
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))  
        return True
    except:
        my_message = """OUT PROVIDER WRITE OUT BOUNDING BOX LOOK UP CSV FILE 
        """ 
        my_message = ' '.join(my_message.split())
        my_message += '\n' + traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return False

def writeProviderFiles(config, large_provider_df, other_provider_df, start_time):
    """
    generates the provider files for large and other providers

    Arguments In:
        config:             a dictionary that contains the configuration
                            information of various steps of NMB2 data 
                            processing
        large_provider_df:  a pandas dataframe containing aggregated data for
                            large broadband providers (geographic coverage)
        other_provider_df:  a pandas dataframe containing aggretaged data for 
                            all other broadband providers that dont qualify
                            for "large"
        start_time:         a time structure variable that indicates when 
                            the current step started

    Arguments Out:
        continue_run:       a boolean variable that indicates if the routine
                            successfully completed and whether the next 
                            steps should be exectuted
    """
    try:
        # write the large provider file
        temp_time = time.localtime()
        file_name = config['temp_speed_geojson_path']+'%s_prov_lg.geojson' % config['fbd_vintage']
        nbmf.silentDelete(file_name)
        large_provider_df.to_file(file_name, driver='GeoJSON')
        my_message = """
            INFO - STEP 5 (MASTER): TASK 12 OF 13 - COMPLETED CREATING LARGE 
            PROVIDER GEOJSON
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
    except:
        my_message = """
            ERROR - STEP 5 (MASTER): TASK 12 OF 13 - FAILED CREATING LARGE 
            PROVIDER GEOJSON
            """
        my_message = ' '.join(my_message.split())+'\n'+traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return False

    try:
        temp_time = time.localtime()
        file_name = config['temp_speed_geojson_path']+'%s_prov_other.geojson' % config['fbd_vintage'] 
        nbmf.silentDelete(file_name)       
        other_provider_df.to_file(file_name, driver='GeoJSON')
        my_message = """
            INFO - STEP 5 (MASTER): TASK 13 OF 13 - COMPLETED CREATING OTHER 
            PROVIDER GEOJSON
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return True
    except:
        my_message = """
            ERROR - STEP 5 (MASTER): TASK 13 OF 13 - FAILED CREATING OTHER 
            PROVIDER GEOJSON
            """
        my_message = ' '.join(my_message.split())+'\n'+traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return False

def concatCountyHoCoNum(config, df_holder, start_time):
    """
    concatenates all of the county level dataframes for provider information
    and prepares two final data frames to be written as geojson files

    Arguments In:
        config:             a dictionary that contains the configuration
                            information of various steps of NMB2 data 
                            processing
        df_holder:          a list of pandas dataframes, each containing county 
                            level tract data following a lengthy dissolve
        start_time:         a time structure variable that indicates when 
                            the current step started

    Arguments Out:
        continue_run:       a boolean variable that indicates if the routine
                            successfully completed and whether the next 
                            steps should be exectuted
        large_provider_df:  a pandas dataframe containing aggregated data for
                            large broadband providers (geographic coverage)
        other_provider_df:  a pandas dataframe containing aggretaged data for 
                            all other broadband providers that dont qualify
                            for "large"

    """
    try:
        temp_time = time.localtime()
        big_df = pd.concat(df_holder)
        large_provider_df = big_df.loc[big_df.hoconum.isin(config['large_providers'])]
        large_provider_df.sort_values(['hoconum','county_id'],ascending=[True,True],inplace=True)
        other_provider_df = big_df.loc[~big_df.hoconum.isin(config['large_providers'])]
        other_provider_df.sort_values(['hoconum','county_id'],ascending=[True,True],inplace=True)

        my_message = """
            INFO - STEP 5 (MASTER): COMPLETED CREATING LARGE AND OTHER PROVIDER
            DATA FRAMES
            """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return True, large_provider_df, other_provider_df
    except:
        my_message = """
            ERROR - STEP 5 (MASTER): FAILED TO CREATE LARGE AND OTHER PROVIDER
            DATA DATA FRAMES
            """
        my_message = ' '.join(my_message.split())+'\n'+traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return False, None, None

def loadCountyHoCoNumQueue(config, input_queue, start_time):
    """
    loads the county level information into the distributed queue that 
    will allow the routine to relatively quickly perform the county level 
    dissolve with holding company information

    Arguments In:
        config:             a dictionary that contains the configuration
                            information of various steps of NMB2 data 
                            processing
        input_queue:        a multiprocessing queue that can be shared 
                            across multiple servers and cores.  All 
                            information to be processed is loaded into the 
                            queue
        start_time:         a time structure variable that indicates when 
                            the current step started

    Arguments Out:
        continue_run:       a boolean variable that indicates if the routine
                            successfully completed and whether the next 
                            steps should be exectuted
        county_counter:     an interger variable that indicates how many county lev
    """
    temp_time = time.localtime()
    county_counter = 0

    try:
        block_files = glob.glob(config['temp_geog_geojson_path']+'/county_block/block_df_*.geojson')
        for b in block_files:
            county_id = b.split('.')[0][-5:]
            f = config['temp_csvs_dir_path']+'county_fbd/fbd_df_%s.csv' % county_id
            county_counter += 1
            input_queue.put((b, f, config, start_time))

        my_message = """
            INFO - STEP 5 (MASTER): COMPLETED INPUTTING %s COUNTIES AND FBD 
            DATA INTO QUEUE
            """ % str(county_counter)
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return True, county_counter
    except:
        my_message = """
            ERROR - STEP 5 (MASTER): FAILED INPUTTING COUNTIES AND FBD DATA INTO
            QUEUE
            """
        my_message = ' '.join(my_message.split())+'\n'+traceback.format_exc()
        print(nbmf.logMessage(my_message, temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return False, None

def makeProviderFiles(config, input_queue, output_queue, message_queue, 
                        start_time):
    """
    high-level routine that calls the subroutines that generate the provider
    files

    Arguments In: 
        config:             a dictionary that contains the configuration
                            information of various steps of NMB2 data 
                            processing
        input_queue:        a multiprocessing queue that can be shared 
                            across multiple servers and cores.  All 
                            information to be processed is loaded into the 
                            queue
        output_queue:       a multiprocessing queue that can be shared 
                            across multiple servers and cores.  All results 
                            from the various processes are loaded into the 
                            queue
        message_queue:      a multiprocessing queue variable that is used to 
                            communicate between the master and servants
        start_time:         a time structure variable that indicates when 
                            the current step started

    Arguments Out:
        continue_run:       a boolean variable that indicates if the routine
                            successfully completed and whether the next 
                            steps should be exectuted
        block_df:           a pandas dataframe containing the block level
                            geometries

    """
    for _ in range(config['number_servers']):
        message_queue.put('provider_files')

    continue_run, county_counter = loadCountyHoCoNumQueue(config, input_queue,
                                    start_time)
    if continue_run:
        continue_run, df_holder = s5f.dissolve(config, county_counter, input_queue, 
                                    output_queue, start_time)
    if continue_run:
        continue_run, large_provider_df, other_provider_df = concatCountyHoCoNum(
                                                config, df_holder, start_time)
    if continue_run:
        continue_run = writeProviderFiles(config, large_provider_df, 
                        other_provider_df, start_time)
    if continue_run:
        continue_run = makeProviderBBoxFile(config, large_provider_df, 
                        other_provider_df, start_time)

    return continue_run