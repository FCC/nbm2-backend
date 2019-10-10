from NBM2_process_config import config
import NBM2_functions as nbmf
import geopandas as gpd
import pandas as pd 
import traceback
import time
import os

# The assumption is that this becomes a part of step5_task_12


################################################################################
# This routine needs to be added to the Step5_task_12 code. 
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
################################################################################


################################################################################
# This is just set up code that will not be required when moved to step 5
# creating the start time, config, and lg_provider_df and other_provider_df
# which will match arguments coming out of one of the routines in step5 task 12
print('starting to read in provider geojsons')
start_time = time.time()
large_provider_df = gpd.read_file(config['temp_speed_geojson_path'] + "%s_prov_lg.geojson" % config['fbd_vintage'])
print("reading time for large provider file was %s minutes " % str((time.time()-start_time)/60))
temp_time = time.time()
other_provider_df = gpd.read_file(config['temp_speed_geojson_path'] + "%s_prov_other.geojson" % config['fbd_vintage'])
print("reading time for other provider file was %s minutes ", (time.time()-temp_time)/60)
print("reading time for both files was %s minutes " % str((time.time()-start_time)/60))

start_time = time.localtime()
continue_run = True
################################################################################


################################################################################
# This routine needs to be added as is to step5_task_12.py 

################################################################################
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


################################################################################
# This is the code that will be added as the last routine call in 
# step5_task12.makeProviderFiles
if continue_run:
    continue_run = makeProviderBBoxFile(config, large_provider_df, other_provider_df, start_time)
################################################################################