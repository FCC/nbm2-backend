import NBM2_functions as nbmf
from pathlib import Path
import geopandas as gpd
import traceback 
import zipfile 
import glob
import time 

def unzipCriticalFiles(glob_path, target_list, splitter, shape_type, start_time):
    """
    checks for the existence of shape files or their zipped archives and if 
    it identifies the archive, it unzips it

    Arguments In: 
        glob_path:          a string variable that contains the path in which
                            the shape files exist
        target_list:        a list of strings indicating the specific shape
                            file to locate
        splitter:           a string variable that provides the pattern for
                            splitting the path string so files can be found
        shape_type:         a string variable indicating what type of shape
                            file is being checked
    	start_time:	        the clock time that the step began using the 
    					    time.clock() format

    Arguments Out:
        continue_run:       a boolean variable that indicates if the routine
                            successfully completed and whether the next steps
                            should be executed
    """
    # determine if shape files are missing
    temp_time = time.localtime()
    shape_files = glob.glob(glob_path)
    for s in shape_files:
        shape_id = s.split(splitter)[1]
        shape_id = shape_id.split('_')[0]
        try:
            target_list.remove(shape_id)
        except:
            pass

    # determine if the zip file exists for the missing shapes and unzip them
    zip_files = glob.glob(glob_path.split('.')[0]+'.zip')
    for z in zip_files:
        temp_time = time.localtime()
        zip_id = z.split(splitter)[1]
        zip_id = zip_id.split('_')[0]

        z = z.replace("\\","/") #<-- this is used in case we are running in windows
        shape_dir = '/'.join(z.split('/')[:-1])+'/'

        if zip_id in target_list:
            try:
                zip_ref = zipfile.ZipFile(z)                       
                zip_ref.extractall(shape_dir)    
                zip_ref.close() 
                target_list.remove(zip_id)
                my_message = """
                    INFO - STEP 0 (MASTER): TASK 2 OF 13 - UNZIPPED %s SHAPE
                    FILE FOR %s
                    """ % (shape_type.upper(), zip_id)
                my_message = ' '.join(my_message.split())
                print(nbmf.logMessage(my_message,temp_time, time.localtime(), 
                    time.mktime(time.localtime())-time.mktime(start_time)))
            except:
                my_message = """
                    ERROR - STEP 0 (MASTER): TASK 2 of 13 - FAILED TO UNZIP %s 
                    SHAPE FILE FOR %s
                    """ % (shape_type.upper(), zip_id)
                my_message = ' '.join(my_message.split())+'\n'+traceback.format_exc()
                print(nbmf.logMessage(my_message,temp_time, time.localtime(), 
                    time.mktime(time.localtime())-time.mktime(start_time)))
                return False

    # see if there are any missing shape and zip files
    if len(target_list) > 0:
        for t in target_list:
            my_message = """
                ERROR - STEP 0 (MASTER): TASK 2 OF 13 - MISSING THE %s SHAPE 
                FILE FOR %s
                """ % (shape_type.upper(), t)
            my_message = ' '.join(my_message.split())
            print(nbmf.logMessage(my_message,temp_time, time.localtime(), 
                time.mktime(time.localtime())-time.mktime(start_time)))
        return False 
    else:
        my_message = """
            INFO - STEP 0 (MASTER): TASK 2 OF 13 - CONFIRMED EXISTENCE OF ALL %s 
            SHAPE FILES
            """ % shape_type.upper()
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message,temp_time, time.localtime(), 
            time.mktime(time.localtime())-time.mktime(start_time)))
        return True

def checkShapeFiles(config,start_time):
    """
    subroutine that manages the shape file checking prior to beginning the
    step 0 processes

    Arguments In: 
        config:			    the json variable that contains all configration
    					    data required for the data processing
    	start_time:	        the clock time that the step began using the 
    					    time.clock() format

    Arguments Out:
        continue_run:       a boolean variable that indicates if the routine
                            successfully completed and whether the next steps
                            should be executed
    """
    try:
        flag = True
        temp_time = time.localtime()

        # get the list of state ids and then prepare for each of the different
        # shape files.  Tracts shape file does not have all of the states and
        # territories.  Assumes the state file has already been unzipped
        state_ids = gpd.read_file(config['shape_files_path']+config['state_shape_file_name'] )
        bl_ids = state_ids['GEOID'].tolist() 
        pl_ids = bl_ids[:]
        tr_ids = bl_ids[:]
        shape_dict = {  'state':[config['state_shape_file_name'], ['us']],
                        'county_tl':[config['county_tl_shape_file_name'], ['us']],
                        'county_cb':[config['county_cb_shape_file_name'], ['us']],
                        'tribe':[config['tribe_shape_file_name'],['us']],
                        'cbsa':[config['cbsa_shape_file_name'],['us']],
                        'congress':[config['congress_shape_file_name'],['us']],
                        'block':['block/tl_%s_*_tabblock%s.shp' % (config['census_vintage'], config['census_vintage'][2:]), bl_ids],
                        'places':['place/tl_%s_*_place.shp' % config['geometry_vintage'], pl_ids],
                        'tracts':['tract/gz_%s_*_140_00_500k.shp' % config['census_vintage'], tr_ids]}

        for shp in shape_dict:
            glob_path = config['shape_files_path']+shape_dict[shp][0]
            splitter  = shape_dict[shp][0].split("/")[1]
            splitter  = "_".join(splitter.split("_")[:2])+'_'
            continue_run = unzipCriticalFiles(glob_path, shape_dict[shp][1], splitter, shp, start_time)
            if not continue_run:
                flag = False
        if flag:
            my_message = """
                INFO - STEP 0 (MASTER): TASK 2 OF 13 - COMPLETED CHECKING ALL INPUT
                SHAPE FILES - ALL FILES PRESENT
                """
        else:
            my_message = """
                ERROR - STEP 0 (MASTER): TASK 2 OF 13 - COMPLETED CHECKING ALL INPUT
                SHAPE FILES - SOME FILES MISSING
                """            
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message,temp_time, time.localtime(), 
            time.mktime(time.localtime())-time.mktime(start_time)))

    except:
        my_message = """
            ERROR - STEP 0 (MASTER): TASK 2 OF 13 - FAILED TO CHECK ALL INPUT
            SHAPE FILES
            """            
        my_message = ' '.join(my_message.split())+'\n'+traceback.format_exc()
        print(nbmf.logMessage(my_message,temp_time, time.localtime(), 
            time.mktime(time.localtime())-time.mktime(start_time)))

    return flag        

def checkCSVFiles(config,start_time):
    """
    subroutine that checks to ensure the required CSV files are present 
    prior to beginning the step 0 processes

    Arguments In: 
        config:			    the json variable that contains all configration
    					    data required for the data processing
    	start_time:	        the clock time that the step began using the 
    					    time.clock() format

    Arguments Out:
        continue_run:       a boolean variable that indicates if the routine
                            successfully completed and whether the next steps
                            should be executed
    """
    try:
        flag = True 
        missing_files = []
        temp_time = time.localtime()
        csv_dict = {'fbd':config['input_csvs_path']+config['fbData'],
                    'cnty2cbsa': config['input_csvs_path']+config['county_to_cbsa'][0],
                    'bms':config['input_csvs_path']+config['block_master_static'][0],
                    'hhp':config['input_csvs_path']+config['hu_hh_pop'][0],
                    'tracts':config['input_csvs_path']+config['tract_area_file'],
                    'blocks':config['input_csvs_path']+config['large_blocks_file'],
                    'lookup':config['input_csvs_path']+config['previous_lookup_table'],
                    'prov_lookup':config['input_csvs_path']+config['previous_provider_lookup_table']}
        for c in csv_dict:
            my_csv = Path(csv_dict[c])
            if not my_csv.exists():
                missing_files.append(csv_dict[c])
                flag = False
        if len(missing_files) > 0:
            print("ERROR - STEP 0 (MASTER): TASK 2 of 13 - MISSING THE FOLLOWING FILES")
            for m in missing_files:
                print('\t',m)
        if flag:
            my_message = """
                INFO - STEP 0 (MASTER): TASK 2 OF 13 - COMPLETED CHECKING ALL INPUT
                CSV FILES - ALL FILES PRESENT
                """
        else:
            my_message = """
                ERROR - STEP 0 (MASTER): TASK 2 OF 13 - COMPLETED CHECKING ALL INPUT
                CSV FILES - SOME FILES MISSING
                """
        my_message = ' '.join(my_message.split())
        print(nbmf.logMessage(my_message,temp_time, time.localtime(), 
            time.mktime(time.localtime())-time.mktime(start_time)))   

    except:
        flag = False
        my_message = """
            ERROR - STEP 0 (MASTER): TASK 2 OF 13 - FAILED TO CHECK ALL INPUT
            CSV FILES
            """            
        my_message = ' '.join(my_message.split())+'\n'+traceback.format_exc()
        print(nbmf.logMessage(my_message,temp_time, time.localtime(), 
            time.mktime(time.localtime())-time.mktime(start_time)))

    return flag

def checkFiles(config, start_time):
    """
    subroutine used to manage all file checking and extracting processes

    Arguments In: 
        config:			    the json variable that contains all configration
    					    data required for the data processing
    	start_time:	        the clock time that the step began using the 
    					    time.clock() format

    Arguments Out:
        continue_run:       a boolean variable that indicates if the routine
                            successfully completed and whether the next steps
                            should be executed
    """
    try:
        temp_time = time.localtime()
        continue_run = checkShapeFiles(config, start_time)

        # if continue_run:
        #     continue_run = checkCSVFiles(config, start_time)

        if continue_run:
            my_message = """
                INFO - STEP 0 (MASTER): TASK 2 of 13 - CONFIRMED PRESENCE OF REQUIRED
                INPUT FILES
                """
            print(nbmf.logMessage(' '.join(my_message.split()),temp_time, time.localtime(),
                time.mktime(time.localtime())-time.mktime(start_time)))
            return True 

        else:
            my_message = """
                ERROR - STEP 0 (MASTER): TASK 2 of 13 - MISSING REQUIRED INPUT 
                FILES - SEE LOG FOR MISSING FILES
                """
            print(nbmf.logMessage(' '.join(my_message.split()),temp_time, time.localtime(),
                time.mktime(time.localtime())-time.mktime(start_time)))
            return False             

    except:
        my_message = """
            ERROR - STEP 0 (MASTER): TASK 2 of 13 - FAILED TO FIND ALL REQUIRED
            INPUT FILES - SEE LOG FOR ERRORS
            """
        print(nbmf.logMessage(' '.join(my_message.split()),temp_time, time.localtime(),
            time.mktime(time.localtime())-time.mktime(start_time)))
        return False 

