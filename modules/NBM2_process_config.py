################################################################################
# Created by Steven Charbonneau, Ahmad Abruzzi, and Mohammed Kemal
#
# Version 2.3 - File is still in developmental
#
# Purpose: Provide the parameters and values required to run the NBM2 data 
# processing scripts.  
#
#
# Created On: 0800 23 OCT 2018
################################################################################
import socket
import json
import os

# extracts the location of the file.  This is used to minimize the effort to run
# in a test on a local machine or on the cloud.
nbm2_root = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
config = {}
#--------------------------------------------------------------------------------

# the number of servers available and used in the distributed process
config['number_servers'] = 3
#--------------------------------------------------------------------------------

# Turn Processes on or off.  Setting a value to False will allow the process to be skipped
# in the NBM_MASTER_SCRIPT routine once it is written. 
#                   Step Number      Run    Comments
config['steps'] = { 'step0':        False,  # create the Block Master Table  
                    'step1':        False,  # create the provider table
                    'step2':        False,  # create the block_numprov and area_table files
                    'step3':        False,  # create the tract_ and county_numprov files
                    'step4':        False,  # create the map box tiles
                    'step5':        True,  # create the geojson files
                    'step6':        True,  # run tippecanoe
                    'step7':        False,  # not used (will be old step 10 - load mbtiles into mapbox studio) 
                    'step8':        False}  # not used (will be new process - load csvs into socrata)
#--------------------------------------------------------------------------------

# general directory structure used by multiple steps
config['nbm2_root']                     = nbm2_root
config['input_csvs_path']               = nbm2_root + "/inputs/csv/"                # should almost never change
config['temp_csvs_dir_path']            = nbm2_root + "/temp/csv/"                  # should almost never change
config['output_dir_path']               = nbm2_root + "/outputs/csv/"               # should almost never change
config['fcc_files_path']                = config['input_csvs_path']                 # should almost never change
config['sql_files_path']                = nbm2_root + "/sql/"                       # should almost never change
config['shape_files_path']              = nbm2_root + "/inputs/shape/"              # should almost never change
config['temp_geog_geojson_path']        = nbm2_root + "/temp/geo_geojson/"          # should almost never change
config['output_mbtiles_path']           = nbm2_root + "/outputs/mapbox_tile/"       # should almost never change
config['input_geog_geojsons_path']      = nbm2_root + "/inputs/geo_geojson/"        # should almost never change
config['temp_speed_geojson_path']       = nbm2_root + "/temp/speed_geojson/"        # should almost never change
config['temp_mbtiles_path']             = nbm2_root + "/temp/mapbox_tiles/"         # should almost never change
config['temp_pickles']                  = nbm2_root + "/temp/pickles/"              # should almost never change
#--------------------------------------------------------------------------------

# names of key data input files used throughout the data processing phase
# json key name                             file name                                   source                          notes
config['fbData']                            = "fbd_us_with_satellite_jun2018_v1.csv"    #   WCB
config['blockmaster_data_file']             = 'blockmaster_jun2018.csv'                 #   created during run time
config['previous_lookup_table']             = '2018_Geography_Lookup_Table.csv'         #   copied from previous vintage run
config['previous_provider_lookup_table']    = 'provider_lookup_table_dec2017.csv'       #   downloaded from the fbd data page
#--------------------------------------------------------------------------------

# data vintage parameters
config['census_vintage']    = "2010"        # should change once a decade after the census count
config['geometry_vintage']  = "2018"        # should change once a year
config['household_vintage'] = "2018"        # should change once a year <-- this is for the us20XX table with hh_hu_pop data
config['fbd_vintage']       = "jun2018"     # should change every 6 months

#########################################################################################################################
#########################################################################################################################
# specific to Step 0

#--------------------------------------------------------------------------------
# specify which files in step 0 get processed
config['step0'] = { 'check_files':          False,       # Task  2: should only be True once every year (geometry_vintage)
                    'create_gzm_file':      False,       # Task  3: should only be True once every decade (census_vintage)
                    'census_block_shape':   False,       # Task  4: should only be True once every decade (decennial census)
                    'census_place_shape':   False,       # Task  4: should only be True once a year (geometry_vintage)
                    'census_csv':           False,       # Task  5: should always be True  
                    'census_shape':         False,       # Task  5: should only be True once a year (geometry_vintage)
                    'parse_blocks':         False,       # Task  6: should only be True once every decade (decennial census) 
                    'initial_spatial':      False,       # Task  7: should only be True once every year (geometry_vintage)
                    'complete_spatial':     False,       # Task  8: should only be True once every year (geometry_vintage)
                    'county_block':         False,       # Task  9: should always be True 
                    'block_master':         False,       # Task 10: should always be True 
                    'name_table':           False,       # Task 11: should always be True 
                    'lookup_table':         False,       # Task 12: should always be True 
                    'parse_fbd':            True,       # Task 13: should always be True 
                    }

#--------------------------------------------------------------------------------
# this list is used to iterate over the csv files. The names in this list must 
# match the keys in the csv file data. If you add new csv, add it to this list
# this should rarely change
config['csv_file_list']         = ['block_master_static', 'county_to_cbsa', 'hu_hh_pop']     

# csv file data                         File name                                index field                            source
config['block_master_static']   = ["blockmaster_static2018.csv",            'geoid%s' % config['census_vintage'][2:]]   # WCB
config['county_to_cbsa']        = ["county_to_cbsa_lookup2018_revised.csv", 'county_fips']                              # OMD
config['hu_hh_pop']             = ["us2017.csv",                            'block_fips']                               # WCB
#--------------------------------------------------------------------------------

# these lists are used to iterate over the shape files. The names in this list must 
# match the keys in the shape file data. If you add new shapes, add it to this list
# this should rarely change
config['shape_file_list']           = ['state', 'tribe', 'county', 'cbsa', 'congress']  

# shape file data                       File name                                                                   source
config['state_shape_file_name']     = "state/tl_%s_us_state.shp"         % config['geometry_vintage']          # ftp2.census.gov --> geo/tiger<geometry_vintage>/STATE
config['tribe_shape_file_name']     = "tribe/tl_%s_us_aiannh.shp"        % config['geometry_vintage']          # ftp2.census.gov --> geo/tiger<geometry_vintage>/AIANNH
config['county_tl_shape_file_name'] = "county/tl_%s_us_county.shp"       % config['geometry_vintage']          # ftp2.census.gov --> geo/tiger<geometry_vintage>/COUNTY 
config['county_cb_shape_file_name'] = "county/gz_%s_us_050_00_500k.shp"  % config['census_vintage']            #! ftp2.census.gov --> geo/tiger<geometry_vintage>/COUNTY 
config['county_shape_file_name']    = "county/tl_%s_us_county.shp"       % config['geometry_vintage']            # created on the fly in Step 0 task 3 (soon)
config['county_gzm_shape_file_name']= "county/gzm_%s_us_050_00_500k.shp" % config['census_vintage']            # created on the fly in Step 0 task 3
config['cbsa_shape_file_name']      = "cbsa/tl_%s_us_cbsa.shp"           % config['geometry_vintage']          # ftp2.census.gov --> geo/tiger<geometry_vintage>/CBSA
config['congress_shape_file_name']  = "congress/tl_%s_us_cd116.shp"      % config['geometry_vintage']          # ftp2.census.gov --> geo/tiger<geometry_vintage>/CD

# shape directory data for block and place shape files.  These should rarely change
# shape file data                directory name                                 source
config['block_shape_dir_name'] = "block"                                    # ftp2.census.gov --> geo/tiger<geometry_vintage>/TABBLOCK
config['place_shape_dir_name'] = "place"                                    # ftp2.census.gov --> geo/tiger<geometry_vintage>/PLACE
#--------------------------------------------------------------------------------

# index fields for block and place shape files.  These should rarely change
config['block_indexes']     = ['"BLOCK_FIPS"', '"COUNTY_FIPS"', '"GEOMETRY"']
config['place_indexes']     = ['"GEOMETRY"']
#--------------------------------------------------------------------------------

# This list is used to iterate over the sql files used for spatial analysis 
# These should only change if a new spatial analysis is required 
config['spatial_list']              = ['tribe', 'place', 'congress']

#--------------------------------------------------------------------------------

# counties periodically change geometry (and or appear and disappear)
# need to go to the following website and determine what has changed with respect to counties
# https://www.census.gov/geo/reference/county-changes.html
# The list is additive (each year more is added) and starts again each decade
# so usually you will just add to the lists.
#  
# Look in the sections called "Deleted Counties or County Equivalent Entities" 
# and "Name and/or Code Changes or Corrections for Counties or County Equivalent Entities"
# both of these are treated more or less the same.  Enter the "FROM --> TO" pairs only
# dont need to worry about "name changes only"
#                                   FROM_FIPS  TO_FIPS
config['county_change_name_fips'] = [
                                    ["02270", "02158"],     # From Wade Hampton to Kusilvak Alaska  
                                    ["46113", "46102"],     # From Shannon County to Oglala County South Dakota
                                    ["51515", "51019"]      # From Bedford City to Bedford County Virginia
                                    ]
#--------------------------------------------------------------------------------

# Look in the section called "Substantial County or County Equivalent Boundary Changes"
# and if the county is listed in the section and not already included in the 
# change fips list above, add it to the following list (no dashes)
config['county_changes_substantial'] = [
                                        '02105',        # Hoonah-Angoon Alaska
                                        '02195',        # Petersburg Borough Alaska
                                        '02198'         # Prince of Wales-Hyder Alaska
                                        ]

#########################################################################################################################
#########################################################################################################################
# specific to Step 1
#--------------------------------------------------------------------------------

# These are the columns to be used from the fbd file to create the provider table.  This is loaded by pandas
config['fbd_data_columns']      = ['BlockCode', 'Consumer', 'HocoNum', 'TechCode', 'MaxAdDown', 'MaxAdUp']
config['bm_data_columns']       = ['geoid%s' % config['census_vintage'][2:],'pop']
#--------------------------------------------------------------------------------

# Change column names.  Sometimes the raw data files don't have the same names as we expect.  You need to change the 
# header names so they match the expected column names.  If you add a column to the list associated with the required 
# columns (see config['fbd_data_columns]) above) then you must also confirm if there are name changes associated with 
# the column and if so, add it here.  These should only change if there have been changes in the way the files are 
# generated
#                                   Actual Column Name (in file)                Required name (in pandas)
config['fbd_rename_columns']    = { 'Census Block FIPS Code':                       'BlockCode',
                                    'Consumer':                                     'Consumer',
                                    'Holding Company Number':                       'HocoNum',
                                    'Technology Code':                              'TechCode',
                                    'Max Advertised Downstream Speed (mbps)':       'MaxAdDown',
                                    'Max Advertised Upstream Speed (mbps)':         'MaxAdUp'}

config['bm_rename_columns']     = { 'geoid%s' % config['census_vintage'][2:]:       'BlockCode'}
#--------------------------------------------------------------------------------

# Specify the data types for key columns that may be modified by pandas as it is ingested
#                                   Column name         data type                                    
config['fbd_data_types']        = { 'BlockCode':                                    'object'}
config['fbd_data_types_old']    = { 'Census Block FIPS Code':                       'object'}
config['bm_data_types']         = { 'geoid%s' % config['census_vintage'][2:]:       'object'}
#--------------------------------------------------------------------------------

# Specify the upload and download speeds for aggregating customers and technologies.
# Note that this is different than the up and down speeds used in other portions of
# the full data process.  This should rarely change - only if there is a commission ruling
config['d_column_list']     = ['d_1', 'd_2', 'd_3', 'd_4', 'd_5', 'd_6', 'd_7', 'd_8']
config['d_val_arr']         = [0.2,4,10,25,100,250,500,1000] 
config['u_column_list']     = ['u_1', 'u_2', 'u_3', 'u_4', 'u_5', 'u_6', 'u_7', 'u_8', 'u_9']
config['u_val_arr']         = [0.2,1,3,10,25,100,250,500,1000]
#--------------------------------------------------------------------------------

# This list is used to pull out the specific technologies used to build the provider table.
# the list consists of tuples of codes and descripters.  The code must match one of the 
# keys in the tech_dict (search this file for tech_dict) and the descripter shoudl be 
# provided by the government.  This should change very rarely and would be driven
# by an FCC ruling change.
config['tech_stack'] = [('a', 'adsl'), ('c', 'cable'), ('o', 'other')]


#########################################################################################################################
#########################################################################################################################
# specific to Step 2

#--------------------------------------------------------------------------------
# The information in this section should only be changed if there are commission ruling changes (i.e., almost never)
tech_dict={}
tech_dict['a']          = [10,11,12,13]   # ADSL: ADSL2 (11), VDSL (12), other DSL (10)
tech_dict['c']          = [40,41,42,43]   # cable: DOCSIS 3.0 (42), DOCSIS 1, 1.1, 2.0 (41), other DOCSIS (40)
tech_dict['f']          = [50]            # FTTP
tech_dict['o']          = [0,90,20,30]    # Other: SDSL (20), Other copper (30), EPL (90), All other (0)
tech_dict['s']          = [60]            # Satellite
tech_dict['w']          = [70]            # fixed Wireless -- NOT MOBILE
config['tech_dict']     = tech_dict

config['techlist']      =['a','c','f','o','s','w']
config['down_speed']    = (0.2,4,10,25,100,250,1000)
config['up_speed']      = (0.2,1,1,3,10,25,100)

#--------------------------------------------------------------------------------
# These terms should only be updated if headings change in the initial input files (csv's) for step 2 
config['geog_dict']         ={'cplace_id':'place','cdist_id':'cd','cbsa_code':'cbsa','county_fips':'county','tribal_id':'tribal','state_fips':'state','country':'nation'}
config['blockm_keep']       =['BlockCode', 'pop', 'state_fips', 'urban_rural', 'county_fips', 'cbsa_code', 'tribal_non', 'tribal_id', 'aianhhcc', 'cplace_id', 'cdist_id', 'country']
config['area_table_cols']   =['type','id','tech','urban_rural','tribal_non','speed','has_0','has_1','has_2','has_3more']

config['blockm_dtype']      ={  'geoid%s' % config['census_vintage'][2:]: 'object', 'hu': 'int', 'hh': 'float', 'pop':'int', 
                                'state_fips': 'object', 'urban_rural':'object', 'county_fips': 'object', 'cbsa_code': 'object', 
                                'tribal_non': 'object', 'tribal_id': 'object', 'aiannhcc': 'object', 'cplace_id': 'object', 
                                'cdist_id': 'object'}

#--------------------------------------------------------------------------------

# This line of information is used by both Steps 2 and 3
# provides the sting representation of the combined upload and download speeds
# This should rarely (if ever) change
config['speedList'] = ('200', '4_1', '10_1', '25_3', '100_10', '250_25', '1000_100')

#########################################################################################################################
#########################################################################################################################
# Step 4 parameters

# Specify which tasks in step 4 get executed:
config['step4'] = { 'bounding_box':             True,      # Task 1: should always be True, only change to False if rerunning due to error
                    'block_bounding_box':       True,      # Task 1: should only be true when census vintage changes
                    'tract_bounding_box':       True,      # Task 2: should only be true when census vintage changes 
                    'make_tiles':               True       # Task 3: should always be True, only change to False if rerunning due to error 
                    }    
#--------------------------------------------------------------------------------

# the zoom limits for each level of geometry.  These should rarely change
#                           geometry            zoom limit
config['zoom_params'] = {   "state":           ["2","5"],
                            "county":          ["0","7"],
                            "carto":           ["3","9"],
                            "congress":        ["2","8"],
                            "tribe":           ["4","9"],
                            "place":           ["6","14"],
                            "cbsa":            ["3","6"],
                            "block":           ["10","14"],
                            "tract":           ["8","9"]}
#--------------------------------------------------------------------------------

# these lists account for the type of processing that needs to take place to build
# the geojson files.  Most geometries are presented in the database, but tracts is
# presented in multiple files.  

# These are the geometries with single shape files.  This should rarely change
config['basic_geojson'] = ['congress', 'tribe', 'cbsa', 'county', 'state', 'place', 'block'] 
config['iter_geojson']  = ['tract']

#--------------------------------------------------------------------------------

# These are modifications to existing bounding boxes that are manually calculated 
# and are then used to replace the automatically calculated bounding boxes.  
# These are necessary because Alaska spans the international date line at the 
# Aleutian Islands and their lats revert to East numeric instead of West Numeric
# which results in a bounding box for Alaska that wraps around the world and 
# excludes Alaska.  These changes in latitude force the bounding box to include
# Alaska and its specific counties and congressional districts.
# This should almost never change.
# json key                   shape type     geoid           value
config['modify_bbox'] = [   ('state',       '02',       '[-188.148909,51.214183,-128.77847,71.365162]'),
                            ('county',      '02016',    '[-187.75585,51.214183,-165.77847,57.249626]' ),    # This appears to have been corrected in the Census Shape file
                            ('congress',    '0200',     '[-188.148909,51.214183,-128.77847,71.365162]')]
#--------------------------------------------------------------------------------

# These are the list of column names that are retained and used in the map box file
config['keep_columns']          = ['GEOID','NAME', 'NAMELSAD','bbox_arr','GEOMETRY']
config['keep_columns_tribe']    = ['GEOID', 'NAMELSAD','bbox_arr','GEOMETRY']
config['keep_columns_block']    = ['BLOCK_FIPS','bbox_arr','GEOMETRY']

#########################################################################################################################
#########################################################################################################################
# Step 5 parameters

# specify which tasks in step 5 get executed
config['step5'] = { 'block_data_frame':         False,      # Task  1: should only be True once every decade  (when census_vintage changes) 
                    'block_us.sort.geojson':    False,      # Task  2: should only be True once every decade  (when census_vintage changes)
                    'h2only_undev.csv':         False,      # Task  3: currently should be True once a year   (when geometry_vintage changes)
                    'bigtract_blocks.geojson':  False,      # Task  4: should only be True once every decade  (when census_vintage changes)
                    'large_blocks.geojson':     False,      # Task  5: should only be True once every decade  (when census_vintage changes)
                    'tracts.sort.geojson':      False,      # Task  6: should only be True once every decade  (when census_vintage changes)
                    'tracts_area.csv':          False,      # Task  7: should only be True once every decade  (when census_vintage changes)
                    'large_tracts.geojson':     False,      # Task  8: should only be True once every decade  (when census_vintage changes)
                    'land_tracts.geojson':      False,      # Task  9: should only be True once every decade  (when census_vintage changes)
                    'tract_carto.geojson':      False,      # Task 10: should only be True once every decade  (when census_vintage changes)
                    'county.geojson':           True,      # Task 11: should only be True once every year    (when census_vintage changes)
                    'provider.geojson':         False       # Task 12: should be True every six months        (when fbd_vintage changes) 
                    }     
#--------------------------------------------------------------------------------

# input shape files required for step 5.  These are less detailed data files concerning the boundaries
# for the US and PR
# json key name                         file name                               source
#! config['us_gz_file']                = "gz_2010_us_050_00_500k.shp"        # https://www.census.gov/geo/maps-data/data/tiger-cart-boundary.html
#! config['pr_gz_file']                = "gz_2010_72_140_00_500k.shp"        # https://www2.census.gov/geo/tiger/GENZ2010/ 

config['tract_area_file']           = "tract_area_m.csv"                        #!@
config['large_blocks_file']         = "largeblocks.csv"                         #!@

config['households_columns']        = [ 'block_fips', 
                                        'hu%s' % config['household_vintage'],  
                                        'pop%s'  % config['household_vintage']]
#--------------------------------------------------------------------------------

# These are the providers that have large footprints around the country.  This list should be reviewed every 6-months
# with the stakeholder to ensure these are a) still large providers and b) the only large providers
config['large_providers']           = ['300167','290111','130627','130077','130228','130317','130258']

#########################################################################################################################
#########################################################################################################################
# Step 6 parameters

# Specify which tasks in step 6 get executed
config['step6'] = { 'make_data_files':          True,      # Task 1: should always be True only change to False if rerunning due to error
                    'create_zoom_files':        True,      # Task 2A: should always be True only change to False if rerunning due to error
                    'create_large_zoom_files':  True,      # Task 2B:should always be True only change to False if rerunning due to error
                    'combine_speeds':           True,      # Task 3: should always be True only change to False if rerunning due to error
                    'prepare_provider':         True,      # Task 4: should always be True only change to False if rerunning due to error
                    'make_provider_files':      True       # Task 5: should always be True only change to False if rerunning due to error
                    }

#########################################################################################################################
#########################################################################################################################
# create the json file
my_json = str(os.path.dirname(os.path.realpath(__file__)))+'/process_config.json'
with open(my_json, 'w') as cf:
    json.dump(config, cf)
