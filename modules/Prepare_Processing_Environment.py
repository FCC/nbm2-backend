###  NOTE: Place this file in /data/efs/nbm2_<monthyear>
###  And run this script first. It will setup the folder structure and 
###  copy over and download raw data.



import os
import shutil
import glob
import zipfile
import requests
import urllib.request
import time
import logging


############################# Getting Info About Run ##############################
###################################################################################

########## Define Run ################
######################################

while True:
    month = (input("Month of run: ")).lower()
    if month == "june":
        month = 'jun'
        break
    elif month == "december":
        month = 'dec'
        break
    else:
        print("Invalid month, please try again")
        continue

while True:
    year = input("Year of run: ")
    try:
        if int(year) > 2016:
            break
        else:
            print("invalid year, please try again")  # exception raised if less than 2017
            continue
    except:
        print("invalid year, please try again") # exception raised if contains a letter
        continue

run = "nbm2_"+month+year

print('Current run is', run)

######## Define Previous Run #########
######################################

if month == "jun": 
    prev_month = "dec"
    prev_year = int(year) - 1
else:
    prev_month = "jun"
    prev_year = int(year)

prev_run = "nbm2_"+ prev_month + str(prev_year)

print("Previous run was", prev_run)


###################### Making Directory Structure #################################
###################################################################################

print("CREATING DIRECTORY")

dir_list = [
    run,
        run+'/inputs',
            run+'/inputs/csv',
            #run+'/inputs/geo_geojson', Not used anymore
            run+'/inputs/shape',
                run+'/inputs/shape/block',
                run+'/inputs/shape/cbsa',
                run+'/inputs/shape/congress',
                run+'/inputs/shape/county',
                run+'/inputs/shape/place',
                #run+'/inputs/shape/speed', No longer using this. Both county datasets go into county folder
                run+'/inputs/shape/state',
                run+'/inputs/shape/tract',
                run+'/inputs/shape/tribe',
            #run+'/inputs/speed_geojson', Not used anymore
        run+'/outputs',
            run+'/outputs/csv',
            run+'/outputs/mapbox_tile',
        run+'/temp',
            run+'/temp/csv',
                run+'/temp/csv/block_numprov',
                run+'/temp/csv/county_fbd',
                run+'/temp/csv/county_numprov',
                run+'/temp/csv/tract_numprov',
            run+'/temp/geo_geojson',
                run+'/temp/geo_geojson/county_block',
            run+'/temp/speed_geojson',
            run+'/temp/mapbox_tiles',
            run+'/temp/pickles',
        run+'/logs',
        run+'/modules',
        run+'/sql']


for folder in dir_list:
    try:
        os.makedirs(folder)
    except:
        print(folder, "folder already exists")
        continue


##################### Set up Logging ##############################################
###################################################################################

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(levelname)s - %(message)s')


fh = logging.FileHandler('/data/efs/'+run+'/logs/Prep_Processing_Env_Log_'+ run + '.txt')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(fh)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)

logger.info("Running %s", run)

# A new text file will be created the first time this is run.  But each successive time this script is run, it opens the text file that already exists, and adds to text that is already there.
# formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
# This is reporting incorrect times, but it does work on my local PC

##################### Get Info From User About Data ###############################
###################################################################################

## Ask whether CBSA to County table will need to be copied over. This table is not updated on a predictable schedule. If OMB hasn't released a new one, it will get copied over from previous run later in this script.

while True:
    copy_cbsa2county = input("Copy county_to_CBSA Lookup table from previous run? Check this website to see if a new version has been released since the previous run: https://www.census.gov/geographies/reference-files/time-series/demo/metro-micro/delineation-files.html   ").lower()
    if copy_cbsa2county == "yes":
        logger.info('county_to_CBSA Lookup table will be copied')
        break
    elif copy_cbsa2county == "no":
        logger.info("Download and reformat CBSA Lookup table from this website: https://www.census.gov/geographies/reference-files/time-series/demo/metro-micro/delineation-files.html. Remember to rename it and place it in the inputs/csv folder")
        break 
    else:
        print("invalid answer, please try again")
        continue


##################### Copying Files From Previous Run #############################
###################################################################################

#### Copy over zipped shapefiles #####
######################################

## This needs to be updated to copy 2010 counties from County folder.  Need to first download zipped versions of these files.

print("COPYING SHAPEFILES ")

# copy over all zipped shps for dec runs
if month == "dec":
    files = glob.glob("/data/efs/" + prev_run + "/inputs/shape/*/*.zip")
    for src in files:
        print("copying", src.split("/")[-1])
        path = src.split("/")[-3] + "/" + src.split("/")[-2]
        dst = "/data/efs/" + run + "/inputs/" + path
        shutil.copy(src,dst)
# copy over only 2010 shapefiles for june runs
else:
    files = glob.glob("/data/efs/" + prev_run + "/inputs/shape/speed/*.zip")
    files.extend(glob.glob("/data/efs/" + prev_run + "/inputs/shape/block/*.zip"))
    files.extend(glob.glob("/data/efs/" + prev_run + "/inputs/shape/tract/*.zip"))
    for src in files:
        print("copying", src.split("/")[-1])
        path = src.split("/")[-3] + "/" + src.split("/")[-2]
        dst = "/data/efs/" + run + "/inputs/" + path
        shutil.copy(src,dst)



###### Copy over CSVs ########
##############################

print("COPYING CSVS")
print("Moving CSVs from run", prev_run)

src_path = "/data/efs/" + prev_run + "/inputs"
dst_path = "/data/efs/" + run + "/inputs"

csvs = []
csvs.append("/data/efs/"  + prev_run + "/inputs/csv/largeblocks.csv")
csvs.append("/data/efs/"  + prev_run + "/inputs/csv/tract_area_m.csv")
csvs.append("/data/efs/"  + prev_run + "/inputs/csv/blockmaster_static" + str(prev_year) + ".csv")  #this will need to be renamed with new year
csvs.append("/data/efs/"  + prev_run + "/outputs/csv/" + str(prev_year) +"_Geography_Lookup_table.csv") #does not get renamed with new year
csvs.append("/data/efs/"  + prev_run + "/outputs/csv/provider_lookup_table_" + prev_month + str(prev_year) + ".csv" #does not get renamed with new year

# The County to CBSAs tables are not updated on a regular basis.  Just whenever OMB releases a new one.  Check this website: https://www.census.gov/geographies/reference-files/time-series/demo/metro-micro/delineation-files.html

if copy_cbsa2county == "yes":
        print('copying county_to_CBSA Lookup table')
        csvs.append("/data/efs/"  + prev_run + "/inputs/csv/county_to_cbsa_lookup" + str(prev_year) + "_revised.csv") 


# households are copied over for June runs, but downloaded for December runs
# example: 2017 households used for Jun 2018 only. Dec 2018 will use 2018 households

if month == 'jun':
    print('Copying household csv')
    csvs.append("/data/efs/"  + prev_run + "/inputs/csv/us" + str(prev_year) + ".csv") 

for src_csv in csvs:
    print("copying", src_csv.split("/")[-1])
    try:
        dst_csv = "/data/efs/" + run + "/inputs/csv"
        shutil.copy(src_csv,dst_csv)
    except:
        logger.warning("%s DOES NOT EXIST AND WAS NOT COPIED OVER", src_csv.split("/")[-1])
        continue

#rename blockmaster static:
print("Renaming Blockmaster with new year")
os.rename('/data/efs/'+ run + '/inputs/csv/blockmaster_static' + str(prev_year) + '.csv', '/data/efs/' + run + '/inputs/csv/blockmaster_static' + year + '.csv')


#### Copy over Modules ###############
######################################
print("COPYING MODULES")
print("Moving modules from run", prev_run)

modules = glob.glob("/data/efs/" + prev_run + "/modules/*.py")

for mod in modules:
    name = mod.split("/")[-1]
    if name[0:4] == 'test': continue
    elif name[0] == 'z': continue
    else:
        print("copying", name)
        dst = "/data/efs/" + run + "/modules"
        shutil.copy(mod,dst)


#### Copy over SQL files ###############
########################################

print("COPYING SQL FILES")
print("Moving SQL files from run", prev_run)

sql_files = glob.glob("/data/efs/" + prev_run + "/sql/*.sql")

for sql in sql_files:
    print("copying", sql.split("/")[-1])
    dst = "/data/efs/" + run + "/sql"
    shutil.copy(sql,dst)

##### Copy over geojsons ###############
########################################


print("COPYING geojson FILES")
print("Moving geojson files from run", prev_run)

gj_files = glob.glob("/data/efs/" + prev_run + "/temp/geo_geojson/county_block/*.geojson")

for gj in gj_files:
    print("copying", gj.split("/")[-1])
    dst = "/data/efs/" + run + "/temp/geo_geojson/county_block"
    shutil.copy(gj, dst)

#### Copy over block_df.pkl ###############
###########################################

print("COPYING block_df.pkL")

pickel = "/data/efs/" + prev_run + "/temp/pickles/block_df.pkl"

dst = "/data/efs/" + run + "/temp/pickles"
shutil.copy(pickel,dst)


##################### Download Files ##############################################
###################################################################################

print("DOWNLOADING DATA")

## Geometry Vintage Census Shapefiles ##
########################################

## Download 56 place shapefiles:

for i in range(1,79):   
        try:
                url = "https://www2.census.gov/geo/tiger/TIGER" + year + "/PLACE/tl_" + year + "_{:02d}_place.zip".format(i)
                place_path = '/data/efs/' + run + '/inputs/shape/place/' 
                urllib.request.urlretrieve(url, place_path + url.split('/')[-1])
        except:
                logger.warning('Unable to download %s', url.split('/')[-1])
                continue

## Download the rest of the shapefiles (there is just one of each of these)
paths = {"congress" : "https://www2.census.gov/geo/tiger/TIGER" + year + "/CD/tl_" + year + "_us_cd116.zip", #note the 116th congress lasts until 2021
         "cbsa" : "https://www2.census.gov/geo/tiger/TIGER" + year + "/CBSA/tl_" + year + "_us_cbsa.zip", 
         "tribe" : "https://www2.census.gov/geo/tiger/TIGER" + year + "/AIANNH/tl_" + year + "_us_aiannh.zip",
         "county" : "https://www2.census.gov/geo/tiger/TIGER" + year + "/COUNTY/tl_" + year + "_us_county.zip"	, 
         "state" : "https://www2.census.gov/geo/tiger/TIGER" + year + "/STATE/tl_" + year + "_us_state.zip"}

def downloadshp(url, folder):
    try:
        print('Downloading', url.split('/')[-1])
        path = '/data/efs/' + run + '/inputs/shape/' + folder
        urllib.request.urlretrieve(url, path + '/'+ url.split('/')[-1])
    except:
        logger.warning('unable to download %s', url.split('/')[-1])


for key, value in paths.items():
    downloadshp(value, key)


###### Household Data ################
######################################

# Household data is released once a year and is generally released 6 months after the geometry data is released (approximately the June timeframe).
# For June runs, copy data over from previous run.  For December runs, download latest data
# this will be download link https://transition.fcc.gov/bureaus/wcb/cpd/us2017.csv.zip except change year
# Need to update this. Socrata requires strictly UTF-8 encoding so we have to replace any Unicode characters with the closest match in UTF-8

try:
    if month == 'dec':
        print('Downloading Household Data')
        url = 'https://transition.fcc.gov/bureaus/wcb/cpd/us' + year + '.csv.zip'
        hh_path = '/data/efs/' + run + '/inputs/csv' 
        urllib.request.urlretrieve(url, hh_path + '/' + url.split('/')[-1])
        # Unzip:
        print("Unzipping household data")
        zip_ref = zipfile.ZipFile(hh_path + '/' + url.split('/')[-1])                 
        zip_ref.extractall(hh_path)  
        zip_ref.close()
    else:
        print("Finished Downloading Data")
except:
    logger.warning('Unable to download Household Data. Please check that the website provides this data.')



##################### Unzipping Data ##############################################
###################################################################################



#### Unzip states #########################
###########################################

# states are needed for step 0 task 3, before the shapefiles get unzipped, so we unzip them here

print("UNZIPPING STATE SHAPEFILE") # The rest of the Shapefiles get unzipped in later a later script

state_path = "/data/efs/" + run + "/inputs/shape/state"
state_shp = state_path + "/tl_" + year + "_us_state.zip" #this will be just one file
zip_ref = zipfile.ZipFile(state_shp)                       
zip_ref.extractall(state_path)    
zip_ref.close()

###### 477 Data #######################
######################################


# Released every 6 months.  This gets downloaded for every run.
# Download the data manually when provided to you, and move to server
# Because of the way the 477 data is zipped, 7zip is the only way to unzip it:

try: 
    print("Unzipping 477 Data")
    #cmd = '7z e US-Fixed-with-Satellite-Dec2017.zip'
    zip_name = 'US-Fixed-with-Satellite-' + month + year + '.zip'
    cmd = '7z e' + zip_name
    path = run + '/inputs/csv'
    os.chdir(path)
    os.system(cmd)
    print("Finished unzipping 477 Data")
except:
    logger.warning('Unable to unzip 477 Data.')


    


