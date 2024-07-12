#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 25 14:13:33 2022

@author: albertor
"""

import os
import glob
import sys
import argparse
import requests
import pandas as pd
import numpy as np
from io import StringIO
import xarray as xr
import json
import yaml
import logging
import logging.handlers
import re
from datetime import datetime, timedelta, date, timezone
from calendar import monthrange, month_name


def get_performance_category(chosen_category):
    performance_dict = {
    'A':'A - The sensor type fulfills the requirements from WMO/CIMOs on measurement accuracy, calibration and maintenance.', 
    'B':'B - Rutines for calibration and maintanance are known. Control of the montage exists. The precision of the measurment is lower than the WMO/CIMO requirements.',
    'C': 'C - The sensor type is assumed to fulfill the WMO/CIMO requirements. Missing measurement for control, rutines for calibration, or maintanence.',
    'D': 'D - The sensor type is assumed to fulfill the WMO/CIMO requirements. Some controls show deviations from the WMO/CIMO requirements.'}
    return performance_dict.get(chosen_category)


def get_keywords_from_json(chosen_category, document_path):
    
    something_dict = json.load(open(document_path))
    if chosen_category not in something_dict:
        print(chosen_category,'the variable requested is not listed in the keywords dictionary')
    return something_dict.get(chosen_category)


def get_keywords_from_csv(variable, csv_path):
    
    avoid = ['of', 'in', 'at', 'from', 'or', 'and']
    
    keywords = pd.read_csv(csv_path, header=0)
    columns = {'Detailed_Variable':1, 'Variable_Level_3':2, 'Variable_Level_2':3, 'Variable_Level_1':2, 'Term':1}
    
    cat_keys = variable.upper().split('_')
    for i in avoid:
        a = 0
        while a < cat_keys.count(i):
            cat_keys.remove(i)
            a += 1
    
    indexes = []
    for cat in cat_keys:
        plus = 1
        for c in columns.keys():
            ind = keywords[c].str.contains(cat)
            indice = ind[ind==1].index
            indexes = indexes + list(indice)*columns[c]
            plus += 1
    
    maximum = 0
    row = None
    for i in np.unique(np.array(indexes)):
        reps = indexes.count(i)
        if reps > maximum:
            maximum = reps
            row = i
        elif reps == maximum and reps != 0:
            bad_indexes={}
            for j in [row, i]:
                words = []
                for c in columns:
                    word = keywords.loc[j,c]
                    if isinstance(word, str):
                        list_words = word.split(' ')
                    else:
                        continue
                    finalwords = [x.split('/') for x in list_words]
                    for a in avoid:
                        try:
                            while True:
                                finalwords.remove(a)
                        except ValueError:
                            pass
                    words = words + finalwords
                for cat in cat_keys:
                    try:
                        words.remove(cat)
                    except ValueError:
                        continue
                bad_indexes[str(j)] = len(words)
            if bad_indexes[str(row)] > bad_indexes[str(i)]:
                row = i
        else:
            continue
    
    str_keywords = ''.join([str(keywords.loc[row, 'Category']),' > ',
                            str(keywords.loc[row, 'Topic']),' > ',
                            str(keywords.loc[row, 'Term'])+' > ',
                            str(keywords.loc[row, 'Variable_Level_1'])])
    if isinstance(keywords.loc[row, 'Variable_Level_2'], str):
        str_keywords = ''.join([str_keywords, ' > ', str(keywords.loc[row, 'Variable_Level_2'])])
        if isinstance(keywords.loc[row, 'Variable_Level_3'], str):
            str_keywords = ''.join([str_keywords, ' > ', str(keywords.loc[row, 'Variable_Level_3'])])
            if isinstance(keywords.loc[row, 'Detailed_Variable'], str):
                str_keywords = ''.join([str_keywords, ' > ', str(keywords.loc[row, 'Detailed_Variable'])])
                
    return str_keywords
    

def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c","--cfg",dest="cfgfile",
            help="Configuration file", required=True)
    parser.add_argument("-s","--startday",dest="startday",
            help="Start day in the form YYYY-MM-DD", required=False)
    parser.add_argument("-e","--endday",dest="endday",
            help="End day in the form YYYY-MM-DD", required=False)
    parser.add_argument("-a","--allhist",dest="allhistory",
            help="To download all data to date", required=False, action='store_true')
    parser.add_argument("-u","--upt",dest="update",
            help="To update data", required=False, action='store_true')
    args = parser.parse_args()

    if args.startday is None:
        pass
    else:
        try:
            datetime.strptime(args.startday,'%Y-%m-%d')
        except ValueError:
            raise ValueError
    if args.endday is None:
        pass
    else:
        try:
            datetime.strptime(args.endday,'%Y-%m-%d')
        except ValueError:
            raise ValueError

    if args.cfgfile is False:
        parser.print_help()
        parser.exit()

    return args


def parse_cfg(cfgfile):
    # Read config file
    print("Reading", cfgfile)
    with open(cfgfile, 'r') as ymlfile:
        cfgstr = yaml.full_load(ymlfile)

    return cfgstr


def initialise_logger(outputfile = './log'):
    # Check that logfile exists
    logdir = os.path.dirname(outputfile)
    if not os.path.exists(logdir):
        try:
            os.makedirs(logdir)
        except:
            raise IOError
    # Set up logging
    mylog = logging.getLogger()
    mylog.setLevel(logging.INFO)
    #logging.basicConfig(level=logging.INFO, 
    #        format='%(asctime)s - %(levelname)s - %(message)s')
    myformat = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(myformat)
    mylog.addHandler(console_handler)
    file_handler = logging.handlers.TimedRotatingFileHandler(
            outputfile,
            when='w0',
            interval=1,
            backupCount=7)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(myformat)
    mylog.addHandler(file_handler)

    return(mylog)


def pull_request(site, request, frostcfg, mylog, s = None, data = False):
    
    msger = False
    try:
        r = requests.get(site,
                request,
                auth=(frostcfg['client_id'],""))
    except:
        mylog.error('Something went wrong extracting metadata.')
        msger = True
    # Check if the request worked, print out any errors
    if not r.ok:
        mylog.error('Returned status code was %s saying %s', r.status_code, r.text)
        #print('>>>>',r.text)
        msger = True
     # Check if the request worked, print out any errors
    if r.status_code == 412 or r.status_code == 404:
        mylog.error('Information returned indicates that no data is available for this time period for station %s', s)
        msger = True
    if not r.status_code == 200:
        mylog.error('Returned status code was %s saying:\n%s', r.status_code, r.text)
        msger = True
    
    if data:
        metadata = r
    else:
        metadata = json.loads(r.text)
    
    return (metadata, msger)


def get_stations(frostcfg, pars, mylog):
    
    """
    Connect and read metadata about the stations
    If configuration asks for a specific layer (:0,:1), this is removed in the request here.
    """
    
    if frostcfg['stations'] is not None:
        # Retrieve selected stations identified in cfg
        mylog.info('Retrieving selected '+frostcfg['st_type']+' stations in FROST.')
        stations = frostcfg['stations']       
        myrequest = 'ids='+','.join(stations.keys())
        metadata, msger = pull_request(frostcfg['endpointmeta'], myrequest, frostcfg, mylog)
    else:
        # Retrieve all stations found
        mylog.info('Retrieving all '+frostcfg['st_type']+' stations in FROST. %s')
        if st_type == 'permafrost':
            myrequest = 'types=SensorSystem&elements=soil_temperature'
        elif st_type == 'moving':
            myrequest = 'types=SensorSystem&municipality=skip'
        elif st_type == 'fixed':
            myrequest = 'types=SensorSystem&elements!=soil_temperature&municipality!=skip'
        metadata, msger = pull_request(frostcfg['endpointmeta'], myrequest, frostcfg, mylog)
        stations = list(set([x['id'] for x  in metadata['data']]))       
    
    if stations:
        stations_dicts =  metadata['data']
        return (stations, stations_dicts)
    else:
        mylog.error('None station was set for downloading data.')
        

def get_vars(request, frostcfg, mylog, msg):
    
    mylog.info(msg)
    site = frostcfg['endpointparameters']
    metadata, msger = pull_request(site, request, frostcfg, mylog)
    return metadata


"""
Not sure on this but believe it is listing periods with actual data.
Meaqning, dividing the request into segmented requests.
"""
def get_periods(pars, metadata, direc, backwards=None):
    
    today = date.today()
    periods = []
    if pars.allhistory:
        from_day = datetime.strptime(metadata['validFrom'],'%Y-%m-%dT%H:%M:%S.%fZ')
        to_day = date.today()
        periods = list(gen_periods(from_day, to_day))
    elif pars.update:
        from_day = datetime.strptime(f'{today.year:02d}-{today.month:02d}-01','%Y-%m-%d')
        to_day = date.today()
        periods = list(gen_periods(from_day, to_day))
    elif pars.startday and pars.endday:
        from_day = datetime.strptime(pars.startday,'%Y-%m-%d')
        to_day = datetime.strptime(pars.endday,'%Y-%m-%d')
        periods = list(gen_periods(from_day, to_day))
    else:
        if backwards == True:
            st_startyear = datetime.strptime(metadata['validFrom'],'%Y-%m-%dT%H:%M:%S.%fZ').year
        elif isinstance(backwards, int):
            st_startyear = date.today().year - backwards
        else: 
            st_startyear = date.today().year
        for y in range(st_startyear, date.today().year+1):
            to_day = datetime(y, 12, 31)
            if metadata['id'] in os.path.split(direc):
                folder = os.path.join(direc, str(y))
            else:
                folder = os.path.join(direc, metadata['id'], str(y))
            files = glob.glob(folder+'/*.nc')
            dia = [datetime.strptime(os.path.split(f)[1][19:29], '%Y-%m-%d') for f in files]
            if not dia:
                from_day = datetime(y, 1, 1)
                periods = periods + list(gen_periods(from_day, to_day))
            elif max(dia).month == 12 and max(dia).day == 31:
                continue
            else:
                if max(dia).month != 12 and max(dia).day == monthrange(y,max(dia).month)[1]:
                    from_day = datetime(y, max(dia).month+1, 1)
                else:
                    from_day = datetime(y, max(dia).month, 1)
                periods = periods + list(gen_periods(from_day, to_day))
                #to_day = datetime.datetime(y, month+1, monthrange(y, month+1)[1])
                
    return periods

"""
Fix this so it actually reflects the requested time period. It doesn't now.
Assuming the need for this is to segment data into monthly files.
A preliminary fix has been added temporarily, but still not fully good.
"""
def gen_periods(from_day, to_day):
    
    for j in range(from_day.year, to_day.year+1):
        aa = list(month_name).index('January')
        zz = len(month_name)
        if j == from_day.year:
            aa = from_day.month   
        if j == to_day.year:
            zz = to_day.month+1
        else:
            pass
        for i in range(aa, zz):
            starting_point = ''.join([str(j), '-', '%02d'%i, '-', '01'])
            end_point = ''.join([str(j), '-', '%02d'%i, '-', str(monthrange(j, i)[1])])
            false_end = (datetime.strptime(end_point,'%Y-%m-%d')+timedelta(days=1)).strftime('%Y-%m-%d')
            yield (starting_point, false_end)
            

# This doesn't work entirely as expected, time is not handled...
"""
Handle data type conversions when writing NetCDF files.
This is particularly important for publishing in TDS v4
"""
def set_encoding(ds, fill=-999, time_name = 'time'):

    all_encode = {}

    for v in list(ds.data_vars):
        if 'float' in str(ds[v].dtype):
            dtip = 'f4'
        elif 'int' in str(ds[v].dtype):
            dtip = 'i4'
        else:
            #dtip = str(ds[v].dtype)
            dtip = 'S1'
        encode = {'zlib': True, 'complevel': 9, 'dtype': dtip, '_FillValue':fill}
        all_encode[v] = encode

    for v in list(ds.coords):
        if 'float' in str(ds.coords[v].dtype):
            dtip = 'f4'
        elif 'int' in str(ds.coords[v].dtype):
            dtip = 'i4'
        if v == time_name:
            dtip = 'i4'
        encode = {'zlib': True, 'complevel': 9, 'dtype': dtip, }
        all_encode[v] = encode
        
    #print(json.dumps(all_encode, indent=4))
    return all_encode

def add_global_attrs(sttype, ds, dsmd, stmd, stmdd, dyninfo, kw, bbox=None):
    """
    Adds global attributes according to ACDD to NetCDF file.

    Parameters
    ----------
    sttype: type of station, fixed, moving, permafrost
    ds: xarray object
        xarray dataset
    dsmd: dict
        generic global attributesa from configuration file
    stmd: dict
        Metadata from frost
    stmdd: dict
        Detailed metadata per station (not required)
    dyninfo: list
        start and end of dataset
    kw: list
        GCMD science keywords
    bbox: dict
        geographical bounding box

    Returns
    -------
    ds: xarray dataset
        xarray dataset
    """

    # Title and summary
    if sttype == "permafrost":
        ds.attrs['title'] = np.array(f"Permafrost station {stmd['data'][0]['name']} ({stmd['data'][0]['id']})".encode("utf-8")) 
        ds.attrs['featureType'] = 'timeSeriesProfile'
    elif sttype == "moving":
        # This is yet not tested and need more development...
        ds.attrs['title'] = np.array(f"Weather station from ship {stmd['data'][0]['name']} ({stmd['data'][0]['id']})".encode("utf-8")) 
        ds.attrs['featureType'] = 'Trajectory'
    else:
        ds.attrs['title'] = np.array(f"Weather station {stmd['data'][0]['name']} ({stmd['data'][0]['id']})".encode("utf-8")) 
        ds.attrs['featureType'] = 'timeSeries'
    ds.attrs['summary'] = np.array(f"Information from the station {stmd['data'][0]['name']} with MET station number {stmd['data'][0]['id']}.  {dsmd['abstract']}".encode("utf-8"))

    # Keywords
    if kw != None:
        ds.attrs['keywords'] = ', '.join(kw)
        ds.attrs['keywords_vocabulary'] = 'GCMDSK:GCMD Science Keywords:https://gcmd.earthdata.nasa.gov/kms/concepts/concept_scheme/sciencekeywords'

    # Add GEMET and NORTHEMES keywords to fixed station type data.
    if sttype == 'fixed':
        keywords = ('GEMET:Meteorological geographical features, GEMET:Atmospheric conditions,'
                    ' NORTHEMES:Weather and climate')
        vocab = ('GEMET:INSPIRE Themes:https://inspire.ec.europa.eu/theme,'
                 ' NORTHEMES:GeoNorge Themes:https://register.geonorge.no/metadata-kodelister/nasjonal-temainndeling')

        try:
            ds.attrs['keywords'] = ds.attrs['keywords'] + ', ' + keywords
        except KeyError:
            ds.attrs['keywords'] = keywords

        try:
            ds.attrs['keywords_vocabulary'] = ds.attrs['keywords_vocabulary'] + ', ' + vocab
        except KeyError:
            ds.attrs['keywords_vocabulary'] = vocab

    # License (TODO, not complete)
    if stmdd:
        stmddkeys = stmdd.keys()
        if 'license' in stmddkeys:
            ds.attrs['license'] = stmdd['license']
        else:
            ds.attrs['license'] = dsmd['license']

    # Spatiotemporal information
    ds.attrs['time_coverage_start'] = dyninfo['datasetstart']
    ds.attrs['time_coverage_end'] = dyninfo['datasetend']
    if sttype == "moving" and bbox != None:
        ds.attrs['geospatial_lat_min'] = bbox['lat_min']
        ds.attrs['geospatial_lat_max'] = bbox['lat_max']
        ds.attrs['geospatial_lon_min'] = bbox['lon_min']
        ds.attrs['geospatial_lon_max'] = bbox['lon_max']
    else:
        ds.attrs['geospatial_lat_min'] = stmd['data'][0]['geometry']['coordinates'][1]
        ds.attrs['geospatial_lat_max'] = stmd['data'][0]['geometry']['coordinates'][1]
        ds.attrs['geospatial_lon_min'] = stmd['data'][0]['geometry']['coordinates'][0]
        ds.attrs['geospatial_lon_max'] = stmd['data'][0]['geometry']['coordinates'][0]

    # People and institutions contributing (TODO, not complete)
    if stmdd:
        if 'PrincipalInvestigator' in stmddkeys:
            ds.attrs['creator_name'] = stmdd['PrincipalInvestigator'] 
        else:
            ds.attrs['creator_name'] = dsmd['PrincipalInvestigator'] 
        if 'PrincipalInvestigatorEmail' in stmddkeys:
            ds.attrs['creator_email'] = stmdd['PrincipalInvestigatorEmail']
        else:
            ds.attrs['creator_email'] = dsmd['PrincipalInvestigatorEmail']
        if 'PrincipalInvestigatorOrganisationURL' in stmddkeys:
            ds.attrs['creator_url'] = stmdd['PrincipalInvestigatorOrganisationURL']
        else:
            ds.attrs['creator_url'] = dsmd['PrincipalInvestigatorOrganisationURL']
        if 'PrincipalInvestigatorOrganisation' in stmddkeys:
            ds.attrs['creator_institution'] = stmdd['PrincipalInvestigatorOrganisation']
        else:
            ds.attrs['creator_institution'] = dsmd['PrincipalInvestigatorOrganisation']
        if 'contributor_name' in stmddkeys:
            ds.attrs["contributor_name"] = np.array(stmdd['contributor_name'].encode("utf-8"))
        elif 'contributor_name' in dsmd:
            ds.attrs["contributor_name"] = np.array(dsmd['contributor_name'].encode("utf-8"))
        if 'contributor_email' in stmddkeys:
            ds.attrs["contributor_email"] = np.array(stmdd['contributor_email'].encode("utf-8"))
        else:
            ds.attrs["contributor_email"] = np.array(dsmd['contributor_email'].encode("utf-8"))
        if 'contributor_role' in stmddkeys:
            ds.attrs["contributor_role"] = np.array(stmdd['contributor_role'].encode("utf-8"))
        elif 'contributor_role' in dsmd:
            ds.attrs["contributor_role"] = np.array(dsmd['contributor_role'].encode("utf-8"))
        if 'contributor_institution' in stmddkeys:
            ds.attrs["contributor_institution"] = np.array(stmdd['contributor_institution'].encode("utf-8"))
        else:
            ds.attrs["contributor_institution"] = np.array(dsmd['contributor_institution'].encode("utf-8"))
    else:
        ds.attrs['creator_name'] = dsmd['PrincipalInvestigator'] 
        ds.attrs['creator_email'] = dsmd['PrincipalInvestigatorEmail']
        ds.attrs['creator_url'] = dsmd['PrincipalInvestigatorOrganisationURL']
        ds.attrs['creator_institution'] = dsmd['PrincipalInvestigatorOrganisation']
        if 'contributor_name' in dsmd:
            ds.attrs["contributor_name"] = np.array(dsmd['contributor_name'].encode("utf-8"))
        if 'contributor_email' in dsmd:
            ds.attrs["contributor_email"] = np.array(dsmd['contributor_email'].encode("utf-8"))
        if 'contributor_institution' in dsmd:
            ds.attrs["contributor_institution"] = np.array(dsmd['contributor_institution'].encode("utf-8"))
        if 'contributor_role' in dsmd:
            ds.attrs["contributor_role"] = np.array(dsmd['contributor_role'].encode("utf-8"))

    # Data center information
    ds.attrs['publisher_name'] = 'Norwegian Meteorological Institute / Arctic Data Centre'
    ds.attrs['publisher_email'] = 'adc-support@met.no'
    ds.attrs['publisher_url'] = 'https://adc.met.no/'
    ds.attrs['publisher_institution'] = 'Norwegian Meteorological Institute'

    # Conventions specification
    ds.attrs['Conventions'] = 'ACDD, CF-1.8'

    # Provenance information
    ds.attrs['date_created'] = stmd['createdAt']
    ds.attrs['history'] = stmd['createdAt']+': Data extracted from the MET Observation Database through Frost and stored as NetCDF-CF'
    ds.attrs['source'] = 'Norwegian Meteorological Institute archive of historical weather and climate data' 

    # Identifiers
    ds.attrs['wigosId'] = stmd['data'][0]['wigosId']
    ds.attrs['MET_Identifier'] =  stmd['data'][0]['id']

    # Project linkages
    if stmdd:
        if 'Project' in stmddkeys:
            ds.attrs['project'] = stmdd['Project']
        else:
            ds.attrs['project'] = dsmd['Project']

    return(ds)

def extractdata(frostcfg, pars, log, stmd, output, simple=True):
    '''
    Does the actual exrtraction of data

    Args:
        frostcfg: dictionary with endpoints and stations to collect.
        pars: 
        log: logger stream to use
        stmd: default discovery metadata
        output: where to put information and log
        simple: not sure if this is used
        est: type of station

    Returns:
        NA
    '''
    

    resols = ('PT1S', 'PT1M', 'PT5M', 'PT10M', 'PT15M', 'PT20M', 'PT30M', 'PT1H', 'PT3H', 'PT6H', 'PT12H', 'P1D',
              'P1M', 'P3M', 'P6M', 'P1Y')
    freq_dict = {'PT1S':'S', 'PT1M':'min', 'PT5M':'5min', 'PT10M':'10min', 'PT15M':'15min', 'PT20M':'20min',
                 'PT30M':'30min', 'PT1H':'H', 'PT3H':'3H', 'PT6H':'6H', 'PT12H':'12H', 'P1D':'D', 'P1M':'M', 
                 'P3M':'3M', 'P6M':'6M', 'P1Y':'Y'}
    freq_dict_attr = {'PT1S':'1 second', 'PT1M':'1 minute', 'PT5M':'5 minutes',
                      'PT10M':'10 minutes', 'PT15M':'15 minutes', 'PT20M':'20 minutes',
                 'PT30M':'30 minutes', 'PT1H':'1 hour', 'PT3H':'3 hours', 'PT6H':'6 hours',
                 'PT12H':'12 hours', 'P1D':' 1 day', 'P1M':'1 month', 
                 'P3M':'3 months', 'P6M':'6 months', 'P1Y':'1 year'} 
    performances = ('A', 'B', 'C', 'D')
    #avoid_var = ['min', 'max', 'mean', 'PT', 'over_time', 'sum', 'P1D']
    avoid_var = ['min', 'max', 'over_time', 'best_estimate', 'accumulated', 'P1D']
    irradstvars = ['surface_downwelling_shortwave_flux_in_air',
            'surface_upwelling_shortwave_flux_in_air',
            'surface_downwelling_longwave_flux_in_air',
            'surface_upwelling_longwave_flux_in_air',
            'duration_of_sunshine',]
    myfillvalue = -999
    
    # Get a list with all stations and some metadata
    
    est = frostcfg['st_type']
    sts, sts_dicts = get_stations(frostcfg, pars, log)
    
    # STATIONS LOOP
    for s in sts:
        
        if os.path.exists(os.path.join(output['destdir'], s)):
            pass
        else:
            os.mkdir(os.path.join(output['destdir'], s))
        
        #station_dict = [x for x in sts_dicts if x['id']==s][0]
        station_dict = [x for x in sts_dicts if x['id']==s]
        # Bail out if requester station does not exist...
        if not station_dict:
            log.warning(f'The requested station {s} was not found.')
            continue

        station_dict = station_dict[0]
        
        # Connect and read metadata about the station
        log.info("New station\n==========")
        log.info('Retrieving station metadata for station: %s', s)
        myrequest_station = 'ids='+s
        metadata, msger = pull_request(frostcfg['endpointmeta'], myrequest_station, frostcfg, log, s=s)
        #print(json.dumps(metadata, indent=4))
        #print(metadata['data'][0]['name'])
        
        # Identification of temporal segments to extract (monthly)
        periods = get_periods(pars, station_dict, output['destdir']) #this is a generator giving pairs of startday and ending day
        # Loop through temporal segments, extract and dump data
        for p in periods:
            log.info('Downloading data for period: %s', p)
            # Check that the station has data in the period requested.
            # Sometimes this will fail anyway since there is no data due to technical issues and the station is still considered active.
            if 'validTo' in metadata['data'][0].keys():
                if datetime.strptime(p[0],'%Y-%m-%d') > datetime.strptime(metadata['data'][0]['validTo'],'%Y-%m-%dT%H:%M:%S.%fZ'): 
                    log.warning('Station %s doesn\'t contain data as late as this.', s)
                    return
            if 'validFrom' in metadata['data'][0].keys():
                if datetime.strptime(p[1],'%Y-%m-%d') < datetime.strptime(metadata['data'][0]['validFrom'],'%Y-%m-%dT%H:%M:%S.%fZ'):
                    log.warning('Station %s doesn\'t contain data as early as this.', s)
                    return
        
            """
            Get a list of elements & Connect and read metadata about the variables.
            If the configuration file contain sourceId per station, the specified sources are extracted, if not only source 0 is extracted.
            Source is appended to output folder name if different from 0.
            """
            # TODO add support for multiple time resolutions...
            mm = ''.join(['Retrieving variables metadata for station: ', s])
            myrequest_vars = 'sources='+s+'&referencetime='+'/'.join(p)+'&timeresolutions='+frostcfg['frequency']
            variables = get_vars(myrequest_vars, frostcfg, log, mm)
            #print('>>>> ', json.dumps(variables['data'], indent=2))
            #sys.exit()
            if 'data' in variables.keys():
                elements = set([i['elementId'] for i in variables['data']])
            else:
                continue
            
            # Loop through ELEMENTS (variables) and create list for extraction, irradiance can be handled in a standalone mode if necessary
            # TODO Check this further
            # legge til vars_to_down her???
            dir_elements_resol = {}
            for e in elements:
                if (est != 'irradiance') and simple and any(a in e for a in avoid_var):
                    continue
                elif est == 'irradiance':
                    match = False
                    for x in irradstvars:
                        if x in e:
                            match = True
                            break
                    if not match:
                        continue
                times = [j['timeResolution'] for j in variables['data'] if j['elementId']==e]
                max_resol = resols[min([resols.index(t) for t in times])]
                perfs = [j['performanceCategory'] for j in variables['data'] if (j['elementId']==e and j['timeResolution']==max_resol)]
                max_perf = performances[min([performances.index(pf) for pf in perfs])]
                var_dict = [x for x in variables['data'] if x['elementId']==e][0]
                dir_elements_resol[e] = (max_resol, max_perf, var_dict)

            #print(json.dumps(dir_elements_resol, indent=2))
            #print('>>>> ', times)
           
            #TIME RESOLUTIONS LOOP
            # TODO Why two takes on the resols loop? Øystein Godøy, METNO/FOU, 2023-03-08 
##            time_dim = {}
##            for t in resols:
##                # Only handling one observation frequency at the time
##                if t != frostcfg['frequency']:
##                    continue
##                vars_to_down = [x for x in dir_elements_resol if dir_elements_resol[x][0]==t]
##                #print('>>>> ', vars_to_down)
##                if not vars_to_down:
##                    continue
##                dates = pd.date_range(p[0], p[1], freq=freq_dict[t])
##                #print('>>>> ', dates)
##                #print('>>>> ', len(dates))
##                if len(dates) == 0:
##                    continue
##                dates = dates.drop(dates[-1])
##                t_name = ''.join(['time_', t])
##                time_dim[t_name] = ([t_name], dates)
               
            # Set up the time dimension ++
            dates = pd.date_range(p[0], p[1], freq=freq_dict[frostcfg['frequency']])
            dates = dates.drop(dates[-1]) # Means that last time step will be dropped
            t_name = 'time'

            vars_to_down = [x for x in dir_elements_resol]
            # TODO for some reason functions are not converted... check
            if est=='permafrost' and not 'soil_temperature' in vars_to_down:
                log.warning('No soil_temperature for this station.')
                continue
            if not vars_to_down:
                log.warning('No data found.')
                continue
            # check if can be changed, t_name and time_dim
            #t_name = ''.join(['time_', t])

            # TODO check if this is needed...
            if not 'all_ds_station' in locals():
                #all_ds_station = xr.Dataset(coords={t_name:time_dim[t_name]})
                all_ds_station = xr.Dataset(coords={t_name:dates})
                #print('>>>> ', all_ds_station.dims)
        
            # Parse sourceId if present and create station source to extract
            if frostcfg['stations'][s] != None:
                if 'sourceId' in frostcfg['stations'][s]:
                    mysource = ':'.join([s,str(frostcfg['stations'][s]['sourceId'])])
                else:
                    mysource = s
            else:
                mysource = s
            # Create request for observations
            log.info('Retrieving data for station: %s, and period: %s/%s', s, p[0], p[1])
            log.info('Variables found for this station and time resolution: %s', vars_to_down)
            if est == "permafrost":
                myrequest_data = ('sources='+mysource+'&elements=soil_temperature'
                    +'&fields='+','.join(frostcfg['fields'])
                    +'&referencetime='+'/'.join([p[0],p[1]])+'&timeresolutions='+frostcfg['frequency'])
            else:
                # Drop soil_temperature for fixed stations, to be revisited later TODO
                if 'soil_temperature' in vars_to_down:
                    vars_to_down.remove('soil_temperature')
                myrequest_data = ('sources='+mysource+'&elements='
                    +', '.join(vars_to_down)
                    +'&fields='+','.join(frostcfg['fields'])
                    +'&referencetime='+'/'.join([p[0],p[1]])+'&timeresolutions='+frostcfg['frequency'])
            #print(myrequest_data)
            #sys.exit()

            # Connect and read observations
            data, msg_err = pull_request(frostcfg['endpointobs'], 
                         myrequest_data, frostcfg, mylog, s=mysource, data=True)
            if msg_err:
                log.warning('Error experienced downloading data %s', msg_err)
                continue
            #print(type(data))
            #print(data.text)
            #sys.exit()

            # Read into Pandas DataFrame
            # Dump to file is only temporarily TODO
            df = pd.read_csv(StringIO(data.text),header=0,
                parse_dates=False,
                index_col=False,na_values=['-'])
            fp = open('myfile.txt', 'w')
            fp.write(df.to_string())
            fp.close()
            # There are issues with naming conventions between different end points, doing a partial renaming for string matching, full renaming further down
            mycolnames = df.keys()
            mynewcolnames = {}
            for it in mycolnames:
                itnew = re.sub('pt1h',lambda ele: ele.group(0).upper(),it)
                itnew = re.sub('\(-\)','',itnew)
                mynewcolnames.update({it:itnew})

            #print('>>>> ', mynewcolnames)
            #sys.exit()
            df.rename(columns=mynewcolnames, inplace=True)
            
            # Parsing time
            timos = [datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%fZ') for x in df['referenceTime']]
            mindate = min(timos)
            maxdate = max(timos)
            datasetstart = mindate.strftime('%Y-%m-%dT%H:%M:%SZ')
            datasetend = maxdate.strftime('%Y-%m-%dT%H:%M:%SZ')
            # String to use in output file name
            filenamestr = datasetstart[:10]+f'_{maxdate.year}-{maxdate.month:02}-{monthrange(maxdate.year,maxdate.month)[1]}'
            df.loc[:,t_name] = timos
            df.drop(['referenceTime'], axis=1, inplace=True)
            # TODO check, something is wrong...

            # Check if column names has to be modified before this block
            # Restructure permafrost data
            # double check why missing levels are pushed through...
            if est=='permafrost':
                perma = 'soil_temperature'
                # Identify which columns that contain soil_temperature
                cols = df.columns
                soil_num = [df.columns.get_loc(x) for x in cols if perma in x]
                # Since every second column is soil_temperature and depth, identify depth columns
                depth_num = [x+1 for x in soil_num]
                ntime = 0
                mytimes = []
                myprofiles = list()
                mydepths = list()
                quality_check = []
                # Loop over time and reshape
                # TODO fix match on depths and values...
                for i in df.loc[:,t_name]:
                    mydata = {
                            'depth':df.iloc[ntime, depth_num].values,
                            perma:df.iloc[ntime, soil_num].values
                            }
                    mytmpdata = pd.DataFrame(mydata).sort_values(by='depth')
                    """
                    print('##########')
                    print(i)
                    print(type(mytmpdata))
                    """
                    if mytmpdata[perma].isnull().all():
                        quality_check.append(False)
                    else:
                        quality_check.append(True)
                    mytmpdata.fillna(value=myfillvalue, inplace = True)
                    mytmpdata = mytmpdata.drop(mytmpdata[mytmpdata['depth'] == -999.0].index)
                    # Make sure depth is consistent across all profiles opf the month
                    mydepths = [ele for ele in mytmpdata['depth'] if ele not in mydepths]
                    myprofiles.append(mytmpdata)
                    mytimes.append(i)
                    ntime += 1
                # Consolidate depth of profiles for month
                maxdepths = 0
                depthlevs = []
                for item in myprofiles:
                    tmparr = item.depth.astype('int32').to_list()
                    for ele in tmparr: 
                        if ele not in depthlevs:
                            depthlevs.append(ele)
                depthlevs.sort()
                depthlevs = [float(i) for i in depthlevs]
                # Now make sure all levels are handled by adding missing
                # TODO fix
                i = 0
                myarray = []
                for item in myprofiles:
                    for z in depthlevs:
                        zint = float(z)
                        tmparr = item.depth.astype('int32')
                        if zint not in tmparr.values:
                            newrec = {'depth': z, 'soil_temperature': -999.}
                            item.loc[len(item)] = newrec
                            myprofiles[i] = item
                    myprofiles[i] = myprofiles[i].sort_values(by='depth', ascending=True)
                    myarray.append(myprofiles[i].soil_temperature)
                    i += 1
                myarray2d = []
                myarray2d = np.array([series.to_numpy() for series in myarray])
                if True in quality_check:   
                    try:
                        da_profile = xr.DataArray(myarray2d, 
                                dims=['time','depth'],
                                coords={
                                    'time':mytimes,
                                    'depth':depthlevs})
                    except Exception as e:
                        mylog.error('Something failed creating the DataArray: %s', e)
                        raise(e)
                
                df.drop(df.columns[depth_num+soil_num],axis=1,inplace=True)
                if perma in vars_to_down:
                    vars_to_down.remove(perma)
                
            # SOME CLEANNING
            df = df.set_index(t_name)
                
            included = list()
            excluded = list()
            # Not entirely sure on the use case for the code below
            # Double check...
            # Øystein Godøy, METNO/FOU, 2023-03-10 
            cols = df.keys()
            for el in vars_to_down:
                if el in cols:
                    included.append(el)
                else:
                    excluded.append(el)
            if not included and est != "permafrost":
                log.warning('No variables to process for some odd reason...')
                continue
            for inc in included:
                try:
                    df[inc]
                except KeyError:
                    included.remove(inc)
            df = df[included].copy()                
            if excluded:
                for ex in excluded:
                    df.loc[:,ex] = [myfillvalue]*len(df.index)

            # Create Dataset from Dataframe
            df.reset_index(drop=False, inplace=True)
            big_dictio = {"coords":{}, "dims":t_name, "data_vars":{}}
            for col in df.columns:
                if col == t_name:
                    big_dictio["coords"][col] = {"dims":col, "data":timos}
                else:
                    big_dictio["data_vars"][col] = {"dims":t_name, "data":df[col].values}
            """
            print('>>>> ',big_dictio['data_vars'].keys())
            print('grass ', big_dictio['data_vars']['grass_temperature'])
            print('rh ', big_dictio['data_vars']['relative_humidity'])
            print('dp ', big_dictio['data_vars']['dew_point_temperature'])
            print('precip ',big_dictio['data_vars']['accumulated(precipitation_amount)'])
            """
            ds_station = xr.Dataset.from_dict(big_dictio)

            voc_list =[]              

            if est=='permafrost' and 'da_profile' in locals():
                
                # To include only the soil temperature
                ds_station = ds_station.drop([v for v in ds_station.data_vars])
                
                ds_station = ds_station.assign_coords(depth=da_profile.depth.values,time=da_profile.time.values)
                ds_station['depth'].attrs['standard_name'] = 'depth'
                ds_station['depth'].attrs['long_name'] = 'depth below surface'
                ds_station['depth'].attrs['units'] = 'cm'
                ds_station[t_name] = (('time') , mytimes)
                ds_station[perma] = (('time', 'depth'), da_profile.values)
                ds_station[perma].attrs['long_name'] = perma.replace('_', ' ')
                ds_station[perma].attrs['standard_name'] = perma
                ds_station[perma].attrs['units'] = 'degC'
                ds_station[perma].attrs['coordinates'] = t_name
                ds_station[perma].attrs['performance_category'] = get_performance_category(dir_elements_resol[perma][1])
                var_dims = [item for v in ds_station.data_vars for item in ds_station[v].dims]
                """
                print('###')
                print(ds_station)
                """
                for dd in list(ds_station.dims):
                    if not dd in var_dims:
                        ds_station = ds_station.drop_dims(dd)
                        try:
                            if dd == 'profile':
                                continue
                            if 'time' in dd:
                                continue
                            ds_station = ds_station.drop(dd)
                        except KeyError:
                            continue
                del da_profile


            # Modify variable names to remove functions and time sampling
            mycolnames = list(ds_station.keys())
            mynewcolnames = {}
            for it in mycolnames:
                itnew = re.sub('PT1H|[\ ()-]','',it)
                itnew = re.sub('(mean|sum)','\g<1>_',itnew)
                mynewcolnames.update({it:itnew})
            #print('>>>> ', mynewcolnames)
            ds_station = ds_station.rename_vars(mynewcolnames)

            # Specify variable attributes, time is converted further down
            ds_station[t_name].attrs['standard_name'] = 'time'
            ds_station[t_name].attrs['long_name'] = 'time with frequency of '+freq_dict_attr[frostcfg['frequency']]
            ds_station[t_name].attrs['units'] = 'seconds since 1970-01-01T00:00:00+0'
            check_list = []
            # Loop through variables and update global keywords attributes as well as variable attributes like units, standard_name, long_name and performance category for measurements
            #for vname in list(ds_station.data_vars):
            for vname in mynewcolnames.keys():
                if vname in check_list:
                    continue
                else:
                    ds_station.assign()
                    try:
                        """
                        This doesn't make sense for permafrost at least
                        val_unit = str(dir_elements_resol[vname][2]['level']['value']) + ' ' + str(dir_elements_resol[vname][2]['level']['unit'])
                        print(val_unit)
                        """
                        ds_station[mynewcolnames[vname]].attrs['long_name'] = vname.replace('_',' ') 
                    except KeyError:
                        ds_station[mynewcolnames[vname]].attrs['long_name'] = mynewcolnames[vname].replace('_', ' ')
                    ds_station[mynewcolnames[vname]].attrs['standard_name'] = re.sub('mean_|sum_','', mynewcolnames[vname])
                    try:
                        ds_station[mynewcolnames[vname]].attrs['units'] = dir_elements_resol[vname][2]['unit']
                    except KeyError:
                        ds_station[mynewcolnames[vname]].attrs['units'] = 'S1'
                    # Performance category loookup is based on Frost variable names
                    ds_station[mynewcolnames[vname]].attrs['performance_category'] = get_performance_category(dir_elements_resol[vname][1])
                    #ds_station[vname].attrs['fillvalue'] = float(myfillvalue)
                    # Keywords lookup is based on CF standard names
                    voc_list.append(get_keywords_from_json(re.sub('mean_|sum_','',mynewcolnames[vname]), output['json_path']))
                    check_list.append(''.join(['GCMDSK:', vname]))

            # Replace the dataset totally for permafrost
            if est == "permafrost":
                all_ds_station = ds_station
            else:
                for v in list(ds_station.variables):
                    if est=='permafrost' and v != perma:
                        continue
                    if 'time' in v:
                        continue
                    all_ds_station[v] = ds_station[v]
            del ds_station
            #print(all_ds_station['time_PT1H'])
            
            if msger:
                continue
            else:
                pass

            if 'all_ds_station' in  locals():
                # Generate BBOX for moving stations
                if est == 'moving' and 'latitude' in all_ds_station.data_vars:
                    lats = np.array(all_ds_station.data_vars['latitude'].values).flatten().astype('float')
                    lons = np.array(all_ds_station.data_vars['longitude'].values).flatten().astype('float')
                    bbox = list()
                    bbox['lat_min'] = np.nanmin(lats)
                    bbox['lat_max'] = np.nanmax(lats)
                    bbox['lon_min'] = np.nanmin(lons)
                    bbox['lon_max'] = np.nanmax(lons)
                else:
                    bbox = None
                
                try:
                    voc_list = [''.join(['GCMDSK:', x]) for x in voc_list]
                except TypeError:
                    voc_list = None 

                # Dump to NetCDF in monthly files, continues updates are overwriting the last file.
                out_folder = os.path.join(output['destdir'], s, str(datetime.strptime(p[0],'%Y-%m-%d').year))
                if frostcfg['stations'][s] != None:
                    if 'sourceId' in frostcfg['stations'][s]:
                        outputfile = os.path.join(out_folder, mysource.replace(':','-')+'_'+filenamestr+'_time_resolution_'+str(frostcfg['frequency'])+'.nc')
                    else:
                        outputfile = os.path.join(out_folder, s+'_'+filenamestr+'_time_resolution_'+str(frostcfg['frequency'])+'.nc')
                else:
                    outputfile = os.path.join(out_folder, s+'_'+filenamestr+'_time_resolution_'+str(frostcfg['frequency'])+'.nc')
                if os.path.exists(out_folder):
                    pass
                else:
                    os.mkdir(out_folder)
                try:
                    if all_ds_station.data_vars: 
                        #To pass time to int32, otherwise the netcdf will be written with time in int64
                        ds_dictio = all_ds_station.to_dict()
                        alltimes = [x for x in ds_dictio['coords'] if 'time' in x]
                        for t_c in alltimes:
                            bad_time = ds_dictio['coords'][t_c]['data']
                            ds_dictio['coords'][t_c]['data'] = np.array([ti.replace(tzinfo=timezone.utc).timestamp() for ti in bad_time]).astype('i4')
                        
                        all_ds_station_period = xr.Dataset.from_dict(ds_dictio)
                        # Add global attributes
                        all_ds_station_period = add_global_attrs(est, all_ds_station_period, stmd, metadata, frostcfg['stations'][s], {'datasetstart': datasetstart,'datasetend': datasetend}, voc_list, bbox)
                        # Set missing values
                        #print(all_ds_station_period)
                        """
                        print(all_ds_station_period['time_PT1H'])
                        sys.exit()
                        """
                        all_ds_station = all_ds_station.fillna(myfillvalue)
                        # Dump data
                        all_ds_station_period.to_netcdf(outputfile, encoding=set_encoding(all_ds_station_period, time_name=alltimes[0]))
                        del all_ds_station
                        del all_ds_station_period
                    else:
                        continue
                #except TypeError:
                except Exception as e:
                    log.error("Something went wrong dumping data to file: %s", e)
                    sys.exit()
                    continue



if __name__ == '__main__':
    
    # Parse command line arguments
    try:
        args = parse_arguments()
    except:
        raise SystemExit('Command line arguments didn\'t parse correctly.')

    # Parse configuration file
    cfgstr = parse_cfg(args.cfgfile)

    # Initialise logging
    output_dir = cfgstr['output']
    mylog = initialise_logger(output_dir['logfile'])
    mylog.info('Configuration of logging is finished.')

    # Query data and create netcdf
    mylog.info('Process stations requested in configuration file.')
    try:
        extractdata(cfgstr['frostcfg'], args, mylog, cfgstr['attributes'], output_dir)
    except Exception as e:
        mylog.error('Something failed %s', e)

