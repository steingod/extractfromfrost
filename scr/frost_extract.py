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
from datetime import datetime, timedelta, date
from calendar import monthrange, month_name


def get_performance_category(chosen_category):
    preformance_dict = {
    'A':'A - The sensor type fulfills the requirements from WMO/CIMOs on measurement accuracy, calibration and maintenance.', 
    'B':'B - Rutines for calibration and maintanance are known. Control of the montage exists. The precision of the measurment is lower than the WMO/CIMO requirements.',
    'C': 'C - The sensor type is assumed to fulfill the WMO/CIMO requirements. Missing measurement for control, rutines for calibration, or maintanence.',
    'D': 'D - The sensor type is assumed to fulfill the WMO/CIMO requirements. Some controls show deviations from the WMO/CIMO requirements.'}
    return preformance_dict.get(chosen_category)


def get_keywords_from_json(chosen_category, document_path):
    
    something_dict = json.load(open(document_path))
    if chosen_category not in something_dict:
        print('the variable requested is not listed in the keywords dictionary')
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
    parser.add_argument("-i","--hist",dest="history",
            help="To download all data to date", required=False, action='store_true')
    parser.add_argument("-u","--upt",dest="update",
            help="To update data", required=False, action='store_true')
    parser.add_argument("-a","--all",dest="stations",
            help="To download/update data from all stations", required=False, action='store_true')
    parser.add_argument("-t","--type",dest="type_station",
            help="To select the type of stations; fixed, permafrost, or moving", required=False)
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

    if args.cfgfile is None:
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
        mylog.error('Returned status code was %s saying %s', r.status_code)
        #print('>>>>',r.text)
        msger = True
     # Check if the request worked, print out any errors
    if r.status_code == 412 or r.status_code == 404:
        mylog.error('Information returned indicates that no data is available for this time period for station %s', s)
        msger = True
    if not r.status_code == 200:
        mylog.error('Returned status code was %s\nmessage:\n%s', r.status_code)
        msger = True
    
    if data:
        metadata = r
    else:
        metadata = json.loads(r.text)
    
    return (metadata, msger)


def get_stations(frostcfg, pars, mylog, st_type='fixed'):
    
    # Connect and read metadata about the station
    
    
    if pars.stations is None:
        mylog.info('Retrieving selected '+st_type+' stations in FROST. %s')
        stations = frostcfg['stations']       
        myrequest = 'ids='+','.join(stations)
        metadata, msger = pull_request(frostcfg['endpointmeta'], myrequest, frostcfg, mylog)
    else:
        mylog.info('Retrieving all '+st_type+'stations in FROST. %s')
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


def get_periods(pars, metadata, direc, backwards=None):
    
    periods = []
    if pars.history:
        from_day = datetime.strptime(metadata['validFrom'],'%Y-%m-%dT%H:%M:%S.%fZ')
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
            dia = [datetime.strptime(os.path.split(f)[1][29:39], '%Y-%m-%d') for f in files]
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

def gen_periods(from_day, to_day):
    
    
    for j in range(from_day.year, to_day.year+1):
        aa = list(month_name).index('January')
        zz = len(month_name)
        if j == from_day.year:
            aa = from_day.month   
        elif j == to_day.year:
            zz = to_day.month+1
        else:
            pass
        for i in range(aa, zz):
            starting_point = ''.join([str(j), '-', '%02d'%i, '-', '01'])
            end_point = ''.join([str(j), '-', '%02d'%i, '-', str(monthrange(j, i)[1])])
            false_end = (datetime.strptime(end_point,'%Y-%m-%d')+timedelta(days=1)).strftime('%Y-%m-%d')
            yield (starting_point, false_end)
            

def set_encoding(ds, fill=-999, time_name = 'time'):
    
    all_encode = {}
        
    for v in list(ds.keys()):
        if v == time_name:
            dtip = 'datetime64[m]'
        else:
            dtip = 'float32'
        encode = {'zlib': True, 'complevel': 9, 'dtype': dtip, '_FillValue':fill,'scale_factor': 0.1}
        #encode = {'zlib': True, 'shuffle': False, 'complevel': 9, 'fletcher32': False, 
         # 'contiguous': False, 'dtype': dtip, '_FillValue':fill,'scale_factor': 0.1}
            
        all_encode[v] = encode
        
    return all_encode
    

def extractdata(frostcfg, pars, log, stmd, output, simple=True, est='fixed'):
    
    #resols = ('PT1S', 'PT1M', 'PT5M', 'PT10M', 'PT15M', 'PT20M', 'PT30M', 'PT1H', 'PT3H', 'PT6H', 'PT12H', 'P1D', 'P1M', 'P3M', 'P6M', 'P1Y')
    resols = ('PT1M', 'PT5M', 'PT10M', 'PT15M', 'PT20M', 'PT30M', 'PT1H', 'PT3H', 'PT6H', 'PT12H', 'P1D', 'P1M', 'P3M', 'P6M', 'P1Y')
    #We decided to remove 1 sec resolution
    performances = ('A', 'B', 'C', 'D')
    avoid_var = ['min', 'max', 'mean', 'PT', 'over_time', 'sum', 'P1D']
    myfillvalue = -999
    
    
    # Get a list with all stations and some metadata
    
    sts, sts_dicts = get_stations(frostcfg, pars, log, st_type = est)
    
    #STATIONS LOOP
    for s in sts:
        
        if os.path.exists(os.path.join(output['destdir'], s)):
            pass
        else:
            os.mkdir(os.path.join(output['destdir'], s))
        
        station_dict = [x for x in sts_dicts if x['id']==s][0]
        
        # Connect and read metadata about the station
        log.info('Retrieving station metadata for station: %s', s)
        myrequest_station = 'ids='+s
        metadata, msger = pull_request(frostcfg['endpointmeta'], myrequest_station, frostcfg, log, s=s)
        
        #PERIOD LOOP
        periods = get_periods(pars, station_dict, output['destdir']) #this is a generator giving pairs of startday and endnig day
        #print(periods)
        for p in periods:
            print(p)
            #log.info('Downloading data for period: %p', p)
            # Check that the station has data in the period requested.
            # Sometimes this will fail anyway since there is no data due to technical issues and the station is still considered active.
            if 'validTo' in metadata['data'][0].keys():
                if datetime.strptime(p[0],'%Y-%m-%d') > datetime.strptime(metadata['data'][0]['validTo'],'%Y-%m-%dT%H:%M:%S.%fZ'): 
                    log.warn('Station %s doesn\'t contain data as late as this.', s)
                    return
            if 'validFrom' in metadata['data'][0].keys():
                if datetime.strptime(p[1],'%Y-%m-%d') < datetime.strptime(metadata['data'][0]['validFrom'],'%Y-%m-%dT%H:%M:%S.%fZ'):
                    log.warn('Station %s doesn\'t contain data as early as this.', s)
                    return
        
            # Get a list of elements & Connect and read metadata about the variables
            mm = ''.join(['Retrieving variables metadata for station: ', s])
            myrequest_vars = 'sources='+s+'&referencetime='+'/'.join(p)
            variables = get_vars(myrequest_vars, frostcfg, log, mm)
            if 'data' in variables.keys():
                elements = list(set([i['elementId'] for i in variables['data']]))
            else:
                continue
            
            #ELEMENTS LOOP
            dir_elements_resol = {}
            for e in elements:
                if simple and any(a in e for a in avoid_var):
                    continue
                times = [j['timeResolution'] for j in variables['data'] if j['elementId']==e]
                max_resol = resols[min([resols.index(t) for t in times])]
                perfs = [j['performanceCategory'] for j in variables['data'] if (j['elementId']==e and j['timeResolution']==max_resol)]
                max_perf = performances[min([performances.index(pf) for pf in perfs])]
                var_dict = [x for x in variables['data'] if x['elementId']==e][0]
                dir_elements_resol[e] = (max_resol, max_perf, var_dict)

            #TIME RESOLUTIONS LOOP
            counter = True
            for t in resols:
                vars_to_down = [x for x in dir_elements_resol if dir_elements_resol[x][0]==t]
                if not vars_to_down:
                    if t == resols[0]:
                        dates = pd.date_range(p[0], p[1], freq='min')
                        dates = dates.drop(dates[-1])
                        all_ds_station = xr.Dataset(coords={'time':(['time'], dates)})
                        counter = False
                    continue
            
                # Create request for observations
                log.info('Retrieving data for station: %s', s)
                myrequest_data = ('sources='+s+'&elements='
                    +', '.join(vars_to_down)
                    +'&fields='+','.join(frostcfg['fields'])
                    +'&referencetime='+'/'.join([p[0],p[1]])+'&timeresolutions='+t)
                # Connect and read observations
                data, msg_err = pull_request(frostcfg['endpointobs'], 
                             myrequest_data, frostcfg, mylog, s=s, data=True)
                if msg_err:
                    continue

                # Read into Pandas DataFrame
                df = pd.read_csv(StringIO(data.text),header=0,
                    mangle_dupe_cols=True, parse_dates=['referenceTime'],
                    index_col=False,na_values=['-'])
                
                datasetstart = min(df['referenceTime']).strftime('%Y-%m-%dT%H:%M:%SZ')
                datasetend = max(df['referenceTime']).strftime('%Y-%m-%dT%H:%M:%SZ')

                df.rename(columns={'referenceTime':'time'}, inplace = True)
                time_val = df['time'].apply(lambda x: x.replace(tzinfo=None))
     
                if est=='permafrost':
                    perma = 'soil_temperature'
                    cols = df.columns
                    soil_num = [df.columns.get_loc(x) for x in cols if perma in x]
                    depth_num = [x+1 for x in soil_num]
                    ntime = 0
                    mytimes = []
                    myprofiles = list()
                    mydepths = list()
                    quality_check = []
                    for i in df.loc[:,'time']:
                        mydata = {
                                'depth':df.iloc[ntime, depth_num].values,
                                perma:df.iloc[ntime, soil_num].values
                                }
                        mytmpdata = pd.DataFrame(mydata).sort_values(by='depth')
                        if mytmpdata[perma].isnull().all():
                            quality_check.append(False)
                        else:
                            quality_check.append(True)
                            mydepths = mytmpdata.depth
                        mytmpdata.fillna(value=myfillvalue, inplace = True)
                        myprofiles.append(mytmpdata.soil_temperature)
                        mytimes.append(i)
                        ntime += 1
                    if True in quality_check:   
                        da_profile = xr.DataArray(myprofiles, 
                                dims=['time','depth'],
                                coords={
                                    'time':mytimes,
                                    'depth':mydepths})
                    
                    df.drop(df.columns[depth_num+soil_num],axis=1,inplace=True)
                    if perma in vars_to_down:
                        vars_to_down.remove(perma)

                df = df.set_index('time')
                cols = df.columns
                for c in cols:
                    if c.find('(') > 0 and c.find('(')==c.rfind('('):
                        df.rename(columns={c:c[:c.find('(')]}, inplace=True)
                    elif c.find('(') > 0 and c.find('(') < c.rfind('('):
                        df.rename(columns={c:c[:c.rfind('(')]}, inplace=True)

                included =[]
                excluded =[]
                for el in vars_to_down:
                    for c in cols:
                        if el in c:
                            included.append(el)
                            break
                    else:
                        excluded.append(el)
                if not included:
                    continue
                df = df[included].copy()                
                if excluded:
                    for ex in excluded:
                        df.loc[:,ex] = [myfillvalue]*len(df.index)
                
                # Create Dataset from Dataframe
                ds_station = xr.Dataset.from_dataframe(df)
                ds_station = ds_station.assign_coords(time=time_val.values)
                if est=='permafrost' and 'da_profile' in locals():
                    ds_station = ds_station.assign_coords(depth=da_profile.depth.values)
                    ds_station['depth'].attrs['standard_name'] = 'depth'
                    ds_station['depth'].attrs['long_name'] = 'depth below surface'
                    ds_station['depth'].attrs['units'] = 'cm'

                    ds_station[perma] = (('time', 'depth'), da_profile.values)
                    ds_station[perma].attrs['long_name'] = perma.replace('_', ' ')
                    ds_station[perma].attrs['standard_name'] = perma
                    ds_station[perma].attrs['units'] = 'degC'
                    ds_station[perma].attrs['performance_category'] = get_performance_category(dir_elements_resol[perma][1])
                    ds_station[perma].attrs['fillvalue'] = float(myfillvalue)
                    ds_station[perma].attrs['keywords'] = get_keywords_from_json(perma, output['json_path'])
                    del da_profile
                
                # Specify variable attributes
                ds_station['time'].attrs['standard_name'] = 'time'
                ds_station['time'].attrs['long_name'] = 'time'
                ds_station['time'].encoding['units'] = 'minutes since '+p[0]
                check_list = []
                for vname in vars_to_down:
                    if vname in check_list:
                        continue
                    else:
                        ds_station.assign()
                        try:
                            val_unit = str(dir_elements_resol[vname][2]['level']['value']) + ' ' + str(dir_elements_resol[vname][2]['level']['unit'])
                            ds_station[vname].attrs['long_name'] = vname.replace('_',' ') + ' ' + val_unit
                        except KeyError:
                            ds_station[vname].attrs['long_name'] = vname.replace('_', ' ')
                        ds_station[vname].attrs['standard_name'] = vname
                        ds_station[vname].attrs['units'] = dir_elements_resol[vname][2]['unit']
                        ds_station[vname].attrs['performance_category'] = get_performance_category(dir_elements_resol[vname][1])
                        ds_station[vname].attrs['fillvalue'] = float(myfillvalue)
                        ds_station[vname].attrs['keywords'] = get_keywords_from_json(vname, output['json_path'])
                        check_list.append(vname)
                 
                #Aggregate datasets with same dimension, but different dimension length
                if counter:
                    all_ds_station = ds_station
                    counter = False
                else:
                    all_ds_station = xr.merge([all_ds_station, ds_station], 
                                              fill_value = myfillvalue, join='left',
                                              combine_attrs ='no_conflicts')

            else:
                if msger:
                    continue
                else:
                    pass
                
            # Add global attributes
            if est == 'moving' and 'latitude' in all_ds_station.data_vars:
                all_ds_station.attrs['title'] = 'Weather station information from ship '+s
                lats = np.array(all_ds_station.data_vars['latitude'].values).flatten().astype('float')
                lons = np.array(all_ds_station.data_vars['longitude'].values).flatten().astype('float')
                lats[lats<-900] = np.nan
                lons[lons<-900] = np.nan
                all_ds_station.attrs['geospatial_lat_min'] = np.nanmin(lats)
                all_ds_station.attrs['geospatial_lat_max'] = np.nanmax(lats)
                all_ds_station.attrs['geospatial_lon_min'] = np.nanmin(lons)
                all_ds_station.attrs['geospatial_lon_max'] = np.nanmax(lons)
            elif est == 'moving':
                all_ds_station.attrs['title'] = 'Weather station information from ship '+s
            else:
                all_ds_station.attrs['title'] = 'Weather station '+s
                all_ds_station.attrs['geospatial_lat_min'] = station_dict['geometry']['coordinates'][1]
                all_ds_station.attrs['geospatial_lat_max'] = station_dict['geometry']['coordinates'][1]
                all_ds_station.attrs['geospatial_lon_min'] = station_dict['geometry']['coordinates'][0]
                all_ds_station.attrs['geospatial_lon_max'] = station_dict['geometry']['coordinates'][0]
            
            all_ds_station.attrs['featureType'] = 'timeSeries'
            all_ds_station.attrs['summary'] = stmd['abstract']
            all_ds_station.attrs['license'] = metadata['license']
            all_ds_station.attrs['time_coverage_start'] = datasetstart
            all_ds_station.attrs['time_coverage_end'] = datasetend
            all_ds_station.attrs['creator_name'] = stmd['PrincipalInvestigator'] 
            all_ds_station.attrs['creator_email'] = stmd['PrincipalInvestigatorEmail']
            all_ds_station.attrs['creator_url'] = stmd['PrincipalInvestigatorOrganisationURL']
            all_ds_station.attrs['creator_institution'] = stmd['PrincipalInvestigatorOrganisation']
            all_ds_station.attrs['keywords_vocabulary'] = 'GCMD'
            all_ds_station.attrs['publisher_name'] = ''
            all_ds_station.attrs['publisher_email'] = 'adc@met.no'
            all_ds_station.attrs['publisher_url'] = 'https://adc.met.no/'
            all_ds_station.attrs['publisher_institution'] = 'Norwegian Meteorlogical Institute'
            all_ds_station.attrs['Conventions'] = 'ACDD, CF-1.8'
            all_ds_station.attrs['date_created'] = metadata['createdAt']
            all_ds_station.attrs['history'] = metadata['createdAt']+': Data extracted from the MET Observation Database through Frost and stored as NetCDF-CF'
            all_ds_station.attrs['wigosId'] = station_dict['wigosId']
            all_ds_station.attrs['METNOId'] =  s
            all_ds_station.attrs['project'] = stmd['Project']
        
            # Dump to Netcdf
            out_folder = os.path.join(output['destdir'], s, str(datetime.strptime(p[0],'%Y-%m-%d').year))
            outputfile = os.path.join(out_folder, s+'_'+datasetstart+'_'+datasetend+'.nc')
            if os.path.exists(out_folder):
                pass
            else:
                os.mkdir(out_folder)
            #all_ds_station = xr.decode_cf(all_ds_station)
            # The above line release an error: TypeError: The DTypes <class 'numpy.dtype[int64]'> and <class 'numpy.dtype[datetime64]'> do not have a common DType. For example they cannot be stored in a single array unless the dtype is `object`.
            try:
                if all_ds_station.data_vars:    
                    all_ds_station.to_netcdf(outputfile,
                                     encoding=set_encoding(all_ds_station, fill=myfillvalue))
                else:
                    continue
            except TypeError:
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
    if args.type_station:
        extractdata(cfgstr['frostcfg'], args, mylog, cfgstr['attributes'], output_dir, est=args.type_station)
    else:
        extractdata(cfgstr['frostcfg'], args, mylog, cfgstr['attributes'], output_dir)

