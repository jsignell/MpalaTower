# -*- coding: utf-8 -*-
# Convert Flux Tower Time Series data CSV file to NetCDF CF-1.6 timeSeries file


from netcdfSetup import * # this provides a handy spot to keep track of DIR names
import numpy as np
import pandas as pd
import netCDF4
import re
import os
import shutil
import urllib
import xray
import zipfile
import datetime as dt

ROOTDIR = "C:/Users/Julia/Dropbox (PE)/netcdfDevelopment/netcdf/"
#DATALOC = "C:/Users/Julia/Dropbox (PE)/KenyaLab/Data/Tower/TowerData/"
DATALOC = "C:/Users/Julia/Dropbox (PE)/KenyaLab/Data/Tower/TowerDataArchive/test/"
TSLOC = DATALOC +'CR3000_SN4709_ts_data/'
NETCDFLOC = "C:/Users/Julia/Dropbox (PE)/netcdfDevelopment/netcdf/output/"

station_name = 'MPALA Tower'
lon = 36.9   # degrees east
lat = 0.5    # degrersityes north
timezone = +0300 #Africa/Nairobi
    
# add highly recommended metadata from:
# http://wiki.esipfed.org/index.php/Attribute_Convention_for_Data_Discovery_1-3#Highly_Recommended
title = 'Flux Tower Data from MPALA'
summary = 'This raw data comes from the MPALA Flux Tower, which is maintained by the Ecohydrology Lab at Mpala Research Centre in Laikipia, Kenya. It is part of a long-term monitoring project that was originally funded by NSF and now runs with support from Princeton. Its focus is on using stable isotopes to better understand water balance in drylands, particularly transpiration and evaporation fluxes.'
license = 'MIT License'   # see choosealicense.com.   You can also make up your own and just say what the license is
institution = 'Princeton University'
acknowledgement = 'Funded by NSF and Princeton University'
naming_authority= 'caylor.princeton.edu'
dset_id = station_name
creator_name = 'Kelly Caylor'
creator_email = 'kcaylor@princeton.edu'
keywords = ['eddy covariance','isotope hydrology', 'land surface flux']

# metadata needed to be CF-1.6 compliant
def createDF(input_file):  
    df = pd.read_csv(input_file,skiprows=[0,2,3],parse_dates=True,index_col=0,low_memory=False)
    
    # group dataframe by day of the year
    DFList = []
    for group in df.groupby([df.index.year,df.index.month,df.index.day]):
        DFList.append(group[1])
    return df, DFList

#create the output filenames
def createmeta(df,DFList,input_file):
    ncfilename = []
    for i in range(len(DFList)):
        doy = DFList[i].index[0].dayofyear
        y = DFList[i].index[0].year
        ncfilename.append('raw_MpalaTower_%i_%03d.nc' %(y,doy))
    
    # in this CSV file, some metadata were written in the 1st row
    df_meta = pd.read_csv(input_file,nrows=1)
    meta=list(df_meta.columns.values)
    
    # use the 2nd item in list as instrument
    instrument = meta[1]
    source = 'Flux tower sensor data %s_%s.dat, %s, %s' % (meta[1],meta[7],meta[5],meta[6])
    
    # in this CSV file, the units were written in the 3rd row
    df_units = pd.read_csv(input_file,skiprows=[0,1],nrows=1)
    unit_names = list(df_units.columns.values)
    
    # in this CSV file, the measurement types were written in the 4th row
    df_types = pd.read_csv(input_file,skiprows=[0,1,2],nrows=1)
    type_names = list(df_types.columns.values)
    
    return ncfilename,meta,instrument,source,unit_names,type_names

def pd_to_secs(df):
    """
    convert a pandas datetime index to "seconds since 1970-01-01 00:00"
    """
    import calendar
    return np.asarray([ calendar.timegm(x.timetuple()) for x in df.index ], dtype=np.int64)

def determineHeight(meta):
    if meta[7] == 'upper':
        height = 24
    else: 
        height = 0
    return height
    
def createNC(NETCDFLOC,DFList,ncfilename,title,summary,license,institution,acknowledgement,naming_authority,
            dset_id,creator_name,creator_email,meta,instrument,source,station_name,unit_names,type_names,lon,lat,
            height):
    i=0
    for df_day in DFList:
        if ncfilename[i] in NETCDFLOC+meta[7]+'/': 
            print 'check out %s' %ncfilename[i]
            continue
        else: pass
        output_file = NETCDFLOC+meta[7]+'/'+ ncfilename[i]
        print 'trying to write to %s' %output_file
        i+=1
        ntime,ncols = np.shape(df_day)
        #create NetCDF file
        nc = netCDF4.Dataset(output_file,mode='w',clobber=True)
        # add some global attributes
        nc.Conventions='CF-1.6'
        nc.featureType='timeSeries'
        nc.title=title
        nc.summary=summary
        nc.license=license
        nc.institution=institution
        nc.acknowledgement = acknowledgement
        nc.naming_authority = naming_authority
        nc.id = dset_id 
        nc.creator_name = creator_name
        nc.creator_email = creator_email
        nc.instrument = instrument
        nc.source = source
        #create time dimension
        nc.createDimension('time',ntime)    # create fixed time dimension
        #nc.createDimension('time',None)     # create unlimited time dimension (for appending more data later)
        # create dimension for station_name 
        nchar_max=len(station_name)
        nc.createDimension('name_strlen',nchar_max);
        #Create time,lon,lat,altitude variables
        tvar = nc.createVariable('time','f8',dimensions=('time'))
        tvar.units='seconds since 1970-01-01 00:00 +03:00'
        tvar.standard_name='time'
        tvar.calendar='gregorian'
        #create timeseries_id variable
        stavar = nc.createVariable('station_name','S1',dimensions=('name_strlen'))
        stavar.long_name = 'station name'
        stavar.cf_role='timeseries_id'
        lonvar = nc.createVariable('lon','f4')
        lonvar.units = 'degrees east'
        lonvar.standard_name = 'longitude'
        latvar = nc.createVariable('lat','f4')
        latvar.units = 'degrees north'
        latvar.standard_name = 'latitude'
        evar = nc.createVariable('elevation','f4')
        evar.units = 'm'
        evar.standard_name = 'height'
        evar.positive = 'up'
        evar.axis='Z'
        #create variables from CSV file 
        var_names = list(df_day.columns.values)
        var=[]
        # the first 10 columns have time values, not data, so start in 11th column [10]
        startcol=1
        for var_name in var_names[startcol:]:
            var_name = re.sub('[)()]', '_', var_name)    # netcdf doesn't like var names like "temp(17)", but "temp_17_" is okay.
            var.append(nc.createVariable(var_name,'f4',dimensions=('time'),zlib=True, complevel=4))
        #add attributes to variables from CSV file
        k=startcol
        for v in var:
            v.units = (re.sub('[.]',':',unit_names[k])).split(':')[0]     # units like "deg C.17" or "deg C:17" should just be "deg C"
            v.comment = (re.sub('[.]',':',type_names[k])).split(':')[0]
            v.coordinates = 'time lon lat elevation'
            v.content_coverage_type= 'physicalMeasurement'
            k+=1
        # write lon,lat, elevation and station id
        lonvar[0]=lon
        latvar[0]=lat
        evar[0]=height
        stavar[:]=netCDF4.stringtoarr(station_name,nchar_max)
        #write time values
        tvar[:]=pd_to_secs(df_day)
        #Write data from CSV to NetCDF
        k=startcol
        for v in var:
            v[:]=df_day.iloc[:,k].values
            k+=1
        nc.close()
        print '%s finished processing' %ncfilename[i-1]

def checkmerge(NETCDFLOC):
    l = []
    list_elements = []
    to_merge = []
    for i in os.walk(NETCDFLOC):
        l.append(i)
    for folder in l:
        for elements in folder[2]: 
            list_elements.append(elements)
    for element in set(list_elements):
        if list_elements.count(element)>=2:
            to_merge.append(element)
            print element, 'occurs in', list_elements.count(element), 'out of 8 possible'
    return to_merge
    
def createSummaryTable(NETCDFLOC):
    l = []
    for i in os.walk(NETCDFLOC):
        l.append(i)
    columns = l[0][1]
    todays_date = dt.datetime.now().date()
    first_date= dt.datetime(2010,01,01).date()
    index = pd.date_range(start =first_date, end = todays_date, freq='D',tz = 'Africa/Nairobi')
    df_ = pd.DataFrame(index=index, columns=columns)
    df_ = df_.fillna(' ') # with 0s rather than NaNs
    for day in range(len(df_.index)):
        doy = df_.index[day].timetuple().tm_yday
        y = df_.index[day].year
        filename = 'raw_MpalaTower_%d_%03d.nc'%(y,doy)
        for folder in l:
            if filename in folder[2]:
                column = folder[0].partition('output/')[2]
                df_[column][df_.index[day]]=True
    df_.to_csv(NETCDFLOC+'SummaryTable.csv')
    print 'Updated the Summary Table!'

def tsmain():
    df_header = pd.read_csv(TSLOC+'header.txt',skiprows=[0],nrows=1)
    header_names = list(df_header.columns.values)
    for day in os.listdir(TSLOC):
        archive = zipfile.ZipFile(TSLOC+day, 'r')
        input_file = archive.extract('share/'+ day.partition('.zip')[0], NETCDFLOC)
        print '%s successfully unzipped!'%input_file
        df = pd.read_csv(input_file,header = None, names= header_names, parse_dates=True,index_col=0,low_memory=False)
        ncfilename,meta,instrument,source,unit_names,type_names = createmeta(df,[df],TSLOC+'header.txt')
        createNC(NETCDFLOC, [df],ncfilename,title,summary,license,institution,acknowledgement,naming_authority,
            dset_id,creator_name,creator_email,meta,instrument,source,station_name,unit_names,type_names,lon,lat,
            height)
        os.remove(input_file)
    createSummaryTable(NETCDFLOC)
    
def fluxmain():
    DATAFILERE = re.compile('^CR\d{4}_SN\d+_(.*?)\.dat$')
    for f in os.listdir(DATALOC):
        if DATAFILERE.match(f):pass
        else:continue
        input_file = DATALOC+f
        df, DFList = createDF(input_file)
        ncfilename,meta,instrument,source,unit_names,type_names = createmeta(df,DFList,input_file)
        height = determineHeight(meta)
        createNC(NETCDFLOC, DFList,ncfilename,title,summary,license,institution,acknowledgement,naming_authority,
            dset_id,creator_name,creator_email,meta,instrument,source,station_name,unit_names,type_names,lon,lat,
            height)
        shutil.move(DATALOC+f, DATALOC.partition('test/')[0]+'processed/'+f)   
    createSummaryTable(NETCDFLOC)
            
if __name__ =='__main__':
    #tsmain()
    fluxmain()
    
