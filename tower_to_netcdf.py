#Script:tower_to_netcdf.py
#       Converts tower timeseries data .dat file to daily raw NetCDF CF-1.6 
#       timeSeries file. Then zips or copies the data to store on dropbox in 
#       NETCDFPUB. The files have the format raw_MpalaTower_YYYY_MM_DD.nc
#       and are stored according to their data file names. 
#Modified by: Julia Signell
#Date created: 2014-11-10
#Date modified: 2015-04-07

import numpy as np
import pandas as pd
import netCDF4
import re
import os
import shutil
import datetime as dt
import zipfile
import matplotlib.pyplot as plt

usr = 'Julia'
ROOTDIR = 'C:/Users/%s/Dropbox (PE)/KenyaLab/Data/Tower/'%usr
E = 'E:/'
ARCHIVEDIR = E + 'TowerDataArchive/BigFiles/'
DATALOC = ROOTDIR + 'TowerData/'
TSLOC = DATALOC +'CR3000_SN4709_ts_data/'
SPLITLOC = 'C:/cygwin/home/Julia/BigFiles/'
NETCDFLOC = DATALOC+'raw_netCDF_output/'
NETCDFPUB = DATALOC+'raw_netCDF_output/'

station_name = 'MPALA Tower'
lon = 36.9   # degrees east
lat = 0.5    # degrees north
timezone = +0300 #Africa/Nairobi
    
# add highly recommended metadata from:
# http://wiki.esipfed.org/index.php/Attribute_Convention_for_Data_Discovery_1-3
    
title = 'Flux Tower Data from MPALA'
summary = ('This raw data comes from the MPALA Flux Tower, which is '+
           'maintained by the Ecohydrology Lab at Mpala Research Centre in '+
           'Laikipia, Kenya. It is part of a long-term monitoring project '+
           'that was originally funded by NSF and now runs with support '+
           'from Princeton. Its focus is on using stable isotopes to better '+
           'understand water balance in drylands, particularly '+
           'transpiration and evaporation fluxes.')
license = 'MIT License'   
institution = 'Princeton University'
acknowledgement = 'Funded by NSF and Princeton University'
naming_authority= 'caylor.princeton.edu'
dset_id = station_name
creator_name = 'Kelly Caylor'
creator_email = 'kcaylor@princeton.edu'
keywords = ['eddy covariance','isotope hydrology', 'land surface flux']

def createDF(input_file,header=None):  
    if header == None:
        df_ = pd.read_csv(input_file, skiprows=[0,2,3], parse_dates=True, 
                          index_col=0, iterator=True, 
                          chunksize=100000, low_memory=False)
    else:
        df_ = pd.read_csv(input_file, header = None, parse_dates=True, 
                          index_col=0, iterator=True, names =header,
                          chunksize=100000, low_memory=False)
    df = pd.concat(df_)
    df = df.tz_localize('Africa/Nairobi')      #explain the local timezone
    df = df.tz_convert('UTC')                  #convert df to UTC
    
    # group dataframe by day of the year in UTC
    DFList = []
    for group in df.groupby([df.index.year,df.index.month,df.index.day]):
        DFList.append(group[1])
    DFList = DFList[0:-1] #cut off last day as it is likely partial 
    return DFList

def createmeta(DFList,input_file): # metadata needed to be CF-1.6 compliant
    ncfilename = []
    for i in range(len(DFList)):
        doy = DFList[i].index[0].dayofyear
        y = DFList[i].index[0].year
        ncfilename.append('raw_MpalaTower_%i_%03d.nc' %(y,doy)) 
        #create the output filenames
    
    # in the .dat datafiles, some metadata are written in the 1st row
    df_meta = pd.read_csv(input_file,nrows=1)
    meta=list(df_meta.columns.values)
    
    # use the 2nd item in list as instrument
    instrument = meta[1]
    source_info = (meta[1],meta[7],meta[5],meta[6])
    source = 'Flux tower sensor data %s_%s.dat, %s, %s'%source_info
    
    # in the .dat datafiles, the units are written in the 3rd row
    df_units = pd.read_csv(input_file,skiprows=[0,1],nrows=1)
    unit_names = list(df_units.columns.values)
    
    # in the .dat datafiles, the measurement types are written in the 4th row
    df_types = pd.read_csv(input_file,skiprows=[0,1,2],nrows=1)
    type_names = list(df_types.columns.values)
    return ncfilename,meta,instrument,source,unit_names,type_names

def pd_to_secs(df):
    #convert a pandas datetime index to "seconds since 1970-01-01 00:00"
    import calendar
    return np.asarray([calendar.timegm(x.timetuple()) for x in df.index],
                       dtype=np.int64)

def determineHeight(meta):
    if meta[7] == 'upper':
        height = 24
    else: 
        height = 0
    return height
    
def createNC(NETCDFLOC,DFList,ncfilename,input_file,title,summary,license,
             institution,acknowledgement,naming_authority,dset_id,creator_name,
             creator_email,meta,instrument,source,station_name,unit_names,
             type_names,lon,lat,height,old):
    #this is the powerhouse function where the data in DFList moves to netCDF    
    i = -1
    for df_day in DFList:
        i += 1
        if ncfilename[i] in os.listdir(NETCDFLOC+meta[7]+'/'): 
            if old == True:
                overlap = open(NETCDFLOC+'overlap_days.txt','a') 
                overlap.write('check out %s --> %s\n'%(input_file,ncfilename[i]))
                overlap.close()
            continue
        output_file = NETCDFLOC+meta[7]+'/'+ncfilename[i]
        print('trying to write to %s'%output_file)
        ntime,ncols = np.shape(df_day)
        
        #create NetCDF file
        nc = netCDF4.Dataset(output_file,mode='w',clobber=True)
        
        # add some global attributes
        nc.Conventions = 'CF-1.6'
        nc.featureType = 'timeSeries'
        nc.title = title
        nc.summary = summary
        nc.license = license
        nc.institution = institution
        nc.acknowledgement = acknowledgement
        nc.naming_authority = naming_authority
        nc.id = dset_id 
        nc.creator_name = creator_name
        nc.creator_email = creator_email
        nc.instrument = instrument
        nc.source = source
        
        #create time dimension
        nc.createDimension('time',ntime)    # create fixed time dimension
        
        # create dimension for station_name 
        nchar_max=len(station_name)
        nc.createDimension('name_strlen',nchar_max);
        
        #Create time,lon,lat,altitude variables
        tvar = nc.createVariable('time','f8',dimensions=('time'))
        tvar.units='seconds since 1970-01-01 00:00'
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
        evar.axis = 'Z'
        
        #create variables from CSV file 
        var_names = list(df_day.columns.values)
        var = []
        startcol = 1
        for var_name in var_names[startcol:]:
            var_name = re.sub('[)()]','_',var_name)    
            # netcdf doesn't like var names like "temp(17)", but "temp_17_" is okay.
            var.append(nc.createVariable(var_name,'f4',dimensions=('time'),
                                         zlib=True,complevel=4))
        
        #add attributes to variables from datafile
        k=startcol
        for v in var:
            v.units = (re.sub('[.]',':',unit_names[k])).split(':')[0]     
            # units like "deg C.17" or "deg C:17" should just be "deg C"
            v.comment = (re.sub('[.]',':',type_names[k])).split(':')[0]
            v.coordinates = 'time lon lat elevation'
            v.content_coverage_type = 'physicalMeasurement'
            k+=1
        # write lon,lat, elevation and station id
        lonvar[0] = lon
        latvar[0] = lat
        evar[0] = height
        stavar[:] = netCDF4.stringtoarr(station_name,nchar_max)
        #write time values
        tvar[:] = pd_to_secs(df_day)
        #Write data from datafile to NetCDF
        k = startcol
        for v in var:
            v[:] = df_day.iloc[:,k].values
            k += 1
        nc.close()
        print '%s finished processing'%ncfilename[i-1]

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
            print (element,'occurs in',list_elements.count(element),
                   'out of 8 possible')
    return to_merge
    
def createSummaryTable(NETCDFLOC):
    l = []
    for i in os.walk(NETCDFLOC):
        l.append(i)
    columns = ['fileName']+l[0][1]
    todays_date = dt.datetime.utcnow().date()
    first_date = dt.datetime(2010,01,01).date()
    index = pd.date_range(start=first_date,end=todays_date,
                          name='date_time_UTC',freq='D')
    df_ = pd.DataFrame(index=index,columns=columns)
    for day in range(len(df_.index)):
        doy = df_.index[day].timetuple().tm_yday
        y = df_.index[day].year
        filename = 'raw_MpalaTower_%d_%03d.nc'%(y,doy)
        df_['fileName'][df_.index[day]]= filename
        for folder in l:
            if filename in folder[2]:
                column = folder[0].partition('output/')[2]
                statinfo = os.stat(os.path.join(folder[0],filename))
                file_size = statinfo.st_size/1024 
                #so we can tell when a file is partial
                df_[column][df_.index[day]]=file_size
    df_.to_csv(NETCDFLOC+'DatafileAvailability.csv')
    
    #make a figure so that we can visually see data gaps
    ax = df_.plot(logy=True,figsize=(16,8),linewidth=2.5,colormap='rainbow',
                  title='Summary of Datafile Availability and Filesize')
    ax.set_ylabel('Filesize [KB]')
    plt.savefig(NETCDFLOC+'DatafileAvailability.jpg')
    print 'Updated the Data Availability docs!' 
    
def zip_and_move(input_dir,output_dir):
    for files in os.listdir(input_dir):
        if '.' in files:
            shutil.copy2(input_dir+files,output_dir+files)
    l = []
    for i in os.walk(input_dir):
        l.append(i)
    
    folders = l[0][1]
    for folder in folders:
        src = input_dir+folder
        dst = output_dir+folder
        
        # ts_data is not zipped because the compression is less than 20% 
        # and it needs zip64
        if 'ts_data' in folder: 
            for filename in os.listdir(src): 
                if filename not in os.listdir(dst):
                    shutil.copy2(src+'/'+filename,dst+'/'+filename)
        else: #pass
            shutil.make_archive(dst,'zip',src) 
            #this takes a while to run, but dropbox is smart enough to 
            #know that it is an edit, so the upload time is minimal
        print 'done zipping or copying %s'%folder

def process(DIR,input_file,DFList,f,header=None,old=False):
    #runs all the easy parts of the functions
    if header==None: 
        header = input_file    
    meta_tup = createmeta(DFList,header)
    ncfilename,meta,instrument,source,unit_names,type_names = meta_tup
    height = determineHeight(meta)
    createNC(NETCDFLOC,DFList,ncfilename,input_file,title,summary,license,
             institution,acknowledgement,naming_authority,dset_id,creator_name,
             creator_email,meta,instrument,source,station_name,unit_names,
             type_names,lon,lat,height,old=old)
    processed = open(DIR+'processed2netCDF.txt','a')
    processed.write(f+'\n')
    processed.close
    
    
def tsmain(TSLOC,NETCDFLOC,old=False):
    print 'running tsmain with TSLOC = %s' %TSLOC
    df_header = pd.read_csv(TSLOC+'header.txt',skiprows=[0],nrows=1)
    header_names = list(df_header.columns.values)
    for f in os.listdir(TSLOC):
        if '.dat' in f: pass
        else: continue
        if f not in open(TSLOC+'processed2netCDF.txt','r').read():
            if 'zip' in f:
                archive = zipfile.ZipFile(TSLOC+f,'r')
                try:input_file = archive.extract('share/'+f.partition('.zip')[0],NETCDFLOC)
                except:
                    try: input_file = archive.extract('sn4709_ts_5_min_data/'+f.partition('.zip')[0],NETCDFLOC)
                    except: input_file = archive.extract(f.partition('.dat.zip')[0]+'_0000.dat',NETCDFLOC+'share/')
                print '%s successfully unzipped!'%input_file
            else:
                input_file = TSLOC+f
            df_meta = pd.read_csv(input_file,nrows=1)
            meta = list(df_meta.columns.values)
            if meta[7] == 'ts_data':
                DFList = createDF(input_file)
            else:
                df_ = pd.read_csv(input_file,iterator=True,skiprows=[0,1,2,3],
                                  chunksize=10000,
                                  header = None, names= header_names,
                                  parse_dates=True,index_col=0,low_memory=False)
                df = pd.concat(df_)
                df = df.tz_localize('Africa/Nairobi')      #explain the local timezone
                df = df.tz_convert('UTC')                  #convert df to UTC
                if old == True:
                    DFList = []
                    for group in df.groupby([df.index.year,df.index.month,df.index.day]):
                        DFList.append(group[1])
                else: DFList = [df]
            process(TSLOC,input_file,DFList,f,header = TSLOC+'header.txt')
            '''meta_tup = createmeta(DFList,df,TSLOC+'header.txt')
            ncfilename,meta,instrument,source,unit_names,type_names = meta_tup
            height = determineHeight(meta)
            createNC(NETCDFLOC,DFList,ncfilename,input_file,title,summary,license,
                     institution,acknowledgement,naming_authority,dset_id,creator_name,
                     creator_email,meta,instrument,source,station_name,unit_names,
                     type_names,lon,lat,height)
            processed = open(TSLOC+'processed2netCDF.txt','a')
            processed.write(day+'\n')
            processed.close'''
    
def fluxmain(DATALOC,NETCDFLOC,old=False):
    print 'running fluxmain with DATALOC = %s' %DATALOC
    DATAFILERE = re.compile('^CR\d{4}_SN\d+_(.*?)\.dat$')
    for f in os.listdir(DATALOC):
        if DATAFILERE.match(f):
            print f
            pass
        else:continue
        input_file = DATALOC+f
        DFList = createDF(input_file)
        process(DATALOC,input_file,DFList,f)
        '''meta_tup = createmeta(DFList,df,input_file)
        ncfilename,meta,instrument,source,unit_names,type_names = meta_tup
        height = determineHeight(meta)
        createNC(NETCDFLOC,DFList,ncfilename,input_file,title,summary,license,
                 institution,acknowledgement,naming_authority,dset_id,creator_name,
                 creator_email,meta,instrument,source,station_name,unit_names,
                 type_names,lon,lat,height)
        processed = open(DATALOC+'processed2netCDF.txt','a')
        processed.write(f+'\n')
        processed.close  '''

def splitmain(SPLITLOC,NETCDFLOC,old=True):
    for f in os.listdir(SPLITLOC):
        if 'split' in f:
            n = f.split('_')
            name = n[1]+'_'+n[2]+'_'+n[3]+'_'+n[4]
            num = n[5]
            pass
        else: continue
        input_file = SPLITLOC + f
        if num == '00':
            DFList = createDF(input_file)
            header_file = input_file
        else:
            header_file = SPLITLOC+'header_%s.txt'%name
            df_header = pd.read_csv(header_file,skiprows=[0],nrows=1)
            header = list(df_header.columns.values)
            DFList = createDF(input_file, header=header)
        process(DATALOC,input_file,DFList,f,header=header_file,old=True)   
                
if __name__ =='__main__':
    #tsmain(ROOTDIR+'TowerDataArchive/towerraw/ts_data/',NETCDFLOC,archive=True)  
    #tsmain(TSLOC,NETCDFLOC)
    #fluxmain(ARCHIVEDIR,NETCDFLOC,old=True)
    splitmain(SPLITLOC,NETCDFLOC)
    try: shutil.rmtree(NETCDFLOC+'share/')
    except: pass
    createSummaryTable(NETCDFLOC)
    #zip_and_move(NETCDFLOC,NETCDFPUB)
    print ('all done with the raw file production for today: %s!'%
           dt.datetime.today())

