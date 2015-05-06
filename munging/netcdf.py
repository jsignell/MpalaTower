# -*- coding: utf-8 -*-
"""
Created on Tue May 05 09:10:48 2015

@author: Julia
"""
import pandas as pd
import os
import posixpath
import numpy as np
import xray
import netCDF4

input_dir = 'C:/Users/Julia/Documents/GitHub/MpalaTower/munging/current_data/'
datafiles = ['lws', 'upper', 'flux']
output_dir = 'C:/Users/Julia/Documents/GitHub/MpalaTower/inspection/raw_netCDF_output/'

attrs = {}

coords  = dict(
    station_name='MPALA Tower',
    lon=36.9,         # degrees east
    lat=0.5,          # degrees north
    elevation=1610    # m above sea level
    )


def get_files(input_dir, datafiles):
    # takes a data dir and a list of datafiles to look for
    # gives a list of dicts containing dir, filename, datafile
    input_dicts = []
    files = os.listdir(input_dir)
    for f in files:
        for d in datafiles:
            if d in f:
                input_dicts.append({'dir': input_dir,
                                    'filename': f,
                                    'datafile': d})
    return input_dicts


def manage_dtypes(x):
    if x.values.dtype == np.dtype('int64') or np.dtype('float64'):
        return x
    else:
        try:
            return x.astype('float64')
        except:
            return x


def createDF(input_file, old=True, header_file=None):
    # takes a path to a file and a header file if applicable
    # gives a list of daily dataframes in UTC
    kw = dict(parse_dates=True, index_col=0, iterator=True,
              chunksize=100000, low_memory=False)
    if header_file is None:
        df_ = pd.read_csv(input_file, skiprows=[0, 2, 3], **kw)
    else:
        df_header = pd.read_csv(header_file, skiprows=[0], nrows=1)
        header = list(df_header.columns.values)
        df_ = pd.read_csv(input_file, header=None, names=header, **kw)
    df = pd.concat(df_)
    df.index.name = 'time'
    df = df.apply(manage_dtypes)               # try to make all fields numeric
    df = df.tz_localize('Africa/Nairobi')      # explain the local timezone
    df = df.tz_convert('UTC')                  # convert df to UTC

    # group dataframe by day of the year in UTC
    DFList = []
    for group in df.groupby([df.index.year, df.index.month, df.index.day]):
        DFList.append(group[1])
    if old is False:
        DFList = DFList[0:-1]  # for current data discard last partial day
    return DFList


def get_ncnames(DFList):
    # takes a list of daily dataframes in UTC
    # gives a list of corresponding netcdf filenames
    ncfilenames = []
    for i in range(len(DFList)):
        doy = DFList[i].index[0].dayofyear
        y = DFList[i].index[0].year
        # create the output filenames
        ncfilenames.append('raw_MpalaTower_%i_%03d.nc' % (y, doy))
    return ncfilenames


def get_attrs(DFList, ncfilenames, attrs, header_file):
    # metadata needed to be CF-1.6 compliant
    # in the .dat datafiles, some metadata are written in the 1st row
    df_meta = pd.read_csv(header_file, nrows=1)
    meta_list = list(df_meta.columns.values)

    # use the 2nd item in list as instrument
    attrs.update({'format': meta_list[0],
                  'logger': meta_list[1],
                  'program': meta_list[5].split(':')[1],
                  'datafile': meta_list[7]})
    source_info = (attrs['logger'], attrs['datafile'], attrs['program'],
                   meta_list[6])
    source = 'Flux tower sensor data %s_%s.dat, %s, %s' % source_info
    attrs.update({'source': source})

    df_names = pd.read_csv(input_file, skiprows=[0], nrows=2)
    df_names.index=('units','')
    local_attrs = df_names.to_dict()

    return attrs, local_attrs


def get_coords(ds):
    ds.coords['lon'].attrs = dict(units='degrees east',
                                  standard_name='longitude',
                                  axis='X')
    ds.coords['lat'].attrs = dict(units='degrees north',
                                  standard_name='latitude',
                                  axis='Y')
    ds.coords['elevation'].attrs = dict(units='m',
                                        standard_name='elevation',
                                        positive='up',
                                        axis='Z')
    return ds


def createDS(df, ncfilename, attrs, coords, local_attrs):
    ds = xray.Dataset(attrs=attrs, coords=coords)
    ds.update(xray.Dataset.from_dataframe(df))
    ds = get_coords(ds)
    for name in ds.data_vars.keys():
        ds[name].attrs = local_attrs[name]
    ds.to_netcdf(path=output_dir+ncfilename, mode='w', format='NETCDF3_64BIT')

    return ds


input_dicts = get_files(input_dir, datafiles)
for input_dict in input_dicts:
    input_file = posixpath.join(input_dict['dir'], input_dict['filename'])
    DFList = createDF(input_file)
    ncfilenames = get_ncnames(DFList)
    attrs, local_attrs = get_attrs(DFList, ncfilenames, attrs, input_file)
    for i in range(len(DFList)):
        df = DFList[i]        
        ncfilename = ncfilenames[i]
        ds = createDS(df, ncfilename, attrs, local_attrs)