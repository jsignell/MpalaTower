# Meta Data Dictionary
# Author: Julia Signell
# Date created: 2015-04-17
# Date modified: 2015-04-18

# This notebook contains everything you need to create a nice neat list of
# meta data dictionaries out of netcdf files. In this case we have made one
# meta data dictionary for each day in a five year span. The dictionaries are
# only created when there is data available on the given day, and there are
# up to 8 datafiles represented on each day. Each files contains data from
# various sensors and that is reported out in a whole slew of variables. Each
# variable has attributes associated with it in the netcdf file. These
# attributes are carried over into the dict and other attributes are added,
# such as a flag variable that can be raised for various problematic data
# situations (missing data, unreasonable data, ...)


#    data_dict = {title: 'Mpala Flux Tower Data',
#                 conventions: 'CF 1.6',
#                 Year: 2010,
#                 DOY: 001,
#                 Month: 01
#                 date: 2010-01-01,
#                 files: [{filename: Table1,
#                          logger: 'CR3000_SN4709',
#                          frequency: 1,
#                          variables: [{var: 'Tsoil10cm_Avg',
#                                       units : 'ft',
#                                       flags : ['data missing']}]}]}

from __future__ import print_function
import pandas as pd
import datetime as dt
import numpy as np
import os
import xray
from posixpath import join

ROOTDIR = 'C:/Users/Julia/Documents/GitHub/MpalaTower/raw_netcdf_output/'

# Setting expected ranges for units. It is ok to include multiple ways of
# writing the same unit, just put all the units in a list.
temp_min = 0
temp_max = 40
temp = ['Deg C', 'C']

percent_min = 0
percent_max = 100
percent = ['percent', '%']

shf_min = ''
shf_max = ''
shf = ['W/m^2']

shf_cal_min = ''
shf_cal_max = ''
shf_cal = ['W/(m^2 mV)']

batt_min = 11
batt_max = 240
batt = ['Volts', 'V']

PA_min = ''
PA_max = ''
PA = ['uSec']

datas = ['upper', 'Table1', 'lws', 'licor6262', 'WVIA',
         'Manifold', 'flux', 'ts_data', 'Table1Rain']
non_static_attrs = ['instrument', 'source', 'program', 'logger']
static_attrs = ['station_name', 'lat', 'lon', 'elevation',
                'Year', 'Month', 'DOM', 'Minute', 'Hour',
                'Day_of_Year', 'Second', 'uSecond', 'WeekDay']


def process_netcdf(input_dir, data, f, static_attrs):
    ds = xray.Dataset()
    ds = xray.open_dataset(join(input_dir, data, f),
                           decode_cf=True, decode_times=True)
    df = ds.to_dataframe()

    # drop from df, columns that don't change with time
    exclude = [var for var in static_attrs if var in df.columns]
    df_var = df.drop(exclude, axis=1)  # dropping vars like lat, lon
    df_clean = df_var.dropna(axis=1, how='all')  # dropping NAN vars

    # get some descriptive statistics on each of the variables
    df_int = df_clean.describe()
    df_summ = pd.DataFrame(df_int, dtype=str)
    for i in df_int:
        # this loop outputs rounded string values instead of floats
        # this helps to control the size of the final array
        for k in range(len(df_int)):
            try:
                precision = 2  # higher numbers improve precision
                sigfig = precision-int(np.log10(abs(df_int[i][k])))
                command = '%2.'+str(sigfig)+'f'
                df_summ[i][k] = command % df_int[i][k]
            except:
                pass
    return ds, df_var, df_clean, df_summ


def generate_local_attrs(ds, df_summ, var, flag_by_units):
    local_attrs = {'var': var}

    local_attrs.update(ds[var].attrs)
    local_attrs.update(df_summ[var].to_dict())

    # check status of data and raise flags
    flags = []
    
    if len(ds['time'])/2 < local_attrs['count'] < len(ds['time']):
        flags.append('missing some data')
    elif local_attrs['count'] <= len(ds['time'])/2:
        flags.append('missing lots of data')

    if var.startswith('del'):
        pass
    elif local_attrs['comment'] == 'Std':  # don't check std_dev
        pass
    else:
        try:
            if local_attrs['max'] > flag_by_units[local_attrs['units']]['max']:
                flags.append('contains high values')
            if local_attrs['min'] < flag_by_units[local_attrs['units']]['min']:
                flags.append('contains low values')
        except:
            pass
    local_attrs['flags'] = flags
    
    return local_attrs


def generate_datafile_attrs(data, ds, df_var, df_clean, df_summ,
                            flag_by_units, non_static_attrs):
    datafile_attrs = {'filename': data, 'variables': []}

    # add in non_static datafile attributes from the global ones
    for attr in [d for d in non_static_attrs if d in ds.attrs]:
        datafile_attrs.update({attr : ds.attrs[attr]})

    # calculate average frequency
    times = ds.coords['time'].values
    freq = np.diff(times, axis=-1)
    a = freq.mean()
    freq = a.astype('timedelta64[s]')
    datafile_attrs['frequency'] = freq.astype(float)

    # populate the empty variable list with dataless variables
    empty_vars = [{'var': var, 'flag' : 'no data'} for var in df_var if var not in df_clean]
    empty_vars.sort()
    for var in empty_vars:
        datafile_attrs['variables'].append(var)
    
    # populate it with local attributes
    full_vars = list(df_summ.columns)
    full_vars.sort()
    for var in full_vars:
        local_attrs = generate_local_attrs(ds, df_summ, var, flag_by_units)
        datafile_attrs['variables'].append(local_attrs)

    return datafile_attrs


def send(data_list):
    from pymongo import MongoClient
    from pylab import array, nbytes
    
    db_uri = 'mongodb://joey:joejoe@dogen.mongohq.com:10097/mpala_tower_metadata'
    client = MongoClient(db_uri)
    db = client.mpala_tower_metadata
    Metadata = db.metadata
    
    A = array(data_list)
    print(A.nbytes,'bytes')

    Metadata.remove({})
    Metadata.insert(data_list)


def main():

    flag_by_units = {}
    for unit in temp:
        flag_by_units.update({unit: {'min': temp_min, 'max': temp_max}})
    for unit in percent:
        flag_by_units.update({unit: {'min': percent_min, 'max': percent_max}})
    for unit in batt:
        flag_by_units.update({unit: {'min': batt_min, 'max': batt_max}})

    data_dict = None
    data_list = []
    start = '2010-01-01'
    end = dt.datetime.utcnow()
    rng = pd.date_range(start, end, freq='D')
    for date in rng:
        i = 0
        y = date.year
        m = date.month
        d = date.dayofyear
        f = 'raw_MpalaTower_%i_%03d.nc' % (y, d)
        if any(f in os.listdir(join(ROOTDIR, data)) for data in datas):
            print(date)
            data_dict = {'Year': y, 'Month': m, 'DOY': d, 'date': date, 'files': []}
            i += 1
        for data in datas:
            if f in os.listdir(join(ROOTDIR, data)):
                print(f, data)
                ds, df_var, df_clean, df_summ = process_netcdf(ROOTDIR, data, f, static_attrs)
                datafile_attrs = generate_datafile_attrs(data, ds, df_var, df_clean, df_summ,
                                                         flag_by_units, non_static_attrs)
                data_dict['files'].append(datafile_attrs)
    
        if i == 1:
            for attr in [d for d in ds.attrs if d not in non_static_attrs]:
                data_dict.update({attr : ds.attrs[attr]})
            data_list.append(data_dict)

    send(data_list)

if __name__ == '__main__':
    main()
