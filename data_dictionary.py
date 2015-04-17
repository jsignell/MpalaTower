from __future__ import print_function
import pandas as pd
import datetime as dt
import numpy as np
import os
import xray
from posixpath import join

ROOTDIR = 'C:/Users/Julia/Documents/GitHub/MpalaTower/raw_netcdf_output/'

# Setting expected ranges for units
temp_min = 0
temp_max = 40
temp = ('Deg C', 'C')

percent_min = 0
percent_max = 100
percent = ('percent', '%')

shf_min = ''
shf_max = ''
shf = 'W/m^2'

shf_cal_min = ''
shf_cal_max = ''
shf_cal = 'W/(m^2 mV)'

batt_min = 11
batt_max = 240
batt = ('Volts', 'V')

PA_min = ''
PA_max = ''
PA = 'uSec'
   
datas = ['lws', 'licor6262', 'WVIA', 'flux', 'ts_data', 'upper', 
         'Manifold', 'Table1', 'Table1Rain']
non_static_attrs = ['instrument', 'source', 'program', 'logger']
static_attrs = ['station_name', 'lat', 'lon', 'elevation',
                'Year', 'Month', 'DOM', 'Minute', 'Hour',
                'Day_of_Year', 'Second', 'uSecond', 'WeekDay']
                
def process_netcdf(input_dir, data, f, static_attrs):
    ds = xray.Dataset()
    ds = xray.open_dataset(join(input_dir, data, f), decode_cf=True, decode_times=True)
    df = ds.to_dataframe()
    
    exclude = [var for var in static_attrs if var in df.columns]
    
    df_var = df.drop(exclude, axis=1)
    df_clean = df_var.dropna(axis=1, how='all')
    df_summ = df_clean.describe()
    return ds, df_var, df_clean, df_summ
    
def generate_local_attrs(ds, df_summ, variable, flag_by_units):
    local_attrs = dict(variable=variable)

    local_attrs.update(ds[variable].attrs)
    local_attrs.update(df_summ[variable].to_dict())

    # check status of data and raise flags
    flags = []
    
    if len(ds['time'])/2 < local_attrs['count'] < len(ds['time']):
        flags.append('missing some data')
    elif local_attrs['count'] <= len(ds['time'])/2:
        flags.append('missing lots of data')

    if variable.startswith('del'):
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

    datafile_attrs = {data : {}}

    #add in non_static datafile attributes from the global ones
    for attr in [d for d in non_static_attrs if d in ds.attrs]:
        datafile_attrs[data].update({attr : ds.attrs[attr]})

    # calculate average frequency
    times = ds.coords['time'].values
    freq = np.diff(times, axis = -1)
    a = freq.mean()
    freq = a.astype('timedelta64[s]')
    datafile_attrs[data]['frequency'] = freq.astype(int)

    # create a list of empty variables
    empty_vars = [var for var in df_var if var not in df_clean]
    empty_vars.sort()
    datafile_attrs[data]['empty_variables'] = empty_vars
    
    # create an empty variables dict and populate it with local attributes
    datafile_attrs[data]['variables'] = {}

    for variable in df_summ.columns:
        local_attrs = generate_local_attrs(ds, df_summ, variable, flag_by_units)
        datafile_attrs[data]['variables'].update({variable : local_attrs})

    return datafile_attrs[data]
    
def main():
    flag_by_units = {}
    for unit in temp:
        flag_by_units.update({unit : {'min' : temp_min, 'max' : temp_max}})
    for unit in percent:
        flag_by_units.update({unit : {'min' : percent_min, 'max' : percent_max}})
    for unit in batt:
        flag_by_units.update({unit : {'min' : batt_min, 'max' : batt_max}})
  
    data_dict = None
    data_list = []
    start = '2010-01-01'
    end = dt.datetime.utcnow()
    rng = pd.date_range(start, end, freq='D')
    for date in rng:
        i = 0
        y = date.year
        d = date.dayofyear
        f = 'raw_MpalaTower_%i_%03d.nc' % (y, d)
        if any(f in os.listdir(join(ROOTDIR, data)) for data in ['upper','Table1']):
            print(date)
            data_dict = {'Year': y, 'DOY': d, 'date' : date, 'datafile' : {}}
            i+=1
            print(data_dict)
        for data in ['upper','Table1']:
            if f in os.listdir(join(ROOTDIR, data)):
                print(f)
                ds, df_var, df_clean, df_summ = process_netcdf(ROOTDIR, data, f, static_attrs)
                datafile_attrs = generate_datafile_attrs(data, ds, df_var, df_clean, df_summ,
                                                         flag_by_units, non_static_attrs)
                data_dict['datafile'].update({data : datafile_attrs})
    
        if i == 1:
            data_list.append(data_dict)            
    
    
    from pymongo import MongoClient    
    
    db_uri = 'mongodb://joey:joejoe@dogen.mongohq.com:10097/mpala_tower_metadata'
    client = MongoClient(db_uri)
    db = client.mpala_tower_metadata
    Metadata = db.metadata

    Metadata.insert(data_list)


if __name__ == '__main__':
    main()



