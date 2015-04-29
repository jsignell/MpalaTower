# Script:tower_to_netcdf.py
#        contains funtions to convert tower timeseries data .dat file to UTC
#        daily raw NetCDF CF-1.6 timeSeries file.
#        The files have the format: raw_MpalaTower_YYYY_MM_DD.nc
#        and are stored in folders according to their data file names.
# Modified by: Julia Signell
# Date created: 2014-11-10
# Date modified: 2015-04-26

import numpy as np
import pandas as pd
import netCDF4
import re
import os
import datetime as dt
import matplotlib.pyplot as plt
from posixpath import join

coords  = dict(
    station_name = 'MPALA Tower',
    lon = 36.9,         # degrees east
    lat = 0.5,          # degrees north
    elevation = 1610    # m above sea level
    )

attrs = dict(
    title = 'Flux Tower Data from MPALA',
    summary = ('This raw data comes from the MPALA Flux Tower, which is ' +
               'maintained by the Ecohydrology Lab at Mpala Research Centre ' +
               'in Laikipia, Kenya. It is part of a long-term monitoring ' +
               'project that was originally funded by NSF and now runs with ' +
               'support from Princeton. Its focus is on using stable ' +
               'isotopes to better understand water balance in drylands, ' +
               'particularly transpiration and evaporation fluxes.'),
    license = 'MIT License',
    institution = 'Princeton University',
    acknowledgement = 'Funded by NSF and Princeton University',
    naming_authority = 'caylor.princeton.edu',
    id = 'MPALA Tower',
    creator_name = 'Kelly Caylor',
    creator_email = 'kcaylor@princeton.edu',
    keywords = ['eddy covariance', 'isotope hydrology', 'land surface flux'],
    Conventions = 'CF-1.6',
    featureType = 'timeSeries',
    local_timezone = 'Africa/Nairobi'
    )


def createDF(input_file, header_file=None):
    if header_file is None:
        df_ = pd.read_csv(input_file, skiprows=[0, 2, 3], parse_dates=True,
                          index_col=0, iterator=True,
                          chunksize=100000, low_memory=False)
    else:
        df_header = pd.read_csv(header_file, skiprows=[0], nrows=1)
        header = list(df_header.columns.values)
        df_ = pd.read_csv(input_file, header=None, parse_dates=True,
                          index_col=0, iterator=True, names=header,
                          chunksize=100000, low_memory=False)
    df = pd.concat(df_)
    df = df.tz_localize('Africa/Nairobi')      # explain the local timezone
    df = df.tz_convert('UTC')                  # convert df to UTC
    return df


def group_df_by_day(df, old=False):
    # group dataframe by day of the year in UTC
    DFList = []
    for group in df.groupby([df.index.year, df.index.month, df.index.day]):
        DFList.append(group[1])
    if old is False:
        DFList = DFList[0:-1]  # for current data discard last partial day
    return DFList


def createmeta(DFList, attrs, header_file):
    # metadata needed to be CF-1.6 compliant
    ncfilenames = []
    for i in range(len(DFList)):
        doy = DFList[i].index[0].dayofyear
        y = DFList[i].index[0].year
        # create the output filenames
        ncfilenames.append('raw_MpalaTower_%i_%03d.nc' % (y, doy))

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

    # in the .dat files, the units are written in the 3rd row
    df_units = pd.read_csv(header_file, skiprows=[0, 1], nrows=1)
    # skip first value because that is the timestamp
    units = list(df_units.columns.values[1:])

    # in the .dat datafiles, the measurement types are written in the 4th row
    df_types = pd.read_csv(header_file, skiprows=[0, 1, 2], nrows=1)
    # skip first value because that is the timestamp
    types = list(df_types.columns.values[1:])

    return ncfilenames, attrs, units, types


def pd_to_secs(df):
    # convert a pandas datetime index to "seconds since 1970-01-01 00:00"
    return np.asarray([netCDF4.date2num(x,
                                        units='seconds since 1970-01-01 00:00:00',
                                        calendar='Gregorian')
                                        for x in df.index], dtype=np.float)


def createNC(output_dir, input_file, DFList, ncfilenames, attrs, coords,
             units, types, old=False):
    # this is the powerhouse function where the data in DFList moves to netCDF
    i = -1
    for df_day in DFList:
        i += 1
        #if ncfilenames[i] in os.listdir(join(output_dir, datafile)):
        #    continue
        output_file = join(output_dir, attrs['datafile'], ncfilenames[i])
        print('trying to write to %s' % output_file)
        ntime, ncols = np.shape(df_day)

        # create NetCDF file
        nc = netCDF4.Dataset(output_file, mode='w', clobber=True,
                             format='NETCDF3_CLASSIC')
                             # leave this out to make the file netCDF4

        # add some global attributes
        nc.setncatts(attrs)

        # create time dimension
        nc.createDimension('time', ntime)    # create fixed time dimension

        # create dimension for station_name
        nchar_max = len(coords['station_name'])
        nc.createDimension('name_strlen', nchar_max)

        # Create time,lon,lat,altitude variables
        tvar = nc.createVariable('time', 'f8', dimensions=('time'))
        tvar.units = 'seconds since 1970-01-01 00:00:00'
        tvar.standard_name = 'time'
        tvar.calendar = 'gregorian'

        # create timeseries_id variable
        stavar = nc.createVariable('station_name', 'S1',
                                   dimensions=('name_strlen'))
        stavar.long_name = 'station name'
        stavar.cf_role = 'timeseries_id'
        lonvar = nc.createVariable('lon', 'f4')
        lonvar.units = 'degrees east'
        lonvar.standard_name = 'longitude'
        latvar = nc.createVariable('lat', 'f4')
        latvar.units = 'degrees north'
        latvar.standard_name = 'latitude'
        evar = nc.createVariable('elevation', 'f4')
        evar.units = 'm'
        evar.standard_name = 'elevation'
        evar.positive = 'up'
        evar.axis = 'Z'

        # create variables from CSV file
        var_names = list(df_day.columns.values)
        var = []
        startcol = 1
        for var_name in var_names[startcol:]:
            var_name = re.sub('[)()]', '_', var_name)
            # netcdf doesn't like var names like "temp(17)",
            # but "temp_17_" is okay.
            var.append(nc.createVariable(var_name, 'f4', dimensions=('time'),
                                         zlib=True, complevel=4))

        # add attributes to variables from datafile
        k = startcol
        for v in var:
            v.units = (re.sub('[.]', ':', units[k])).split(':')[0]
            # units like "deg C.17" or "deg C:17" should just be "deg C"
            v.comment = (re.sub('[.]', ':', types[k])).split(':')[0]
            v.coordinates = 'time lon lat elevation'
            v.content_coverage_type = 'physicalMeasurement'
            k += 1
        # write lon,lat, elevation and station id
        lonvar[0] = coords['lon']
        latvar[0] = coords['lat']
        evar[0] = coords['elevation']
        stavar[:] = netCDF4.stringtoarr(coords['station_name'], nchar_max)
        # write time values
        tvar[:] = pd_to_secs(df_day)
        # Write data from datafile to NetCDF
        k = startcol
        for v in var:
            v[:] = df_day.iloc[:, k].values
            k += 1
        nc.close()
        print('%s finished processing' % ncfilenames[i])


def createSummaryTable(output_dir):
    l = []
    for i in os.walk(output_dir):
        l.append(i)
    columns = ['fileName']+l[0][1]
    todays_date = dt.datetime.utcnow().date()
    first_date = dt.datetime(2010, 01, 01).date()
    index = pd.date_range(start=first_date, end=todays_date,
                          name='date_time_UTC', freq='D')
    df_ = pd.DataFrame(index=index, columns=columns)
    for day in range(len(df_.index)):
        doy = df_.index[day].timetuple().tm_yday
        y = df_.index[day].year
        filename = 'raw_MpalaTower_%d_%03d.nc' % (y, doy)
        df_['fileName'][df_.index[day]] = filename
        for folder in l:
            if filename in folder[2]:
                column = folder[0].partition('output/')[2]
                statinfo = os.stat(os.path.join(folder[0], filename))
                file_size = statinfo.st_size/1024
                # so we can tell when a file is partial
                df_[column][df_.index[day]] = file_size
    df_.to_csv(join(output_dir, 'DatafileAvailability.csv'))

    # make a figure so that we can visually see data gaps
    ax = df_.plot(logy=True, figsize=(16, 8), linewidth=2, colormap='rainbow',
                  title='Summary of Datafile Availability and Filesize')
    ax.set_ylabel('Filesize [KB]')
    plt.savefig(join(output_dir, 'DatafileAvailability.jpg'))
    print('Updated the Data Availability docs!')


def process(input_dir, input_file, output_dir, DFList, f, attrs,
            header_file=None, old=False):
    # runs all the easy parts of the functions
    if header_file is None:
        header_file = input_file
    ncfilenames, attrs, units, types = createmeta(DFList, attrs, header_file)
    createNC(output_dir, input_file, DFList, ncfilenames, attrs, coords,
             units, types, old=old)
    processed = open(join(input_dir, 'processed2netCDF.txt'), 'a')
    processed.write(f + '\n')
    processed.close


def ts_run(input_dir, input_file, header_file, old=False):
    df_meta = pd.read_csv(input_file, nrows=1)
    meta = list(df_meta.columns.values)
    if old is False:  # for now we are sorting into UTC even for new data
        DFList = group_df_by_day(createDF(input_file, header_file=header_file),
                                 old=old)
    else:
        if meta[7] == 'ts_data':  # has header in file           
            DFList = group_df_by_day(createDF(input_file), old=old)
        else:  # doesn't have header
            DFList = group_df_by_day(createDF(input_file,
                                     header_file=header_file), old=old)
    return DFList
