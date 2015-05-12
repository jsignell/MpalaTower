'''
Create a netcdf file from a campbell sci data file

Julia Signell
2015-05-07
'''

from __init__ import *


def manage_dtypes(x):
    '''Ensure that all data columns are either intergers or floats'''
    if x.values.dtype == np.dtype('int64') or x.values.dtype == np.dtype('<M8[ns]'):
        return x
    else:
        try:
            return x.astype('float64')
        except:
            return x


def createDF(input_file, input_dict):
    '''Create a list of daily dataframes in UTC from a .dat file.'''
    kw = dict(parse_dates=True, index_col=0, iterator=True,
              chunksize=100000, low_memory=False)
    if input_dict['has_header'] is True:
        df_ = pd.read_csv(input_file, skiprows=[0, 2, 3], **kw)
    else:
        df_header = pd.read_csv(input_dict['header_file'],
                                skiprows=[0], nrows=1)
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
    return DFList


def group_by_day(DFL):
    '''Create a list of dataframes from split datafiles.'''
    DFList = []
    if len(DFL[0]) > 1:
        for df in DFL[0][0:-1]:
            DFList.append(df)
    if DFL[0][-1].index[-1].date() == DFL[1][0].index[0].date():
        df = pd.concat([DFL[0][-1], DFL[1][0]])
        DFL[1].pop(0)
    else:
        df = DFL[0][-1]
    DFList.append(df)
    DFL.pop(0)
    print('DFL has length %s' % len(DFL))
    print('DFL[0] has length %s' % len(DFL[0]))
    if len(DFL[0]) == 0:
        DFL = []
    return DFList, DFL


def get_ncnames(DFList):
    '''Create a list of netcdf filenames correspoding to days in DFList.'''
    ncfilenames = []
    for i in range(len(DFList)):
        doy = DFList[i].index[0].dayofyear
        y = DFList[i].index[0].year
        # create the output filenames
        ncfilenames.append('raw_MpalaTower_%i_%03d.nc' % (y, doy))
    return ncfilenames


def get_attrs(header_file, attrs):
    '''Augment attributes by inspecting the header of the .dat file.'''
    # metadata needs to be CF-1.6 compliant

    # in the TOA5 datafile headers, some metadata are written in the 1st row
    df_meta = pd.read_csv(header_file, nrows=1)
    meta_list = list(df_meta.columns.values)

    attrs.update({'format': meta_list[0],
                  'logger': meta_list[1],
                  'program': meta_list[5].split(':')[1],
                  'datafile': meta_list[7]})
    source_info = (attrs['logger'], attrs['datafile'], attrs['program'],
                   meta_list[6])
    source = 'Flux tower sensor data %s_%s.dat, %s, %s' % source_info
    attrs.update({'source': source})

    # the local attributes are in the 2nd, 3rd, and 4th rows
    df_names = pd.read_csv(header_file, skiprows=[0], nrows=2, dtype='str')
    df_names = df_names.astype('str')
    df_names.index = ('units', 'comment')
    local_attrs = df_names.to_dict()

    return attrs, local_attrs


def get_coords(ds):
    '''Get coordinate attributes and augment dataset.'''
    ds.coords['station_name'].attrs = dict(long_name='station name',
                                           cf_role='timeseries_id')
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


def fix_time(ds):
    a = {'units': 'milliseconds since 2010-01-01'}
    return ds.update({'time': ('time', ds['time'].values, a)})


def createDS(df, input_dict, attrs, coords, local_attrs):
    '''Create an xray.Dataset object from dataframe and dicts of parts'''
    ds = xray.Dataset(attrs=attrs, coords=coords)
    ds.update(xray.Dataset.from_dataframe(df))
    ds = get_coords(ds)
    for name in ds.data_vars.keys():
        ds[name].attrs = local_attrs[name]
        ds[name].attrs.update(dict(coordinates='time lon lat elevation',
                                   content_coverage_type='physicalMeasurement'))
    return ds
